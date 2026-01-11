"""Remote HTTP-backed file-like stream.

Provides RemoteStream, an io.RawIOBase-compatible stream that reads
remote files using HTTP Range requests. Intended for archive libraries
to access remote archives without a full download.

Classes:
    RemoteStream: Lazily-fetching HTTP-backed read-only stream.
"""

import io
import time
import httpx

MIN_FETCH_SIZE = 25 * 1024 * 1024  # 25 MiB
LARGE_REQUEST_THRESHOLD = 10 * 1024 * 1024  # 10 MiB
DEFAULT_FETCH_SIZE = 100 * 1024 * 1024 # 100 MiB

class RemoteStream(io.RawIOBase):
    """File-like stream backed by an HTTP resource using Range requests.

    This class implements a lightweight, read-only file-like object that
    fetches remote byte ranges on demand. It exposes a minimal subset of
    the file API used by archive libraries: read, seek and tell.

    Notes:
        The buffer size has been increased to 100 MiB to better accommodate
        large amounts of read requests and improve performance when dealing
        with large files while still keeping memory usage reasonable.
        However, callers should be aware that each RemoteStream instance
        can consume up to `buffer_size` bytes of memory for the internal
        buffer, so creating many instances simultaneously may lead to
        high memory usage.

    Attributes:
        url (str): Remote resource URL.
        buffer_size (int): Preferred size (bytes) for range fetches. Default 100 MiB.
        pos (int): Current logical read position in the virtual file.
        client (httpx.Client): HTTP client used for requests (keep-alive).
        _buffer (bytes): Single in-memory cached region fetched from the server.
        _buffer_start (int): Absolute start offset of `_buffer` in the file.
        _metadata_cache (dict): Small cached regions (initial/final) used to
            avoid extra requests for common archive metadata.
    """
    def __init__(self, url: str, buffer_size: int = DEFAULT_FETCH_SIZE):
        """Create a RemoteStream.

        Args:
            url (str): HTTP(S) URL of the resource to stream.
            buffer_size (int): Preferred fetch size in bytes (default 100 MiB).

        Raises:
            ConnectionError: If the initial probe fails or the server returns
                an unexpected status code.
            httpx.HTTPError / httpx.HTTPStatusError: For network or HTTP issues
                during subsequent operations.

        Notes:
            The constructor uses a small ranged GET probe (bytes=0-0) to
            discover content length (if available). This is cheaper and
            more reliable than relying on an unconditional HEAD request,
            and it allows callers to SEEK_END without performing later
            additional ranged requests.
        """
        self.url = url
        headers = {
            "User-Agent": "aria2/1.36.0",
            "Accept": "*/*",
            "Connection": "keep-alive"}

        self.buffer_size = buffer_size
        self.pos: int = 0
        self._size: int | None = None

        # Internal buffer (single region cache)
        # We keep only one contiguous region in memory to keep memory
        # usage predictable and to avoid complex cache eviction logic.
        self._buffer: bytes = b""
        self._buffer_start: int = 0

        # HTTP client (keep-alive across multiple range requests)
        self.client = httpx.Client(headers=headers, follow_redirects=True, timeout=httpx.Timeout(10.0, read=300.0))

        # Probe the resource with a minimal ranged GET so we can determine
        # the resource size (if provided by the server) and avoid HEAD.
        probe_headers = {"Range": "bytes=0-0"}
        with self.client.stream("GET", self.url, headers=probe_headers) as r:
            if r.status_code not in (200, 206):
                raise ConnectionError(f"Server returned {r.status_code}")
            content_range = r.headers.get("Content-Range")
            if content_range:
                # Content-Range: bytes 0-0/12345 -> final part is total size
                self._size = int(content_range.split('/')[-1])
            else:
                # Fall back to Content-Length if Content-Range is absent.
                self._size = int(r.headers.get("Content-Length", 0))

        # If server omits Content-Length we'll treat size as 0 (EOF semantics).
        # This keeps the stream usable for small reads even when size is unknown.

        self._metadata_cache: dict = {}
        # Try to prefetch small initial/final regions to capture archive
        # metadata (central directories, headers) which archive libraries
        # commonly request repeatedly.
        # The metadata cache is separate from the main `_buffer` and is
        # intended for small, frequently-accessed regions (start/end of file).
        self._prefetch_metadata()

    def _prefetch_metadata(self):
        """Attempt to cache small initial and final regions.

        The function issues two ranged GETs (first/last up to 2MiB each)
        and stores the results in `_metadata_cache` keyed as 'initial' and
        'final'. Failures are ignored since this is an optimization only.

        Notes:
            Prefetches are best-effort: if a request fails we record an
            empty entry and keep the stream usable.
        """
        # First 2 MiB (or `self.size` if smaller)
        initial_fetch_size = min(2 * 1024 * 1024, self.size)
        # Last 2 MiB
        final_fetch_size = min(2 * 1024 * 1024, self.size)
        # Compute the starting offset for the final region. If size is
        # 0 (unknown/empty) this will be 0 — the subsequent GET may return
        # nothing and that's handled gracefully.
        final_fetch_start = self.size - final_fetch_size

        # We explicitly request byte ranges using HTTP Range headers.
        # Each cached entry contains: data bytes, start offset, and end offset.
        # Archive readers often need both the start and the end of an archive
        # (e.g. Zip's central directory), so caching these avoids repeated
        # small range requests.
        for label, r_range in [("initial", f"bytes=0-{initial_fetch_size - 1}"),
                               ("final", f"bytes={final_fetch_start}-{self.size - 1}")]:
            headers = {"Range": r_range}
            try:
                response = self.client.get(self.url, headers=headers)
                response.raise_for_status()
                actual_start = 0 if label == "initial" else final_fetch_start
                self._metadata_cache[label] = {
                    "data": response.content,
                    "start": actual_start,
                    "end": actual_start + len(response.content) - 1
                }
            except (httpx.HTTPError, httpx.HTTPStatusError, httpx.TimeoutException) as e:
                # Ignore metadata prefetch failures; caller can still use the stream.
                # We print a short message to make debugging easier when running
                # manually; production usage may prefer logging instead.
                print(f"Metadata prefetch failed for {label} bytes: {str(e)}")
                self._metadata_cache[label] = {"data": b"", "start": 0, "end": -1}


    @property
    def size(self) -> int:
        """Return detected content length in bytes.

        Returns:
            int: Content-Length reported by the server, or 0 if unknown.
        """
        return self._size or 0

    def readable(self) -> bool:
        """Indicate the stream supports read operations."""
        return True

    def seekable(self) -> bool:
        """Indicate the stream supports seeking."""
        return True

    def writable(self) -> bool:
        """Indicate the stream is read-only (writing is unsupported)."""
        return False

    def tell(self) -> int:
        """Return the current stream position.

        Returns:
            int: Current absolute byte offset in the virtual file.
        """
        return self.pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """Move the logical stream position.

        Args:
            offset (int): Offset relative to `whence`.
            whence (int): One of io.SEEK_SET, io.SEEK_CUR, io.SEEK_END.

        Returns:
            int: New absolute position.

        Notes:
            Seeking only updates the logical position; no network requests
            are made until `read` is called. This keeps seeks cheap for
            archive libraries which often probe headers.
        """
        if whence == io.SEEK_SET:
            self.pos = offset
        elif whence == io.SEEK_CUR:
            self.pos += offset
        elif whence == io.SEEK_END:
            self.pos = self.size + offset

        return self.pos

    def _fetch(self, size: int):
        """Fetch a region containing the current position.

        The fetched region is stored in the single-region cache (`_buffer`) and
        `_buffer_start` is set to the absolute offset. The method performs
        simple retry/backoff on transient HTTP errors.

        Args:
            size (int): Minimum number of bytes the caller needs starting at
                the current logical position `self.pos`.

        Raises:
            httpx.HTTPError / httpx.HTTPStatusError: If requests consistently fail.
            EOFError: If the server returns empty content for a non-zero range.

        Notes:
            - The fetch size uses heuristics to amortize HTTP overhead for
              larger reads while keeping small reads reasonably sized.
            - The method intentionally overwrites the previous buffer to
              keep memory usage predictable.
        """
        # Fetch heuristics: for very large reads prefer a larger fetch window
        # to amortize HTTP overhead; for small reads use the configured buffer.

        # Use 100MB for big requests, but use at least 25MB for small ones
        fetch_size = max(size, MIN_FETCH_SIZE) if size <= LARGE_REQUEST_THRESHOLD else self.buffer_size
        # Ensure we aren't past EOF
        fetch_size = min(fetch_size, self.size - self.pos)

        # end_range is inclusive per HTTP Range header
        end_range = self.pos + fetch_size - 1

        # Use a Range GET so we only transfer the bytes we need.
        headers = {"Range": f"bytes={self.pos}-{end_range}"}

        # Retry logic: attempt up to 5 times on transient errors
        for attempt in range(5):
            try:
                response = self.client.get(self.url, headers=headers)
                if response.status_code == 429:
                    # Server asks us to retry later; follow Retry-After if present.
                    # We sleep a small amount then retry. This keeps the
                    # caller simple and avoids immediate failure on rate-limits.
                    wait_time = int(response.headers.get("Retry-After", 3))
                    wait_time = max(wait_time, 1)
                    print("Received 429 Too Many Requests, retrying after " + str(wait_time) + " seconds.")
                    time.sleep(wait_time)
                    continue
                response.raise_for_status()

                if not response.content and fetch_size > 0:
                    # If the server returned no bytes for a range that should
                    # have contained data, surface an error — this likely
                    # indicates a server-side problem.
                    raise EOFError("Server returned empty content for non-zero range request.")

                # Cache the fetched region as the single buffer region. We purposefully
                # overwrite previous buffer state to keep memory usage predictable.
                # Overwriting means callers should rely on `_metadata_cache` for
                # other frequently-used small regions (start/end) instead of
                # expecting `_buffer` to grow into a multi-region cache.
                self._buffer = response.content
                self._buffer_start = self.pos
                break  # Successful fetch, exit retry loop
            except (httpx.HTTPError, httpx.HTTPStatusError, httpx.TimeoutException) as e:
                if attempt == 4:
                    # On final attempt, re-raise the exception to the caller.
                    raise e
                # Otherwise, retry with a simple backoff. We keep backoff simple
                # (linear) because this code is intended for interactive use and
                # typical archives are small enough that long exponential backoffs
                # are unnecessary.
                wait_time = (attempt + 1) * 2
                print(f"HTTP error on attempt {attempt + 1}: {str(e)}.\nRetrying after {wait_time} seconds.")
                time.sleep(wait_time)

    def read(self, size: int = -1) -> bytes:
        """Read up to `size` bytes from the stream.

        This method first consults the small metadata cache (initial/final
        prefetched regions), then the single-region `_buffer`. If the
        requested range is not available locally it triggers `_fetch`.

        Args:
            size (int): Number of bytes to read. If -1 (default), read to EOF.

        Returns:
            bytes: Data read (empty bytes on EOF).

        Raises:
            httpx.HTTPError / httpx.HTTPStatusError: On network errors while fetching ranges.
        """
        # Normalize request size to available bytes
        # If size == -1 or the request would run past EOF, clamp to EOF.
        if (size == -1) or (self.pos + size > self.size):
            size = self.size - self.pos
        if size <= 0:
            return b""

        # Check metadata cache first (initial/final small regions)
        # This is a targeted fast-path: many archive formats (ZIP, 7z)
        # require small reads from the start or end of the file. Prefetching
        # those regions prevents repeated tiny range requests.
        for cache in self._metadata_cache.values():
            if cache and self._is_in_cache_range(cache, size):
                offset = self.pos - cache["start"]
                available = len(cache["data"]) - offset
                if available > 0:
                    chunk = min(size, available)
                    data = cache["data"][offset: offset + chunk]
                    self.pos += len(data)
                    return data

        # Fast-path: if requested range is inside the cached buffer, return
        # without creating a network request. Archive libraries often re-read
        # small header ranges repeatedly so this avoids extra requests.
        buf_end = self._buffer_start + len(self._buffer)
        if (self._buffer_start <= self.pos < buf_end) and (self.pos + size <= buf_end):
            offset = self.pos - self._buffer_start
            data = self._buffer[offset: offset + size]
            self.pos += size
            return data

        # Populate the buffer with a fetched region and re-attempt reading.
        # `_fetch` will fill `_buffer` and set `_buffer_start` so the next
        # read attempt can return data from the in-memory buffer.
        self._fetch(size)

        # Recurse to re-use the same logic now that `_buffer` is populated.
        # Using recursion keeps the read-path simple and reuses the existing
        # fast-path checks above. The recursion depth is bounded because
        # `_fetch` always populates `_buffer` (or raises), so the second
        # call will hit a fast-path or raise an exception.
        return self.read(size)

    def _is_in_cache_range(self, cache, size: int) -> bool:
        return (cache["start"] <= self.pos < cache["end"] + 1) and (self.pos + size - 1 <= cache["end"])

    def close(self):
        """Close the underlying HTTP client and stream.

        After calling this method the stream should not be used. Closing the
        HTTP client releases sockets and other resources.

        Returns:
            None
        """
        self.client.close()
        super().close()


# Quick manual test when run directly
if __name__ == "__main__":
    test_url = "http://127.0.0.1:8000/file.zip"

    stream = RemoteStream(test_url)
    print(f"File size detected: {stream.size / 1024: .2f} KB")

    stream.seek(0)
    magic_bytes = stream.read(4)
    print(f"Magic bytes: {magic_bytes.hex().upper()}")

    import zipfile

    with zipfile.ZipFile(stream) as zf:
        print("Files found in the archive:")
        for file in zf.namelist():
            print(f"- {file}")

    stream.close()
