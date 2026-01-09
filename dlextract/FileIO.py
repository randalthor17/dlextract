"""Remote HTTP-backed file-like stream.

Provides RemoteStream, an io.RawIOBase-compatible stream that reads
remote files using HTTP Range requests. Intended for archive libraries
to access remote archives without full downloads.

Classes:
    RemoteStream: Lazily-fetching HTTP-backed read-only stream.
"""

import io

import httpx


class RemoteStream(io.RawIOBase):
    """File-like stream backed by an HTTP resource using Range requests.

    This class exposes a subset of the file-like API (read, seek, tell)
    and performs HTTP Range requests to fetch missing regions on demand.

    Attributes:
        url (str): Remote resource URL.
        buffer_size (int): Preferred size (bytes) for range fetches.
        pos (int): Current read position in the virtual file.
        client (httpx.Client): HTTP client used for requests.
    """

    def __init__(self, url: str, buffer_size: int = 256 * 1024):
        """Create a RemoteStream.

        Args:
            url (str): HTTP(S) URL of the resource to stream.
            buffer_size (int): Preferred fetch size in bytes (default 256 KiB).

        Raises:
            httpx.HTTPError: On network errors while performing the HEAD request.
            httpx.HTTPStatusError: If the server responds with a non-success status.

        Notes:
            A HEAD request is used to discover the Content-Length header so
            callers can seek relative to the end and detect EOF.
        """
        self.url = url
        self.buffer_size = buffer_size
        self.pos: int = 0
        self._size: int | None = None

        # Internal buffer
        self._buffer: bytes = b""
        self._buffer_start: int = 0

        # HTTP client (keep-alive across multiple range requests)
        self.client = httpx.Client(follow_redirects=True)

        # Discover size: HEAD is cheap and lets us perform SEEK_END
        # without issuing extra ranged GETs later.
        response = self.client.head(self.url)
        response.raise_for_status()
        # If server omits Content-Length we'll treat size as 0 (EOF semantics)
        # which allows callers to still use the stream for small reads.
        self._size = int(response.headers.get("Content-Length", 0))

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
            are made until `read` is called. This keeps seeks cheap and
            predictable for archive libraries which often probe headers.
        """
        if whence == io.SEEK_SET:
            self.pos = offset
        elif whence == io.SEEK_CUR:
            self.pos += offset
        elif whence == io.SEEK_END:
            self.pos = self.size + offset

        return self.pos

    def read(self, size: int = -1) -> bytes:
        """Read up to `size` bytes from the stream.

        Args:
            size (int): Number of bytes to read. If -1 (default), read to EOF.

        Returns:
            bytes: Data read (empty bytes on EOF).

        Raises:
            httpx.HTTPError: On network errors while fetching ranges.
            httpx.HTTPStatusError: If server returns a non-success status for the GET.
        """
        # Normalize request size to available bytes
        if (size == -1) or (self.pos + size > self.size):
            size = self.size - self.pos
        if size <= 0:
            return b""

        # Fast-path: if requested range is inside the cached buffer, return
        # without creating a network request. This is important because
        # archive libraries often re-read small header ranges repeatedly.
        buf_end = self._buffer_start + len(self._buffer)
        if (self._buffer_start <= self.pos < buf_end) and (self.pos + size <= buf_end):
            offset = self.pos - self._buffer_start
            data = self._buffer[offset: offset + size]
            self.pos += size
            return data

        # Fetch heuristics: for very large reads prefer a larger fetch window
        # to amortize HTTP overhead; for small reads use the configured buffer.
        if size > 1024 * 1024:
            # If caller requests >1MiB, fetch a larger window (4MiB) to
            # reduce subsequent requests when reading sequentially.
            current_buffer_goal = 4 * 1024 * 1024
        else:
            current_buffer_goal = self.buffer_size

        fetch_size = max(size, current_buffer_goal)
        # end_range is inclusive per HTTP Range header
        end_range = min(self.pos + fetch_size - 1, self.size - 1)

        # Use a Range GET so we only transfer the bytes we need.
        headers = {"Range": f"bytes={self.pos}-{end_range}"}
        response = self.client.get(self.url, headers=headers)
        response.raise_for_status()

        # Cache the fetched region as the single buffer region. We purposefully
        # overwrite previous buffer state to keep memory usage predictable.
        self._buffer = response.content
        self._buffer_start = self.pos

        # Return only the requested slice from the new buffer
        data = self._buffer[:size]
        self.pos += len(data)
        return data

    def close(self):
        """Close the underlying HTTP client and stream.

        After calling this method the stream should not be used.
        Closing the HTTP client releases sockets and other resources.
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
