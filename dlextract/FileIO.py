import io, httpx
from abc import ABC

from py7zr import Py7zIO

class RemoteStream(io.RawIOBase):
    def __init__(self, url: str, buffer_size: int = 256 * 1024):
        self.url = url
        self.buffer_size = buffer_size
        self.pos: int = 0
        self._size: int | None = None

        # Internal buffer
        self._buffer: bytes = b""
        self._buffer_start: int = 0

        # Init session
        self.client = httpx.Client(follow_redirects=True)

        # Get total file size
        response = self.client.head(self.url)
        response.raise_for_status()  # Check if there's an error
        self._size = int(response.headers.get("Content-Length", 0))

    # Define properties
    @property
    def size(self) -> int:
        return self._size or 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def tell(self) -> int:
        return self.pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self.pos = offset
        elif whence == io.SEEK_CUR:
            self.pos += offset
        elif whence == io.SEEK_END:
            self.pos = self.size + offset

        return self.pos

    def read(self, size: int = -1) -> bytes:
        if (size == -1) or (self.pos + size > self.size):
            size = self.size - self.pos
        if size <= 0: return b""  # Return nothing if we're at the end

        # Check if the data we need is already in the local buffer
        buf_end = self._buffer_start + len(self._buffer)
        if (self._buffer_start <= self.pos < buf_end) and (self.pos + size <= buf_end):
            offset = self.pos - self._buffer_start
            data = self._buffer[offset: offset + size]  # get the bytes from offset to offset+size
            self.pos += size
            return data

        # If the data we're fetching is big, then fetch more
        if size > 1024 * 1024:
            current_buffer_goal = 4 * 1024 * 1024
        else:
            current_buffer_goal = self.buffer_size

        # If the data is not in the buffer, fetch anew
        fetch_size = max(size, current_buffer_goal)  # fetch at least the buffer amount
        end_range = min(self.pos + fetch_size - 1, self.size - 1)

        # Make the request
        headers = {"Range": f"bytes={self.pos}-{end_range}"}
        response = self.client.get(self.url, headers=headers)
        response.raise_for_status()

        # Put the data into the buffer
        self._buffer = response.content
        self._buffer_start = self.pos

        # Return only the data requested
        data = self._buffer[:size]
        self.pos += len(data)
        return data

    def close(self):
        self.client.close()
        super().close()

# TEST
if __name__ == "__main__":
    test_url = "http://127.0.0.1:8000/file.zip"

    stream = RemoteStream(test_url)
    print(f"File size detected: {stream.size / 1024: .2f} KB")

    # see if the file is a zip
    stream.seek(0)
    magic_bytes = stream.read(4)
    print(f"Magic bytes: {magic_bytes.hex().upper()}")  # should be 504B0304 for zips

    # Attempt to read the zip
    import zipfile

    with zipfile.ZipFile(stream) as zf:
        print("Files found in the archive:")
        for file in zf.namelist():
            print(f"- {file}")

    stream.close()
