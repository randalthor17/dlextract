"""7z archive engine adapter.

This module contains a small adapter around `py7zr.SevenZipFile` to
list and extract members from a 7z archive. The adapter accepts a
`dlextract.FileIO.RemoteStream` instance so archives hosted over HTTP
can be consumed without downloading the entire archive first.
"""

from pathlib import Path
from typing import List

import py7zr

from .FileIO import RemoteStream
from .Protocols import ArchiveEngineProtocol


class SevenZipWriter(py7zr.io.Py7zIO):
    """A write-only file-like object used by py7zr to stream extracted data.

    This class implements the minimal writer interface py7zr expects when a
    custom writer factory is provided. It writes bytes directly to a target
    filesystem path while optionally invoking a progress callback.

    Attributes:
        target_path (Path): Destination file path for the extracted data.
        progress_callback (callable|None): Optional callback called with the
            number of bytes written on each write().
        _file (io.BufferedWriter): Underlying binary file handle.
        _length (int): Number of bytes written so far.
    """
    def __init__(self, target_path: Path, progress_callback=None):
        """Create a writer that writes data to `target_path`.

        Args:
            target_path (Path): Filesystem path where data will be written.
            progress_callback (callable|None): Optional function called with
                bytes-written increments so callers can track progress.
        """
        self.target_path = target_path
        self.progress_callback = progress_callback
        # Ensure destination directory exists before opening the file.
        self.target_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._file = open(target_path, 'wb')
        except Exception as e:
            # Re-raise to preserve original behavior; caller will handle it.
            raise e
        self._length = 0

    def write(self, data: bytes) -> int:
        """Write bytes to disk and update progress.

        Args:
            data (bytes): Chunk to write.

        Returns:
            int: Number of bytes written.
        """
        written = self._file.write(data)
        self._length += written
        # Call progress callback with the raw increment so callers can sum it.
        if self.progress_callback:
            self.progress_callback(written)
        return written

    def read(self, size: int | None = None) -> bytes:
        """Return empty bytes because this writer is not readable.

        py7zr's IO abstractions expect writer objects to have a read method in
        some code paths, but for extraction we only need the write() side. To
        remain compatible we provide a no-op read implementation.

        Returns:
            bytes: Always returns an empty bytes object.
        """
        return b""

    def flush(self) -> None:
        """Flush the underlying file buffer to disk.

        This is a thin wrapper around the file handle's flush(); kept for
        clarity and to satisfy py7zr's expected interface.
        """
        self._file.flush()

    def close(self):
        """Close the underlying file handle."""
        self._file.close()

    def seek(self, offset: int, whence: int = 0) -> int:
        """Seek in the underlying file and return the new position.

        Args:
            offset (int): Offset to seek to.
            whence (int): Seek origin (0=absolute, 1=relative, 2=end).

        Returns:
            int: New file position.
        """
        return self._file.seek(offset, whence)

    def tell(self) -> int:
        """Return the current write position in the file."""
        return self._file.tell()

    def size(self) -> int:
        """Return the total number of bytes written so far.

        This is used by callers that want to know how much data has been
        streamed to disk.
        """
        return self._length


class SevenZipWriterFactory(py7zr.io.WriterFactory):
    """Factory that creates `SevenZipWriter` instances.

    py7zr will call the factory's `create()` method for each file it needs
    to write. We always return a writer that writes to the same `target_path`.
    """
    def __init__(self, target_path, progress_callback=None):
        """Initialize factory with a destination path and optional progress callback.

        Args:
            target_path (Path): Destination file path to pass to writers.
            progress_callback (callable|None): Optional progress callback.
        """
        self.target_path = target_path
        self.progress_callback = progress_callback

    def create(self, filename: str) -> SevenZipWriter:
        """Create and return a `SevenZipWriter` for `filename`.

        Args:
            filename (str): Archive-internal filename py7zr is asking to create.

        Returns:
            SevenZipWriter: A writer writing to `self.target_path`.
        """
        # We ignore the provided filename because the caller requested a
        # specific `target_path` to write to; this keeps extraction simple.
        return SevenZipWriter(self.target_path, self.progress_callback)


class SevenZipArchiveEngine(ArchiveEngineProtocol):
    """
    7z archive engine using py7zr.

    Attributes:
        stream (RemoteStream): The remote stream of the 7z archive.
        password (bytes | None): The password for the archive, if any.
        archive (py7zr.SevenZipFile): The py7zr archive instance.
    """

    def __init__(self, stream: RemoteStream, password: str | None = None) -> None:
        """
        Initialize the SevenZipArchiveEngine.

        Args:
            stream (RemoteStream): The remote stream of the 7z archive.
            password (str | None): Optional password for encrypted archives.

        Raises:
            py7zr.exceptions.Bad7zFile: If the archive is invalid.
            py7zr.exceptions.ArchiveError: Generic archive errors.
            py7zr.exceptions.PasswordRequired: If a password is required but not provided.
        """
        # Store inputs
        # py7zr expects the password as bytes in some versions, so encode it.
        self.stream = stream
        self.password = password.encode("utf-8") if password else None

        try:
            # Initialize py7zr; this validates headers and might read the
            # archive tail from the provided stream.
            self.archive = py7zr.SevenZipFile(
                self.stream, mode="r", password=self.password
            )
        except py7zr.exceptions.Bad7zFile as e:
            print("Failed: Bad 7z file")
            raise e
        except py7zr.exceptions.ArchiveError as e:
            print("Failed: Archive error")
            raise e
        except py7zr.exceptions.PasswordRequired as e:
            print("Failed: Password required")
            raise e

    def get_files(self) -> List[Path]:
        """
        Retrieve non-directory file paths contained in the archive.

        Returns:
            List[Path]: A list of pathlib.Path objects for regular files.
        """
        # Map py7zr's list entries to pathlib.Path, filtering out directories
        return [Path(f.filename) for f in self.archive.list() if not f.is_directory]

    def extract_to_disk(self, filename: Path, target_path: Path, progress_callback=None):
        """
        Extract a single file from the archive to disk.

        Args:
            filename (Path): Path inside the archive to extract.
            target_path (Path): Destination filesystem path for the extracted file.
            progress_callback (callable|None): Optional function called with bytes-written increments.

        Raises:
            FileNotFoundError: If py7zr did not produce the expected file.
        """
        # Reset internal reader state before extraction (py7zr uses internal
        # pointers that can be left in a non-starting state after listing).
        self.archive.reset()
        # Use a writer factory so py7zr streams data directly to our requested
        # target_path instead of creating files relative to the current
        # working directory and later moving them.
        factory = SevenZipWriterFactory(target_path, progress_callback)
        self.archive.extract(targets=[str(filename)], factory=factory)

        """OLD CODE"""
        # Reset archive internal state before extraction
        # self.archive.reset()

        # Ask py7zr to extract the single target into the current working directory
        # self.archive.extract(targets=[str(filename)], path=".")

        # After extraction, move the file from the extracted path to the requested target
        # if Path(str(filename)).exists(): # filename was originally inside the archive, not in the filesystem
        #     if filename != target_path:
        # Ensure the destination directory exists
        #         os.makedirs(os.path.dirname(os.path.abspath(target_path)), exist_ok=True)
        #         shutil.move(filename, target_path)

        # Best-effort cleanup: remove a top-level directory py7zr may have created
        #         filename_dir_chain = Path(filename).parts
        #         if len(filename_dir_chain) > 1:
        #             top_dir = filename_dir_chain[0]
        #             if os.path.isdir(top_dir):
        #                 try:
        #                     shutil.rmtree(top_dir)
        #                 except OSError:
        # Ignore cleanup failures
        #                     pass
        # else:
        # Extraction did not produce the expected file
        #     raise FileNotFoundError(f"Failed to extract {filename}")
