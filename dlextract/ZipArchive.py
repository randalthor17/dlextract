"""ZIP archive engine adapter.

Provides a minimal adapter around the standard library `zipfile.ZipFile`
class to list and extract files from a ZIP archive. The adapter accepts a
`dlextract.FileIO.RemoteStream` so archives hosted over HTTP can be read
without downloading the whole file first.
"""

import os
import zipfile
from pathlib import Path
from typing import List

from .FileIO import RemoteStream
from .Protocols import ArchiveEngineProtocol


class ZipArchiveEngine(ArchiveEngineProtocol):
    """
    ZIP archive engine using the stdlib zipfile module.

    Attributes:
        stream (RemoteStream): The remote stream of the ZIP archive.
        password (bytes | None): The password used for encrypted members, if any.
        archive (zipfile.ZipFile): The ZipFile instance used to inspect and extract members.
    """

    def __init__(self, stream: RemoteStream, password: str | None = None) -> None:
        """
        Initialize the ZipArchiveEngine.

        Args:
            stream (RemoteStream): The remote stream of the ZIP archive.
            password (str | None): Optional password for encrypted archives.

        Raises:
            zipfile.BadZipFile: If the provided stream does not contain a valid ZIP archive.
        """
        self.stream = stream
        self.password = password.encode("utf-8") if password else None
        try:
            # Create a ZipFile instance from the provided stream. zipfile will
            # read central directory headers as needed.
            self.archive = zipfile.ZipFile(self.stream)
        except zipfile.BadZipFile as e:
            print("Failed: Bad Zipfile")
            raise e

    def get_files(self) -> List[Path]:
        """
        Retrieve non-directory file paths contained in the archive.

        Returns:
            List[Path]: A list of pathlib.Path objects for regular files.
        """
        # Map ZipInfo entries to pathlib.Path and filter out directories
        return [Path(f.filename) for f in self.archive.filelist if not f.is_dir()]

    def extract_to_disk(self, filename: Path, target_path: Path, progress_callback=None):
        """
        Extract a specific file from the ZIP archive to disk.

        Args:
            filename (Path): The name/path of the file inside the archive to extract.
            target_path (Path): Filesystem path where the extracted file will be written.

        Raises:
            RuntimeError: If extraction fails due to encryption or bad password.
            zipfile.BadZipFile: If the archive is invalid or corrupted.
        """
        # Ensure the destination directory exists
        os.makedirs(os.path.dirname(os.path.abspath(target_path)), exist_ok=True)

        # If a password was supplied, set it on the ZipFile instance
        if self.password:
            self.archive.setpassword(self.password)

        try:
            # Stream the file contents from the archive to the target file in chunks
            with (
                self.archive.open(str(filename)) as source,
                open(target_path, "wb") as target_file,
            ):
                # The flow is RemoteStream -> ZipArchiveEngine -> local file
                chunk_size = 128 * 1024  # 128 KB
                while chunk := source.read(chunk_size):
                    target_file.write(chunk)
                    if progress_callback:
                        # Pass the number of bytes written to the callback
                        progress_callback(len(chunk))
        except RuntimeError as e:
            # zipfile raises RuntimeError for encrypted members or bad passwords
            if "encrypted" in str(e).casefold():
                print(f"Failed: {str(filename)} requires a password")
            if "bad password" in str(e).casefold():
                print(f"Failed: Wrong password")
            raise e
        except zipfile.BadZipFile as e:
            print(f"Failed: Bad Zipfile or password")
            raise e
