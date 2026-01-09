"""7z archive engine adapter.

This module contains a small adapter around `py7zr.SevenZipFile` to
list and extract members from a 7z archive. The adapter accepts a
`dlextract.FileIO.RemoteStream` instance so archives hosted over HTTP
can be consumed without downloading the entire archive first.
"""

import os
import shutil
from pathlib import Path
from typing import List

import py7zr

from dlextract.FileIO import RemoteStream
from dlextract.Protocols import ArchiveEngineProtocol


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

    def extract_to_disk(self, filename: Path, target_path: Path):
        """
        Extract a single file from the archive to disk.

        Args:
            filename (Path): Path inside the archive to extract.
            target_path (Path): Destination filesystem path for the extracted file.

        Raises:
            FileNotFoundError: If py7zr did not produce the expected file.
        """
        # Reset archive internal state before extraction
        self.archive.reset()

        # Ask py7zr to extract the single target into the current working directory
        self.archive.extract(targets=[str(filename)], path=".")

        # After extraction, move the file from the extracted path to the requested target
        if Path(str(filename)).exists(): # filename was originally inside the archive, not in the filesystem
            if filename != target_path:
                # Ensure the destination directory exists
                os.makedirs(os.path.dirname(os.path.abspath(target_path)), exist_ok=True)
                shutil.move(filename, target_path)

                # Best-effort cleanup: remove a top-level directory py7zr may have created
                filename_dir_chain = Path(filename).parts
                if len(filename_dir_chain) > 1:
                    top_dir = filename_dir_chain[0]
                    if os.path.isdir(top_dir):
                        try:
                            shutil.rmtree(top_dir)
                        except OSError:
                            # Ignore cleanup failures
                            pass
        else:
            # Extraction did not produce the expected file
            raise FileNotFoundError(f"Failed to extract {filename}")
