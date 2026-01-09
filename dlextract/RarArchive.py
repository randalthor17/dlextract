import os
import shutil
from pathlib import Path
from typing import List

import rarfile

from dlextract.FileIO import RemoteStream
from dlextract.Protocols import ArchiveEngineProtocol


def _check_unrar_in_path():
    """
    Checks if the 'unrar' executable is available in the system PATH.

    Raises:
        EnvironmentError: If 'unrar' is not found in the system PATH.
    """
    if not shutil.which(str("unrar")):
        raise EnvironmentError(
            "The 'unrar' executable is not found in PATH. Please install it or add it to PATH."
        )


class RarArchiveEngine(ArchiveEngineProtocol):
    """
    A class to handle RAR archive extraction using the rarfile library.

    Attributes:
        stream (RemoteStream): The remote stream of the RAR file.
        password (bytes | None): The password for the RAR file, if any.
        archive (rarfile.RarFile): The rarfile object representing the archive.
    """

    def __init__(self, stream: RemoteStream, password: str | None = None) -> None:
        """
        Initializes the RarArchiveEngine with a remote stream and optional password.

        Args:
            stream (RemoteStream): The remote stream of the RAR file.
            password (str | None): The password for the RAR file, if any.

        Raises:
            rarfile.BadRarFile: If the RAR file is invalid.
        """
        _check_unrar_in_path()

        self.stream = stream
        self.password = password.encode("utf-8") if password else None

        try:
            self.archive = rarfile.RarFile(self.stream)
        except rarfile.BadRarFile as e:
            print("Failed: Bad RAR file")
            raise e

    def get_files(self) -> List[Path]:
        """
        Retrieves a list of files in the RAR archive.

        Returns:
            List[Path]: A list of file paths in the archive.
        """
        return [Path(f.filename) for f in self.archive.infolist() if not f.is_dir()]

    def extract_to_disk(self, filename: Path, target_path: Path):
        """
        Extracts a specific file from the RAR archive to the local disk.

        Args:
            filename (Path): The name of the file to extract.
            target_path (Path): The target path where the file will be extracted.

        Raises:
            rarfile.PasswordRequired: If the file requires a password and none is provided.
            rarfile.BadRarFile: If the RAR file is invalid or the password is incorrect.
        """
        # Make extraction dir
        os.makedirs(os.path.dirname(os.path.abspath(target_path)), exist_ok=True)

        try:
            with (
                self.archive.open(str(filename), pwd=self.password) as source,
                open(target_path, "wb") as target_file,
            ):
                # The flow is RemoteStream -> RarArchiveEngine -> local file
                chunk_size = 128 * 1024  # 128 KB
                while chunk := source.read(chunk_size):
                    target_file.write(chunk)
        except rarfile.PasswordRequired as e:
            print(f"Failed: {str(filename)} requires a password")
            raise e
        except rarfile.BadRarFile as e:
            print(f"Failed: Bad RAR file or password")
            raise e
