"""Archive engine protocol definitions.

This module declares `ArchiveEngineProtocol`, the interface that archive
adapter classes (ZIP, 7z, RAR, etc.) must implement. The protocol keeps
implementations decoupled from the rest of the codebase and documents the
minimal contract an archive engine must satisfy.
"""

# The template for all archive formats
from pathlib import Path
from typing import Protocol, List
from .FileIO import RemoteStream


class ArchiveEngineProtocol(Protocol):
    """Protocol describing the minimal archive engine interface.

    Implementations should provide ways to inspect archive contents and
    to extract individual members to disk.
    """
    stream: RemoteStream

    def get_files(self) -> List[Path]:
        """Return a list of file paths contained in the archive.

        Returns:
            List[pathlib.Path]: A list of pathlib.Path objects for regular files inside the archive.
        """
        ...

    def extract_to_disk(self, filename: Path, target_path: Path, progress_callback=None):
        """Extract a single archive member to a filesystem path.

        Args:
            filename (Path): Path of the member inside the archive to extract.
            target_path (Path): Destination path on the local filesystem.
            progress_callback (callable|None): Optional callback called with the
            number of bytes written on each write().

        Notes:
            Implementations should create parent directories for `target_path`
            as needed and raise appropriate exceptions on failure.
        """
        ...
