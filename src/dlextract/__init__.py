"""dlextract package initializer.

This module provides the package-level public surface for the small
`dlextract` library. It intentionally exports a few convenience symbols:

- __version__: Package version string.
- get_extractor: Convenience function to obtain an archive engine for a URL.
- RemoteStream: HTTP-backed file-like stream useful for consuming remote archives.
- ArchiveEngineProtocol: Protocol describing archive engine implementations.
- cli: The CLI entrypoint function (click command) exposed for programmatic use.

The goal is to keep the package import cheap: heavy work (network I/O,
archive probing) is performed only when the exported functions are called.

Example:
    from dlextract import get_extractor, RemoteStream
    extractor = get_extractor("http://example.com/archive.zip")

"""

# Public version string
__version__ = "0.1.0"

# Lightweight re-exports for convenience
from .ArchiveEngine import get_extractor  # function that returns an extractor for a URL
from .FileIO import RemoteStream  # HTTP-backed stream used by the extractors
from .Protocols import ArchiveEngineProtocol  # protocol describing extractor behavior

# Expose the CLI command object so callers can reuse or register it in other tools.
from .CLI import extract as cli  # click CLI command

# Define the public API
__all__ = [
    "__version__",
    "get_extractor",
    "RemoteStream",
    "ArchiveEngineProtocol",
    "cli",
]
