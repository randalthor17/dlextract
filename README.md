dlextract
=======

Small, dependency-light utilities to stream and extract remote archive files without downloading them fully.

Overview
--------

The library exposes a lightweight HTTP-backed file-like stream and a set of archive engine implementations that can probe a remote URL and extract files on demand. The implementation is optimized for interactive use and avoids full-file downloads by using HTTP Range requests.

Main entrypoint
---------------

The command-line interface is the primary entrypoint for this project and is  implemented in `dlextract/CLI.py` (the `extract` Click command). Example  usage from a shell:

    dlextract <url> --output extracted/

This will probe the given remote URL, list the archive contents, and optionally stream-extract files to the specified output directory while showing progress.

Quick API reference
-------------------

- `dlextract.FileIO.RemoteStream` - file-like stream backed by HTTP Range requests. Use this when you need to feed remote data to archive libraries without a full download.

- `dlextract.ArchiveEngine.get_extractor(url, password=None)` - probe `url` and return an engine implementing the archive protocol (Zip, RAR, 7z for now).

- `dlextract.CLI.extract` - click command object used by the CLI; the module documents how to invoke it interactively.

Developer notes
---------------

- The project uses `httpx` for HTTP requests and `rich` for terminal output.
- Archive implementations live in `dlextract/*.py` and try to keep imports cheap; heavy work (network I/O, probing) happens only when needed.
- To run the CLI during development you can call the command directly (after installing click/rich/httpx), or run the package's CLI entry if configured by your packaging tooling.

License and contributions
-------------------------

Simple, permissive MIT License is being used. Contributions are welcome!

