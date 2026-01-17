"""dlextract CLI entrypoint.

This module provides the `extract` click command which probes a remote archive URL, lists the archive contents, and optionally extracts the files to a local output directory while displaying progress.

Usage example (from shell):
    dlextract <url> --password secret -o extracted/

The implementation intentionally delegates archive handling to `dlextract.ArchiveEngine.get_extractor`, so this module focuses on the CLI user interaction, progress reporting, and writing files to disk.
"""

from .ArchiveEngine import get_extractor
from pathlib import Path

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, DownloadColumn, TransferSpeedColumn
from rich.table import Table

import gc

# Create a single console instance for the CLI UI (rich console handles colors/formatting)
console = Console()
@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.argument("url", type=str)
@click.option("--password", "-p", type=str, default=None, help="Password for encrypted archives")
@click.option("--output", "-o",
              type=click.Path(file_okay=False, dir_okay=True, writable=True, path_type=Path),
              default=Path("../extracted"),
              help="Output directory for extracted files")

def extract(url: str, password: str, output: Path):
    """Extract files from a remote archive URL.

    The command will:

    - Probe the provided URL and use the archive engine to detect the archive type.

    - List the files contained in the archive for user confirmation.

    - Extract each file to the given output directory while displaying progress.

    Args:

        url: A URL or path-like string pointing to a remote archive resource.

        password: Optional password to open encrypted archives. If provided, it is passed through to the underlying archive engine.

        output: A pathlib.Path pointing to the directory where extracted files should be written. The directory will be created if it doesn't exist.

    Raises:

        Exception: Any unexpected error encountered during probing or extraction is propagated after being printed to the console for visibility.
    """
    try:

        # Probe the remote archive to obtain an extractor compatible with the
        # archive format (zip, rar, 7z, tar, etc.). This may perform network IO.
        with console.status("Probing archive..."):
            extractor = get_extractor(url, password)

        if extractor:
            # Query the archive for a list of files (relative paths inside the archive)
            files = extractor.get_files()

            # Present a simple table of files to the user for confirmation before writing
            table = Table(title="Archive Contents")
            table.add_column("File Path", justify="left")
            for f in files:
                table.add_row(str(f))
            console.print(table)

            # Ask the user whether to proceed with extraction
            if not click.confirm(f"Extract these files to ''{output}'?"):
                console.print("Extraction cancelled.")
                return

            # Ensure the output directory exists (creates parent directories too)
            output.mkdir(parents=True, exist_ok=True)

            # Use a rich progress bar to show overall extraction progress.
            # The extractor provides `total_uncompressed_size` for progress scaling.
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                console=console
            ) as progress:
                overall_task = progress.add_task("Extracting files...", total=extractor.total_uncompressed_size)

                # The progress callback will be invoked with the number of bytes written
                # by each archive engine implementation while streaming files to disk.
                def progress_callback(bytes_written):
                    progress.update(overall_task, advance=bytes_written)

                # Extract files one-by-one. For each file we compute the target path
                # under the output directory and delegate the streaming extraction to
                # the extractor implementation.
                for file in files:
                    progress.console.print(f"Processing: {file}")
                    target = output / file
                    extractor.extract_to_disk(file, target, progress_callback=progress_callback)

                    # Trigger a small garbage collection after each file to reduce
                    # transient memory spikes when streaming large files.
                    gc.collect()

            console.print("Extraction complete.")
    except Exception as e:
        # Surface the error to the user and re-raise for callers / tests to handle.
        console.print(f"[red]Error:[/red] {str(e)}")
        raise e

