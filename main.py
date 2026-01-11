from dlextract.ArchiveEngine import get_extractor
from pathlib import Path

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, DownloadColumn, TransferSpeedColumn
from rich.table import Table

console = Console()
@click.command()
@click.argument("url", type=str)
@click.option("--password", "-p", type=str, default=None, help="Password for encrypted archives")
@click.option("--output", "-o", type=click.Path(file_okay=False, dir_okay=True, writable=True, path_type=Path), default=Path("extracted"), help="Output directory for extracted files")

def extract(url: str, password: str, output: Path):
    """Extract files from a remote archive URL."""
    try:

        with console.status("Probing archive..."):
            extractor = get_extractor(url, password)

        if extractor:
            files = extractor.get_files()
            table = Table(title="Archive Contents")
            table.add_column("File Path", justify="left")
            for f in files:
                table.add_row(str(f))
            console.print(table)

            if not click.confirm("Extract these files to '{output}'?"):
                console.print("Extraction cancelled.")
                return

            output.mkdir(parents=True, exist_ok=True)

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                console=console
            ) as progress:
                overall_task = progress.add_task("Extracting files...", total=extractor.stream.size)

                def progress_callback(bytes_written):
                    progress.update(overall_task, advance=bytes_written)

                for file in files:
                    progress.console.print(f"Processing: {file}")
                    target = output / file
                    extractor.extract_to_disk(file, target, progress_callback=progress_callback)

            console.print("Extraction complete.")
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise e



if __name__ == "__main__":
    # Example URLs for testing different archive formats
    # Uncomment the desired test URL
    # test_url = "http://127.0.0.1:8000/file.zip"
    # test_url = "http://127.0.0.1:8000/file.7z"
    # test_url = "http://127.0.0.1:8000/file.rar"
    # test_url = "https://www.learningcontainer.com/download/sample-large-zip-file/?wpdmdl=1639&refresh=69416ec3d62801765895875"
    # test_url = "https://getsamplefiles.com/download/7z/sample-3.7z"
    # test_url = "" # Add a valid RAR from maybe a trustable "source"

    # password = None  # Set password if the archive is encrypted
    #
    # # Attempt to get the appropriate extractor for the archive
    # extractor = get_extractor(test_url, password)
    #
    # if extractor:
    #     files = extractor.get_files()  # Retrieve the list of files in the archive
    #
    #     print("Archive map:")
    #     for f in files:
    #         print("- " + str(f))
    #
    #     if files:
    #         for file in files:
    #             print(f"Extracting {file}...")
    #             # Extract each file to the "extracted" directory
    #             extractor.extract_to_disk(file, Path(f"extracted/{file}"))
    #         print("Extraction complete.")
    extract()
