# Import necessary modules from the dlextract package
from pathlib import Path

from dlextract.FileIO import RemoteStream
from dlextract.Protocols import ArchiveEngineProtocol
from dlextract.RarArchive import RarArchiveEngine
from dlextract.SevenZipArchive import SevenZipArchiveEngine
from dlextract.ZipArchive import ZipArchiveEngine

# Archive file signatures, from Wikipedia
# These signatures are used to identify the type of archive file based on its magic bytes
SIGNATURES = {
    # zip
    b"PK\x03\x04": "zip",
    b"PK\x05\x06": "zip",  # Empty archive
    b"PK\x07\x08": "zip",  # Spanned archive
    # tar
    b"ustar\x00\x30\x30": "tar",  # No compression
    b"ustar\x20\x20\x00": "tar",  # Again, no compression
    b"\x1f\x8b": "tar",  # GZIP compressed
    b"\xfd7zXZ\x00": "tar",  # XZ compressed
    b"BZh": "tar",  # BZIP2 compressed
    b"\x28\xb5\x2f\xfd": "tar",  # ZSTD compressed
    # 7z
    b"7z\xbc\xaf\x27\x1c": "7z",
    # RAR
    b"Rar!\x1a\x07\x00": "rar",  # >= v1.50
    b"Rar!\x1a\x07\x01\x00": "rar",  # >= v5.00
}


def get_extractor(
    url: str, password: str | None = None
) -> ArchiveEngineProtocol | None:
    """
    Determines the appropriate archive extractor based on the file signature.

    Args:
        url (str): The URL of the archive file to be extracted.
        password (str | None): Optional password for encrypted archives.

    Returns:
        ArchiveEngineProtocol | None: An instance of the appropriate archive engine or None if unsupported.

    Raises:
        ValueError: If the file format is unknown.
        NotImplementedError: If the archive type is recognized but not implemented.
    """
    stream = RemoteStream(url)
    stream.seek(0)
    magic_bytes = stream.read(8)  # Read the first 8 bytes to identify the file type
    stream.seek(0)  # Reset the stream position

    for signature, mode in SIGNATURES.items():
        if magic_bytes.startswith(signature):
            if mode == "zip":
                print("Detected File Format: ZIP")
                return ZipArchiveEngine(
                    stream, password=(password if password else None)
                )
            elif mode == "tar":
                print("Detected File Format: TAR")
                # TODO
                raise NotImplementedError("Tar hasn't been implemented")
            elif mode == "rar":
                print("Detected File Format: RAR")
                # RAR extraction is partially implemented
                return RarArchiveEngine(
                    stream, password=(password if password else None)
                )
            elif mode == "7z":
                print("Detected File Format: 7z")
                # 7z extraction is partially implemented
                return SevenZipArchiveEngine(
                    stream, password=(password if password else None)
                )
    else:
        # Raise an error if the file format is unknown
        raise ValueError(
            f"Unknown File Format with signature: {magic_bytes.hex().upper()}"
        )


if __name__ == "__main__":
    # Example URLs for testing different archive formats
    # Uncomment the desired test URL
    # test_url = "http://127.0.0.1:8000/file.zip"
    # test_url = "http://127.0.0.1:8000/file.7z"
    test_url = "http://127.0.0.1:8000/file.rar"
    # test_url = "https://www.learningcontainer.com/download/sample-large-zip-file/?wpdmdl=1639&refresh=69416ec3d62801765895875"
    # test_url = "https://getsamplefiles.com/download/7z/sample-3.7z"

    password = None  # Set password if the archive is encrypted

    # Attempt to get the appropriate extractor for the archive
    extractor = get_extractor(test_url, password)

    if extractor:
        files = extractor.get_files()  # Retrieve the list of files in the archive

        print("Archive map:")
        for f in files:
            print("- " + str(f))

        if files:
            for file in files:
                print(f"Extracting {file}...")
                # Extract each file to the "extracted" directory
                extractor.extract_to_disk(file, Path(f"extracted/{file}"))
            print("Extraction complete.")