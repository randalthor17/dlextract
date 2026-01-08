import os
import zipfile
from typing import List, Protocol

from dlextract.RemoteStream import RemoteStream

# Archive file signatures, from Wikipedia
SIGNATURES = {
    # zip
    b"PK\x03\x04": "zip",
    b"PK\x05\x06": "zip", # Empty archive
    b"PK\x07\x08": "zip", # Spanned archive
    # tar
    b"ustar\x00\x30\x30": "tar", # No compression
    b"ustar\x20\x20\x00": "tar", # Again, no compression
    b"\x1f\x8b": "tar", # GZIP compressed
    b"\xfd7zXZ\x00": "tar", # XZ compressed
    b"BZh": "tar", # BZIP2 compressed
    b"\x28\xb5\x2f\xfd": "tar", #ZSTD compressed
    # 7z
    b"7z\xbc\xaf\x27\x1c": "7z",
    # RAR
    b"Rar!\x1a\x07\x00": "rar", # >= v1.50
    b"Rar!\x1a\x07\x01\x00": "rar" # >= v5.00
}

# The template for all archive formats
class ArchiveEngine(Protocol):
    def get_files(self) -> List[str]: ...
    def extract_to_disk(self, filename: str, target_path: str): ...

class ZipArchiveEngine:
    def __init__(self, stream: RemoteStream, password: str | None = None) -> None:
        self.stream = stream # This already fetches the tail
        self.password = password.encode('utf-8') if password else None
        self.archive = zipfile.ZipFile(self.stream)

    def get_files(self) -> List[str]: return self.archive.namelist()

    # Check if the file is encrypted
    # TODO: Unused, fix
    def check_encryption(self, filename: str) -> bool:
        info = self.archive.getinfo(filename)
        # Bit 0 of flag_bits indicates encryption
        return bool(info.flag_bits & 0x1)

    def extract_to_disk(self, filename: str, target_path: str):
        # Make extraction dir
        os.makedirs(os.path.dirname(target_path), exist_ok=True)

        # Set the password
        if self.password: self.archive.setpassword(self.password)

        try:
            with self.archive.open(filename) as source, open(target_path, 'wb') as target_file:
                # The flow is RemoteStream -> ZipArchiveEngine -> local file
                chunk_size = 128*1024 # 128 KB
                while chunk := source.read(chunk_size):
                    target_file.write(chunk)
        except RuntimeError as e:
            if "encrypted" in str(e).casefold():
                print(f"Failed: {filename} requires a password")
            if "bad password" in str(e).casefold():
                print(f"Failed: Wrong password")
            raise e
        except zipfile.BadZipFile as e:
            print(f"Failed: Bad Zipfile or password")
            raise e

def get_extractor(url: str, password: str | None = None) -> ArchiveEngine | None:
    stream = RemoteStream(url)
    stream.seek(0)
    magic_bytes = stream.read(8) # We read 8 bytes to get all the cases
    stream.seek(0) # Reset current byte
    for signature, mode in SIGNATURES.items():
        if magic_bytes.startswith(signature):
            if mode == "zip":
                print("Detected File Format: ZIP")
                return ZipArchiveEngine(stream, password=(password if password else None))
            elif mode == "tar":
                print("Detected File Format: TAR")
                # TODO
                raise NotImplementedError("Tar hasn't been implemented")
            elif mode == "rar":
                print("Detected File Format: RAR")
                # TODO
                raise NotImplementedError("RAR hasn't been implemented")
            elif mode == "7z":
                print("Detected File Format: 7z")
                # TODO
                raise NotImplementedError("7z hasn't been implemented")
        else:
            raise ValueError(f"Unknown File Format with signature: {magic_bytes.hex().upper()}")
    return None


if __name__ == "__main__":
    # test_url = "http://127.0.0.1:8000/file.zip"
    test_url = "https://www.learningcontainer.com/download/sample-large-zip-file/?wpdmdl=1639&refresh=69416ec3d62801765895875"
    password = None
    extractor = get_extractor(test_url, password)
    if extractor:
        files = extractor.get_files()

        print(f"Archive map: {files}")
        if files:
            target = files[0]
            print(f"Extracting {target}")
            extractor.extract_to_disk(target, f"extracted/{target}")
            print("Done.")