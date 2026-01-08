from dlextract.Protocols import ArchiveEngineProtocol
from dlextract.FileIO import RemoteStream
import zipfile, os
from typing import List

class ZipArchiveEngine(ArchiveEngineProtocol):
    def __init__(self, stream: RemoteStream, password: str | None = None) -> None:
        self.stream = stream
        self.password = password.encode('utf-8') if password else None
        try:
            self.archive = zipfile.ZipFile(self.stream)
        except zipfile.BadZipFile as e:
            print("Failed: Bad Zipfile")
            raise e

    def get_files(self) -> List[str]: return self.archive.namelist()

    # Check if the file is encrypted
    # TODO: Unused, fix
    def check_encryption(self, filename: str) -> bool:
        info = self.archive.getinfo(filename)
        # Bit 0 of flag_bits indicates encryption
        return bool(info.flag_bits & 0x1)

    def extract_to_disk(self, filename: str, target_path: str):
        # Make extraction dir
        os.makedirs(os.path.dirname(os.path.abspath(target_path)), exist_ok=True)

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
