import os
import shutil

from dlextract.FileIO import RemoteStream
from dlextract.Protocols import ArchiveEngineProtocol
import py7zr, io
from abc import ABC
from typing import List

# The following class is for writing data from py7zr to disk
class DiskSink(py7zr.Py7zIO, ABC):
    def __init__(self, target_path: str):
        self.target_path = target_path
        self.file: io.BufferedWriter | None = None

    def write(self, data: bytes):
        if not self.file:
            try:
                self.file = open(self.target_path, "wb")
            except OSError as e:
                print(f"Failed to open {self.target_path}: {e}")
                raise e
        return self.file.write(data)

    def close(self):
        if self.file:
            self.file.close()

class SevenZipArchiveEngine(ArchiveEngineProtocol):
    def __init__(self, stream: RemoteStream, password: str | None = None) -> None:
        self.stream = stream
        self.password = password.encode('utf-8') if password else None

        try:
            # Initializing already fetches the tail
            self.archive = py7zr.SevenZipFile(self.stream, mode="r", password=self.password)
        except py7zr.exceptions.Bad7zFile as e:
            print("Failed: Bad 7z file")
            raise e
        except py7zr.exceptions.ArchiveError as e:
            print("Failed: Archive error")
            raise e
        except py7zr.exceptions.PasswordRequired as e:
            print("Failed: Password required")
            raise e

    def get_files(self) -> List[str]:
        return [f.filename for f in self.archive.list() if not f.is_directory]


    def extract_to_disk(self, filename: str, target_path: str):
        # 7z files are usually solid
        # so we have to figure out which block the file is in
        self.archive.reset()
        self.archive.extract(targets=[filename], path=".")
        if os.path.exists(filename):
            if filename != target_path:
                os.makedirs(os.path.dirname(os.path.abspath(target_path)), exist_ok=True)
                shutil.move(filename, target_path)
                filename_dir_chain = filename.split("/")
                if len(filename_dir_chain) > 1:
                    top_dir = filename_dir_chain[0]
                    if os.path.isdir(top_dir):
                        shutil.rmtree(top_dir)
        else:
            raise FileNotFoundError(f"Failed to extract {filename}")
