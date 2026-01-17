import tarfile

from dlextract.FileIO import RemoteStream
from dlextract.Protocols import ArchiveEngineProtocol

TAR_COMPRESSION_TYPES = {
    # tar
    b"ustar\x00\x30\x30": None,  # No compression
    b"ustar\x20\x20\x00": None,  # Again, no compression
    b"\x1f\x8b": "gz",  # GZIP compressed
    b"\xfd7zXZ\x00": "xz",  # XZ compressed
    b"BZh": "bz2",  # BZIP2 compressed
    b"\x28\xb5\x2f\xfd": "zst",  # ZSTD compressed

}


class TarArchiveEngine(ArchiveEngineProtocol):

    def __init__(self, stream: RemoteStream, password: str | None = None) -> None:
        self.stream = stream
        self.password = password.encode('utf-8') if password else None

        # Figure out compression type
        stream.seek(0)
        magic_bytes = stream.read(8)  # Read 8 bytes to cover all compression types
        stream.seek(0)  # Reset current byte
        for signature, comp in TAR_COMPRESSION_TYPES.items():
            if magic_bytes.startswith(signature):
                if not comp:
                    print("No compression detected")
                    mode = "r|"
                else:
                    print(f"{comp} compression detected")
                    mode = f"r|{comp}"
            else:  # IDEK if this is possible
                raise ValueError(f"Somehow, unknown compression detected")

        self.archive = tarfile.open(fileobj=self.stream, mode=mode)
