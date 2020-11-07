import io
import zipfile
from pyxeed.utils.exceptions import XeedDecodeError
from ..decoder import Decoder

class ZipDecoder(Decoder):
    def __init__(self):
        super().__init__()
        self.support_encodes = ['zip']

    def _encode_to_blob(self, data_or_io, from_encode, **kwargs):
        if isinstance(data_or_io, io.BufferedIOBase):
            archive = zipfile.ZipFile(data_or_io)
        elif isinstance(data_or_io, bytes):
            archive = zipfile.ZipFile(io.BytesIO(data_or_io))
        else:
            self.logger.error("Data type {} not supported".format(data_or_io.__class__.__name__))
            raise XeedDecodeError("XED-000007")
        for file in archive.namelist():
            yield archive.read(file)