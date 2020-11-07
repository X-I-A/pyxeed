import io
import gzip
import base64
import logging
from pyxeed.utils.exceptions import XeedDecodeError

__all__ = ['Decoder']

class Decoder():
    def __init__(self):
        self.support_encodes = ['blob', 'flat', 'gzip', 'b64g']
        self.logger = logging.getLogger("Xeed.Decoder")
        formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                      ':%(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def basic_encoder(self, data, from_encode, to_encode):
        if from_encode not in ['blob', 'flat', 'gzip', 'b64g']:
            self.logger.error("Cannot decoder to {}".format(to_encode))
            raise XeedDecodeError("XED-000006")
        if to_encode not in ['blob', 'flat', 'gzip', 'b64g']:
            self.logger.error("Cannot decoder from {} with basic encoder".format(to_encode))
            raise XeedDecodeError("XED-000007")

        if from_encode == to_encode:
            return data
        if from_encode == 'gzip' and to_encode == 'flat':
            return gzip.decompress(data).decode()
        elif from_encode == 'b64g' and to_encode == 'flat':
            return gzip.decompress(base64.b64decode(data.encode())).decode()
        elif from_encode == 'blob' and to_encode == 'flat':
            return data.decode()
        elif from_encode == 'flat' and to_encode == 'b64g':
            return base64.b64encode(gzip.compress(data.encode())).decode()
        elif from_encode == 'gzip' and to_encode == 'b64g':
            return base64.b64encode(data).decode()
        elif from_encode == 'blob' and to_encode == 'b64g':
            return base64.b64encode(gzip.compress(data)).decode()
        elif from_encode == 'flat' and to_encode == 'gzip':
            return gzip.compress(data.encode())
        elif from_encode == 'b64g' and to_encode == 'gzip':
            return base64.b64decode(data.encode())
        elif from_encode == 'blob' and to_encode == 'gzip':
            return gzip.compress(data)
        elif from_encode == 'flat' and to_encode == 'blob':
            return data.encode()
        elif from_encode == 'gzip' and to_encode == 'blob':
            return gzip.decompress(data)
        elif from_encode == 'b64g' and to_encode == 'blob':
            return gzip.decompress(base64.b64decode(data.encode()))

    def _encode_to_blob(self, data_or_io, from_encode, **kwargs):
        raise NotImplementedError

    def decoder(self, data_or_io, from_encode, to_encode, **kwargs):
        """
        Decode data to the the final encode
        final encode must be one of [gzip, b64g, flat, blob]
        :param data:
        :param encode:
        :return:
        """
        if from_encode == to_encode:
            yield data_or_io

        if len(self.support_encodes) == 0:
            raise NotImplementedError

        if not data_or_io:
            self.logger.warning("No data or IO found at {}".format(self.__class__.__name__))
            yield data_or_io

        if from_encode not in self.support_encodes:
            self.logger.error("Decoder of {} not found at {}".format(from_encode, self.__class__.__name__))
            raise XeedDecodeError("XED-000007")

        if not isinstance(data_or_io, (str, bytes, io.BufferedIOBase)):
            self.logger.error("Data type {} not supported".format(data_or_io.__class__.__name__))
            raise XeedDecodeError("XED-000007")

        if to_encode not in ['blob', 'flat', 'gzip', 'b64g']:
            self.logger.error("Cannot decoder to {}".format(to_encode))
            raise XeedDecodeError("XED-000006")

        if from_encode in ['blob', 'flat', 'gzip', 'b64g']:
            if isinstance(data_or_io, io.BufferedIOBase):
                if from_encode in ['flat', 'b64g']:
                    data = data_or_io.read().decode()
                else:
                    data = data_or_io.read()
            else:
                data = data_or_io
            for output in self.basic_encoder(data, from_encode, to_encode):
                yield output
        else:
            for output in self._encode_to_blob(data_or_io, from_encode, **kwargs):
                yield self.basic_encoder(output, 'blob', to_encode)

