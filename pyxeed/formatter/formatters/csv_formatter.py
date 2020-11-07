import io
import csv
import codecs
import json
import gzip
from pyxeed.utils.core import MESSAGE_SIZE
from pyxeed.utils.exceptions import XeedFormatError
from ..formatter import Formatter

class CSVFormatter(Formatter):
    def __init__(self):
        super().__init__()
        self.support_formats = ['csv']

    def _format_to_record(self, data_or_io, from_format, **kwargs):
        if isinstance(data_or_io, io.BufferedIOBase):
            StreamReader = codecs.getreader('utf-8')
            reader_io = StreamReader(data_or_io)
        elif isinstance(data_or_io, bytes):
            reader_io = io.StringIO(data_or_io.decode())
        elif isinstance(data_or_io, str):
            reader_io = io.StringIO(data_or_io)
        else:
            self.logger.error("Data type {} not supported".format(data_or_io.__class__.__name__))
            raise XeedFormatError("XED-000010")
        if 'message_size' in kwargs:
            message_size = kwargs['message_size']
        else:
            message_size = MESSAGE_SIZE
        counter, size, data, chunk = 0, 0, list(), list()
        dialect = csv.Sniffer().sniff(reader_io.read(4096))
        reader_io.seek(0)
        reader = csv.DictReader(reader_io, dialect=dialect)
        for row in reader:
            counter += 1
            chunk.append(dict(row))
            if counter % 64 == 0:
                data.extend(chunk)
                size += len(gzip.compress(json.dumps(chunk).encode()))
                if size >= message_size:
                    size, chunk = 0, list()
                    yield data
                    data = list()
        data.extend(chunk)
        yield data


    def _format_to_list(self, data_or_io, from_format, **kwargs):
        for data in self._format_to_record(data_or_io, from_format, **kwargs):
            yield self.record_to_list(data)