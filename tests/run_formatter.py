import os
import json
from pyxeed.formatter.formatter import Formatter
from pyxeed.formatter.formatters.csv_formatter import CSVFormatter
from pyxeed.decoder.decoders.zip_decoder import ZipDecoder

d = ZipDecoder()
c = CSVFormatter()
with open(os.path.join('.', 'input', 'codec', 'person_simple.zip'), 'rb') as f:
    # record = json.load(f)
    for data_or_io in d.decoder(f, 'zip', 'blob'):
        for data in c.formatter(data_or_io, 'csv', message_size = 2 ** 14):
            print(data)