import os
from pyxeed.decoder.decoder import Decoder
from pyxeed.decoder.decoders.zip_decoder import ZipDecoder

d = ZipDecoder()
with open(os.path.join('.', 'input', 'codec', 'zip_decoder.zip'), 'rb') as f:
    for data in d.decoder(f, 'zip', 'blob'):
        print(data)

d1 = Decoder()

with open(os.path.join('.', 'input', 'codec', 'aged_package.gz'), 'rb') as f:
    for data in d1.decoder(f, 'gzip', 'blob'):
        print(data)


