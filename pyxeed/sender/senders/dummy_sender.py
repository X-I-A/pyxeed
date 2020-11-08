import os
import base64
import json
from ..sender import Sender

class DummySender(Sender):
    def _send_by_file(self, gzip_data: bytes):
        with open(self.file_client, 'wb') as f:
            f.write(gzip_data)
        return self.file_client

    def _send_by_message(self, header: dict, gzip_data):
        if header['data_store'] == 'body':
            header['data_encode'] = 'b64g'
            header['data'] = base64.b64encode(gzip_data).decode()
        else:
            header['data'] = gzip_data
        with open(self.message_client, 'w') as f:
            f.write(json.dumps(header))
        return 'dummy'