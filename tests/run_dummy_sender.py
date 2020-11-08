import os
import json

from pyxeed.sender.senders.dummy_sender import DummySender

with open(os.path.join('.', 'input', 'person_simple', '000001.json')) as f:
    record = json.load(f)
s = DummySender()
header = {'age': 2, 'data_store': 'body'}
s.set_message_client(os.path.join('.', 'output', '000001.initial'))
s.set_file_client(os.path.join('.', 'output', '000001.gz'))
s.send(header, record)
