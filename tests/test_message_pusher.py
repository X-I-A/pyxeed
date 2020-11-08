import os
import re
import json
import pytest
from pyxeed.pusher import Pusher
from pyxeed.sender.senders.dummy_sender import DummySender

@pytest.fixture(scope='module')
def pusher():
    s = DummySender()
    s.set_message_client(os.path.join('.', 'output', 'message'))
    pusher = Pusher(sender=s)
    yield pusher

def test_simple_push(pusher):
    with open(os.path.join('.', 'input', 'person_simple', '000001.json'), 'rb') as f:
        record = f.read()
        header = {'age': 2, 'start_seq': '20201108160000000000',
                  'data_store': 'body', 'data_encode': 'blob', 'data_format': 'record'}
    pusher.push_data(header, record, message_size=2 ** 12)
