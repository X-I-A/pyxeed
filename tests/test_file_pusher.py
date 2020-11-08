import os
import re
import json
import pytest
from pyxeed.pusher import Pusher
from pyxeed.sender.senders.dummy_sender import DummySender

@pytest.fixture(scope='module')
def pusher():
    s = DummySender()
    s.set_message_client(os.path.join('.', 'output', 'file'))
    s.set_file_client(os.path.join('.', 'output', 'file'))
    pusher = Pusher(sender=s)
    yield pusher

    header = {'age': 2, 'data_store': 'body'}

def test_simple_age_push(pusher):
    with open(os.path.join('.', 'input', 'person_simple', '000001.json'), 'rb') as f:
        record = f.read()
        header = {'age': 2, 'end_age': 100, 'start_seq': '20201108160000000000',
                  'data_store': 'body', 'data_encode': 'blob', 'data_format': 'record'}
    pusher.push_data(header, record, message_size=2 ** 12, file_size=2 ** 16)

def test_simple_normal_push(pusher):
    with open(os.path.join('.', 'input', 'person_simple', '000001.json'), 'rb') as f:
        record = f.read()
        header = {'start_seq': '20201108170000000000',
                  'data_store': 'body', 'data_encode': 'blob', 'data_format': 'record'}
    pusher.push_data(header, record, message_size=2 ** 12, file_size=2 ** 16)

def test_zip_io_normal_push(pusher):
    with open(os.path.join('.', 'input', 'codec', 'zip_decoder.zip'), 'rb') as f:
        header = {'start_seq': '20201108180000000000',
                  'data_store': 'file', 'data_encode': 'zip', 'data_format': 'record'}
        pusher.push_data(header, f, message_size=2 ** 12, file_size=2 ** 16)

def test_zip_csv_io_normal_push(pusher):
    with open(os.path.join('.', 'input', 'codec', 'person_simple.zip'), 'rb') as f:
        header = {'start_seq': '20201108190000000000',
                  'data_store': 'file', 'data_encode': 'zip', 'data_format': 'csv'}
        pusher.push_data(header, f, message_size=2 ** 12, file_size=2 ** 16)