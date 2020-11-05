import os
import pytest
from pyxeed import Streamer
from pyxeed.extractor.extractors.basic_extractor import BasicExtractor
from pyxeed.listener.listeners.basic_listener import BasicListener

def test_stream_message():
    x = BasicExtractor()
    x.init_extractor(source_dir=os.path.join('.', 'stream', 'channel1'),
                     topic_id='test-008',
                     table_id='person_simple',
                     aged=True)
    l = BasicListener()
    l.set_extractor(x)
    s = Streamer(listener=l)
    s.stream_and_push()