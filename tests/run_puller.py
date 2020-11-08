import os
import pytest
from pyxeed import Puller

def test_send_aged_message():
    x = Puller()
    x.extrator.init_extractor(source_dir=os.path.join('.', 'input', 'person_simple'),
                              topic_id='test-003',
                              table_id='person_simple',
                              aged=True)
    x.pull_and_push()

def test_send_normal_message():
    x = Puller()
    x.extrator.init_extractor(source_dir=os.path.join('.', 'input', 'person_simple'),
                              topic_id='test-004',
                              table_id='person_simple',
                              aged=False)
    x.pull_and_push()
