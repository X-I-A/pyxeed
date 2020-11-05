import os
import re
import json
import pytest
from pyxeed.extractor.extractors.basic_extractor import BasicExtractor

@pytest.fixture(scope='module')
def extractor():
    extractor = BasicExtractor()
    extractor.init_extractor(source_dir=os.path.join('.', 'input', 'person_simple'))
    yield extractor

def test_filename_pattern(extractor):
    assert extractor.data_file_regex.search("130242.json")
    assert not extractor.data_file_regex.search("130a42.json")
    assert extractor.header_file_regex.search("schema01.json")
    assert not extractor.header_file_regex.search("01schema.json")

def test_get_header(extractor):
    header = extractor.get_table_header()
    assert 'meta_data' in header

def test_get_aged_data(extractor):
    counter = 0
    for data_item in extractor.get_aged_data():
        counter += 1
        assert 'age' in data_item
        assert len(json.loads(data_item['data'])) == 1000
    assert counter == 50

def test_get_normal_data(extractor):
    counter = 0
    for data_item in extractor.get_normal_data():
        counter += 1
        assert 'age' not in data_item
        assert len(json.loads(data_item['data'])) == 1000
    assert counter == 50