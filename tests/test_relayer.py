import os
from xialib import BasicPublisher
from pyxeed.relayer import Relayer
import pytest

destination = os.path.join('.', 'output', 'relayer')

@pytest.fixture(scope='module')
def relayer():
    publishers = {'client-001': BasicPublisher()}
    relayer = Relayer(publishers=publishers)
    yield relayer

def test_push_header(relayer):
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), 'rb') as f:
        data = f.read()
        header = {'start_seq': '20201108170000000000', 'data_spec': 'x-i-a', 'age': '1',
                  'data_store': 'body', 'data_encode': 'blob', 'data_format': 'record'}
    relayer.push_data(header, data, 'client-001', destination, 'test-00', 'simple', 0)

def test_pass_through(relayer):
    with open(os.path.join('.', 'input', 'person_simple', '000001.json'), 'rb') as f:
        data = f.read()
        header = {'start_seq': '20201108170000000000', 'data_spec': 'x-i-a',
                  'data_store': 'body', 'data_encode': 'blob', 'data_format': 'record'}
    relayer.push_data(header, data, 'client-001', destination, 'test-00', 'simple', 0)

def test_simple_normal_flow(relayer):
    with open(os.path.join('.', 'input', 'person_simple', '000001.json'), 'rb') as f:
        data = f.read()
        header = {'start_seq': '20201108170000000000',
                  'data_store': 'body', 'data_encode': 'blob', 'data_format': 'record'}
    relayer.push_data(header, data, 'client-001', destination, 'test-01', 'simple', 0)

def test_zip_io_normal_push(relayer):
    header = {'start_seq': '20201108180000000000',
                  'data_store': 'file', 'data_encode': 'zip', 'data_format': 'record'}
    data = os.path.join('.', 'input', 'codec', 'zip_decoder.zip')
    relayer.push_data(header, data, 'client-001', destination, 'test-02', 'zip', 0)

def test_zip_csv_io_normal_push(relayer):
    header = {'start_seq': '20201108190000000000',
              'data_store': 'file', 'data_encode': 'zip', 'data_format': 'csv'}
    data = os.path.join('.', 'input', 'codec', 'person_simple.zip')
    relayer.push_data(header, data, 'client-001', destination, 'test-03', 'zip', 0)

def test_zip_csv_io_normal_chunk_push(relayer):
    header = {'start_seq': '20201108190000000000',
              'data_store': 'file', 'data_encode': 'zip', 'data_format': 'csv'}
    data = os.path.join('.', 'input', 'codec', 'person_simple.zip')
    relayer.push_data(header, data, 'client-001', destination, 'test-04', 'zip', 4096)

def test_simple_age_flow(relayer):
    with open(os.path.join('.', 'input', 'person_simple', '000001.json'), 'rb') as f:
        data = f.read()
        header = {'start_seq': '20201108170000000000', 'age': 2, 'end_age': 100,
                  'data_store': 'body', 'data_encode': 'blob', 'data_format': 'record'}
    relayer.push_data(header, data, 'client-001', destination, 'test-05', 'simple', 0)

def test_zip_io_age_push(relayer):
    header = {'start_seq': '20201108180000000000', 'age': 2, 'end_age': 100,
                  'data_store': 'file', 'data_encode': 'zip', 'data_format': 'record'}
    data = os.path.join('.', 'input', 'codec', 'zip_decoder.zip')
    relayer.push_data(header, data, 'client-001', destination, 'test-06', 'zip', 0)

def test_zip_csv_io_age_push(relayer):
    header = {'start_seq': '20201108190000000000', 'age': 2, 'end_age': 100,
              'data_store': 'file', 'data_encode': 'zip', 'data_format': 'csv'}
    data = os.path.join('.', 'input', 'codec', 'person_simple.zip')
    relayer.push_data(header, data, 'client-001', destination, 'test-07', 'zip', 0)

def test_zip_csv_io_age_chunk_push(relayer):
    header = {'start_seq': '20201108190000000000', 'age': 2, 'end_age': 100,
              'data_store': 'file', 'data_encode': 'zip', 'data_format': 'csv'}
    data = os.path.join('.', 'input', 'codec', 'person_simple.zip')
    relayer.push_data(header, data, 'client-001', destination, 'test-08', 'zip', 8192)

def test_zip_io_age_chunk_push(relayer):
    header = {'start_seq': '20201108180000000000', 'age': 2, 'end_age': 100,
                  'data_store': 'file', 'data_encode': 'zip', 'data_format': 'record'}
    data = os.path.join('.', 'input', 'codec', 'zip_decoder.zip')
    relayer.push_data(header, data, 'client-001', destination, 'test-09', 'zip', 8192)

def test_zip_io_age_chunk_store_push(relayer):
    header = {'start_seq': '20201108180000000000', 'age': 2, 'end_age': 100,
                  'data_store': 'file', 'data_encode': 'zip', 'data_format': 'record'}
    data = os.path.join('.', 'input', 'codec', 'zip_decoder.zip')
    store_path = os.path.join('.', 'output', 'storer') + os.path.sep
    relayer.push_data(header, data, 'client-001', destination, 'test-10', 'zip', 8192, 'file', store_path)

def test_exceptions(relayer):
    header = {'start_seq': '20201108170000000000', 'data_store': 'body', 'data_encode': 'blob', 'data_format': 'record'}
    with pytest.raises(ValueError):
        err_header = header.copy()
        err_header.pop('start_seq')
        relayer.push_data(err_header, '', 'client-001', '', '', '', 0)
    with pytest.raises(ValueError):
        err_header = header.copy()
        err_header['data_encode'] = 'err'
        relayer.push_data(err_header, '', 'client-001', '', '', '', 0)
    with pytest.raises(ValueError):
        err_header = header.copy()
        err_header['data_format'] = 'err'
        relayer.push_data(err_header, '', 'client-001', '', '', '', 0)
    with pytest.raises(ValueError):
        err_header = header.copy()
        err_header['data_spec'] = 'err'
        relayer.push_data(err_header, '', 'client-001', '', '', '', 0)
    with pytest.raises(ValueError):
        relayer.push_data(header, '', 'err', '', '', '', 0)
    with pytest.raises(ValueError):
        relayer.push_data(header, '', 'client-001', '', '', '', 0, 'err')
    with pytest.raises(ValueError):
        err_header = header.copy()
        err_header['data_store'] = 'err'
        relayer.push_data(err_header, '', 'client-001', '', '', '', 0)
    with pytest.raises(ValueError):
        header = {'start_seq': '20201108180000000000', 'age': 2, 'end_age': 10,
                  'data_store': 'file', 'data_encode': 'zip', 'data_format': 'record'}
        data = os.path.join('.', 'input', 'codec', 'zip_decoder.zip')
        relayer.push_data(header, data, 'client-001', destination, 'test-09', 'zip', 100)