import os
from xialib import BasicPublisher
from pyxeed.seeder import Seeder
import pytest

destination = os.path.join('.', 'output', 'seeder')

@pytest.fixture(scope='module')
def seeder():
    publisher = {'client-001': BasicPublisher()}
    seeder = Seeder(publisher=publisher)
    yield seeder

def test_push_header(seeder):
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), 'rb') as f:
        data = f.read()
        header = {'start_seq': '20201108170000000000', 'data_spec': 'x-i-a', 'age': '1',
                  'data_store': 'body', 'data_encode': 'blob', 'data_format': 'record'}
    seeder.push_data(header, data, destination, 'test-00', 'simple', 0)

def test_pass_through(seeder):
    with open(os.path.join('.', 'input', 'person_simple', '000001.json'), 'rb') as f:
        data = f.read()
        header = {'start_seq': '20201108170000000000', 'data_spec': 'x-i-a',
                  'data_store': 'body', 'data_encode': 'blob', 'data_format': 'record'}
    seeder.push_data(header, data, destination, 'test-00', 'simple', 0)

def test_simple_normal_flow(seeder):
    with open(os.path.join('.', 'input', 'person_simple', '000001.json'), 'rb') as f:
        data = f.read()
        header = {'start_seq': '20201108170000000000',
                  'data_store': 'body', 'data_encode': 'blob', 'data_format': 'record'}
    seeder.push_data(header, data, destination, 'test-01', 'simple', 0)

def test_zip_io_normal_push(seeder):
    header = {'start_seq': '20201108180000000000',
                  'data_store': 'file', 'data_encode': 'zip', 'data_format': 'record'}
    data = os.path.join('.', 'input', 'codec', 'zip_decoder.zip')
    seeder.push_data(header, data, destination, 'test-02', 'zip', 0)

def test_zip_csv_io_normal_push(seeder):
    header = {'start_seq': '20201108190000000000',
              'data_store': 'file', 'data_encode': 'zip', 'data_format': 'csv'}
    data = os.path.join('.', 'input', 'codec', 'person_simple.zip')
    seeder.push_data(header, data, destination, 'test-03', 'zip', 0)

def test_zip_csv_io_normal_chunk_push(seeder):
    header = {'start_seq': '20201108190000000000',
              'data_store': 'file', 'data_encode': 'zip', 'data_format': 'csv'}
    data = os.path.join('.', 'input', 'codec', 'person_simple.zip')
    seeder.push_data(header, data, destination, 'test-04', 'zip', 4096)

def test_simple_age_flow(seeder):
    with open(os.path.join('.', 'input', 'person_simple', '000001.json'), 'rb') as f:
        data = f.read()
        header = {'start_seq': '20201108170000000000', 'age': 2, 'end_age': 100,
                  'data_store': 'body', 'data_encode': 'blob', 'data_format': 'record'}
    seeder.push_data(header, data, destination, 'test-05', 'simple', 0)

def test_zip_io_age_push(seeder):
    header = {'start_seq': '20201108180000000000', 'age': 2, 'end_age': 100,
                  'data_store': 'file', 'data_encode': 'zip', 'data_format': 'record'}
    data = os.path.join('.', 'input', 'codec', 'zip_decoder.zip')
    seeder.push_data(header, data, destination, 'test-06', 'zip', 0)

def test_zip_csv_io_age_push(seeder):
    header = {'start_seq': '20201108190000000000', 'age': 2, 'end_age': 100,
              'data_store': 'file', 'data_encode': 'zip', 'data_format': 'csv'}
    data = os.path.join('.', 'input', 'codec', 'person_simple.zip')
    seeder.push_data(header, data, destination, 'test-07', 'zip', 0)

def test_zip_csv_io_age_chunk_push(seeder):
    header = {'start_seq': '20201108190000000000', 'age': 2, 'end_age': 100,
              'data_store': 'file', 'data_encode': 'zip', 'data_format': 'csv'}
    data = os.path.join('.', 'input', 'codec', 'person_simple.zip')
    seeder.push_data(header, data, destination, 'test-08', 'zip', 8192)

def test_zip_io_age_chunk_push(seeder):
    header = {'start_seq': '20201108180000000000', 'age': 2, 'end_age': 100,
                  'data_store': 'file', 'data_encode': 'zip', 'data_format': 'record'}
    data = os.path.join('.', 'input', 'codec', 'zip_decoder.zip')
    seeder.push_data(header, data, destination, 'test-09', 'zip', 8192)

def test_zip_io_age_chunk_store_push(seeder):
    header = {'start_seq': '20201108180000000000', 'age': 2, 'end_age': 100,
                  'data_store': 'file', 'data_encode': 'zip', 'data_format': 'record'}
    data = os.path.join('.', 'input', 'codec', 'zip_decoder.zip')
    store_path = os.path.join('.', 'output', 'storer') + os.path.sep
    seeder.push_data(header, data, destination, 'test-10', 'zip', 8192, 'file', store_path)

def test_exceptions(seeder):
    header = {'start_seq': '20201108170000000000', 'data_store': 'body', 'data_encode': 'blob', 'data_format': 'record'}
    with pytest.raises(ValueError):
        err_header = header.copy()
        err_header.pop('start_seq')
        seeder.push_data(err_header, '', '', '', '', 0)
    with pytest.raises(ValueError):
        err_header = header.copy()
        err_header['data_encode'] = 'err'
        seeder.push_data(err_header, '', '', '', '', 0)
    with pytest.raises(ValueError):
        err_header = header.copy()
        err_header['data_format'] = 'err'
        seeder.push_data(err_header, '', '', '', '', 0)
    with pytest.raises(ValueError):
        err_header = header.copy()
        err_header['data_spec'] = 'err'
        seeder.push_data(err_header, '', '', '', '', 0)
    with pytest.raises(ValueError):
        seeder.push_data(header, '', 'err', '', '', '', 0)
    with pytest.raises(ValueError):
        seeder.push_data(header, '', '', '', '', 0, 'err')
    with pytest.raises(ValueError):
        err_header = header.copy()
        err_header['data_store'] = 'err'
        seeder.push_data(err_header, '', '', '', '', 0)
    with pytest.raises(ValueError):
        header = {'start_seq': '20201108180000000000', 'age': 2, 'end_age': 10,
                  'data_store': 'file', 'data_encode': 'zip', 'data_format': 'record'}
        data = os.path.join('.', 'input', 'codec', 'zip_decoder.zip')
        seeder.push_data(header, data, destination, 'test-09', 'zip', 100)
