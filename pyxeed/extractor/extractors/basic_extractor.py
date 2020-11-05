import os
import re
import json
from pyinsight.utils.core import encoder
from ..extractor import Extractor

class BasicExtractor(Extractor):
    """
    Extract all json files a directory with the following default setting
    * Table structure will be first json file which doesn't start with a number
    * Data filename should contains only number.
    * Age number deducted by file name ######.json
    """
    def __init__(self):
        super().__init__()
        self.data_encode = 'flat'
        self.data_format = 'record'
        self.data_spec = ''
        self.data_store = 'body'
        self.source_dir = os.path.join(os.path.expanduser('~'), 'xeed-extractor')
        self.header_file_regex = re.compile(r'^\D.*\.json$')
        self.data_file_regex = re.compile(r'^[0-9]*.json$')

    def init_extractor(self, **kwargs):
        for key, value in kwargs.items():
            if key == 'source_dir':
                self.source_dir = value
            elif key == 'header_file_regex':
                self.header_file_regex = re.compile(value)
            elif key == 'data_file_regex':
                self.data_file_regex = re.compile(value)

    def get_header(self) -> dict:
        """
        Get Table Header
        :return: "meta_data": table related data; "data": column related data
        """
        for filename in os.listdir(self.source_dir):
            if self.header_file_regex.search(filename):
                data_value = list()
                with open(os.path.join(self.source_dir, filename)) as f:
                    content = json.load(f)
                for key, value in content.items():
                    if isinstance(value, list):
                        for u in value:
                            if isinstance(u, dict):
                                data_value = value
                                break
                        if data_value:
                            content.pop(key)
                            return {'meta_data': json.dumps(content),
                                    'data': encoder(json.dumps(data_value), 'flat', self.data_encode)}

    def get_aged_data(self):
        """
        Get Table Data
        :return: dictionary with 'age', 'end_age' and 'data'.
        """
        data_list = [f for f in os.listdir(self.source_dir) if self.data_file_regex.search(f)]
        data_list = sorted(data_list, key=lambda x:int(x[:-5]))
        data_file, start_age, content = '', 0, None
        for data_file in data_list:
            with open(os.path.join(self.source_dir, data_file)) as f:
                if start_age:
                    result = dict()
                    result['age'] = start_age
                    result['end_age'] = int(data_file[:-5]) - 1
                    result['data'] = content
                    yield result
                start_age = int(data_file[:-5])
                content = f.read()
        if start_age:
            result = dict()
            result['age'] = start_age
            result['end_age'] = int(data_file[:-5]) - 1
            result['data'] = content
            yield result

    def get_normal_data(self):
        for result in self.get_aged_data():
            result.pop('age', None)
            result.pop('end_age', None)
            yield result