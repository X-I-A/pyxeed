import os
import re
import json
from ..extractor import Extractor

class DirExtractor(Extractor):
    """
    Extract all files from a directory with the following default setting
    * Table structure file will be first file which doesn't start with a number
    * Data filename should contains only number.
    * Age number deducted by file name ######.json
    """
    def init_extractor(self, **kwargs):
        self.dir_path = kwargs['dir_path']
        self.extension = kwargs['extension']
        if 'header_regex' in kwargs:
            re.compile(kwargs['header_regex'])
        else:
            self.header_file_regex = re.compile(r'^\D.*\.' + re.escape(self.extension) + r'$')
        self.data_file_regex = re.compile(r'^[0-9]*.' + re.escape(self.extension) + r'$')

    def extract(self):
        result = dict()
        # Step 1: get header file
        for filename in os.listdir(self.dir_path):
            if self.header_file_regex.search(filename):
                with open(os.path.join(self.dir_path, filename), 'rb') as file_io:
                    result['data'] = file_io
                    result['extract_info'] = {'type': 'header', 'name': filename,
                                              'fullname': os.path.join(self.dir_path, filename),
                                              'data_type': file_io.__class__.__name__}
                    yield result

        # Step 2: Get file one by one
        for filename in sorted(os.listdir(self.dir_path), key=lambda x: str(x).zfill(20)):
            if self.data_file_regex.search(filename):
                with open(os.path.join(self.dir_path, filename), 'rb') as file_io:
                    result['data'] = file_io
                    result['extract_info'] = {'type': 'data', 'name': filename,
                                              'age_hint': int(filename.split('.')[0]),
                                              'fullname': os.path.join(self.dir_path, filename),
                                              'data_type': file_io.__class__.__name__}
                    yield result
