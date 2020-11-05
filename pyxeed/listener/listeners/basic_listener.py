import os
import re
import json
from pyinsight.utils.core import encoder, get_current_timestamp
from ..listener import Listener

class BasicListener(Listener):
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
        self.data_store = 'body'
        self.source_dir = os.path.join(os.path.expanduser('~'), 'xeed-extractor')
        self.header_file_regex = re.compile(r'^\D.*\.json$')
        self.data_file_regex = re.compile(r'^[0-9]*.json$')