import os
from pyxeed.extractor.extractors.dir_extractor import DirExtractor

e = DirExtractor()
e.init_extractor(extension='json', dir_path=os.path.join('.', 'input', 'person_simple'))

for data in e.extract():
    print(data['extract_info'])