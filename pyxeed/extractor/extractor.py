import logging

__all__ = ['Extractor', 'AgedExtractor', 'NormalExtractor']


class Extractor():
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger("Xeed.Extractor")
        self.data_encode = ''
        self.data_format = ''
        self.data_spec = ''
        self.data_store = ''
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          ':%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def init_extractor(self, **kwargs):
        pass

    def get_header(self):
        pass

    def get_aged_data(self):
        pass

    def get_normal_data(self):
        pass

