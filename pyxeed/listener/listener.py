import logging

__all__ = ['Listener']


class Listener():
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger("Xeed.Extractor")
        self.topic_id = ''
        self.table_id = ''
        self.data_encode = ''
        self.data_format = ''
        self.data_header_spec = ''
        self.data_body_spec = ''
        self.data_store = ''
        self.aged = False
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          ':%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)