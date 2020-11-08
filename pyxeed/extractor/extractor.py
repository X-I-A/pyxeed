import io
import logging

__all__ = ['Extractor']


class Extractor():
    def __init__(self):
        self.logger = logging.getLogger("Xeed.Extractor")
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          ':%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def init_extractor(self, **kwargs):
        raise NotImplementedError

    def extract(self):
        """

        :return:
        [0] Metadata
        [1] Data or IO handlers
        """
        raise NotImplementedError
