import logging

from pyinsight import Messager
from pyxeed.utils.core import LOGGING_LEVEL
from pyxeed.utils.exceptions import XeedTypeError
from pyxeed import Extractor

__all__ = ['Xeed']


class Xeed():
    def __init__(self):
        self.logger = logging.getLogger("Xeed")
        self.logger.setLevel(LOGGING_LEVEL)

class SimpleXeed(Xeed):
    """
    Extractor to Messager
    """
    def __init__(self, extractor, messager):
        super().__init__()
        if isinstance(extractor, Extractor):
            self.extrator = Extractor
        else:
            self.logger.error("The Choosen Extractor has a wrong Type")
            raise XeedTypeError("XED-000001")

        if isinstance(messager, Messager):
            self.messager = messager
        else:
            self.logger.error("The Choosen Messenger has a wrong Type")
            raise XeedTypeError("XED-000002")
