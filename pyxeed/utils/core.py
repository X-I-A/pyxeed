import os
import logging

MESSAGE_SIZE = os.environ.get('XEED_MESSAGE_SIZE', 2 ** 18)
FILE_SIZE = os.environ.get('XEED_FILE_SIZE', 2 ** 26)
LOGGING_LEVEL = os.environ.get('XEED_LOGGING_LEVEL', logging.WARNING)