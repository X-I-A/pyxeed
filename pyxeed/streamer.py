import pyxeed.pusher
from pyxeed.utils.exceptions import XeedTypeError, XeedDataSpecError
from pyxeed.listener.listeners.basic_listener import BasicListener

__all__ = ['Puller']


class Puller(pyxeed.pusher.Pusher):
    """
    Pull from extractor and push to messager
    """
    def __init__(self, listener=None, messager=None, translators=list()):
        super().__init__(messager=messager, translators=translators)
        if not listener:
            self.listener = BasicListener()
        elif isinstance(listener, pyxeed.listener.Listener):
            self.listener = listener
        else:
            self.logger.error("The Choosen Listener has a wrong Type")
            raise XeedTypeError("XED-000001")

    def stream_and_push(self):
        pass

