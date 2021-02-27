import pytest

from pyxeed.xeed import Xeed


def test_exceptions():
    with pytest.raises(TypeError):
        xeed = Xeed(publisher=object())
    with pytest.raises(TypeError):
        xeed = Xeed(storers=object())
    with pytest.raises(TypeError):
        xeed = Xeed(decoders=object())
    with pytest.raises(TypeError):
        xeed = Xeed(formatters=object())
    with pytest.raises(TypeError):
        xeed = Xeed(translators=object())
