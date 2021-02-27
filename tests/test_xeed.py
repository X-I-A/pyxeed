import pytest

from pyxeed.xeed import Xeed


def test_exceptions():
    with pytest.raises(TypeError):
        xeed = Xeed(publisher=object())
    with pytest.raises(TypeError):
        xeed = Xeed(storer=object())
    with pytest.raises(TypeError):
        xeed = Xeed(decoder=object())
    with pytest.raises(TypeError):
        xeed = Xeed(formatter=object())
    with pytest.raises(TypeError):
        xeed = Xeed(translator=object())
