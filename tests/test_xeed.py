import pytest
from xialib import BasicStorer
from xialib import BasicDecoder, ZipDecoder
from xialib import BasicFormatter, CSVFormatter
from xialib import BasicTranslator, SapTranslator
from pyxeed.xeed import Xeed

def test_init():
    xeed = Xeed(storer=BasicStorer())
    xeed = Xeed(storer={"basic": BasicStorer()})
    xeed = Xeed(decoder=BasicDecoder())
    xeed = Xeed(decoder={"basic": BasicDecoder()})
    xeed = Xeed(formatter=BasicFormatter())
    xeed = Xeed(formatter={"basic": BasicFormatter()})
    xeed = Xeed(translator=BasicTranslator())
    xeed = Xeed(translator={"basic": BasicTranslator()})

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
