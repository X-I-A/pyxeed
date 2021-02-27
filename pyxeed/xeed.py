import logging
from typing import List, Dict
from xialib import Service
from xialib import BasicStorer
from xialib import BasicDecoder, ZipDecoder
from xialib import BasicFormatter, CSVFormatter
from xialib import BasicTranslator, SapTranslator
from xialib.storer import Storer
from xialib.decoder import Decoder
from xialib.formatter import Formatter
from xialib.publisher import Publisher
from xialib.translator import Translator

__all__ = ['Xeed']


class Xeed(Service):
    """Xeed Application

    """
    log_level = logging.WARNING
    api_url = 'api.x-i-a.com'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger("Xeed")
        self.log_context = {'context': ''}
        self.logger.setLevel(self.log_level)


        if 'storers' in kwargs:
            storers = [BasicStorer()]
            if not isinstance(kwargs['storers'], list) \
                    or not all(isinstance(storer, Storer) for storer in kwargs['storers']):
                self.logger.error("storer should have type of Storer", extra=self.log_context)
                raise TypeError("XED-000018")
            else:
                storers.extend(kwargs['storers'])
                self.storer = self.get_storer_register_dict(storers)

        if 'decoders' in kwargs:
            decoders = [BasicDecoder(), ZipDecoder()]
            if not isinstance(kwargs['decoders'], list) or \
                    not all(isinstance(decoder, Decoder) for decoder in kwargs['decoders']):
                self.logger.error("decoder should have type of Decoder", extra=self.log_context)
                raise TypeError("XED-000012")
            else:
                decoders.extend(kwargs['decoders'])
                self.decoder = self.get_decoder_register_dict(decoders)

        if 'formatters' in kwargs:
            formatters = [BasicFormatter(), CSVFormatter()]
            if not isinstance(kwargs['formatters'], list) or \
                    not all(isinstance(formatter, Formatter) for formatter in kwargs['formatters']):
                self.logger.error("The Choosen formatter has a wrong Type", extra=self.log_context)
                raise TypeError("XED-000015")
            else:
                formatters.extend(kwargs['formatters'])
                self.formatter = self.get_formatter_register_dict(formatters)

        if 'translators' in kwargs:
            translators = [BasicTranslator(), SapTranslator()]
            if not isinstance(kwargs['translators'], list) or \
                    not all(isinstance(translator, Translator) for translator in kwargs['translators']):
                self.logger.error("The Choosen Translator has a wrong Type", extra=self.log_context)
                raise TypeError("XED-000003")
            else:
                translators.extend(kwargs['translators'])
                self.translator = self.get_translator_register_dict(translators)

    @classmethod
    def get_storer_register_dict(cls, storer_list: List[Storer]) -> Dict[str, Storer]:
        register_dict = dict()
        for storer in storer_list:
            for store_type in storer.store_types:
                register_dict[store_type] = storer
        return register_dict

    @classmethod
    def get_decoder_register_dict(cls, decoder_list: List[Decoder]) -> Dict[str, Decoder]:
        register_dict = dict()
        for decoder in decoder_list:
            for encode in decoder.supported_encodes:
                register_dict[encode] = decoder
        return register_dict

    @classmethod
    def get_formatter_register_dict(cls, formatter_list: List[Decoder]) -> Dict[str, Formatter]:
        register_dict = dict()
        for formatter in formatter_list:
            for format in formatter.support_formats:
                register_dict[format] = formatter
        return register_dict

    @classmethod
    def get_translator_register_dict(cls, translator_list: List[Decoder]) -> Dict[str, Translator]:
        register_dict = dict()
        for translator in translator_list:
            for spec in translator.spec_list:
                register_dict[spec] = translator
        return register_dict