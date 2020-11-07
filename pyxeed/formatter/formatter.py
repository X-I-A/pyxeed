import io
import json
import logging
from functools import reduce
from pyxeed.utils.exceptions import XeedFormatError

class Formatter():
    def __init__(self):
        self.support_formats = ['list', 'record']
        self.logger = logging.getLogger("Xeed.Fromatter")
        formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                      ':%(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def _format_to_record(self, data_or_io, from_format, **kwargs):
        """
        Format data to record format
        Input must be decoded to 'flat' or 'BufferedIO'
        :param data:
        :param format:
        :return:
        """
        raise NotImplementedError

    def _format_to_list(self, data_or_io, from_format, **kwargs):
        """
        Format data to list format
        Input must be decoded to 'flat' or 'BufferedIO'
        :param data:
        :param format:
        :return:
        """
        raise NotImplementedError

    def record_to_list(self, data: list):
        field_list = reduce(lambda a,b : set(a)|set(b), data)
        return {k: [x.get(k, None) for x in data] for k in field_list}

    def list_to_record(self, data: dict):
        line_nbs = [len(value) for key, value in data.items()]
        if len(set(line_nbs)) > 1:
            self.logger.error("list must have identical line numbers")
            raise XeedFormatError("XED-000008")
        return [{key: value[i] for key, value in data.items() if value is not None} for i in range(line_nbs[0])]

    def formatter(self, data_or_io, from_format, to_format, **kwargs):
        if from_format == to_format:
            yield data_or_io

        if len(self.support_formats) == 0:
            raise NotImplementedError

        if not data_or_io:
            self.logger.warning("No data or IO found at {}".format(self.__class__.__name__))
            yield data_or_io

        if from_format not in self.support_formats:
            self.logger.error("Formatter of {} not found at {}".format(from_format, self.__class__.__name__))
            raise XeedFormatError("XED-000010")

        if not isinstance(data_or_io, (str, bytes, io.BufferedIOBase)):
            self.logger.error("Data type {} not supported".format(data_or_io.__class__.__name__))
            raise XeedFormatError("XED-000010")

        if to_format not in ['list', 'record']:
            self.logger.error("Cannot formatter to {}".format(to_format))
            raise XeedFormatError("XED-000011")

        if from_format in ['list', 'record']:
            if isinstance(data_or_io, str):
                data = json.loads(data_or_io)
            elif isinstance(data_or_io, io.BufferedIOBase):
                data = json.load(data_or_io)
            else:
                self.logger.error("Data type {} not supported".format(data_or_io.__class__.__name__))
                raise XeedFormatError("XED-000010")
            if from_format == 'list':
                yield json.dumps(self.list_to_record(data))
            elif from_format == 'record':
                yield json.dumps(self.record_to_list(data))
        elif to_format == 'list':
            for output in self._format_to_list(data_or_io, to_format, **kwargs):
                yield output
        elif to_format == 'record':
            for output in self._format_to_record(data_or_io, to_format, **kwargs):
                yield output
