import gzip
import json
import logging

__all__ = ['Decoder']


class Sender():
    def __init__(self):
        """
        Simple class to send the message or the file
        """
        self.message_client = None
        self.file_client = None
        self.logger = logging.getLogger("Xeed.Sender")
        formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                      ':%(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def check_config(self) -> bool:
        """
        Check if the sender works well
        :return: True = OK, False = KO
        """
        raise NotImplementedError

    def set_message_client(self, message_client):
        self.message_client = message_client

    def set_file_client(self, file_cilent):
        self.file_client = file_cilent

    def _send_by_message(self, header: dict, data) -> str:
        """
        Sending data by message
        :param header: header already be dumped to json format
        :param data: gzipped => to be base64 encode if messager do not support binary format
                     or file_path
        :return: Message ID
        """
        raise NotImplementedError

    def _send_by_file(self, data: bytes) -> str:
        """
        Seding data by File
        :param data: gzipped content
        :return: saved_file_location
        """
        raise NotImplementedError

    def send(self, header: dict, gzip_data: bytes) -> str:
        """
        Sending message
        :param header: X-I Header
        :param data: X-I Data
        :return: Message ID
        """
        if not self.message_client:
            raise NotImplementedError
        # Case 1: Header => Always sent by message
        if int(header.get('age', 0)) == 1:
            # Compress data =>
            return self._send_by_message(header, gzip_data)
        # Case 2: Document Sent
        else:
            if not self.file_client:
                self._send_by_message(header, gzip_data)
            else:
                file_path = self._send_by_file(gzip_data)
                header['data_encode'] = 'gzip'
                header['data_store'] = 'file'
                self._send_by_message(header, file_path)
