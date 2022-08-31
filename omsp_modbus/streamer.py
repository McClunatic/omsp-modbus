"""Defines an OMSP JSON-to-Modbus streaming parser."""

import json
import socket

from typing import Optional, Tuple

import crcmod.predefined

class Streamer:
    """The OMSP JSON-to-Modbus streamer.

    Parameters:
        address: The OMSP ``(host, port)`` to listen on.
        connect: Connect to the port upon construction. Defer connection
            by setting this to ``False``.
    """

    #: The buffer size for receiving data.
    BUFSIZE = 1024

    #: The OMSP NULL byte.
    NULL = b'\x00'

    #: The OMSP CRLF bytes.
    CRLF = b'\r\n'

    def __init__(self, address: Tuple[str, int], connect: bool = True):
        """Contructor."""

        #: The address the Streamer will connect to.
        self.address = address

        #: The current connection for the Streamer.
        self.conn = None

        #: The Streamer buffer.
        self._buf = b''

        #: The Cyclic Redundancy Check, CRC-16-ANSI (x8005), used by OMSP.
        self._crc16 = crcmod.predefined.mkPredefinedCrcFun('modbus')

        if connect:
            self.connect()

    def connect(self, address: Optional[Tuple[str, int]] = None):
        """Creates a connection to listen on `address`.

        Args:
            address: The OMSP ``(host, port)`` to listen on. If `address` is
                not specified, the current address for the Streamer
                will be used.
        """

        if address is not None:
            self.address = address

        if self.conn is not None:
            self.conn.close()

        self.conn = socket.create_connection(self.address)

    def recv_msg(self) -> dict:
        """Receives a message on the current connection.

        Returns:
            The JSON message.
        """

        # Assert there is a connection before receiving
        if self.conn is None:
            raise ValueError(
                'Streamer is not connected! Use .connect() to start listening.'
            )

        # Receive in BUFSIZE chunks until NULL byte is encountered
        msgs = []
        remainder = b''
        while True:
            msg = self.conn.recv(self.BUFSIZE)
            if self.NULL in msg:
                msg, remainder = msg.split(self.NULL, 1)
            msgs.append(msg)
            if remainder:
                break

        # Assemble complete message including remainder from prior receives
        self._buf += b''.join(msgs)

        # Split JSON bytes and checksum
        jsonb, cksum = self._buf.split(self.CRLF)

        # Assert jsonb matches checksum
        crc16 = hex(self._crc16(jsonb))
        if crc16[2:].upper() != cksum.decode():

            # TODO: Improve reporting for this error
            raise ValueError('CRC-16-ANSI checksum does not match')

        # Return the JSON message
        return json.loads(jsonb)