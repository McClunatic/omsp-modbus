"""Defines an OMSP JSON-to-Modbus streaming parser."""

import socket

from typing import Optional, Tuple


class Streamer:
    """The OMSP JSON-to-Modbus streamer.

    Parameters:
        address: The OMSP ``(host, port)`` to listen on.
        connect: Connect to the port upon construction. Defer connection
            by setting this to ``False``.
    """

    def __init__(self, address: Tuple[str, int], connect: bool = True):
        """Contructor."""

        #: The address the Streamer will connect to.
        self.address = address

        #: The current connection for the Streamer.
        self.conn = None

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
