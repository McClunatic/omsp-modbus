"""An OMSP-to-Modbus server."""

import argparse
import asyncio
import logging
import random
import signal
import sys
from webbrowser import get

from pymodbus.datastore import ModbusSlaveContext, ModbusServerContext
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.server.async_io import ModbusTcpServer
from pymodbus.version import version

# Set up basic logging
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
log = logging.getLogger(__name__)


def update_input_registers(context: ModbusSlaveContext):
    """Updates the input registers of slave `context` with new values.

    Args:
        context: Slave context to update.
    """

    # TODO: Integrate with Streamer
    data = [random.random() for _ in range(4)]

    # Update registers from hex address 0 (fx 4 maps to input registers)
    fx = 1
    context.setValues(fx, 0, data)


async def update_context(context: ModbusServerContext, interval: float = 0.1):
    """Updates the server `context` on a regular interval.

    Args:
        context: Slave context to update.
        interval: Interval in seconds between updates.
    """

    # Update slave context on set interval
    unit = 0x00
    slave = context[unit]
    while True:
        try:
            update_input_registers(slave)
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            log.debug('Cancelling context updates')
            break


def get_parser() -> argparse.ArgumentParser:
    """Gets a server argument parser.

    Returns:
        The parser object.
    """

    parser = argparse.ArgumentParser(
        description='An OMSP to Modbus server',
    )
    parser.add_argument(
        '-p', '--port',
        help='The port to bind to',
        type=int,
        default=5020,
    )
    parser.add_argument(
        '-f', '--frequency',
        help='The frequency (in Hz) to update Modbus registers at',
        type=int,
        default=5,
    )
    return parser


async def main(port: int, frequency: float):
    """Runs the asynchronous server.

    Args:
        port: The port to bind the Modbus server to.
        frequency: The frequency (in Hz) to update Modbus registers at.
    """

    # Create the store (a Modbus data model) with fully initialized ranges
    store = ModbusSlaveContext()
    context = ModbusServerContext(slaves=store)

    # Initialize server information
    identity = ModbusDeviceIdentification()
    identity.VendorName = 'Pymodbus'
    identity.ProductCode = 'PM'
    identity.VendorUrl = 'http://github.com/riptideio/pymodbus/'
    identity.ProductName = 'Pymodbus Server'
    identity.ModelName = 'Pymodbus Server'
    identity.MajorMinorRevision = version.short()

    # Add coils updater to event loop
    loop = asyncio.get_event_loop()
    interval = 1 / frequency
    task = loop.create_task(update_context(context, interval))

    # Create the TCP Server
    adr = ('', port)
    server = ModbusTcpServer(context, address=adr, defer_start=True, loop=loop)

    # Add signal handlers for graceful closure
    for sig in (signal.SIGINT, signal.SIGTERM):
        if sys.platform == 'linux':
            loop.add_signal_handler(sig, task.cancel)
            loop.add_signal_handler(sig, server.server_close)
        elif sys.platform == 'win32':

            def cancel(*args, task=task):
                task.cancel()

            def server_close(*args, server=server):
                server.server_close()

            signal.signal(sig, cancel)
            signal.signal(sig, server_close)

    # Start the server
    try:
        await server.serve_forever()
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    args = get_parser().parse_args()
    asyncio.run(main(args.port, args.frequency))
