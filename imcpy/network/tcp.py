import asyncio
import logging

import imcpy

logger = logging.getLogger('imcpy.tcp')


class IMCProtocolTCPClientConnection:
    def __init__(self, name: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.name = name
        self.reader = reader
        self.writer = writer
        self._parser = imcpy.Parser()

    def is_closing(self):
        return self.writer.is_closing()

    async def write_bytes(self, data: bytes):
        """Write the bytes to the connected clients and flush buffer if necessary.

        :param data: The serialized IMC message to write.
        """
        try:
            self.writer.write(data)
            await self.writer.drain()
        except ConnectionError as e:
            logger.error(f'Connection error ({self.name}): {e}')
            await self.close()

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()

    async def handle_data(self, instance):
        """Handle incoming data from the client."""
        try:
            while True:
                data = await self.reader.read(4096)
                if not data:
                    logger.error(f'Connection closed by peer ({self.name})')
                    await self.close()
                    return

                data_remaining = len(data)
                while data_remaining > 0:
                    msg, parsed_bytes = self._parser.parse(data[-data_remaining:])
                    data_remaining -= parsed_bytes
                    if msg is not None:
                        # Log IMC message to file if enabled
                        if instance.log_imc_fh and not instance.log_imc_fh.closed:
                            instance.log_imc_fh.write(data)

                        instance.post_message(msg)
        except ConnectionError as e:
            logger.error(f'Connection error ({self.name}): {e}')
            await self.close()


class IMCProtocolTCPServer:
    def __init__(self, instance) -> None:
        self.instance = instance
        self._clients = set()

    async def on_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            logger.info(f'New connection from {writer.get_extra_info("peername")}')
            client = IMCProtocolTCPClientConnection(writer.get_extra_info('peername'), reader, writer)
            self._clients.add(client)
            await client.handle_data(self.instance)
            self._clients.remove(client)
        except Exception as e:
            logger.error(f'Connnection terminated with error ({e})')

    async def write_message(self, msg: imcpy.Message):
        """Write message to all connected clients.

        :param msg: The IMC message to write.
        """
        b = msg.serialize()
        for client in self._clients:
            if not client.is_closing():
                await client.write_bytes(b)
