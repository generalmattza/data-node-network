import asyncio
import logging
import time

from data_node_network.configuration import config_global

logger = logging.getLogger(__name__)
config = config_global["node_network"]

READ_LIMIT = config["read_limit"]


def handle_request(message):
    if message == "getData":
        return "Sending data"
    elif message == "getTime":
        return time.time_ns()


class NodeServerBase:
    def __init__(self, address):
        self.address = address
        self.host, self.port = address
        self.address_str = f"{self.host}:{self.port}"

    async def handle_client(self, reader, writer):
        raise NotImplementedError("Subclasses must implement handle_client method")

    async def start_server(self):
        raise NotImplementedError("Subclasses must implement start_server method")

    async def ping(self):
        return True


class NodeServerTCP(NodeServerBase):
    async def handle_client(self, reader, writer):
        data = await reader.read(READ_LIMIT)
        message = data.decode()

        response = handle_request(message)

        writer.write(response.encode())
        await writer.drain()

        logger.debug(f"Node received a request: {message}")
        writer.close()

    async def start_server(self):
        server = await asyncio.start_server(self.handle_client, self.host, self.port)

        logger.info(f"Node server running on {server.sockets[0].getsockname()}")

        async with server:
            await server.serve_forever()


class ServerProtocolUDP(asyncio.DatagramProtocol):

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        host, port = addr[0], addr[1]
        logger.debug(f"Node received request from {host}:{port} - '{data.decode()}'")

    def error_received(self, exc):
        logger.error(exc)

    def connection_lost(self, exc):
        logger.debug(f"Connection closed: {exc}")
        self.on_con_lost.set_result(True)


class EchoServerProtocol(ServerProtocolUDP):

    def datagram_received(self, data, addr):
        super().datagram_received(data, addr)
        self.transport.sendto(data, addr)


class NodeServerUDP(NodeServerBase):

    def __init__(self, address: tuple[str, int], protocol=None):
        self.protocol = protocol or EchoServerProtocol
        super().__init__(address=address)

    async def start_server(self):
        loop = asyncio.get_running_loop()
        transport, protocol = await loop.create_datagram_endpoint(
            self.protocol, local_addr=self.address
        )

        logger.info(f"Node server running on {transport.get_extra_info('sockname')}")

        try:
            await asyncio.Future()  # Run forever
        finally:
            transport.close()
