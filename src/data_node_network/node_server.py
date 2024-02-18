import asyncio
import logging
import json
import time

from prometheus_client import start_http_server, Counter, Histogram, Gauge

from data_node_network.configuration import config_global

logger = logging.getLogger(__name__)
config = config_global["node_network"]
READ_LIMIT = config["read_limit"]


class NodeServerBase:
    def __init__(self, address, node_id, parser=None):
        self.address = address
        self.host, self.port = address
        self.address_str = f"{self.host}:{self.port}"
        self.node_id = node_id
        self.parser = json.dumps if parser is None else parser

        self.requests_counter = Counter(
            "node_server_requests_total",
            "Total number of requests received by NodeServer",
        )
        self.bytes_received_counter = Counter(
            "node_server_bytes_received_total",
            "Total number of bytes received by NodeServer",
        )
        self.bytes_sent_counter = Counter(
            "node_server_bytes_sent_total", "Total number of bytes sent by NodeServer"
        )
        self.response_time_histogram = Histogram(
            "node_server_response_time_seconds",
            "Histogram of response time to handle requests",
        )

    async def handle_client(self, reader, writer):
        raise NotImplementedError("Subclasses must implement handle_client method")

    async def start_server(self):
        raise NotImplementedError("Subclasses must implement start_server method")

    def get_client_address(self, writer):
        peername = writer.get_extra_info("peername")
        host, port = peername[0], peername[1]
        return f"{host}:{port}"

    def start_prometheus_server(self, port=8000):
        # Start Prometheus HTTP server
        start_http_server(port)
        logger.info(f"Prometheus server started on port {port}")


class NodeServerTCP(NodeServerBase):

    async def handle_client_pre(self, reader, writer):
        data = await reader.read(READ_LIMIT)
        message = data.decode()
        logger.debug(
            f"Node received a request from {self.get_client_address(writer)}: '{message}'"
        )
        self.requests_counter.inc()
        self.bytes_received_counter.inc(len(data))
        return message

    async def handle_client_post(self, writer, response):
        if self.parser:
            response = self.parser(response)
        writer.write(response.encode())
        await writer.drain()
        writer.close()
        self.bytes_sent_counter.inc(len(response))

    async def handle_message(self, message):
        raise NotImplementedError("Subclasses must implement handle_message method")

    async def handle_client(self, reader, writer):
        start_time = time.perf_counter()

        message = await self.handle_client_pre(reader, writer)
        response = await self.handle_message(message)
        await self.handle_client_post(writer, response)

        self.response_time_histogram.observe(time.perf_counter() - start_time)

    async def start_server(self):
        server = await asyncio.start_server(self.handle_client, self.host, self.port)

        logger.info(f"Node server running on {server.sockets[0].getsockname()}")

        async with server:
            await server.serve_forever()

    def start(self):
        async def _start_default():
            await self.start_server()
            await self.start_prometheus_server(
                port=config["node_server"]["prometheus_port"]
            )

        asyncio.run(_start_default())


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
