import asyncio
import logging
from data_node_network.configuration import config_global

logger = logging.getLogger(__name__)
config = config_global["data_node_network"]


class NodeServerBase:
    def __init__(self, address):
        self.address = address
        self.host, self.port = address

    async def handle_client(self, reader, writer):
        raise NotImplementedError("Subclasses must implement handle_client method")

    async def start_server(self):
        raise NotImplementedError("Subclasses must implement start_server method")


class NodeServerTCP(NodeServerBase):
    async def handle_client(self, reader, writer):
        data = await reader.read(config["node_server"]["buffer_size"])
        message = data.decode()

        # Assuming a simple response for demonstration purposes
        response = "Hello from the node!"

        writer.write(response.encode())
        await writer.drain()

        logger.debug(f"Node received a request: {message}")
        writer.close()

    async def start_server(self):
        server = await asyncio.start_server(self.handle_client, self.host, self.port)

        logger.info(f"Node server running on {server.sockets[0].getsockname()}")

        async with server:
            await server.serve_forever()


class ProtocolUDP(asyncio.DatagramProtocol):
    def __init__(self):
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        message = data.decode()
        logger.debug(f"Node received a request from {addr}: {message}")


class EchoProtocolUDP(ProtocolUDP):

    def datagram_received(self, data, addr):
        super().datagram_received(data, addr)
        # Send response back to the client
        self.transport.sendto(data, addr)


class NodeServerUDP(NodeServerBase):

    def __init__(self, address: tuple[str, int], protocol=None):
        self.protocol = protocol or EchoProtocolUDP
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


# Example usage:
if __name__ == "__main__":
    # Start TCP server
    tcp_server = NodeServerTCP("localhost", 5002)
    asyncio.create_task(tcp_server.start_server())

    # Start UDP server
    udp_server = NodeServerUDP("localhost", 5003)
    asyncio.create_task(udp_server.start_server())

    try:
        asyncio.run(asyncio.sleep(3600))  # Run for an hour
    except KeyboardInterrupt:
        pass
