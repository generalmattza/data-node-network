import asyncio
import logging

logger = logging.getLogger(__name__)


class NodeServerBase:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    async def handle_client(self, reader, writer):
        raise NotImplementedError("Subclasses must implement handle_client method")

    async def start_server(self):
        raise NotImplementedError("Subclasses must implement start_server method")


class NodeServerTCP(NodeServerBase):
    async def handle_client(self, reader, writer):
        data = await reader.read(100)
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


class NodeServerUDPProtocol:
    def __init__(self):
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        message = data.decode()

        # Assuming a simple response for demonstration purposes
        response = "Hello from the node!"

        # Send response back to the client
        self.transport.sendto(response.encode(), addr)

        logger.debug(f"Node received a request: {message}")


class NodeServerUDP(NodeServerBase):
    async def handle_client(self, data, addr):
        message = data.decode()

        # Assuming a simple response for demonstration purposes
        response = "Hello from the node!"

        # Send response back to the client
        self.transport.sendto(response.encode(), addr)

        logger.debug(f"Node received a request: {message}")

    async def start_server(self):
        loop = asyncio.get_running_loop()
        transport, protocol = await loop.create_datagram_endpoint(
            NodeServerUDPProtocol, local_addr=(self.host, self.port)
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
