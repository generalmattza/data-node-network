import asyncio
import time
from prometheus_client import start_http_server, Counter, Histogram
import logging
import json

from data_node_network.configuration import config_global

logger = logging.getLogger(__name__)
config = config_global["data_node_network"]


def convert_bytes_to_human_readable(num: float) -> str:
    """Convert bytes to a human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if num < 1024.0:
            return f"{num:.2f} {unit}"
        num /= 1024.0
    return f"{num:.2f} {unit}"

class Node:
    def __init__(self, node_id, address):
        self.node_id = node_id
        self.address = address
        self.host, self.port = address


class NodeClient:
    def __init__(self, nodes, interval=5, buffer=None, parser=None):
        self.nodes = nodes
        self.interval = interval
        self.stop_event = asyncio.Event()
        self.buffer = buffer or []
        self.parser = json.loads if parser is None else parser

        # Prometheus metrics
        self.request_count = Counter(
            "node_client_requests_total",
            "Total number of requests made by NodeClient",
            labelnames=["node_id"],
        )
        self.successful_request_count = Counter(
            "node_client_successful_requests_total",
            "Total number of successful requests made by NodeClient",
            labelnames=["node_id"],
        )
        self.failed_request_count = Counter(
            "node_client_failed_requests_total",
            "Total number of failed requests made by NodeClient",
            labelnames=["node_id"],
        )
        self.bytes_received_count = Counter(
            "node_client_bytes_received_total",
            "Total number of bytes received by NodeClient",
            labelnames=["node_id"],
        )
        self.waiting_time_histogram = Histogram(
            "node_client_waiting_time_seconds",
            "Histogram of time spent waiting for the node",
            labelnames=["node_id"],
        )

    async def request(self, node, message=""):
        raise NotImplementedError("Subclasses must implement request method")

    async def periodic_request(self, message=""):
        while not self.stop_event.is_set():
            tasks = [self.request(node, message=message) for node in self.nodes]
            await asyncio.gather(*tasks)
            await asyncio.sleep(self.interval)

    def start(self, prometheus_port=8000):
        # Start Prometheus HTTP server
        start_http_server(prometheus_port)
        loop = asyncio.get_event_loop()
        loop.create_task(self.periodic_request("getData"))

        try:
            loop.run_forever()
        finally:
            loop.close()

    def stop(self):
        self.stop_event.set()


class NodeClientTCP(NodeClient):
    async def request(self, node, message=""):
        start_time = time.time()
        writer = None

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(*node.address), timeout=5
            )

            # Send a request to the node with a timeout
            writer.write(message.encode())
            await writer.drain()

            # Read the response from the node with a timeout
            await reader.read(config["node_client"]["buffer_size"])
        except asyncio.TimeoutError:
            logger.warning(f"Node {node.node_id} request timed out.")
            # Increment metrics for failed request
            self.failed_request_count.labels(node_id=node.node_id).inc()
        except Exception as e:
            logger.warning(f"Node {node.node_id} did not respond. Error: {e}")
            # Increment metrics for failed request
            self.failed_request_count.labels(node_id=node.node_id).inc()
        finally:
            if writer is not None:
                # Increment total request count and update duration metric
                self.request_count.labels(node_id=node.node_id).inc()
                duration = time.time() - start_time
                logger.info(
                    f"Node {node.node_id} request duration: {duration:.4f} seconds"
                )

                # Record waiting time in the histogram
                self.waiting_time_histogram.labels(node_id=node.node_id).observe(
                    duration
                )

                writer.close()
                await writer.wait_closed()


class ClientProtocolUDP(asyncio.DatagramProtocol):
    def __init__(self, on_con_lost_future):
        self.on_con_lost_future = on_con_lost_future
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        self.server_address_str = f"{self.transport.get_extra_info('peername')[0]}:{self.transport.get_extra_info('peername')[1]}"
        logger.debug(f"Connected to server at {self.server_address_str}")

    def datagram_received(self, data, addr):
        print(f"Received {convert_bytes_to_human_readable(len(data))} data from {self.server_address_str}")
        logger.debug("Closing the socket")
        self.transport.close()

    def error_received(self, exc):
        logger.error(exc)

    def connection_lost(self, exc):
        logger.debug(f"Connection closed: {exc}")
        self.on_con_lost_future.set_result(True)


# Define a simple DatagramProtocol to request and queue incoming data
class NodeClientProtocolUDP(ClientProtocolUDP):
    def __init__(
        self,
        on_con_lost_future,
        data_received_callback,
        message="",
    ):
        self.message = message
        self.data_received_callback = data_received_callback
        super().__init__(on_con_lost_future)

    def connection_made(self, transport):
        super().connection_made(transport)
        logger.debug(f"Sending message {self.message} to {self.server_address_str}")
        self.transport.sendto(self.message.encode())

    def datagram_received(self, data, addr):
        super().datagram_received(data, addr)
        # Resolve the future to indicate that the data has been received
        self.data_received_callback(data.decode())


class NodeClientUDP(NodeClient):
    async def request(self, node, message=""):
        start_time = time.time()

        try:
            loop = asyncio.get_running_loop()

            # Create a future to signal when data is received
            data_received_future = loop.create_future()
            on_con_lost = loop.create_future()  # not currently used

            
            def data_received_callback(data):
                self.buffer.append(self.parser(data))
                data_received_future.set_result(True)
                self.bytes_received_count.labels(node_id=node.node_id).inc(len(data))
                
            # Create a DatagramProtocol instance with the future
            udp_protocol_factory = lambda: NodeClientProtocolUDP(
                on_con_lost, data_received_callback, message=message
            )

            # Create a UDP connection
            transport, _ = await loop.create_datagram_endpoint(
                udp_protocol_factory, remote_addr=node.address
            )

            # Wait for data to be received or timeout
            await asyncio.wait_for(data_received_future, timeout=5)
        except asyncio.TimeoutError:
            logger.warning(f"Node {node.node_id} request timed out.")
            # Increment metrics for failed request
            self.failed_request_count.labels(node_id=node.node_id).inc()
        except Exception as e:
            logger.warning(f"Node {node.node_id} did not respond. Error: {e}")
            # Increment metrics for failed request
            self.failed_request_count.labels(node_id=node.node_id).inc()
        finally:
            # Increment total request count and update duration metric
            self.request_count.labels(node_id=node.node_id).inc()
            duration = time.time() - start_time
            logger.info(f"Node {node.node_id} request duration: {duration:.4f} seconds")

            # Record waiting time in the histogram
            self.waiting_time_histogram.labels(node_id=node.node_id).observe(duration)

            if transport is not None:
                transport.close()


# Example usage:
if __name__ == "__main__":
    # Create a list of Node objects with TCP information (host and port)
    nodes_list = [
        Node(node_id=1, address=("localhost", 5001)),
        Node(node_id=2, address=("localhost", 5002)),
        # Add more nodes as needed
    ]

    # Create a NodeClientTCP instance with a timeout of 3 seconds
    node_client_tcp = NodeClientTCP(nodes_list, interval=1)

    # Start the NodeClientTCP and Prometheus server
    node_client_tcp.start(prometheus_port=8000)

    # Create a NodeClientUDP instance with a timeout of 3 seconds
    node_client_udp = NodeClientUDP(nodes_list, interval=1)

    # Start the NodeClientUDP and Prometheus server
    node_client_udp.start(prometheus_port=8001)

    # Let the program run for some time
    try:
        asyncio.run(asyncio.sleep(30))
    except KeyboardInterrupt:
        pass

    # Stop the NodeClient instances
    node_client_tcp.stop()
    node_client_udp.stop()
