import asyncio
import time
from prometheus_client import start_http_server, Counter, Histogram
import logging

from data_node_network.configuration import config_global

logger = logging.getLogger(__name__)
config = config_global["data_node_network"]


class Node:
    def __init__(self, node_id, address):
        self.node_id = node_id
        self.address = address
        self.host, self.port = address


class NodeClient:
    def __init__(self, nodes, interval=5):
        self.nodes = nodes
        self.interval = interval
        self.stop_event = asyncio.Event()

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
        loop.create_task(self.periodic_request())

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


class NodeClientUDP(NodeClient):
    async def request(self, node, message=""):
        start_time = time.time()

        try:
            loop = asyncio.get_running_loop()

            # Define a simple DatagramProtocol to handle incoming data
            class UDPProtocol(asyncio.DatagramProtocol):
                def __init__(self, future):
                    self.future = future

                def datagram_received(self, data, addr):
                    # Process the received data (you can customize this part)
                    # For now, just print the received data
                    print(f"Received data from {node.node_id}: {data.decode()}")

                    # Resolve the future to indicate that the data has been received
                    self.future.set_result(True)

            # Create a future to signal when data is received
            data_received_future = loop.create_future()

            # Create a DatagramProtocol instance with the future
            udp_protocol = UDPProtocol(data_received_future)

            # Create a UDP connection
            transport, _ = await loop.create_datagram_endpoint(
                lambda: udp_protocol, remote_addr=node.address
            )

            # Send a request to the node
            transport.sendto(message.encode())

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
