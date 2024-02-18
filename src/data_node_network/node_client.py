import asyncio
import time
from typing import Any
from prometheus_client import start_http_server, Counter, Histogram, Gauge
import logging
import json

from data_node_network.configuration import config_global

logger = logging.getLogger(__name__)
config = config_global["node_network"]

READ_LIMIT = config["read_limit"]


def convert_bytes_to_human_readable(num: float) -> str:
    """Convert bytes to a human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if num < 1024.0:
            return f"{num:.2f} {unit}"
        num /= 1024.0
    return f"{num:.2f} {unit}"


def convert_ns_to_human_readable(nanoseconds):
    # Define conversion factors
    micro_factor = 1e-3
    milli_factor = 1e-6
    second_factor = 1e-9

    if nanoseconds < 1000:
        return f"{nanoseconds} ns"
    elif nanoseconds < 1e6:
        return f"{nanoseconds * micro_factor:.2f} μs"
    elif nanoseconds < 1e9:
        return f"{nanoseconds * milli_factor:.2f} ms"
    else:
        return f"{nanoseconds * second_factor:.2f} s"


async def wait_for_any(futures, timeout):
    # Ensure that all elements in the iterable are instances of asyncio.Future
    if any(not isinstance(future, asyncio.Future) for future in futures):
        raise ValueError(
            "All elements in the iterable must be asyncio.Future instances"
        )

    # Create tasks for each future
    tasks = [asyncio.ensure_future(future) for future in futures]

    # Use asyncio.wait to wait for any of the futures to complete
    done, pending = await asyncio.wait(
        tasks, return_when=asyncio.FIRST_COMPLETED, timeout=timeout
    )

    # Cancel the pending tasks to clean up resources
    for task in pending:
        task.cancel()

    # Return the result of the completed task or raise a TimeoutError if none completed in time
    if done:
        return done.pop().result()
    else:
        raise asyncio.TimeoutError(f"Timeout of {timeout} seconds reached")


class Node:
    def __init__(self, node_id, address):
        self.node_id = node_id
        self.address = address
        self.host, self.port = address


class NodeClient:
    def __init__(self, nodes, buffer=None, parser=None, timeout=10):
        self.nodes = nodes
        self.stop_event = asyncio.Event()
        self.buffer = [] if buffer is None else buffer
        self.data_queue = asyncio.Queue()
        self.parser = json.loads if parser is None else parser
        self.timeout = timeout

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
        self.response_time_histogram = Histogram(
            "node_client_response_time_seconds",
            "Histogram of response time to query the node",
            labelnames=["node_id"],
        )
        self.buffer_length = Gauge(
            "node_client_buffer_length",
            "Length of the buffer in NodeClient",
        )

    async def request(self, node, message="") -> Any:
        raise NotImplementedError("Subclasses must implement request method")

    async def mass_request(self, nodes=None, message=""):
        nodes = nodes or self.nodes
        tasks = [self.request(node, message=message) for node in nodes]
        results = await asyncio.gather(*tasks)
        return results

    async def periodic_request(self, nodes=None, message="", interval=10):
        while not self.stop_event.is_set():
            results = await self.mass_request(message=message, nodes=nodes)
            if results:
                self.buffer.extend(results)
            await self.update_metrics()
            await asyncio.sleep(interval)

    def ping_nodes(self):

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        current_time = time.time_ns()
        try:
            node_times = loop.run_until_complete(self.mass_request(message="get_time"))
        finally:
            loop.close()
        ping_ns = [node_time["request_time"] - current_time for node_time in node_times]
        ping_h = [convert_ns_to_human_readable(ping) for ping in ping_ns]
        return {node.node_id: ping for node, ping in zip(self.nodes, ping_h)}

    def start_periodic_requests(
        self, prometheus_port=8000, message="get_data", interval=None
    ):
        # Start Prometheus HTTP server
        start_http_server(prometheus_port)
        loop = asyncio.get_event_loop()
        loop.create_task(self.periodic_request(message=message, interval=interval))

        try:
            loop.run_forever()
        finally:
            loop.close()

    def stop(self):
        self.stop_event.set()

    async def update_metrics(self):
        # Record the buffer length
        self.buffer_length.set(len(self.buffer))


class NodeClientTCP(NodeClient):
    async def request(self, node, message=""):
        start_time = time.perf_counter()
        writer = None
        result = None

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(*node.address), timeout=self.timeout
            )

            # Send a request to the node with a timeout
            writer.write(message.encode())
            await writer.drain()

            # Read the response from the node with a timeout
            result = await reader.read(READ_LIMIT)
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
                duration = time.perf_counter() - start_time
                # Increment total request count and update duration metric
                self.request_count.labels(node_id=node.node_id).inc()
                logger.info(
                    f"Node {node.node_id} request duration: {duration:.4f} seconds"
                )
                # Record waiting time in the histogram
                self.response_time_histogram.labels(node_id=node.node_id).observe(
                    duration
                )
                writer.close()
                await writer.wait_closed()

        if result:
            self.bytes_received_count.labels(node_id=node.node_id).inc(len(result))
            if self.parser:
                return self.parser(result)
        return result


class ClientProtocolUDP(asyncio.DatagramProtocol):
    def __init__(self, on_con_lost_future):
        self.on_con_lost_future = on_con_lost_future
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        peername = transport.get_extra_info("peername")
        host, port = peername[0], peername[1]
        self.server_address_str = f"{host}:{port}"
        logger.debug(f"Connected to server at {self.server_address_str}")

    def datagram_received(self, data, addr):
        logger.info(
            f"Received {convert_bytes_to_human_readable(len(data))} data from {self.server_address_str}"
        )
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
        logger.debug(f"Sending message '{self.message}' to {self.server_address_str}")
        self.transport.sendto(self.message.encode())

    def datagram_received(self, data, addr):
        super().datagram_received(data, addr)
        # Resolve the future to indicate that the data has been received
        self.data_received_callback(data.decode())


class NodeClientUDP(NodeClient):
    async def request(self, node, message=""):
        start_time = time.time()
        result = None

        try:
            loop = asyncio.get_running_loop()

            # Create a future to signal when data is received
            data_received_future = loop.create_future()
            on_con_lost = loop.create_future()  # not currently used

            def data_received_callback(data):
                nonlocal result
                result = data
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
            # await asyncio.wait_for(data_received_future, timeout=5)
            await wait_for_any(
                [data_received_future, on_con_lost], timeout=self.timeout
            )
        except asyncio.TimeoutError:
            logger.warning(f"Node {node.node_id} request timed out.")
            # Increment metrics for failed request
            self.failed_request_count.labels(node_id=node.node_id).inc()
        except Exception as e:
            logger.warning(f"Node {node.node_id} did not respond. Error: {e}")
            # Increment metrics for failed request
            self.failed_request_count.labels(node_id=node.node_id).inc()
        finally:
            duration = time.time() - start_time
            # Increment total request count and update duration metric
            logger.info(f"Node {node.node_id} request duration: {duration:.4f} seconds")
            self.request_count.labels(node_id=node.node_id).inc()
            # Record waiting time in the histogram
            self.response_time_histogram.labels(node_id=node.node_id).observe(duration)

            if transport is not None:
                transport.close()

        if result:
            self.bytes_received_count.labels(node_id=node.node_id).inc(len(result))
            if self.parser:
                return self.parser(result)
        return result
