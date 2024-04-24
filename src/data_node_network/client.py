#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------
"""A module for the NodeClient class and its subclasses."""
# ---------------------------------------------------------------------------

import asyncio
import time
from typing import Any, Union
from prometheus_client import start_http_server, Counter, Histogram, Gauge
import logging
import json

from data_node_network.configuration import config_global
from data_node_network.node import Node


logger = logging.getLogger("data_node_network.client")

config_local = config_global["node_network"]["client"]

READ_LIMIT = config_global["node_network"]["read_limit"]


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


class NodeClient:

    def __init__(
        self,
        nodes: Union[dict, list[Node]],
        buffer=None,
        parser=None,
        timeout=None,
        config=None,
    ):
        if isinstance(nodes, dict):
            nodes = [Node(**node_config) for node_config in nodes.values()]
        self.nodes = nodes
        self.stop_event = asyncio.Event()
        self.buffer = asyncio.Queue() if buffer is None else buffer
        self.parser = json.loads if parser is None else parser
        self.timeout = timeout or config_local["timeout"]
        self.config = config or config_local

        # Prometheus metrics
        self.request_count = Counter(
            "requests_total",
            "Total number of requests made by NodeClient",
            labelnames=["node_id", "node_name", "node_type"],
        )
        self.successful_request_count = Counter(
            "successful_requests_total",
            "Total number of successful requests made by NodeClient",
            labelnames=["node_id", "node_name", "node_type"],
        )
        self.failed_request_count = Counter(
            "failed_requests_total",
            "Total number of failed requests made by NodeClient",
            labelnames=["node_id", "node_name", "node_type"],
        )
        self.bytes_received_count = Counter(
            "bytes_received_total",
            "Total number of bytes received by NodeClient",
            labelnames=["node_id", "node_name", "node_type"],
        )
        self.bytes_sent_count = Counter(
            "bytes_sent_total",
            "Total number of bytes sent by NodeClient",
            labelnames=["node_id", "node_name", "node_type"],
        )
        self.request_duration_histogram = Histogram(
            "request_duration_seconds",
            "Histogram of response time to query the node",
            labelnames=["node_id", "node_name", "node_type"],
        )
        self.buffer_length = Gauge(
            "buffer_length",
            "Length of the buffer in NodeClient",
        )

    async def request(self, node, message="") -> Any:
        raise NotImplementedError("Subclasses must implement request method")

    async def mass_request(self, nodes=None, message=""):
        nodes = nodes or self.nodes
        tasks = [self.request(node, message=message) for node in nodes]
        results = await asyncio.gather(*tasks)
        return results

    async def periodic_request(self, nodes=None, message="", interval=None):
        interval = interval or self.config["update_interval"]
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

    def get_node_info(self, nodes=None):
        async def _get_node_info():
            return await self.mass_request(nodes=nodes, message="get_node_info")

        return asyncio.run(_get_node_info())

    def start(self, message="get_data", interval=None):

        if self.config["enable_prometheus_server"]:
            self.start_prometheus_server(port=self.config["prometheus_port"])

        async def _start_default():
            await self.periodic_request(message=message, interval=interval)

        asyncio.run(_start_default())

    def stop(self):
        self.stop_event.set()

    async def update_metrics(self):
        # Record the buffer length
        self.buffer_length.set(len(self.buffer))

    def start_prometheus_server(self, port=8000):
        # Start Prometheus HTTP server
        start_http_server(port)
        logger.info(f"Prometheus server started on port {port}")


class NodeClientTCP(NodeClient):
    async def request(self, node, message=""):
        start_time = time.perf_counter()
        writer = None
        result = None

        message = node.parse_command(message)

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(*node.address), timeout=self.timeout
            )

            # Send a request to the node with a timeout
            self.bytes_sent_count.labels(
                node_id=node.node_id, node_name=node.name, node_type=node.node_type
            ).inc(len(message))
            writer.write(message.encode())
            await writer.drain()

            # Read the response from the node with a timeout
            result = await reader.read(READ_LIMIT)
        except asyncio.TimeoutError:
            logger.warning(f"{str(node)} request timed out.")
            # Increment metrics for failed request
            self.failed_request_count.labels(
                node_id=node.node_id, node_name=node.name, node_type=node.node_type
            ).inc()
        except Exception as e:
            logger.warning(f"{str(node)} did not respond. Error: {e}")
            # Increment metrics for failed request
            self.failed_request_count.labels(
                node_id=node.node_id, node_name=node.name, node_type=node.node_type
            ).inc()
        finally:
            if writer is not None:
                duration = time.perf_counter() - start_time
                # Increment total request count and update duration metric
                self.request_count.labels(
                    node_id=node.node_id, node_name=node.name, node_type=node.node_type
                ).inc()
                logger.info(f"{str(node)} request duration: {duration:.4f} seconds")
                # Record waiting time in the histogram
                self.request_duration_histogram.labels(
                    node_id=node.node_id, node_name=node.name, node_type=node.node_type
                ).observe(duration)
                writer.close()
                try:
                    # Still need to test this
                    await writer.wait_closed()
                except ConnectionResetError as e:
                    logger.warning(e)

        if result:
            self.bytes_received_count.labels(
                node_id=node.node_id, node_name=node.name, node_type=node.node_type
            ).inc(len(result))
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

        message = node.parse_command(message)

        try:
            loop = asyncio.get_running_loop()

            # Create a future to signal when data is received
            data_received_future = loop.create_future()
            on_con_lost = loop.create_future()

            def data_received_callback(data):
                nonlocal result
                result = data
                data_received_future.set_result(True)
                self.bytes_received_count.labels(
                    node_id=node.node_id, node_name=node.name, node_type=node.node_type
                ).inc(len(data))

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
            logger.warning(f"{str(node)} request timed out")
            # Increment metrics for failed request
            self.failed_request_count.labels(
                node_id=node.node_id, node_name=node.name, node_type=node.node_type
            ).inc()
        except Exception as e:
            logger.warning(f"{str(node)} did not respond. Error: {e}")
            # Increment metrics for failed request
            self.failed_request_count.labels(
                node_id=node.node_id, node_name=node.name, node_type=node.node_type
            ).inc()
        finally:
            if transport is not None:
                transport.close()
            if result:
                duration = time.time() - start_time
                # Increment total request count and update duration metric
                logger.info(f"{str(node)} request duration: {duration:.4f} seconds")
                self.request_count.labels(
                    node_id=node.node_id, node_name=node.name, node_type=node.node_type
                ).inc()
                # Record waiting time in the histogram
                self.request_duration_histogram.labels(
                    node_id=node.node_id, node_name=node.name, node_type=node.node_type
                ).observe(duration)
                self.bytes_received_count.labels(
                    node_id=node.node_id, node_name=node.name, node_type=node.node_type
                ).inc(len(result))
                if self.parser:
                    result = self.parser(result)
        return result
