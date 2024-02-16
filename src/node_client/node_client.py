#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-01-23
# version ='1.0'
# ---------------------------------------------------------------------------
"""a_short_module_description"""
# ---------------------------------------------------------------------------

import asyncio
import time
from prometheus_client import start_http_server, Counter, Histogram


class Node:
    def __init__(self, node_id, host, port):
        self.node_id = node_id
        self.host = host
        self.port = port


class NodeClient:
    def __init__(self, nodes, interval=5, timeout=5):
        self.nodes = nodes
        self.interval = interval
        self.timeout = timeout
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

    async def request_data(self, node):
        start_time = time.time()
        writer = None

        try:
            async with asyncio.timeout(self.timeout):
                reader, writer = await asyncio.open_connection(node.host, node.port)

            # Send a request to the node with a timeout
            writer.write(b"GET /data HTTP/1.1\r\nHost: example.com\r\n\r\n")
            await asyncio.wait_for(writer.drain(), timeout=self.timeout)

            # Read the response from the node with a timeout
            await asyncio.wait_for(reader.read(100), timeout=self.timeout)
        except asyncio.TimeoutError:
            print(f"Node {node.node_id} request timed out.")
            # Increment metrics for failed request
            self.failed_request_count.labels(node_id=node.node_id).inc()
        except Exception as e:
            print(f"Node {node.node_id} did not respond. Error: {e}")
            # Increment metrics for failed request
            self.failed_request_count.labels(node_id=node.node_id).inc()
        finally:
            if writer is not None:
                # Increment total request count and update duration metric
                self.request_count.labels(node_id=node.node_id).inc()
                duration = time.time() - start_time
                print(f"Node {node.node_id} request duration: {duration:.4f} seconds")

                # Record waiting time in the histogram
                self.waiting_time_histogram.labels(node_id=node.node_id).observe(
                    duration
                )

                writer.close()
                await writer.wait_closed()

    async def periodic_request(self):
        while not self.stop_event.is_set():
            tasks = [self.request_data(node) for node in self.nodes]
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


# Example usage:
if __name__ == "__main__":
    # Create a list of Node objects with TCP information (host and port)
    nodes_list = [
        Node(node_id=1, host="localhost", port=5001),
        Node(node_id=2, host="localhost", port=5002),
        # Add more nodes as needed
    ]

    # Create a NodeClient instance with a timeout of 3 seconds
    node_client = NodeClient(nodes_list, interval=1, timeout=3)

    # Start the NodeClient and Prometheus server
    node_client.start(prometheus_port=8000)

    # Let the program run for some time
    try:
        asyncio.run(asyncio.sleep(30))
    except KeyboardInterrupt:
        pass

    # Stop the NodeClient
    node_client.stop()
