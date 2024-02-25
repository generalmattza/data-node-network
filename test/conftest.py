import pytest
import logging
import json
import time

from data_node_network.client import NodeClientUDP, Node
from data_node_network.server import NodeServerUDP, ServerProtocolUDP
import asyncio


def get_random_temperature():
    import random

    return random.uniform(20.0, 30.0)


@pytest.fixture
def create_nodes():
    def _create_nodes(host, port):
        host = host or "localhost"
        port = port or 0
        nodes_list = [
            Node(node_id=1, host=host, port=port, node_type="data-gatherer"),
            Node(node_id=2, host=host, port=port, node_type="data-gatherer"),
            Node(node_id=3, host=host, port=port, node_type="data-gatherer"),
            Node(node_id=4, host=host, port=port, node_type="data-gatherer"),
            Node(node_id=5, host=host, port=port, node_type="data-gatherer"),
            # Add more nodes as needed
        ]
        return nodes_list

    return _create_nodes


def handle_request(message):
    if message == "get_data":
        return {
            "measurement": "cpu_temperature",
            "fields": {
                "max": get_random_temperature(),
                "min": get_random_temperature(),
                "mean": get_random_temperature(),
            },
            "tags": {"host": "server01", "region": "us-west"},
        }
    elif message == "get_time":
        return {"request_time": time.time_ns()}


class TestNodeProtocolUDP(ServerProtocolUDP):

    def datagram_received(self, data, addr):
        super().datagram_received(data, addr)
        response = handle_request(data.decode())
        response = json.dumps(response)
        # Send response back to the client
        self.transport.sendto(response.encode(), addr)


class TestNodeUDP(NodeServerUDP):
    def __init__(self, host="localhost", port=0):

        command_menu = {
            "get_data": "get_data",
            "get_time": "get_time",
        }
        super().__init__(
            host=host,
            port=port,
            protocol=TestNodeProtocolUDP,
            command_menu=command_menu,
        )

    def get_data(self):
        return {
            "measurement": "cpu_temperature",
            "fields": {
                "max": get_random_temperature(),
                "min": get_random_temperature(),
                "mean": get_random_temperature(),
            },
            "tags": {"host": "server01", "region": "us-west"},
        }

    def get_time(self):
        return {"request_time": time.time_ns()}


class TestNodeClientUDP(NodeClientUDP):
    pass
