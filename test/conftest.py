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
    def _create_nodes(address):
        address = address or ("localhost", 5001)
        host, port = address
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
    def __init__(self, address=("localhost", 0)):
        super().__init__(address=address, protocol=TestNodeProtocolUDP)


class TestNodeClientUDP(NodeClientUDP):
    pass


@pytest.fixture
def test_node():
    def _node_server(address):
        return TestNodeUDP(address)

    return _node_server


@pytest.fixture
def test_node_client():
    def _node_client(nodes, interval, buffer):
        return TestNodeClientUDP(nodes=nodes, interval=interval, buffer=buffer)

    return _node_client
