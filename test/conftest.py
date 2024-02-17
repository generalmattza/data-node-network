import pytest
import logging
import json

from data_node_network.node_client import NodeClientUDP, Node
from data_node_network.node_server import NodeServerUDP, ServerProtocolUDP
import asyncio


def get_random_temperature():
    import random

    return random.uniform(20.0, 30.0)


@pytest.fixture
def create_nodes():
    def _create_nodes(address):
        address = address or ("localhost", 5001)
        nodes_list = [
            Node(node_id=1, address=address),
            Node(node_id=2, address=address),
            Node(node_id=3, address=address),
            Node(node_id=4, address=address),
            Node(node_id=5, address=address),
            Node(node_id=6, address=address),
            Node(node_id=7, address=address),
            # Add more nodes as needed
        ]
        return nodes_list

    return _create_nodes


class TestNodeProtocolUDP(ServerProtocolUDP):
    def datagram_received(self, data, addr):
        super().datagram_received(data, addr)
        self.transport.sendto(data, addr)
        
    def datagram_received(self, data, addr):
        super().datagram_received(data, addr)
        response = {
            "measurement": "cpu_temperature",
            "fields": {
                "max": get_random_temperature(),
                "min": get_random_temperature(),
                "mean": get_random_temperature(),
            },
            "tags": {"host": "server01", "region": "us-west"},
        }
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
