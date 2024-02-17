import pytest
import logging

from data_node_network.node_client import NodeClientUDP
from data_node_network.node_server import NodeServerUDP, ProtocolUDP
import asyncio

logger = logging.getLogger(__name__)


def get_random_temperature():
    import random

    return random.uniform(20.0, 30.0)


class TestNodeProtocolUDP(ProtocolUDP):

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
        # Send response back to the client
        self.transport.sendto(response.encode(), addr)


class TestNode(NodeServerUDP):
    def __init__(self, address=("localhost", 1234)):
        super().__init__(address=address, protocol=TestNodeProtocolUDP)


class TestNodeClient(NodeClientUDP):

    

@pytest.fixture
def test_node():
    def _node_server(address):
        return TestNode(address)

    return _node_server
