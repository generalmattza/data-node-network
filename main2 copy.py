import pytest
import logging
import json
import time
import random

from data_node_network.node_client import NodeClientUDP, Node
from data_node_network.node_server import NodeServerUDP, ServerProtocolUDP
import asyncio

logger = logging.getLogger(__name__)
def get_random_temperature():
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

def handle_request(message):
    if message == "getData":
        return {
            "measurement": "cpu_temperature",
            "fields": {
                "max": get_random_temperature(),
                "min": get_random_temperature(),
                "mean": get_random_temperature(),
            },
            "tags": {"host": "server01", "region": "us-west"},
        }
    elif message == "getTime":
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


def create_node_network():
    addresses = [("localhost", port) for port in range(50000, 50010)]
    loop = asyncio.get_event_loop()

    # Create and start the server
    node_servers = [TestNodeUDP(address=address) for address in addresses]
    server_task = [loop.create_task(node_server.start_server()) for node_server in node_servers]

    # Create the client
    nodes_list = [Node(node_id=i, address=address) for i, address in enumerate(addresses)]
    buffer = []
    # Create a client
    node_client: TestNodeClientUDP = TestNodeClientUDP(nodes_list, interval=1, buffer=buffer)
    node_client.start()
    
    
def test_ping():
    addresses = [("localhost", port) for port in range(50000, 50010)]
    loop = asyncio.get_event_loop()

    # Create and start the server
    # node_servers = [test_node(address=address) for address in addresses]
    # server_task = [loop.create_task(node_server.start_server()) for node_server in node_servers]

    # Create the client
    nodes_list = [Node(node_id=i, address=address) for i, address in enumerate(addresses)]
    buffer = []
    # Create a client
    node_client: TestNodeClientUDP = TestNodeClientUDP(nodes_list, interval=1, buffer=buffer)
    pings = node_client.ping_nodes()
    return pings
    
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    pings = test_ping()
    print(pings)