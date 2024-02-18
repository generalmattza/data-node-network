import pytest
import logging
import json
import time
import random

import asyncio

from data_node_network.node_client import NodeClientTCP, Node
from data_node_network.node_data_gatherer import DataGathererNodeTCP

logger = logging.getLogger(__name__)

# class TestNodeProtocolUDP(ServerProtocolUDP):

#     def datagram_received(self, data, addr):
#         super().datagram_received(data, addr)
#         response = json.dumps(response)
#         # Send response back to the client
#         self.transport.sendto(response.encode(), addr)


# class TestNodeUDP(NodeServerUDP):
#     def __init__(self, address=("localhost", 0)):
#         super().__init__(address=address, protocol=TestNodeProtocolUDP)


# class TestNodeClientUDP(NodeClientUDP):
#     pass


class TestNodeTCP(DataGathererNodeTCP):
    pass


def create_node_network():
    number_of_nodes = 10
    start_port = 50000
    end_port = start_port + number_of_nodes
    addresses = [("localhost", port) for port in range(start_port, end_port)]
    node_ids = [i for i in range(number_of_nodes)]
    loop = asyncio.get_event_loop()

    # Create and start the server
    node_servers = [
        TestNodeTCP(address=address, node_id=id_)
        for address, id_ in zip(addresses, node_ids)
    ]
    server_task = [
        loop.create_task(node_server.start_server()) for node_server in node_servers
    ]

    # Create the client
    nodes_list = [
        Node(node_id=id_, address=address) for address, id_ in zip(addresses, node_ids)
    ]
    buffer = []
    # Create a client
    node_client: NodeClientTCP = NodeClientTCP(nodes_list, buffer=buffer)
    node_client.start_periodic_requests(message="get_data", interval=1)


# def test_ping():
#     addresses = [("localhost", port) for port in range(50000, 50010)]
#     loop = asyncio.get_event_loop()

#     # Create and start the server
#     # node_servers = [test_node(address=address) for address in addresses]
#     # server_task = [loop.create_task(node_server.start_server()) for node_server in node_servers]

#     # Create the client
#     nodes_list = [
#         Node(node_id=i, address=address) for i, address in enumerate(addresses)
#     ]
#     buffer = []
#     # Create a client
#     node_client: TestNodeClientUDP = TestNodeClientUDP(
#         nodes_list, interval=1, buffer=buffer
#     )
#     pings = node_client.ping_nodes()
#     return pings


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    create_node_network()
