from data_node_network.client import Node
from conftest import TestNodeUDP, TestNodeClientUDP
import logging
import asyncio

logger = logging.getLogger(__name__)


def test_node_network(test_node, test_node_client):
    addresses = [("localhost", port) for port in range(50000, 50010)]
    loop = asyncio.get_event_loop()

    # Create and setup the server loop
    node_servers = [test_node(host=address[0], port=address[1]) for address in addresses]
    server_task = [
        loop.create_task(node_server.start_server()) for node_server in node_servers
    ]

    # Create a list of nodes to probe
    nodes_list = [
        Node(node_id=i, host=address[0], port=address[1]) for i, address in enumerate(addresses)
    ]
    buffer = []
    # Create a client
    node_client: TestNodeClientUDP = test_node_client(nodes_list, buffer=buffer)
    node_client.start_periodic_requests(message="getData", interval=1)


def test_ping(test_node, test_node_client):
    addresses = [("localhost", port) for port in range(50000, 50010)]
    loop = asyncio.get_event_loop()

    # Create and start the server
    # node_servers = [test_node(address=address) for address in addresses]
    # server_task = [loop.create_task(node_server.start_server()) for node_server in node_servers]

    # Create the client
    nodes_list = [
        Node(node_id=i, host=address[0], port=address[1]) for i, address in enumerate(addresses)
    ]
    buffer = []
    # Create a client
    node_client: TestNodeClientUDP = test_node_client(
        nodes_list, interval=1, buffer=buffer
    )
    pings = node_client.ping_nodes()

    # try:
    #     # Start the client
    #     node_client_task = loop.create_task(
    #         node_client.periodic_request(message="getData")
    #     )

    #     # Run both server and client tasks concurrently
    #     loop.run_until_complete(asyncio.gather(server_task, node_client_task))
    # except KeyboardInterrupt:
    #     pass
    # finally:
    #     # Stop the NodeClient instance
    #     node_client.stop()
