from data_node_network.node_client import Node
from conftest import TestNodeUDP, TestNodeClientUDP
import logging
import asyncio

logger = logging.getLogger(__name__)


def test_node_network(test_node, test_node_client):
    address = ("localhost", 50000)
    loop = asyncio.get_event_loop()

    # Create and start the server
    node_server = test_node(address=address)
    server_task = loop.create_task(node_server.start_server())

    # Create the client
    nodes_list = [Node(node_id=1, address=address)]
    buffer = []
    # Create a client
    node_client: TestNodeClientUDP = test_node_client(nodes_list, interval=0, buffer=buffer)
    node_client.start()

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
