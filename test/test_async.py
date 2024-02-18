import pytest
import asyncio

# Import your Node and NodeClient classes
from data_node_network.node_client import Node, NodeClientUDP
from conftest import TestNodeUDP

# Define a fixture to create and start the server
@pytest.fixture
async def start_server():
    addresses = [("localhost", port) for port in range(50000, 51000)]
    loop = asyncio.get_event_loop()

    # Create and start the server
    node_servers = [TestNodeUDP(address=address) for address in addresses]
    server_tasks = [loop.create_task(node_server.start_server()) for node_server in node_servers]
    await asyncio.gather(*server_tasks)

    yield addresses

    # Stop the servers after the test
    stop_tasks = [loop.create_task(node_server.stop_server()) for node_server in node_servers]
    await asyncio.gather(*stop_tasks)

# Define a fixture to create the client
@pytest.fixture
def create_client(request, start_server):
    addresses = request.getfixturevalue('start_server')
    nodes_list = [Node(node_id=i, address=address) for i, address in enumerate(addresses)]
    buffer = []
    # Create a client
    node_client = NodeClientUDP(nodes_list, interval=1, buffer=buffer)
    return node_client

# Define a test using the fixtures
@pytest.mark.asyncio
async def test_mass_request(start_server, create_client):
    # Arrange
    # No need to call create_client directly as it's automatically injected due to the fixture dependency

    # Act
    results = await create_client.mass_request(message="getTime")

    # Assert
    assert results is not None
    # Add more assertions as needed