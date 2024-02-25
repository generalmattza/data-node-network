from data_node_network.client import Node
from conftest import TestNodeUDP, TestNodeClientUDP
import logging
import asyncio
import threading

logger = logging.getLogger(__name__)


def create_node_servers():
    addresses = [("localhost", port) for port in range(50000, 50010)]
    return [TestNodeUDP(host=address[0], port=address[1]) for address in addresses]


async def start_servers():
    server_tasks = [
        asyncio.create_task(node_server.start_server()) for node_server in node_servers
    ]
    return node_servers


def start_servers():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)


def spawn_and_run_servers():
    server_thread = threading.Thread(target=start_servers)
    server_thread.start()
    return server_thread


# def spawn_node_servers():

#     node_servers = create_node_servers()
#     # spawn the servers in a separate thread
#     server_tasks = [
#         asyncio.create_task(node_server.start_server()) for node_server in node_servers
#     ]

#     # create thread and start
#     def start_servers():
#         asyncio.run(asyncio.gather(*server_tasks))

#     server_thread = threading.Thread(target=start_servers)
#     return node_servers, server_thread


def test_node_network():

    server_tasks, node_servers = create_node_server_tasks()

    buffer = []
    # Create a client
    node_client = TestNodeClientUDP(nodes=node_servers, buffer=buffer)

    result = asyncio.gather(
        *server_tasks, node_client.periodic_request(message="get_data", interval=1)
    )
    print(result)


def test_ping():
    addresses = [("localhost", port) for port in range(50000, 50010)]
    loop = asyncio.get_event_loop()

    node_servers = create_node_servers()
    server_tasks = [
        loop.create_task(node_server.start_server()) for node_server in node_servers
    ]

    buffer = []
    # Create a client
    node_client = TestNodeClientUDP(node_servers, interval=1, buffer=buffer)
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
