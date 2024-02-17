#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-01-23
# version ='1.0'
# ---------------------------------------------------------------------------
"""a_short_project_description"""
# ---------------------------------------------------------------------------

import asyncio
import logging
from logging.config import dictConfig

from data_node_network.node_client import NodeClientTCP, NodeClientUDP, Node
from data_node_network.node_server import NodeServerTCP, NodeServerUDP


def setup_logging(filepath="config/logger.yaml"):
    import yaml
    from pathlib import Path

    if Path(filepath).exists():
        with open(filepath, "r") as stream:
            config = yaml.load(stream, Loader=yaml.FullLoader)
    else:
        raise FileNotFoundError
    logger = dictConfig(config)
    return logger


def create_nodes():
    nodes_list = [
        Node(node_id=1, host="localhost", port=5001),
        Node(node_id=2, host="localhost", port=5001),
        Node(node_id=3, host="localhost", port=5001),
        Node(node_id=4, host="localhost", port=5001),
        Node(node_id=5, host="localhost", port=5001),
        Node(node_id=6, host="localhost", port=5001),
        Node(node_id=7, host="localhost", port=5001),
        # Add more nodes as needed
    ]
    return nodes_list


def test_network(server=NodeServerTCP, client=NodeClientTCP):
    loop = asyncio.get_event_loop()

    # Create and start the server
    node_server = server("localhost", 5001)
    server_task = loop.create_task(node_server.start_server())

    # Create the client
    nodes_list = create_nodes()
    node_client = client(nodes_list, interval=1)

    try:
        # Start the client
        node_client_task = loop.create_task(node_client.periodic_request())

        # Run both server and client tasks concurrently
        loop.run_until_complete(asyncio.gather(server_task, node_client_task))
    except KeyboardInterrupt:
        pass
    finally:
        # Stop the NodeClient instance
        node_client.stop()


if __name__ == "__main__":
    setup_logging()
    # test_network()
    test_network(server=NodeServerUDP, client=NodeClientUDP)
