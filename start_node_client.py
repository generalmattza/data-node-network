import asyncio
import logging

from data_node_network.node_client import Node, NodeClientTCP
from data_node_network.configuration import node_config


def main():

    node_list = [Node(node) for node in node_config.values()]
    # Create the client
    buffer = []
    # Create a client
    node_client: NodeClientTCP = NodeClientTCP(node_list, buffer=buffer)
    node_client.start(interval=0.1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
