#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-02-19
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------
"""Example code to start a client node to gather data from other nodes in the network."""
# ---------------------------------------------------------------------------

from logging.config import dictConfig

from data_node_network.client import NodeClientTCP
from data_node_network.node import Node
from data_node_network.configuration import node_config


def setup_logging(filepath="config/logger.yaml"):
    import yaml
    from pathlib import Path

    if Path(filepath).exists():
        with open(filepath, "r") as stream:
            config = yaml.load(stream, Loader=yaml.FullLoader)
    else:
        raise FileNotFoundError
    # create folder for log file
    Path("logs").mkdir(parents=True, exist_ok=True)
    logger = dictConfig(config)
    return logger


def main():

    node_list = [Node(**node) for node in node_config.values()]
    # Create the client
    buffer = []
    # Create a client
    node_client: NodeClientTCP = NodeClientTCP(node_list, buffer=buffer)
    node_client.start(interval=0.1)


if __name__ == "__main__":
    setup_logging()
    main()
