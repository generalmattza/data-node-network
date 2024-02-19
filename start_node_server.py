#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-02-19
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------
"""Example code to start a data gatherer node server."""
# ---------------------------------------------------------------------------

from logging.config import dictConfig
from dotenv import load_dotenv
import os

from data_node_network.node_data_gatherer import GathererNodeTCP

load_dotenv()
HOSTNAME = os.getenv('HOSTNAME')
PORT = int(os.getenv('PORT'))

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

def main():
    address = (HOSTNAME, PORT)

    # Create and start the server
    node_server = GathererNodeTCP(address=address)
    node_server.start()

if __name__ == "__main__":
    
    setup_logging()
    main()
