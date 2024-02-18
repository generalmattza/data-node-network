from dataclasses import dataclass
import logging
import time
import random

import asyncio

from data_node_network.node_server import (
    NodeServerTCP,
    NodeServerUDP,
)
from data_node_network.configuration import (
    config_global,
    node_commands,
)

logger = logging.getLogger(__name__)
config = config_global["node_network"]
READ_LIMIT = config_global["node_network"]["read_limit"]


def get_random_temperature():
    return random.uniform(20.0, 30.0)


@dataclass
class DataGathererCommandMenu:
    commands_menu: dict
    node_id: int

    def get_data(self):
        return {
            "measurement": "cpu_temperature",
            "fields": {
                "max": get_random_temperature(),
                "min": get_random_temperature(),
                "mean": get_random_temperature(),
            },
            "tags": {"host": "server01", "region": "us-west"},
        }

    def get_time(self):
        return {f"node-{self.node_id}_time": time.time()}

    def get_status(self):
        return "Node is running"

    def get_config(self):
        return "Node configuration"

    def get_node_info(self):
        return "Node information"

    def get_node_status(self):
        return "Node status"

    def start_logging(self):
        return "Logging started"

    def stop_logging(self):
        return "Logging stopped"

    def reset_node(self):
        return "Node reset"

    def get_file(self):
        return "File"

    def get_file_list(self):
        return "File list"

    def __call__(self, command):
        if command not in self.commands_menu:
            return "Invalid command"
        return getattr(self, command)()


class DataGathererNodeTCP(NodeServerTCP):
    def __init__(self, address, node_id):
        super().__init__(address=address, node_id=node_id)
        self.command_menu = DataGathererCommandMenu(
            commands_menu=node_commands["data-gatherer"], node_id=node_id
        )

    async def handle_message(self, message):
        response = self.command_menu(message)
        return self.parser(response)

    async def start_server(self):
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        logger.info(f"Node server running on {server.sockets[0].getsockname()}")
        self.start_prometheus_server(port=config["node_server"]["prometheus_port"])

        async with server:
            await server.serve_forever()
