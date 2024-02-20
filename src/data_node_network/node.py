from __future__ import annotations
import itertools
from dataclasses import dataclass
import logging

from data_node_network.configuration import (
    config_global,
    node_commands,
)

logger = logging.getLogger("data_node_network")


@dataclass
class NodeCommandProcessor:
    command_menu: dict
    node: Node

    def __call__(self, command):
        if command not in self.command_menu:
            return "Invalid command"
        return getattr(self, command)()

    def parse_message(self, message):
        if message not in self.command_menu:
            return f"Invalid command {message}"
        return self.command_menu["message"]


class Node:
    _ids = itertools.count()

    def __init__(self, config: dict):
        self.node_id = config.get("node_id", self.get_id())
        self.host = config["host"]
        self.port = config["port"]
        self.address = (self.host, self.port)
        self.bucket = config["bucket"]
        self.extra_tags = config["extra_tags"]
        self.priotity = config["priority"]
        self.type = config["type"]
        try:
            self.command_processor = NodeCommandProcessor(node_commands[self.type])
        except AttributeError:
            logger.error(f"Node commands are not defined for Node type {self.type}")

    def get_id(self):
        return next(self._ids)

    def parse_message(self, message):
        return self.command_processor.parse_message(message)
