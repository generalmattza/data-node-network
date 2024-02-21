from __future__ import annotations
import itertools
from dataclasses import dataclass
import logging

from prometheus_client import Counter

from data_node_network.configuration import (
    config_global,
    node_commands,
)

logger = logging.getLogger("data_node_network")


@dataclass
class CommandProcessor:
    command_menu: dict
    node: Node

    def __post_init__(self):
        self.total_commands = Counter(
            "total_commands",
            "Total commands received",
            ["node_id", "command", "node_type"],
        )
        self.invalid_commands = Counter(
            "invalid_commands",
            "Invalid commands received",
            ["node_id", "command", "node_type"],
        )

    def __call__(self, command):
        return getattr(self, self.parse_command(command))()

    def parse_command(self, command):
        if command not in self.command_menu:
            self.invalid_commands.labels(
                node_id=self.node.node_id,
                command=command,
                node_type=self.node.type,
            ).inc()
            return f"Invalid command {command}"
        self.total_commands.labels(
            node_id=self.node.node_id,
            command=command,
            node_type=self.node.type,
        ).inc()
        return self.command_menu[command]


class Node:
    _ids = itertools.count()

    def __init__(
        self,
        node_id=None,
        host="localhost",
        port=0,
        bucket=None,
        extra_tags=None,
        priority=None,
        node_type=None,
    ):
        self.node_id = node_id or self.get_id()
        self.host = host
        self.port = port
        self.address = (self.host, self.port)
        self.bucket = bucket
        self.extra_tags = extra_tags
        self.priotity = priority
        self.type = node_type
        try:
            self.command = CommandProcessor(node_commands[self.type])
        except AttributeError:
            logger.error(f"No commands are not defined for Node of type {self.type}")

    def get_id(self):
        return next(self._ids)

    def parse_command(self, command):
        return self.command.parse_command(command)
