from __future__ import annotations
import itertools
from dataclasses import dataclass
import logging

from prometheus_client import Counter

from data_node_network.configuration import (
    config_global,
    node_commands,
)

logger = logging.getLogger("data_node_network.nodes")

class Node:
    _ids = itertools.count()
    node_total_commands = Counter(
        "total_commands",
        "Total commands received",
        labelnames=("node_id", "command", "node_type"),
    )
    node_invalid_commands = Counter(
        "invalid_commands",
        "Invalid commands received",
        labelnames=("node_id", "command", "node_type"),
    )

    def __init__(
        self,
        name=None,
        node_id=None,
        host="localhost",
        port=0,
        bucket=None,
        extra_tags=None,
        priority=None,
        node_type=None,
    ):
        self.name = name
        self.node_id = node_id or self.get_id()
        self.host = host
        self.port = port
        self.address = (self.host, self.port)
        self.bucket = bucket
        self.extra_tags = extra_tags
        self.priotity = priority
        self.node_type = node_type
        try:
            self.command_menu = node_commands[self.node_type]
        except KeyError:
            logger.warning(f"No commands are not defined for Node of type {self.node_type}")

    def command(self, command):
        return getattr(self, self.parse_command(command))()

    def parse_command(self, command):
        if command not in self.command_menu:
            # update prometheus metric
            self.node_invalid_commands.labels(
                node_id=self.node_id,
                command=command,
                node_type=self.node_type,
            ).inc()
            return f"Invalid command {command}"
        # update prometheus metric
        self.node_total_commands.labels(
            node_id=self.node_id,
            command=command,
            node_type=self.node_type,
        ).inc()
        return self.command_menu[command]

    def get_id(self):
        return next(self._ids)
