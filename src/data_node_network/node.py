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


class NodeError(Exception):
    def __init__(self, message):
        self.message = message
        logger.error(self.message)
        super().__init__(self.message)


class Node:
    _ids = itertools.count()
    node_commands_total = Counter(
        "node_commands_total",
        "Total commands executed|invalid",
        labelnames=("node_id", "command", "outcome", "node_type"),
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
        command_menu=None,
    ):
        self.name = name
        self.node_id = node_id or self.get_id()
        self.host = host
        self.port = port
        self.address = (self.host, self.port)
        self.bucket = bucket
        self.extra_tags = extra_tags
        self.priority = priority
        self.node_type = node_type
        if command_menu is not None:
            self.command_menu = command_menu
        else:
            try:
                self.command_menu = node_commands[self.node_type]
            except KeyError:
                logger.warning(
                    f"No commands are not defined for Node of type {self.node_type}"
                )

    def command(self, command):
        return getattr(self, self.parse_command(command))()

    def parse_command(self, command):
        if not hasattr(self, "command_menu"):
            raise NodeError(
                f"No commands have been defined for node type {self.node_type}. Command {command} failed."
            )
        if command in self.command_menu:
            # update prometheus metric
            self.node_commands_total.labels(
                node_id=self.node_id,
                command=command,
                outcome="executed",
                node_type=self.node_type,
            ).inc()
            return self.command_menu[command]
        # Else command is invalid
        # update prometheus metric
        self.node_commands_total.labels(
            node_id=self.node_id,
            command=command,
            outcome="invalid",
            node_type=self.node_type,
        ).inc()
        return f"Invalid command {command}"

    def get_id(self):
        # Return a unique id for each node
        return next(self._ids)
