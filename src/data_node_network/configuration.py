from __future__ import annotations
from typing import Union
from pathlib import Path
import tomli
import logging

CONFIG_FILEPATH = "config/application.toml"
DEFAULT_NODELIST_FILEPATH = "config/node_list.yaml"
NODE_COMMANDS_FILEPATH = "config/node_commands.yaml"

logger = logging.getLogger("data_node_network.client")

def load_config(filepath: Union[str, Path]) -> dict:
    if isinstance(filepath, str):
        filepath = Path(filepath)

    if not Path(filepath).exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    # if extension is .json
    if filepath.suffix == ".json":
        import json

        with open(filepath, "r") as file:
            return json.load(file)

    # if extension is .yaml
    if filepath.suffix == ".yaml":
        import yaml

        with open(filepath, "r") as file:
            return yaml.safe_load(file)
    # if extension is .toml
    if filepath.suffix == ".toml":
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib

        with open(filepath, "rb") as file:
            return tomllib.load(file)

    # else load as binary
    with open(filepath, "rb") as file:
        return file.read()


config_global = load_config(CONFIG_FILEPATH)
node_commands = load_config(NODE_COMMANDS_FILEPATH)
try:
    node_config = load_config(DEFAULT_NODELIST_FILEPATH)
except FileNotFoundError:
    logger.warning(f"No node list found at {DEFAULT_NODELIST_FILEPATH}. Continuing with empty node list.")
