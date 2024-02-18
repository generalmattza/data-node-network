import asyncio
import logging

from prometheus_client import start_http_server
from data_node_network.node_data_gatherer import DataGathererNodeTCP

logger = logging.getLogger(__name__)


async def main():
    address = ("10.0.0.141", 50_000)
    node_id = 0

    # Create and start the server
    node_server = DataGathererNodeTCP(address=address, node_id=node_id)
    await node_server.start_server()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
