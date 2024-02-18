import asyncio
from data_node_network.node_data_gatherer import DataGathererNodeTCP
import logging

logger = logging.getLogger(__name__)


async def main():
    address = ("localhost", 50_000)
    node_id = 0

    # Create and start the server
    node_server = DataGathererNodeTCP(address=address, node_id=node_id)
    await node_server.start_server()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
