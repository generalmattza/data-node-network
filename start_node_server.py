import asyncio
import logging

from data_node_network.node_data_gatherer import DataGathererNodeTCP

logger = logging.getLogger(__name__)


def main():
    address = ("localhost", 50_000)
    node_id = 0

    # Create and start the server
    node_server = DataGathererNodeTCP(address=address, node_id=node_id)
    node_server.start()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    # asyncio.run(main())
    main()
