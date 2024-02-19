import asyncio
import logging

from data_node_network.node_data_gatherer import GathererNodeTCP

logger = logging.getLogger(__name__)


def main():
    address = ("10.0.0.141", 50_000)
    # address = ("10.0.0.108", 50_000)

    # Create and start the server
    node_server = GathererNodeTCP(address=address)
    node_server.start()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    # asyncio.run(main())
    main()
