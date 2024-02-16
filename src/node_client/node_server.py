import asyncio


async def handle_client(reader, writer):
    data = await reader.read(100)
    message = data.decode()

    # Assuming a simple response for demonstration purposes
    response = "Hello from the node!"

    writer.write(response.encode())
    await writer.drain()

    print("Node received a request:", message)
    writer.close()


async def main():
    server = await asyncio.start_server(handle_client, "localhost", 5002)

    print(f"Node server running on {server.sockets[0].getsockname()}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
