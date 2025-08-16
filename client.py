import asyncio

from fastmcp import Client


async def main() -> None:
    async with Client("http://localhost:8000/mcp") as client:
        search_result = await client.call_tool("search", {"search_text": "GDP"})
        first_id = search_result.content[0]["id"] if search_result.content else "GDP"
        result = await client.call_tool("get_series", {"series_id": first_id})
        print(result.content)


if __name__ == "__main__":
    asyncio.run(main())
