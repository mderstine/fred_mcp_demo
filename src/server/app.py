import asyncio
from src.server.tools import server

async def main():
    print("[FastMCP] Starting on http://127.0.0.1:8000/mcp")
    await server.run_http_async(host="127.0.1", port=8000, path="/mcp")

if __name__ == "__main__":
    asyncio.run(main())
