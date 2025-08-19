# server/app.py
import asyncio
from tools import server  # FastMCP("pricing") with tools registered

if __name__ == "__main__":
    print("[FastMCP] starting on http://127.0.0.1:8000/mcp")
    asyncio.run(server.run_http_async(host="127.0.0.1", port=8000))
