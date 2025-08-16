# fred_mcp_demo

Basic MCP server wrapping around the FRED Python API using [fastmcp](https://github.com/gofastmcp/fastmcp) and [polars](https://pola.rs).

## Setup
This repository uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
uv sync
```

## Running the server
Export your `FRED_API_KEY` and start the MCP server:

```bash
FRED_API_KEY=your_key uv run python server.py
```

The server exposes an HTTP endpoint on `http://localhost:8000`.

## Client example
With the server running:

```bash
uv run python client.py
```

The client first uses the `search` tool to find the series ID for "GDP" and then retrieves that series with `get_series`.

## LLM demo
The `test_llm.py` script demonstrates connecting an OpenAI model to the MCP server. An `OPENAI_API_KEY` is required.

```bash
OPENAI_API_KEY=your_openai_key uv run python test_llm.py
```
