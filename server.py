# server.py
import asyncio
import os
from dotenv import load_dotenv
from fastmcp import FastMCP
from fredapi import Fred
from typing import Optional
import polars as pl

load_dotenv()
fred = Fred(api_key=os.getenv("FRED_API_KEY"))

server = FastMCP("fred")

@server.tool()
def get_series(
    series_id: str,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
) -> list[dict[str, str | float]]:
    """Return a FRED time series as a list of records.

    Dates are returned as ISO formatted strings to keep the output JSON serialisable.
    """
    series = fred.get_series(
        series_id,
        observation_start=observation_start,
        observation_end=observation_end,
    )
    df = pl.DataFrame(
        {
            "date": series.index.to_pydatetime().tolist(),
            "value": series.to_list(),
        }
    ).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))
    return df.to_dicts()

@server.tool()
def search(search_text: str, limit: int = 10) -> list[dict[str, str]]:
    """Search for FRED series IDs matching a query."""
    results = fred.search(search_text)
    df = pl.from_pandas(results[["id", "title"]]).head(limit)
    return df.to_dicts()

if __name__ == "__main__":
    asyncio.run(server.run_http_async())  # default http://127.0.0.1:8000/mcp
