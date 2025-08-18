# server.py
import asyncio
import os
from typing import Optional

from dotenv import load_dotenv
from fastmcp import FastMCP
from fredapi import Fred
import polars as pl

load_dotenv()
fred = Fred(api_key=os.getenv("FRED_API_KEY"))

server = FastMCP("fred")

def _series_to_records(series) -> list[dict]:
    """
    Convert a pandas Series (index=dates) into a list of dicts with JSON-safe values.
    - Dates -> ISO strings
    - NaNs -> None
    """
    # Build via Polars for speed/sanity, then replace NaNs with None
    df = pl.DataFrame(
        {
            "date": series.index.to_pydatetime().tolist(),
            "value": series.to_list(),
        }
    ).with_columns(
        pl.col("date").dt.strftime("%Y-%m-%d"),
        pl.col("value").cast(pl.Float64)
    )
    # Replace any NaNs with None explicitly
    df = df.with_columns(pl.when(pl.col("value").is_nan()).then(None).otherwise(pl.col("value")).alias("value"))
    return df.to_dicts()

@server.tool()
def search(search_text: str, limit: int = 10) -> list[dict[str, str]]:
    """Search for FRED series IDs matching a query."""
    results = fred.search(search_text)
    # Keep only id+title and cap to limit
    df = pl.from_pandas(results[["id", "title"]]).head(limit)
    return df.to_dicts()

@server.tool()
def get_series(
    series_id: str,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
) -> list[dict]:
    """Return a FRED time series as a list of {'date':'YYYY-MM-DD','value':float} records."""
    s = fred.get_series(
        series_id,
        observation_start=observation_start,
        observation_end=observation_end,
    )
    return _series_to_records(s)

@server.tool()
def get_series_info(series_id: str) -> dict:
    """Return metadata for a FRED series ID."""
    info = fred.get_series_info(series_id)  # pandas Series-like
    # Convert to plain dict of JSON-safe scalars
    return {k: (None if (v != v) else v) for k, v in info.to_dict().items()}  # NaN check: v!=v

if __name__ == "__main__":
    # HTTP + SSE transport at http://127.0.0.1:8000/mcp
    asyncio.run(server.run_http_async())
