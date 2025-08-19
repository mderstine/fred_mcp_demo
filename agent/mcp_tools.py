# agent/mcp_tools.py
import asyncio
import json
import os
from typing import Any, Dict

from fastmcp import Client as MCPClient
from langchain.tools import tool

MCP_URL = os.getenv("MCP_SERVER_URL", "http://127.0.0.1:8000/mcp")


# ------------------ Normalization helpers ------------------

def _maybe_json_loads(x: Any) -> Any:
    if isinstance(x, str):
        s = x.strip()
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return json.loads(s)
            except json.JSONDecodeError:
                return x
    return x

def _unwrap_content(x: Any) -> Any:
    # unwrap common content holder attributes
    for attr in ("value", "text", "data", "content", "error"):
        if hasattr(x, attr):
            return getattr(x, attr)
    return x

def _normalize_content(content: Any) -> Any:
    """
    FastMCP responses may be:
      - dict/list of primitives
      - list of wrapper objects (TextContent, JsonContent, etc.)
      - JSON strings, or list containing a JSON string
    Convert to plain dict/list/primitives.
    """
    # Unwrap one level of container attributes
    content = _unwrap_content(content)

    if isinstance(content, list):
        norm_list = []
        for item in content:
            item = _unwrap_content(item)
            item = _maybe_json_loads(item)
            norm_list.append(item)
        # Handle singleton list like: [ "[{...}]" ]
        if len(norm_list) == 1 and isinstance(norm_list[0], (str, list, dict)):
            inner = _maybe_json_loads(norm_list[0])
            return inner
        return norm_list

    if isinstance(content, dict):
        return {k: _normalize_content(v) for k, v in content.items()}

    # If it’s a JSON string, parse it; else return as-is
    return _maybe_json_loads(content)


# ------------------ MCP call helper ------------------

async def _mcp_call(name: str, args: Dict[str, Any]) -> Any:
    """Call an MCP tool and return normalized JSON-serializable content."""
    async with MCPClient(MCP_URL) as c:
        resp = await c.call_tool(name, args)
        return _normalize_content(resp.content)


def _parse_json_input(s: str) -> Dict[str, Any]:
    """Parse Action Input string as JSON object; return {} if invalid/empty."""
    s = (s or "").strip()
    if not s:
        return {}
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return {}  # keep strict: tools expect JSON objects


# ------------------ Tools exposed to the agent ------------------

@tool
def get_curve(input: str) -> str:
    """
    Get a market curve from DuckDB.
    Action Input: { "market": "usd_govt" }
    Returns JSON: [ { "t": <years>, "rate": <decimal> }, ... ]
    """
    args = _parse_json_input(input)
    market = args.get("market")
    if not market:
        return json.dumps({"error": "missing 'market'"})
    data = asyncio.run(_mcp_call("get_curve", {"market": market}))
    return json.dumps(data)


@tool
def price_bond(input: str) -> str:
    """
    Price a fixed-rate bond using either a stored market curve or an explicit curve.

    Action Input (market):
    {
      "market": "usd_govt",
      "face": 100.0, "coupon": 0.05, "frequency": "Semiannual",
      "issue_date": "2024-01-15", "maturity_date": "2027-01-15",
      "calendar": "UnitedStates", "day_count": "Actual365Fixed",
      "business_day_convention": "Following", "settlement_days": 2,
      "valuation_date": "2025-08-19",
      "persist": true
    }

    Action Input (explicit curve):
    {
      "curve": [ {"t":0.5,"rate":0.045}, {"t":1.0,"rate":0.046} ],
      "face": 100.0, "coupon": 0.05, "frequency": "Semiannual",
      "issue_date": "2024-01-15", "maturity_date": "2027-01-15",
      "valuation_date": "2025-08-19"
    }

    Returns JSON:
    { "clean_price": ..., "dirty_price": ..., "accrued": ..., "ytm": ..., "source": "db|computed", ... }
    """
    args = _parse_json_input(input)
    if not args:
        return json.dumps({"error": "invalid or empty JSON"})
    if "market" not in args and "curve" not in args:
        return json.dumps({"error": "provide 'market' or 'curve'"})
    data = asyncio.run(_mcp_call("price_bond", args))
    return json.dumps(data)


@tool
def put_curve(input: str) -> str:
    """
    Insert/refresh a market curve in DuckDB.

    Action Input:
    {
      "market": "usd_govt",
      "curve": [ {"t":0.5,"rate":0.043}, {"t":1.0,"rate":0.044}, {"t":2.0,"rate":0.045}, {"t":3.0,"rate":0.046} ],
      "mode": "replace"   // or "append"
    }

    Returns JSON:
    { "market": "usd_govt", "points": 4, "mode": "replace" }
    """
    args = _parse_json_input(input)
    market = args.get("market")
    curve = args.get("curve")
    mode = (args.get("mode") or "replace").lower()
    if not market or not curve:
        return json.dumps({"error": "missing 'market' or 'curve'"})
    if mode not in ("replace", "append"):
        return json.dumps({"error": "mode must be 'replace' or 'append'"})
    data = asyncio.run(_mcp_call("put_curve", {"market": market, "curve": curve, "mode": mode}))
    return json.dumps(data)
