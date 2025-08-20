"""
MCP Tools for Bond Evaluatior Platform Using FastMCP
Exposes Bond Evaluation and Data Retrieval Functionality as MCP Tools
"""

from typing import Optional, Sequence, List, Dict, Any, Union
from datetime import date, datetime
import polars as pl
from pydantic import BaseModel, Field
from fastmcp import FastMCP
from src.data import retrieval as r


# ----------------- Helpers -----------------

def _df_json(df: pl.DataFrame) -> List[Dict[str, Any]]:
    """Return JSON-serializable rows (list of dicts)."""
    return df.to_dicts()


# ----------------- Request/Response Schemas -----------------

class LatestQuotesEODReq(BaseModel):
    as_of: Union[str, date, datetime] = Field(
        ...,
        description="EOD Date or Timestamp Cutoff"
    )
    security_ids: Optional[Union[int, Sequence[int]]] = Field(
        None,
        description="Filter to These Security IDs"
    )
    prefer_sources: Optional[Sequence[str]] = Field(
        default=("TRACE", "Trader"),
        description="Tie-breaker priority"
    )
    quote_types: Optional[Sequence[str]] = Field(
        default=("CleanPrice", "Yield", "ZSpread"),
        description="Quote types to return"
    )

class QuotesWindowReq(BaseModel):
    beg_ts: Union[str, date, datetime]
    end_ts: Union[str, date, datetime]
    security_id: Optional[Union[int, Sequence[int]]] = None
    quote_type: Optional[Union[str, Sequence[str]]] = None
    source: Optional[str] = None

class GetSecurityReq(BaseModel):
    security_ids: Optional[Union[int, Sequence[int]]] = None
    isins: Optional[Union[str, Sequence[str]]] = None
    cusips: Optional[Union[str, Sequence[str]]] = None

class PositionSnapshotReq(BaseModel):
    as_of: Union[str, date, datetime]
    portfolio_id: Optional[Union[int, Sequence[int]]] = None
    security_ids: Optional[Union[int, Sequence[int]]] = None

class OpenLotsReq(BaseModel):
    as_of: Union[str, date, datetime]
    portfolio_ids: Optional[Union[int, Sequence[int]]] = None

class CurveForMarketReq(BaseModel):
    market: str
    as_of: Union[str, date, datetime]

class RunResultsReq(BaseModel):
    run_id: int
    portfolio_id: Optional[int] = None
    security_ids: Optional[Union[int, Sequence[int]]] = None

class RunCashflowsReq(BaseModel):
    run_id: int
    portfolio_id: Optional[int] = None
    security_id: Optional[int] = None

class PortfolioEODWithQuotesReq(BaseModel):
    as_of: Union[str, date, datetime]
    portfolio_id: int
    prefer_sources: Optional[Sequence[str]] = Field(default=("TRACE", "Trader"))


# ----------------- Server -----------------

mcp = FastMCP("bondbook-mcp")


@mcp.tool(name="list_portfolios", description="List all portfolio_ids present in the database")
def list_portfolios() -> List[Dict[str, Any]]:
    con = r.get_connection()
    df = r.list_portfolios(con)
    return _df_json(df)


@mcp.tool(name="list_securities", description="List security master rows with common identifiers")
def list_securities() -> List[Dict[str, Any]]:
    con = r.get_connection()
    df = r.list_securities(con)
    return _df_json(df)


@mcp.tool(name="get_security", description="Fetch security master rows by id/ISIN/CUSIP (each accepts single or list)")
def get_security(req: GetSecurityReq) -> List[Dict[str, Any]]:
    con = r.get_connection()
    df = r.get_security(
        con,
        security_ids=req.security_ids,
        isins=req.isins,
        cusips=req.cusips,
    )
    return _df_json(df)


@mcp.tool(name="get_position_snapshot", description="EOD positions for a date; filter by portfolio_id and/or security_ids")
def get_position_snapshot(req: PositionSnapshotReq) -> List[Dict[str, Any]]:
    con = r.get_connection()
    df = r.get_position_snapshot(
        con,
        as_of=req.as_of,
        portfolio_id=req.portfolio_id,
        security_ids=req.security_ids,
    )
    return _df_json(df)


@mcp.tool(name="get_open_lots_asof", description="Lots with trade_date <= as_of; optionally filter by portfolio_ids")
def get_open_lots_asof(req: OpenLotsReq) -> List[Dict[str, Any]]:
    con = r.get_connection()
    df = r.get_open_lots_asof(
        con,
        as_of=req.as_of,
        portfolio_ids=req.portfolio_ids,
    )
    return _df_json(df)


@mcp.tool(name="get_quotes_window", description="Quotes between beg_ts and end_ts; optional filters by security_id/type/source")
def get_quotes_window(req: QuotesWindowReq) -> List[Dict[str, Any]]:
    con = r.get_connection()
    df = r.get_quotes_window(
        con,
        beg_ts=req.beg_ts,
        end_ts=req.end_ts,
        security_id=req.security_id,
        quote_type=req.quote_type,
        source=req.source,
    )
    return _df_json(df)


@mcp.tool(name="get_latest_quotes_eod", description="Latest quotes at or before EOD for each (security_id, quote_type)")
def get_latest_quotes_eod(req: LatestQuotesEODReq) -> List[Dict[str, Any]]:
    con = r.get_connection()
    df = r.get_latest_quotes_eod(
        con,
        as_of=req.as_of,
        security_ids=req.security_ids,
        prefer_sources=req.prefer_sources,
        quote_types=req.quote_types,
    )
    return _df_json(df)


@mcp.tool(name="get_curve_header", description="Curve metadata for a curve_id")
def get_curve_header(curve_id: int) -> List[Dict[str, Any]]:
    con = r.get_connection()
    df = r.get_curve_header(con, curve_id)
    return _df_json(df)


@mcp.tool(name="get_curve_nodes", description="Curve nodes for a curve_id")
def get_curve_nodes(curve_id: int) -> List[Dict[str, Any]]:
    con = r.get_connection()
    df = r.get_curve_nodes(con, curve_id)
    return _df_json(df)


@mcp.tool(name="get_latest_curve_for_market", description="Return latest curve_id <= as_of for a market, and its header+nodes")
def get_latest_curve_for_market(req: CurveForMarketReq) -> Dict[str, Any]:
    con = r.get_connection()
    curve_id, hdr, nodes = r.get_latest_curve_for_market(con, req.market, req.as_of)
    return {
        "curve_id": curve_id,
        "header": _df_json(hdr),
        "nodes": _df_json(nodes),
    }


@mcp.tool(name="get_run", description="Valuation run metadata by run_id")
def get_run(run_id: int) -> List[Dict[str, Any]]:
    con = r.get_connection()
    df = r.get_run(con, run_id)
    return _df_json(df)


@mcp.tool(name="get_run_results", description="Valuation results for run_id; optional portfolio/security filters")
def get_run_results(req: RunResultsReq) -> List[Dict[str, Any]]:
    con = r.get_connection()
    df = r.get_run_results(
        con,
        run_id=req.run_id,
        portfolio_id=req.portfolio_id,
        security_ids=req.security_ids,
    )
    return _df_json(df)


@mcp.tool(name="get_run_cash_flows", description="Cashflows for run_id; optional portfolio/security filters")
def get_run_cash_flows(req: RunCashflowsReq) -> List[Dict[str, Any]]:
    con = r.get_connection()
    df = r.get_run_cash_flows(
        con,
        run_id=req.run_id,
        portfolio_id=req.portfolio_id,
        security_id=req.security_id,
    )
    return _df_json(df)


@mcp.tool(name="join_positions_with_results", description="Join EOD positions with results for the given run_id")
def join_positions_with_results(run_id: int) -> List[Dict[str, Any]]:
    con = r.get_connection()
    df = r.join_positions_with_results(con, run_id)
    return _df_json(df)


@mcp.tool(name="portfolio_eod_with_quotes", description="EOD positions plus latest quotes for a portfolio & date")
def portfolio_eod_with_quotes(req: PortfolioEODWithQuotesReq) -> List[Dict[str, Any]]:
    con = r.get_connection()
    df = r.portfolio_eod_with_quotes(
        con,
        as_of=req.as_of,
        portfolio_id=req.portfolio_id,
        prefer_sources=req.prefer_sources,
    )
    return _df_json(df)


if __name__ == "__main__":
    # Run the MCP server (stdio transport by default)
    mcp.run()
