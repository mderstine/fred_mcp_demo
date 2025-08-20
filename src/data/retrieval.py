# src/data/retrieval.py
from __future__ import annotations
from datetime import date, datetime
from typing import Optional, Tuple, Union
import duckdb
import polars as pl
from pathlib import Path
import yaml


# ---------- Connection helpers ----------
def load_config() -> dict:
    """
    Load configuration from YAML file.
    """
    config_path = Path(__file__).parent.parent.parent / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file {config_path} does not exist.")
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    return config


def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Get a connection to the DuckDB database.
    
    Returns:
        duckdb.Connection: Connection object to the DuckDB database.
    """
    config = load_config()
    connection_string = config["database"]["connection_string"]
    
    return duckdb.connect(database=connection_string, read_only=False)


def _to_date(d: Union[str, date, datetime]) -> date:
    """
    Convert various date formats to a date object.
    
    Args:
        d (Union[str, date, datetime]): The date to convert.
        
    Returns:
        date: The corresponding date object.
    """
    if isinstance(d, date) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    return date.fromisoformat(d)


def _to_timestamp(ts: Union[str, date, datetime]) -> datetime:
    """
    Convert various timestamp formats to a datetime object.
    
    Args:
        ts (Union[str, date, datetime]): The timestamp to convert.
        
    Returns:
        datetime: The corresponding datetime object.
    """
    if isinstance(ts, datetime):
        return ts
    if isinstance(ts, date):
        # Interpret a date as EOD in UTC for quote/FX pulls
        return datetime(ts.year, ts.month, ts.day, 23, 59, 59)
    # ISO string
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        # Fallback: treat as date
        d = date.fromisoformat(ts)
        return datetime(d.year, d.month, d.day, 23, 59, 59)


# ---------- Lookups / dictionaries ----------

def list_portfolios(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """
    List all portfolios present in positions or results.
    
    Returns:
        pl.DataFrame: DataFrame containing all portfolio IDs.
    """

    sql = """
        WITH a AS (
            SELECT DISTINCT portfolio_id
            FROM fact_position_snapshot
        ),
        b AS (
            SELECT DISTINCT portfolio_id
            FROM val_result_bond
        )
        SELECT DISTINCT portfolio_id
        FROM (SELECT * FROM a UNION ALL SELECT * FROM b)
        ORDER BY portfolio_id
    """
    
    return conn.sql(query=sql).pl()


def list_securities(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """
    List all securities in the security master.
    
    Returns:
        pl.DataFrame: DataFrame containing all securities with basic identifiers.
    """
    
    sql = """
    SELECT
        security_id,
        isin,
        cusip,
        ticker,
        issuer,
        currency,
        coupon_rate,
        frequency,
        issue_date,
        maturity_date
    FROM dim_security_bond
    ORDER BY security_id
    """
    return conn.sql(query=sql).pl()


def get_security(
    conn: duckdb.DuckDBPyConnection,
    security_ids: Optional[Union[int, str, list[int]]] = None,
    isins: Optional[Union[str, list[str]]] = None,
    cusips: Optional[Union[str, list[str]]] = None
) -> pl.DataFrame:

    clauses, params = [], []

    if security_ids is not None:
        if isinstance(security_ids, int):
            security_ids = [security_ids]
        elif isinstance(security_ids, str):
            # If security_ids is a string, try to convert to int
            try:
                security_ids = [int(security_ids)]
            except ValueError:
                raise TypeError("security_ids must be an int or a sequence of ints, or a string convertible to int")
        elif not isinstance(security_ids, list):
            security_ids = list(security_ids)
        else:
            security_ids = list(security_ids)
        
        placeholders = ",".join(["?"] * len(security_ids))
        clauses.append(f"security_id IN ({placeholders})")
        params.extend(security_ids)

    if isins is not None:
        if not isinstance(isins, list) or isinstance(isins, str):
            isins = [isins]
        placeholders = ",".join(["?"] * len(isins))
        clauses.append(f"isin IN ({placeholders})")
        params.extend(isins)

    if cusips is not None:
        if not isinstance(cusips, list) or isinstance(cusips, str):
            cusips = [cusips]
        placeholders = ",".join(["?"] * len(cusips))
        clauses.append(f"cusip IN ({placeholders})")
        params.extend(cusips)

    if not clauses:
        raise ValueError("Provide security_ids, isins, or cusips")

    sql = f"SELECT * FROM dim_security_bond WHERE {' OR '.join(clauses)}"
    return conn.sql(query=sql, params=tuple(params)).pl()


# ---------- Positions ----------

def get_position_snapshot(
    conn: duckdb.DuckDBPyConnection,
    as_of: Union[str, date, datetime],
    portfolio_id: Optional[Union[int, list[int]]] = None,
    security_ids: Optional[Union[int, list[int]]] = None
) -> pl.DataFrame:
    """
    Get end-of-day position snapshot for a given date and optional portfolio/security filter.
    
    Args:
        conn (duckdb.DuckDBPyConnection): Database connection.
        as_of (Union[str, date, datetime]): The date for which to retrieve positions.
        portfolio_id (Optional[int]): Portfolio ID to filter by.
        security_ids (Optional[Sequence[int]]): List of security IDs to filter by.
        
    Returns:
        pl.DataFrame: DataFrame containing the position snapshot.
    """

    as_of_date = _to_date(as_of)
    clauses = ["as_of_date = ?"]
    params: list = [as_of_date]


    if portfolio_id is not None:
        if isinstance(portfolio_id, int):
            portfolio_id = [portfolio_id]
        elif isinstance(portfolio_id, str):
            try:
                portfolio_id = [int(portfolio_id)]
            except ValueError:
                raise TypeError("portfolio_id must be an int or a sequence of ints, or a string convertible to int")
        clauses.append("portfolio_id IN ({})".format(",".join(["?"] * len(portfolio_id))))
        params.extend(portfolio_id)

    if security_ids is not None:
        if isinstance(security_ids, int):
            security_ids = [security_ids]
        elif isinstance(security_ids, str):
            try:
                security_ids = [int(security_ids)]
            except ValueError:
                raise TypeError("security_ids must be an int or a sequence of ints, or a string convertible to int")
        clauses.append("security_id IN ({})".format(",".join(["?"] * len(security_ids))))
        params.extend(security_ids)

    sql = f"""
        SELECT *
        FROM fact_position_snapshot
        WHERE {' AND '.join(clauses)}
        ORDER BY portfolio_id, security_id
    """
    
    return conn.sql(query=sql, params=tuple(params)).pl()


def get_open_lots_asof(
    conn: duckdb.DuckDBPyConnection,
    as_of: Union[str, date, datetime],
    portfolio_ids: Optional[Union[int, list[int]]] = None
) -> pl.DataFrame:
    """
    Get open lots for a portfolio at a specific date.
    
    Args:
        conn (duckdb.DuckDBPyConnection): Database connection.
        as_of (Union[str, date, datetime]): The date for which to retrieve open lots.
        portfolio_ids (Optional[Union[int, list[int]]]): Portfolio IDs to filter by.
        
    Returns:
        pl.DataFrame: DataFrame containing open lots.
    """
    
    as_of_date = _to_date(as_of)
    clauses = ["as_of_date = ?"]
    params: list = [as_of_date]

    if portfolio_ids is not None:
        if isinstance(portfolio_ids, int):
            portfolio_ids = [portfolio_ids]
        elif isinstance(portfolio_ids, str):
            try:
                portfolio_ids = [int(portfolio_ids)]
            except ValueError:
                raise TypeError("portfolio_ids must be an int or a sequence of ints, or a string convertible to int")
        clauses.append("portfolio_id IN ({})".format(",".join(["?"] * len(portfolio_ids))))
        params.extend(portfolio_ids)

    sql = f"""
        SELECT *
        FROM fact_position_lot
        WHERE {' AND '.join(clauses)} AND position_qty_open > 0
        ORDER BY portfolio_id, security_id, lot_id
    """
    
    return conn.sql(query=sql, params=tuple(params)).pl()
    

# ---------- Quotes / FX ----------

def get_quotes_window(
    conn: duckdb.DuckDBPyConnection,
    beg_ts: Union[str, date, datetime],
    end_ts: Union[str, date, datetime],
    security_id: Optional[Union[int, list[int]]] = None,
    quote_type: Optional[Union[str, list[str]]] = None,
    source: Optional[str] = None
) -> pl.DataFrame:
    """
    Pull quotes within a timestamp window.

    Args:
        conn (duckdb.DuckDBPyConnection): Database connection.
        beg_ts (Union[str, date, datetime]): Start timestamp.
        end_ts (Union[str, date, datetime]): End timestamp.
        security_id (Optional[Union[int, list[int]]]): Security ID(s) to filter by.
        quote_type (Optional[Union[str, list[str]]]): Quote type(s) to filter by.
        source (Optional[str]): Source to filter by.
    Returns:
        pl.DataFrame: DataFrame containing quotes within the specified window.
    """
    t0 = _to_timestamp(beg_ts)
    t1 = _to_timestamp(end_ts)
    clauses = ["as_of_ts BETWEEN ? AND ?"]
    params: list = [t0, t1]

    if security_id is not None:
        clauses.append("security_id = ?")
        params.append(security_id)
    if quote_type is not None:
        clauses.append("quote_type = ?")
        params.append(quote_type)
    if source is not None:
        clauses.append("source = ?")
        params.append(source)

    sql = f"""
        SELECT *
        FROM md_quote_bond
        WHERE {' AND '.join(clauses)}
        ORDER BY as_of_ts
    """

    return conn.sql(query=sql, params=tuple(params)).pl()


def get_latest_quotes_eod(
    conn: duckdb.DuckDBPyConnection,
    as_of: Union[str, date, datetime],
    prefer_sources: Optional[Union[str, list[str]]] = ["TRACE", "Trader"],
    quote_types: list[str] = ["CleanPrice", "Yield", "ZSpread"]
) -> pl.DataFrame:
    """
    Get the latest quotes at or before EOD for each security and quote type.
    
    Args:
        conn (duckdb.DuckDBPyConnection): Database connection.
        as_of (Union[str, date, datetime]): The date for which to get the latest quotes.
        prefer_sources (Optional[Union[str, list[str]]]): List of preferred sources to break ties.
        quote_types (list[str]): List of quote types to retrieve.
    
    Returns:
        pl.DataFrame: DataFrame containing the latest quotes.
    """

    cutoff = _to_timestamp(as_of)

    # Rank by timestamp desc and optional source preference
    # Build a CASE expression for source preference if provided
    if prefer_sources:
        cases = " ".join(
            f"WHEN source = '{s}' THEN {i}" for i, s in enumerate(prefer_sources)
        )
        source_rank = f"(CASE {cases} ELSE 999 END)"
    else:
        source_rank = "0"

    placeholders = ",".join(["?"] * len(quote_types))
    params = [cutoff] + list(quote_types)

    sql = f"""
        WITH filtered AS (
            SELECT *
            FROM md_quote_bond
            WHERE as_of_ts <= ?
                AND quote_type IN ({placeholders})
        ),
        ranked AS (
            SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY security_id, quote_type
                ORDER BY as_of_ts DESC, {source_rank} ASC
            ) AS rn
            FROM filtered
        )
        SELECT security_id, quote_type, source, as_of_ts, value
        FROM ranked
        WHERE rn = 1
        ORDER BY security_id, quote_type
    """

    return conn.sql(query=sql, params=tuple(params)).pl()


# ---------- Curves ----------

def get_curve_header(
        conn: duckdb.DuckDBPyConnection,
        curve_id: int
) -> pl.DataFrame:
    """
    Get a single row with curve metadata.
    
    Args:
        conn (duckdb.DuckDBPyConnection): Database connection.
        curve_id (int): The curve ID to filter by.
        
    Returns:
        pl.DataFrame: DataFrame containing the curve header.
    """
    
    sql = "SELECT * FROM md_curve WHERE curve_id = ?"
    return conn.sql(query=sql, params=(curve_id,)).pl()


def get_curve_nodes(
        conn: duckdb.DuckDBPyConnection,
        curve_id: int
) -> pl.DataFrame:
    """
    Get curve nodes for a specific curve ID.
    
    Args:
        conn (duckdb.DuckDBPyConnection): Database connection.
        curve_id (int): The curve ID to filter by.
        
    Returns:
        pl.DataFrame: DataFrame containing the curve nodes.
    """
    
    sql = """
    SELECT curve_id, node_label, zero_rate, discount_factor
    FROM md_curve_node
    WHERE curve_id = ?
    ORDER BY
      CASE
        WHEN node_label LIKE '%D' THEN CAST(REPLACE(node_label,'D','') AS DOUBLE) / 365.0
        WHEN node_label LIKE '%W' THEN CAST(REPLACE(node_label,'W','') AS DOUBLE) / 52.0
        WHEN node_label LIKE '%M' THEN CAST(REPLACE(node_label,'M','') AS DOUBLE) / 12.0
        WHEN node_label LIKE '%Y' THEN CAST(REPLACE(node_label,'Y','') AS DOUBLE)
        ELSE 9999
      END
    """
    
    return conn.sql(query=sql, params=(curve_id,)).pl()


def get_latest_curve_for_market(
    conn: duckdb.DuckDBPyConnection,
    market: str,
    as_of: Union[str, date, datetime]
) -> Tuple[int, pl.DataFrame, pl.DataFrame]:
    """
    Get the latest curve for a market as of a specific date.
    
    Args:
        conn (duckdb.DuckDBPyConnection): Database connection.
        market (str): The market identifier.
        as_of (Union[str, date, datetime]): The date for which to get the latest curve.
        
    Returns:
        Tuple[int, pl.DataFrame, pl.DataFrame]: Tuple containing the curve ID, header DataFrame, and nodes DataFrame.
    """
    
    as_of_date = _to_date(as_of)
    sql = """
        WITH latest AS (
            SELECT curve_id
            FROM md_curve
            WHERE market = ? AND as_of_date <= ?
            ORDER BY as_of_date DESC, curve_id DESC
            LIMIT 1
        )
        SELECT curve_id FROM latest
    """
    ids = conn.sql(query=sql, params=tuple(market, as_of_date)).pl()

    if ids.height == 0:
        raise ValueError(f"No curve found for market={market!r} as_of<={as_of_date}")
    curve_id = int(ids.item(0, "curve_id"))
    hdr = get_curve_header(con, curve_id)
    nodes = get_curve_nodes(con, curve_id)
    return curve_id, hdr, nodes


# ---------- Valuation runs, results & cashflows ----------

def get_run(conn: duckdb.DuckDBPyConnection, run_id: int) -> pl.DataFrame:
    """
    Get metadata for a valuation run.
    
    Args:
        conn (duckdb.DuckDBPyConnection): Database connection.
        run_id (int): The run ID to filter by.
        
    Returns:
        pl.DataFrame: DataFrame containing the run metadata.
    """
    
    sql = "SELECT * FROM val_run WHERE run_id = ?"
    return conn.sql(query=sql, params=(run_id,)).pl()   


def get_run_results(
    conn: duckdb.DuckDBPyConnection,
    run_id: int,
    portfolio_id: Optional[int] = None,
    security_ids: Optional[Union[int, list[int], str]] = None
) -> pl.DataFrame:
    """
    Get valuation results for a run, optionally filtered by portfolio and/or securities.
    Args:
        conn (duckdb.DuckDBPyConnection): Database connection.
        run_id (int): The run ID to filter by.
        portfolio_id (Optional[int]): Portfolio ID to filter by.
        security_ids (Optional[Union[int, list[int]]]): Security ID(s) to filter by.
    Returns:
        pl.DataFrame: DataFrame containing the valuation results.
    """

    clauses = ["run_id = ?"]
    params: list = [run_id]

    if portfolio_id is not None:
        clauses.append("portfolio_id = ?")
        params.append(portfolio_id)
    if security_ids is not None:
        if isinstance(security_ids, int):
            security_ids = [security_ids]
        elif isinstance(security_ids, str):
            try:
                security_ids = [int(security_ids)]
            except ValueError:
                raise TypeError("security_ids must be an int or a sequence of ints, or a string convertible to int")
        placeholders = ",".join(["?"] * len(security_ids))
        clauses.append(f"security_id IN ({placeholders})")
        params.extend(security_ids)

    sql = f"""
        SELECT *
        FROM val_result_bond
        WHERE {' AND '.join(clauses)}
        ORDER BY portfolio_id, security_id
    """

    return conn.sql(query=sql, params=tuple(params)).pl()


def get_run_cash_flows(
    conn: duckdb.DuckDBPyConnection,
    run_id: int,
    portfolio_id: Optional[int] = None,
    security_id: Optional[int] = None
) -> pl.DataFrame:
    """
    Get cash flows for a valuation run, optionally filtered by portfolio and/or security.
    
    Args:
        conn (duckdb.DuckDBPyConnection): Database connection.
        run_id (int): The run ID to filter by.
        portfolio_id (Optional[int]): Portfolio ID to filter by.
        security_id (Optional[int]): Security ID to filter by.
        
    Returns:
        pl.DataFrame: DataFrame containing the cash flows.
    """
    
    clauses = ["run_id = ?"]
    params: list = [run_id]

    if portfolio_id is not None:
        clauses.append("portfolio_id = ?")
        params.append(portfolio_id)
    if security_id is not None:
        clauses.append("security_id = ?")
        params.append(security_id)

    sql = f"""
        SELECT *
        FROM val_cashflow_bond
        WHERE {' AND '.join(clauses)}
        ORDER BY portfolio_id, security_id, flow_num
    """

    return conn.sql(query=sql, params=tuple(params)).pl()


def join_positions_with_results(
    conn: duckdb.DuckDBPyConnection,
    run_id: int
) -> pl.DataFrame:
    """
    Join EOD positions with valuation results for a specific run.
    Args:
        conn (duckdb.DuckDBPyConnection): Database connection.
        run_id (int): The run ID to filter by.
    Returns:
        pl.DataFrame: DataFrame containing joined positions and results.
    """
    # Get the as_of_date for the run
    run_df = get_run(conn, run_id)
    if run_df.height == 0:
        raise ValueError(f"run_id {run_id} not found")
    
    as_of_date = run_df.item(0, "as_of_date")

    sql = """
    WITH eod AS (
        SELECT s.as_of_date, s.portfolio_id, s.security_id, s.position_qty_eod
        FROM fact_position_snapshot s
        JOIN val_run r ON r.as_of_date = s.as_of_date
        WHERE r.run_id = ?
    )
    SELECT
        r.run_id,
        e.as_of_date,
        r.portfolio_id,
        r.security_id,
        e.position_qty_eod AS qty_eod,
        r.clean_price, r.accrued, r.dirty_price, r.yield_calc, r.z_spread,
        r.duration_mod, r.duration_mac, r.convexity, r.pvbp,
        r.pv_local, r.pv_base_ccy
    FROM val_result_bond r
    JOIN eod e
    ON e.portfolio_id = r.portfolio_id AND e.security_id = r.security_id
    WHERE r.run_id = ?
    ORDER BY r.portfolio_id, r.security_id
    """
    
    return conn.sql(query=sql, params=(run_id, run_id)).pl()


# ---------- Convenience: portfolio EOD with latest quotes ----------

def portfolio_eod_with_quotes(
    conn: duckdb.DuckDBPyConnection,
    as_of: Union[str, date, datetime],
    portfolio_id: int,
    prefer_sources: Optional[Union[str, list[str]]] = ("TRACE", "Trader")
) -> pl.DataFrame:
    """
    Get end-of-day position for a portfolio with latest CleanPrice/Yield/ZSpread quotes attached.
    Args:
        conn (duckdb.DuckDBPyConnection): Database connection.
        as_of (Union[str, date, datetime]): The date for which to retrieve the portfolio EOD.
        portfolio_id (int): The portfolio ID to filter by.
        prefer_sources (Optional[Union[str, list[str]]]): List of preferred sources for quotes.
    Returns:
        pl.DataFrame: DataFrame containing the portfolio EOD with quotes.
    """
    pos = get_position_snapshot(conn, as_of=as_of, portfolio_id=portfolio_id)
    if pos.height == 0:
        return pos  # empty

    quotes = get_latest_quotes_eod(conn, as_of=as_of, prefer_sources=prefer_sources)
    q_pivot = (
        quotes
        .pivot(
            index=["security_id"],
            on="guote_type",
            values="value"
        )
        .select(
            "security_id",
            pl.col("CleanPrice").alias("q_clean"),
            pl.col("Yield").alias("q_yield"),
            pl.col("ZSpread").alias("q_zspread")
        )
    )

    sec = list_securities(conn)

    out = (
        pos
        .join(
            other=sec,
            on="security_id",
            how="left"
        )
        .join(
            other=q_pivot,
            on="security_id",
            how="left"
        )
        .sort(["portfolio_id", "security_id"])
    )

    return out


if __name__ == "__main__":
    conn = get_connection()
    print("Portfolios:\n", list_portfolios(conn))
    print("Securities:\n", list_securities(conn))
    print("EOD 2025-08-19 P10:\n", get_position_snapshot(conn, "2025-08-19", 10))
    print("Latest EOD quotes:\n", get_latest_quotes_eod(conn, "2025-08-19"))
    rid, hdr, nodes = get_latest_curve_for_market(conn, "usd_govt", "2025-08-19")
    print("Curve:", rid, hdr, nodes)
    print("Run 7001 join:\n", join_positions_with_results(conn, 7001))
    print("EOD+quotes:\n", portfolio_eod_with_quotes(conn, "2025-08-19", 10))
