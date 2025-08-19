"""
Data Retrieval Functions for Bond Evaluator Platform (Leveraged by MCP)
Provides Access to Bond Positions, Market Term Structures, and Evaluation Results
"""

import duckdb
import polars as pl
from pathlib import Path
import yaml
from typing import Optional, List, Dict, Union, Any
import datetime as dt


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

def get_bond_positions(
    instrument_ids: Optional[List[str]] = None,
    dates: Optional[List[Union[str, dt.datetime, dt.date]]] = None,
    currencies: Optional[List[str]] = None,
    position_ids: Optional[List[int]] = None
) -> pl.DataFrame:
    """
    Retrieve bond positions based on optional filters.
    
    Args:
        instrument_id (Optional[List[str]]): List of instrument IDs to filter by.
        dates (Optional[List[Union[str, dt.datetime, dt.date]]]): List of dates to filter by.
        currencies (Optional[List[str]]): List of currencies to filter by.
        position_ids (Optional[List[int]]): List of position IDs to filter by.
    
    Returns:
        pl.DataFrame: DataFrame containing the bond positions.
    """
    conn = get_connection()
    
    query = "SELECT * FROM bond_positions"
    conditions = []
    params: Dict[str, Any] = {}
    
    if instrument_ids:
        conditions.append("instrument_ids IN (SELECT * FROM unnest(?))")
        params["instrument_ids"] = instrument_ids
    
    if dates:
        # Ensure dates are between acquisition_date and termination_date (inclusive)
        conditions.append(
            "(" +
            " OR ".join(
                ["(? BETWEEN acquisition_date AND termination_date)" for _ in dates]
            ) +
            ")"
        )
        for idx, date in enumerate(dates):
            params[f"date_{idx}"] = date

    if currencies:
        conditions.append("currency IN (SELECT * FROM unnest(?))")
        params["currencies"] = currencies
    
    if position_ids:
        conditions.append("position_id IN (SELECT * FROM unnest(?))")
        params["position_ids"] = position_ids

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    result = conn.execute(query, params).pl()

    return result

def get_market_term_structures(
    reference_dates: Optional[List[Union[str, dt.datetime, dt.date]]] = None,
    curve_types: Optional[List[str]] = None,
    currencies: Optional[List[str]] = None,
    market_ids: Optional[List[int]] = None
) -> pl.DataFrame:
    """
    Retrieve market term structures based on optional filters.
    
    Args:
        reference_dates (Optional[List[Union[str, dt.datetime, dt.date]]]): List of reference dates to filter by.
        curve_types (Optional[List[str]]): List of curve types to filter by.
        currencies (Optional[List[str]]): List of currencies to filter by.
        market_ids (Optional[List[int]]): List of market IDs to filter by.
    
    Returns:
        pl.DataFrame: DataFrame containing the market term structures.
    """
    conn = get_connection()
    
    query = "SELECT * FROM market_term_structures"
    conditions = []
    params: Dict[str, Any] = {}
    
    if reference_dates:
        conditions.append("reference_date IN (SELECT * FROM unnest(?))")
        params["reference_dates"] = reference_dates
    
    if curve_types:
        conditions.append("curve_type IN (SELECT * FROM unnest(?))")
        params["curve_types"] = curve_types
    
    if currencies:
        conditions.append("curve_currency IN (SELECT * FROM unnest(?))")
        params["currencies"] = currencies
    
    if market_ids:
        conditions.append("market_id IN (SELECT * FROM unnest(?))")
        params["market_ids"] = market_ids

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    result = conn.execute(query, params).pl()

    return result

def get_evaluated_positions(
    position_ids: Optional[List[int]] = None,
    market_ids: Optional[List[int]] = None,
    evaluation_dates: Optional[List[Union[str, dt.datetime, dt.date]]] = None,
    instrument_ids: Optional[List[str]] = None
) -> pl.DataFrame:
    """
    Retrieve evaluated positions based on optional filters.
    
    Args:
        position_ids (Optional[List[int]]): List of position IDs to filter by.
        market_ids (Optional[List[int]]): List of market IDs to filter by.
        evaluation_dates (Optional[List[Union[str, dt.datetime, dt.date]]]): List of evaluation dates to filter by.
        instrument_ids (Optional[List[str]]): List of instrument IDs to filter by.
    
    Returns:
        pl.DataFrame: DataFrame containing the evaluated positions.
    """
    conn = get_connection()
    
    query = "SELECT * FROM evaluated_positions"
    conditions = []
    params: Dict[str, Any] = {}
    
    if position_ids:
        conditions.append("position_id IN (SELECT * FROM unnest(?))")
        params["position_ids"] = position_ids
    
    if market_ids:
        conditions.append("market_id IN (SELECT * FROM unnest(?))")
        params["market_ids"] = market_ids
    
    if evaluation_dates:
        conditions.append("evaluation_date IN (SELECT * FROM unnest(?))")
        params["evaluation_dates"] = evaluation_dates
    
    if instrument_ids:
        conditions.append("instrument_id IN (SELECT * FROM unnest(?))")
        params["instrument_ids"] = instrument_ids

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    result = conn.execute(query, params).pl()

    return result
    
def check_evaluation_exists(
    position_id: int,
    market_id: int,
    evaluation_date: Union[str, dt.datetime, dt.date]
) -> bool:
    """
    Check if an evaluation exists for a given position and market on a specific date.
    
    Args:
        position_id (int): The ID of the position.
        market_id (int): The ID of the market.
        evaluation_date (Union[str, dt.datetime, dt.date]): The evaluation date.
    
    Returns:
        bool: True if the evaluation exists, False otherwise.
    """
    conn = get_connection()
    
    query = """
        SELECT COUNT(*)
        FROM evaluated_positions
        WHERE
            position_id = ?
            AND market_id = ?
            AND evaluation_date = ?
    """

    result = (
        conn.execute(
            query,
            [
                position_id,
                market_id,
                evaluation_date
            ]
        )
        .fetchone()
    )
    
    if result is None:
        return False
    return result[0] > 0

def get_latest_evaluation(
    instrument_id: str,
    currency: Optional[List[str]] = None
) -> pl.DataFrame:
    """
    Retrieve the latest evaluation for a given instrument ID and optional currency.
    
    Args:
        instrument_id (str): The ID of the instrument.
        currency (Optional[List[str]]): List of currencies to filter by.
    
    Returns:
        pl.DataFrame: DataFrame containing the latest evaluation.
    """
    conn = get_connection()
    
    query = """
        SELECT *
        FROM evaluated_positions
        WHERE position_id = (
            SELECT position_id
            FROM bond_positions
            WHERE instrument_id = ?
            {currency_condition}
            ORDER BY acquisition_date DESC
            LIMIT 1
        )
        ORDER BY evaluation_date DESC
        LIMIT 1
    """
    
    currency_condition = ""
    if currency:
        currency_condition = " AND currency IN (SELECT * FROM unnest(?))"
    
    result = (
        conn.execute(
            query.format(currency_condition=currency_condition),
            [instrument_id] + (currency if currency else [])
        )
        .pl()
    )
    
    return result
