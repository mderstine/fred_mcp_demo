"""
MCP Tools for Bond Evaluatior Platform Using FastMCP
Exposes Bond Evaluation and Data Retrieval Functionality as MCP Tools
"""

from typing import List, Optional, Union
import json
from fastmcp import FastMCP
import datetime as dt
from src.data.retrieval import (
    get_bond_positions,
    get_market_term_structures,
    get_evaluated_positions,
    check_evaluation_exists,
    get_latest_evaluation
)
from src.data.utils import (
    get_all_instruments,
    get_all_currencies,
    get_missing_evaluations,
    get_portfolio_evaluation_summary,
    get_historical_evaluations
)


# Initialize FastMCP Server
server = FastMCP("Bond-Evaluator")


@server.tool()
def get_bond_positions_tool(
    instrument_ids: Optional[List[str]] = None,
    dates: Optional[List[Union[str, dt.datetime, dt.date]]] = None,
    currencies: Optional[List[str]] = None,
    position_ids: Optional[List[int]] = None
) -> str:
    """
    Get Bond Positions with Optional Filters.

    Args:
        instrument_ids (Optional[List[str]]): Filter by specific instrument IDs.
        dates (Optional[List[Union[str, dt.datetime, dt.date]]]): Filter by specific dates.
        currencies (Optional[List[str]]): Filter by specific currencies.
        position_ids (Optional[List[int]]): Filter by specific position IDs.

    Returns:
        str: JSON string of bond positions.
    """

    try:
        df = get_bond_positions(
            instrument_ids=instrument_ids,
            dates=dates,
            currencies=currencies,
            position_ids=position_ids
        )

        return df.to_pandas().to_json(orient="records", date_format="iso")
    except Exception as e:
        return json.dumps({"error": str(e)})
    

@server.tool()
def get_market_term_structures_tool(
    reference_dates: Optional[List[Union[str, dt.datetime, dt.date]]] = None,
    curve_types: Optional[List[str]] = None,
    currencies: Optional[List[str]] = None,
    market_ids: Optional[List[int]] = None
) -> str:
    """
    Retrieve market term structures based on optional filters.

    Args:
        reference_dates (Optional[List[Union[str, dt.datetime, dt.date]]]): List of reference dates to filter by.
        curve_types (Optional[List[str]]): List of curve types to filter by.
        currencies (Optional[List[str]]): List of currencies to filter by.
        market_ids (Optional[List[int]]): List of market IDs to filter by.

    Returns:
        str: JSON string of market term structures.
    """
    try:
        df = get_market_term_structures(
            reference_dates=reference_dates,
            curve_types=curve_types,
            currencies=currencies,
            market_ids=market_ids
        )

        return df.to_pandas().to_json(orient="records", date_format="iso")
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def get_evaluated_positions_tool(
    position_ids: Optional[List[int]] = None,
    market_ids: Optional[List[int]] = None,
    evaluation_dates: Optional[List[Union[str, dt.datetime, dt.date]]] = None,
    instrument_ids: Optional[List[str]] = None
) -> str:
    """
    Get Evaluated Positions with Optional Filters.

    Args:
        position_ids (Optional[List[int]]): Filter by specific position IDs.
        market_ids (Optional[List[int]]): Filter by specific market IDs.
        evaluation_dates (Optional[List[Union[str, dt.datetime, dt.date]]]): Filter by specific evaluation dates.
        instrument_ids (Optional[List[str]]): Filter by specific instrument IDs.

    Returns:
        str: JSON string of evaluated positions.
    """
    try:
        df = get_evaluated_positions(
            position_ids=position_ids,
            market_ids=market_ids,
            evaluation_dates=evaluation_dates,
            instrument_ids=instrument_ids
        )

        return df.to_pandas().to_json(orient="records", date_format="iso")
    except Exception as e:
        return json.dumps({"error": str(e)})
    

@server.tool()
def check_evaluation_exists_tool(
    position_id: int,
    market_id: int,
    evaluation_date: Union[str, dt.datetime, dt.date]
) -> str:
    """
    Check if an evaluation exists for a given position, market, and date.

    Args:
        position_id (int): Position ID to check.
        market_id (int): Market ID to check.
        evaluation_date (Union[str, dt.datetime, dt.date]): Evaluation date to check.

    Returns:
        str: JSON string with boolean result.
    """
    try:
        exists = check_evaluation_exists(position_id, market_id, evaluation_date)
        return json.dumps({"exists": exists})
    except Exception as e:
        return json.dumps({"error": str(e)})
    

@server.tool()
def get_latest_evaluation_tool(
    instrument_id: str,
    currency: Optional[str] = None
) -> str:
    """
    Get the latest evaluation for a specific instrument and currency.

    Args:
        instrument_id (str): Instrument ID to get the latest evaluation for.
        currency (Optional[str]): Currency to filter by.

    Returns:
        str: JSON string of the latest evaluation.
    """
    try:
        df = get_latest_evaluation(instrument_id, currency)
        return df.to_pandas().to_json(orient="records", date_format="iso")
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def get_all_instruments_tool() -> str:
    """
    Get all available instruments.

    Returns:
        str: JSON string of all instruments.
    """
    try:
        instruments = get_all_instruments()
        return json.dumps({"instruments": instruments})
    except Exception as e:
        return json.dumps({"error": str(e)})
    

@server.tool()
def get_all_currencies_tool() -> str:
    """
    Get all available currencies.

    Returns:
        str: JSON string of all currencies.
    """
    try:
        currencies = get_all_currencies()
        return json.dumps({"currencies": currencies})
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def get_missing_evaluations_tool(
   evaluation_date: Union[str, dt.datetime, dt.date],
   market_id: int,
) -> str:
    """
    Get bond positions that are missing evaluations for a specific date and market.

    Args:
        evaluation_date (Union[str, dt.datetime, dt.date]): The date for which to check evaluations.
        market_id (int): The market ID to filter by.

    Returns:
        str: JSON string of bond positions missing evaluations.
    """
    try:
        df = get_missing_evaluations(evaluation_date, market_id)
        return df.to_pandas().to_json(orient="records", date_format="iso")
    except Exception as e:
        return json.dumps({"error": str(e)})
    

@server.tool()
def get_portfolio_evaluation_summary_tool(
    evaluation_date: Union[str, dt.datetime, dt.date],
    market_id: int,
    currency: Optional[List[str]] = None
) -> str:
    """
    Get a summary of portfolio evaluations for a specific date and market.

    Args:
        evaluation_date (Union[str, dt.datetime, dt.date]): The date for which to get the summary.
        market_id (int): The market ID to filter by.
        currency (Optional[List[str]]): List of currencies to filter by.

    Returns:
        str: JSON string of the portfolio evaluation summary.
    """
    try:
        summary = get_portfolio_evaluation_summary(evaluation_date, market_id, currency)
        return json.dumps(summary)
    except Exception as e:
        return json.dumps({"error": str(e)})
    

@server.tool()
def get_historical_evaluations_tool(
    instrument_id: str,
    start_date: Union[str, dt.datetime, dt.date],
    end_date: Union[str, dt.datetime, dt.date]
) -> str:
    """
    Get historical evaluations for a specific instrument within a date range.

    Args:
        instrument_id (str): The ID of the instrument.
        start_date (Union[str, dt.datetime, dt.date]): Start date of the range.
        end_date (Union[str, dt.datetime, dt.date]): End date of the range.
        currency (Optional[str]): Currency to filter by.

    Returns:
        str: JSON string of historical evaluations.
    """
    try:
        df = get_historical_evaluations(instrument_id, start_date, end_date)
        return df.to_pandas().to_json(orient="records", date_format="iso")
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def evaluate_bond_position_tool(
    position_id: int,
    market_id: int,
    evaluation_date: Union[str, dt.datetime, dt.date],
    force_recalculate: bool = False
) -> str:
    """
    Evaluate a specific bond position for a given market and date using QuantLib pricing.

    Args:
        position_id (int): The ID of the bond position to evaluate.
        market_id (int): The ID of the market for evaluation.
        evaluation_date (Union[str, dt.datetime, dt.date]): The date for which to evaluate the position.
        force_recalculate (bool): Whether to force recalculation even if an evaluation already exists.

    Returns:
        str: JSON string of the evaluation result.
    """

    # Use QuantLib to Price Bond

    # Use QuantLib to Calculate Macaulay Duration
    # Use QuantLib to Calculate Modified Duration
    # Use QuantLib to Calculate Convexity
    return json.dumps(
        {
            "status": "evaluation not implemented",
            "message": "QuantLib pricing not yet integrated.",
            "position_id": position_id,
            "market_id": market_id,
            "evaluation_date": str(evaluation_date),
        }
    )
