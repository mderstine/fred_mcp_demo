"""
Utility Functions for Bond Evaluator Platform
Provides Higher-Level Operations Combining Multiple Database Operations
"""

import polars as pl
from typing import List, Dict, Any, Optional, Union
import datetime as dt
from src.data.retrieval import(
    get_bond_positions,
    get_evaluated_positions
)


def get_all_instruments() -> List[str]:
    """
    Get a list of all instrument IDs from the bond positions.

    Returns:
        List[str]: List of instrument IDs.
    """
    bond_positions = get_bond_positions()
    return bond_positions["instrument_id"].unique().to_list()


def get_all_currencies() -> List[str]:
    """
    Get a list of all currencies from the bond positions.

    Returns:
        List[str]: List of unique currencies.
    """
    bond_positions = get_bond_positions()
    return bond_positions["currency"].unique().to_list()


def get_missing_evaluations(
    evaluation_date: Union[str, dt.datetime, dt.date],
    market_id: int
) -> pl.DataFrame:
    """
    Get bond positions that are missing evaluations for a specific date and market.

    Args:
        evaluation_date (Union[str, dt.datetime, dt.date]): The date for which to check evaluations.
        market_id (int): The market ID to filter by.

    Returns:
        pl.DataFrame: DataFrame containing bond positions missing evaluations.
    """

    # Get All Bond Positions
    bond_positions = get_bond_positions()

    # Get Evaluated Positions for the Given Date and Market
    evaluated = get_evaluated_positions(
        market_ids=[market_id],
        evaluation_dates=[evaluation_date]
    )

    # If There are Evaluated Positions, Find Ones that are Missing
    if len(evaluated) > 0:

        # Extract Position IDs that Have Evaluated
        evaluated_position_ids = evaluated["position_id"].unique().to_list()

        # Filter Positions With Evaluations
        missing_positions = (
            bond_positions
            .filter(
                ~pl.col("position_id")
                .is_in(evaluated_position_ids)
            )
        )
    else:
        # No Evaluations Exist, All Positions Need Evaluation
        missing_positions = bond_positions

    return missing_positions
         
      
def get_portfolio_evaluation_summary(
    evaluation_date: Union[str, dt.datetime, dt.date],
    market_id: int,
    currency: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Get a summary of portfolio evaluations for a specific date and market.

    Args:
        evaluation_date (Union[str, dt.datetime, dt.date]): The date for which to get the summary.
        market_id (int): The market ID to filter by.
        currency (Optional[List[str]]): List of currencies to filter by.

    Returns:
        Dict[str, Any]: Summary statistics including total positions, total value, etc.
    """

    # Get Evaluated Positions
    params = {
        "market_ids": [market_id],
        "evaluation_dates": [evaluation_date]
    }

    if currency:
        # For Currency Filtering, We Need Instrument IDs First
        positions = get_bond_positions(currencies=currency)

        if len(positions) == 0:
            return {"error": "No evaluations found for the specified criteria."}
        
        position_ids = positions["position_id"].unique().to_list()
        params["position_ids"] = position_ids

    evaluations = get_evaluated_positions(**params)

    if len(evaluations) == 0:
        return {"error": "No evaluations found for the specified criteria."}

    # Calculate Summary Statistics
    total_positions = len(evaluations)
    total_notional = evaluations["notional"].sum()
    total_value = evaluations["clean_price"].sum()
    avg_yield = evaluations["yield"].mean()
    total_mac_duration = evaluations["mac_duration"].sum()
    total_mod_duration = evaluations["mod_duration"].sum()
    total_convexity = evaluations["convexity"].sum()
    avg_mac_duration = evaluations["mac_duration"].mean()
    avg_mod_duration = evaluations["mod_duration"].mean()
    avg_convexity = evaluations["convexity"].mean()

    return {
        "evaluation_date": evaluation_date,
        "market_id": market_id,
        "currency": currency,
        "total_positions": total_positions,
        "total_notional": total_notional,
        "total_value": total_value,
        "avg_yield": avg_yield,
        "total_mac_duration": total_mac_duration,
        "total_mod_duration": total_mod_duration,
        "total_convexity": total_convexity,
        "avg_mac_duration": avg_mac_duration,
        "avg_mod_duration": avg_mod_duration,
        "avg_convexity": avg_convexity
    }


def get_historical_evaluations(
    instrument_id: str,
    market_id: int,
    start_date: Union[str, dt.datetime, dt.date],
    end_date: Union[str, dt.datetime, dt.date]
) -> pl.DataFrame:
    """
    Get historical evaluations for a specific instrument and market within a date range.

    Args:
        instrument_id (str): The ID of the instrument to filter by.
        market_id (int): The market ID to filter by.
        start_date (Union[str, dt.datetime, dt.date]): Start date for the evaluation range.
        end_date (Union[str, dt.datetime, dt.date]): End date for the evaluation range.

    Returns:
        pl.DataFrame: DataFrame containing historical evaluations.
    """
    
    # Get Position IDs for the Instrument
    positions = get_bond_positions(instrument_ids=[instrument_id])

    if len(positions) == 0:
        return pl.DataFrame()
    
    position_id = positions["position_id"].unique().item()

    # Get Historical Evaluations
    # Filter by Date Range
    # Sort by Evaluation Date
    
    evaluations = (
        get_evaluated_positions(
            position_ids=[position_id],
            market_ids=[market_id]
        )
        .filter(
            (pl.col("evaluation_date") >= start_date) &
            (pl.col("evaluation_date") <= end_date)
        )
        .sort("evaluation_date")
    )
    
    return evaluations
