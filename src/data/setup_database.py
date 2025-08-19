"""
Setup script for initializing DuckDB database with required tables and data.
Creates tables for bond positions, market term structures, and evaluated positions.
"""

import os
import duckdb
import yaml
import polars as pl
from pathlib import Path


def load_config() -> None:
    """
    Load configuration from YAML file.
    """
    config_path = Path(__file__).parent.parent.parent / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file {config_path} does not exist.")
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    return config


def create_directory_if_not_exists(path: str) -> None:
    """
    Create a directory if it does not exist.
    """
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def setup_database(connection_string: str) -> None:
    """
    Set up the DuckDB database with required tables.
    
    Args:
        connection_string (str): The connection string for the DuckDB database.
    """
    # Create Directory for Database (If it Doesn't Exist)
    create_directory_if_not_exists(connection_string)
    
    # Connect to the DuckDB Database
    conn = duckdb.connect(database=connection_string, read_only=False)

    # Check if Tables Exist, Drop Them
    conn.execute("DROP TABLE IF EXISTS evaluated_positions")
    conn.execute("DROP TABLE IF EXISTS bond_positions")
    conn.execute("DROP TABLE IF EXISTS market_term_structures")

    # Create Bond Positions Table
    conn.execute(
        """
        CREATE TABLE bond_positions (
            position_id INTEGER PRIMARY KEY,
            instrument_id VARCHAR,
            date DATE,
            notional DECIMAL(20, 2),
            face_value DECIMAL(20, 2),
            coupon_rate DECIMAL(5, 2),
            maturity_date DATE,
            frequency INTEGER,
            day_count_convention VARCHAR,
            settlement_days INTEGER,
            settlement_calendar VARCHAR,
            business_convention VARCHAR,
            compound_type VARCHAR,
            currency VARCHAR
        );
        """
    )

    conn.execute(
        """
        CREATE UNIQUE INDEX idx_bond_position_natural_key
        ON bond_positions (instrument_id, date);
        """
    )

    # Create Market Term Structures Table
    conn.execute(
        """
        CREATE TABLE market_term_structures (
            market_id INTEGER PRIMARY KEY,
            reference_date DATE,
            curve_type VARCHAR,
            curve_data JSON,
            curve_currency VARCHAR
        );
        """
    )

    # Create Evaluated Positions Table
    conn.execute(
        """
        CREATE TABLE evaluated_positions (
            eval_id INTEGER PRIMARY KEY,
            position_id INTEGER,
            market_id INTEGER,
            evaluation_date DATE,
            clean_price DECIMAL(20, 2),
            dirty_price DECIMAL(20, 2),
            accrued_interest DECIMAL(20, 2),
            yield DECIMAL(10, 6),
            mac_duration DECIMAL(10, 6),
            mod_duration DECIMAL(10, 6),
            convexity DECIMAL(10, 6),
            FOREIGN KEY (position_id) REFERENCES bond_positions(position_id),
            FOREIGN KEY (market_id) REFERENCES market_term_structures(market_id)
        );
        """
    )
    
    # Create an Index for Efficient Querying of Evaluated Positions
    conn.execute(
        """
        CREATE INDEX idx_evaluated_positions_natural_key
        ON evaluated_positions (position_id, market_id, evaluation_date);
        """
    )

    
