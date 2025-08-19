"""
Setup script for initializing DuckDB database with required tables and data.
Creates tables for bond positions, market term structures, and evaluated positions.
"""

import os
import duckdb
import yaml
import polars as pl
from pathlib import Path


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
            acquisition_date DATE,
            termination_date DATE NULL,
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
        ON bond_positions (instrument_id, acquisition_date);
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

    
def create_sample_data(connection_string: str) -> None:
    """
    Create sample data for bond positions and market term structures.
    
    Args:
        connection_string (str): The connection string for the DuckDB database.
    """
    conn = duckdb.connect(database=connection_string, read_only=False)

    # Sample Bond Positions
    sample_bond_positions = pl.DataFrame(
        {
            "position_id": [1, 2, 3],
            "instrument_id": [
                "GOVT-USD-05Y",         # Bond 1: Government bond
                "CORP-ABC-USD-10Y",     # Bond 2: Corporate bond
                "GOVT-EUR-07Y"          # Bond 3: Government bond
            ],
            "acquisition_date": ["2025-01-15", "2025-02-12", "2024-12-26"],
            "termination_date": [None, None, None],
            "notional": [1000000.00, 500000.00, 750000.00],
            "face_value": [1000.00, 1000.00, 1000.00],
            "coupon_rate": [0.0275, 0.0325, 0.0150],
            "maturity_date": ["2030-01-01", "2035-01-01", "2030-12-31"],
            "frequency": [2, 2, 1],  # Semi-Annual, Semi-Annual, Annual
            "day_count_convention": ["Actual/Actual", "30/360", "Actual/360"],
            "settlement_days": [2, 2, 2],
            "settlement_calendar": ["US", "US", "TARGET"],
            "business_convention": ["Modified Following", "Modified Following", "Following"],
            "compound_type": ["Compounded", "Compounded", "Simple"],
            "currency": ["USD", "USD", "EUR"]
        }
    )

    # Insert Sample Bond Positions
    conn.execute(
        """
        INSERT INTO bond_positions
        SELECT * FROM sample_bond_positions;
        """
    )

    # Sample Market Term Structures
    sample_market_term_structures = pl.DataFrame(
        {
            "market_id": [1, 2],
            "reference_date": ["2025-08-11", "2025-08-12"],
            "curve_type": ["ZeroCurve", "ZeroCurve"],
            "curve_data": [
                '{"tenors": ["1M", "3M", "6M", "1Y", "2Y", "3Y", "5Y", "10Y"], "rates": [0.01, 0.015, 0.02, 0.025, 0.03, 0.035, 0.04, 0.045]}',
                '{"tenors": ["1M", "3M", "6M", "1Y", "2Y", "3Y", "5Y", "10Y"], "rates": [0.012, 0.017, 0.022, 0.027, 0.032, 0.037, 0.042, 0.047]}'
            ],
            "curve_currency": ["USD", "EUR"]
        }
    )

    # Insert Sample Market Term Structures
    conn.execute(
        """
        INSERT INTO market_term_structures
        SELECT * FROM sample_market_term_structures;
        """
    )

def main():
    """
    Main function to set up the database and create sample data.
    """
    config = load_config()
    connection_string = config["database"]["connection_string"]
    
    setup_database(connection_string)
    create_sample_data(connection_string)
    
    print(f"Database Setup Complete. Data stored in {connection_string}.")


if __name__ == "__main__":
    main()
