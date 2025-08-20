# bond_schema_duckdb.py
import duckdb
import yaml
from pathlib import Path


DDL = r"""
-- 1) Security master
CREATE TABLE IF NOT EXISTS dim_security_bond (
    security_id             BIGINT PRIMARY KEY,
    isin                    VARCHAR(12),
    cusip                   VARCHAR(9),
    figi                    VARCHAR(12),
    issuer                  VARCHAR(200),
    ticker                  VARCHAR(50),

    currency                CHAR(3) NOT NULL,
    face_value              DECIMAL(18,6) NOT NULL,
    coupon_rate             DECIMAL(9,6)  NOT NULL,
    coupon_type             VARCHAR(30)   NOT NULL DEFAULT 'Fixed',
    frequency               VARCHAR(20)   NOT NULL,
    day_count               VARCHAR(30)   NOT NULL,
    business_convention     VARCHAR(30)   NOT NULL,
    termination_convention  VARCHAR(30)   NOT NULL,
    end_of_month_flag       BOOLEAN       NOT NULL,
    date_rule               VARCHAR(30)   NOT NULL,
    first_date              DATE,
    next_to_last_date       DATE,

    issue_date              DATE NOT NULL,
    maturity_date           DATE NOT NULL,
    call_schedule_json      TEXT,
    amortization_json       TEXT,
    redemption_value        DECIMAL(18,6),

    country                 VARCHAR(50),
    sector                  VARCHAR(50),
    seniority               VARCHAR(50),
    ratings_json            TEXT,

    UNIQUE (isin),
    UNIQUE (cusip)
);

-- 2a) Trade / lot level
CREATE TABLE IF NOT EXISTS fact_position_lot (
    lot_id               BIGINT PRIMARY KEY,
    security_id          BIGINT NOT NULL,
    portfolio_id         BIGINT NOT NULL,
    account_id           BIGINT,
    trade_id             VARCHAR(64),

    trade_date           DATE NOT NULL,
    settle_date          DATE NOT NULL,
    quantity             DECIMAL(20,6) NOT NULL,
    clean_price_trade    DECIMAL(18,8) NOT NULL,
    accrued_at_trade     DECIMAL(18,8) NOT NULL,
    dirty_price_trade    DECIMAL(18,8) NOT NULL,  -- explicit (no computed columns)
    yield_at_trade       DECIMAL(18,10),
    z_spread_at_trade    DECIMAL(18,10),

    cost_local           DECIMAL(20,6) NOT NULL,
    fx_rate_trade        DECIMAL(18,10),
    cost_base_ccy        DECIMAL(20,6),

    book                 VARCHAR(50),
    strategy             VARCHAR(100),

    created_at           TIMESTAMP NOT NULL DEFAULT now(),

    FOREIGN KEY (security_id) REFERENCES dim_security_bond(security_id)
);

-- 2b) Daily position snapshots
CREATE TABLE IF NOT EXISTS fact_position_snapshot (
    as_of_date           DATE NOT NULL,
    portfolio_id         BIGINT NOT NULL,
    security_id          BIGINT NOT NULL,

    position_qty_eod     DECIMAL(20,6) NOT NULL,
    amortized_cost_local DECIMAL(20,6),
    pnl_ytd_local        DECIMAL(20,6),

    load_ts              TIMESTAMP NOT NULL DEFAULT now(),

    PRIMARY KEY (as_of_date, portfolio_id, security_id),
    FOREIGN KEY (security_id) REFERENCES dim_security_bond(security_id)
);

-- 3a) Quotes
CREATE TABLE IF NOT EXISTS md_quote_bond (
    as_of_ts            TIMESTAMP NOT NULL,
    security_id         BIGINT NOT NULL,
    source              VARCHAR(50) NOT NULL,
    quote_type          VARCHAR(20) NOT NULL,  -- CleanPrice | Yield | ZSpread
    value               DECIMAL(18,10) NOT NULL,

    PRIMARY KEY (as_of_ts, security_id, source, quote_type),
    FOREIGN KEY (security_id) REFERENCES dim_security_bond(security_id)
);

-- 3b) Curves & nodes
CREATE TABLE IF NOT EXISTS md_curve (
    curve_id            BIGINT PRIMARY KEY,
    market              VARCHAR(50) NOT NULL,  -- usd_govt, ois_usd, etc.
    tenor_type          VARCHAR(20) NOT NULL,  -- Zero | Discount | Forward
    day_count           VARCHAR(30) NOT NULL,
    compounding         VARCHAR(20) NOT NULL,
    frequency           VARCHAR(20) NOT NULL,
    build_method        VARCHAR(50) NOT NULL,  -- flat | bootstrap ...
    as_of_date          DATE NOT NULL,
    description         VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS md_curve_node (
    curve_id            BIGINT NOT NULL,
    node_label          VARCHAR(20) NOT NULL,  -- 6M, 1Y, 2Y, 3Y...
    zero_rate           DECIMAL(18,10),
    discount_factor     DECIMAL(18,15),

    PRIMARY KEY (curve_id, node_label),
    FOREIGN KEY (curve_id) REFERENCES md_curve(curve_id)
);

-- 3c) FX
CREATE TABLE IF NOT EXISTS md_fx_rate (
    as_of_ts            TIMESTAMP NOT NULL,
    ccy_from            CHAR(3) NOT NULL,
    ccy_to              CHAR(3) NOT NULL,
    rate                DECIMAL(18,10) NOT NULL,
    source              VARCHAR(50) NOT NULL,
    PRIMARY KEY (as_of_ts, ccy_from, ccy_to, source)
);

-- 4) Valuation run metadata
CREATE TABLE IF NOT EXISTS val_run (
    run_id              BIGINT PRIMARY KEY,
    as_of_date          DATE NOT NULL,
    portfolio_id        BIGINT,
    curve_id            BIGINT NOT NULL,
    fx_source           VARCHAR(50),
    pricing_dc          VARCHAR(30) NOT NULL,
    compounding         VARCHAR(20) NOT NULL,
    frequency           VARCHAR(20) NOT NULL,
    ql_version          VARCHAR(20),
    model_tag           VARCHAR(50),
    created_ts          TIMESTAMP NOT NULL DEFAULT now(),

    FOREIGN KEY (curve_id) REFERENCES md_curve(curve_id)
);

-- 4b) Per-security results
CREATE TABLE IF NOT EXISTS val_result_bond (
    run_id              BIGINT NOT NULL,
    portfolio_id        BIGINT NOT NULL,
    security_id         BIGINT NOT NULL,

    quantity            DECIMAL(20,6) NOT NULL,
    clean_price         DECIMAL(18,8) NOT NULL,
    accrued             DECIMAL(18,8) NOT NULL,
    dirty_price         DECIMAL(18,8) NOT NULL,  -- explicit
    yield_calc          DECIMAL(18,10) NOT NULL,
    z_spread            DECIMAL(18,10),
    duration_mod        DECIMAL(18,10),
    duration_mac        DECIMAL(18,10),
    convexity           DECIMAL(28,12),
    pvbp                DECIMAL(18,10),

    pv_local            DECIMAL(20,6) NOT NULL,
    pv_base_ccy         DECIMAL(20,6),

    PRIMARY KEY (run_id, portfolio_id, security_id),
    FOREIGN KEY (run_id) REFERENCES val_run(run_id),
    FOREIGN KEY (security_id) REFERENCES dim_security_bond(security_id)
);

-- 4c) Cash Flows
CREATE TABLE IF NOT EXISTS val_cash_flow_bond (
    run_id              BIGINT NOT NULL,
    portfolio_id        BIGINT NOT NULL,
    security_id         BIGINT NOT NULL,
    flow_num            INTEGER NOT NULL,  -- 1..N
    pay_date            DATE NOT NULL,
    flow_type           VARCHAR(20) NOT NULL,  -- Coupon | Redemption | Amort
    nominal             DECIMAL(20,6) NOT NULL,
    df                  DECIMAL(28,16) NOT NULL,
    pv_per100           DECIMAL(18,10) NOT NULL,

    PRIMARY KEY (run_id, portfolio_id, security_id, flow_num),
    FOREIGN KEY (run_id) REFERENCES val_run(run_id),
    FOREIGN KEY (security_id) REFERENCES dim_security_bond(security_id)
);
"""

def create_database(db_path: str = "bondbook.duckdb", overwrite: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Create/open the DuckDB database and build all tables.
    If overwrite is True, removes any pre-existing database file.
    Returns an open connection you can reuse.
    """
    db_file = Path(db_path)
    if overwrite and db_file.exists():
        db_file.unlink()
    conn = duckdb.connect(str(db_file))
    conn.execute(DDL)
    return conn

def insert_sample_data(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Inserts a small, coherent sample across all tables to test joins and reports.
    Adds more examples to each table.
    """
    # 1) Four sample bonds: 2 US Treasuries, 2 Corporates
    conn.execute("""
        INSERT INTO dim_security_bond VALUES
        (
            1001, 'US91282CEJ01', '91282CEJ0', NULL, 'United States Treasury', 'UST',
            'USD', 100.000000, 0.050000, 'Fixed', 'Semiannual', 'Act/Act', 'Following', 'Following', false, 'Backward',
            NULL, NULL,
            DATE '2024-01-15', DATE '2027-01-15', NULL, NULL, 100.000000,
            'USA', 'Government', 'Senior', '{"S&P":"AA+"}'
        ),
        (
            1002, 'US91282CFD01', '91282CFD0', NULL, 'United States Treasury', 'UST',
            'USD', 100.000000, 0.045000, 'Fixed', 'Semiannual', 'Act/Act', 'Following', 'Following', false, 'Backward',
            NULL, NULL,
            DATE '2023-07-15', DATE '2026-07-15', NULL, NULL, 100.000000,
            'USA', 'Government', 'Senior', '{"S&P":"AA+"}'
        ),
        (
            2001, 'US12345ABC01', '12345ABC0', NULL, 'Acme Corp', 'ACME',
            'USD', 100.000000, 0.060000, 'Fixed', 'Semiannual', '30/360', 'Following', 'Following', false, 'Backward',
            NULL, NULL,
            DATE '2023-09-15', DATE '2028-09-15', NULL, NULL, 100.000000,
            'USA', 'Industrial', 'Senior Unsecured', '{"S&P":"BBB+"}'
        ),
        (
            2002, 'US54321XYZ01', '54321XYZ0', NULL, 'Beta Corp', 'BETA',
            'USD', 100.000000, 0.055000, 'Fixed', 'Semiannual', '30/360', 'Following', 'Following', false, 'Backward',
            NULL, NULL,
            DATE '2022-03-15', DATE '2027-03-15', NULL, NULL, 100.000000,
            'USA', 'Industrial', 'Senior Unsecured', '{"S&P":"BBB"}'
        );
    """)

    # 2a) Lots
    conn.execute("""
        INSERT INTO fact_position_lot VALUES
        (
            50001, 1001, 10, NULL, 'T1',
            DATE '2024-01-16', DATE '2024-01-18', 100000.000000, 100.10000000, 0.00000000, 100.10000000,
            0.0498000000, NULL,
            100100.000000, 1.0000000000, 100100.000000,
            'BookA', 'Rates', now()
        ),
        (
            50002, 2001, 10, NULL, 'C1',
            DATE '2024-02-20', DATE '2024-02-22', 50000.000000, 99.50000000, 0.80000000, 100.30000000,
            0.0612000000, 0.0015000000,
            50150.000000, 1.0000000000, 50150.000000,
            'BookA', 'Credit', now()
        ),
        (
            50003, 1002, 11, NULL, 'T2',
            DATE '2023-07-16', DATE '2023-07-18', 75000.000000, 100.05000000, 0.00000000, 100.05000000,
            0.0448000000, NULL,
            75037.500000, 1.0000000000, 75037.500000,
            'BookB', 'Rates', now()
        ),
        (
            50004, 2002, 11, NULL, 'C2',
            DATE '2022-03-20', DATE '2022-03-22', 30000.000000, 98.90000000, 0.70000000, 99.60000000,
            0.0552000000, 0.0010000000,
            29880.000000, 1.0000000000, 29880.000000,
            'BookB', 'Credit', now()
        );
    """)

    # 2b) Daily snapshots (two days, all bonds)
    conn.execute("""
        INSERT INTO fact_position_snapshot VALUES
        (DATE '2025-08-18', 10, 1001, 100000.000000, NULL, NULL, now()),
        (DATE '2025-08-18', 10, 2001,  50000.000000, NULL, NULL, now()),
        (DATE '2025-08-18', 11, 1002,  75000.000000, NULL, NULL, now()),
        (DATE '2025-08-18', 11, 2002,  30000.000000, NULL, NULL, now()),
        (DATE '2025-08-19', 10, 1001, 100000.000000, NULL, NULL, now()),
        (DATE '2025-08-19', 10, 2001,  50000.000000, NULL, NULL, now()),
        (DATE '2025-08-19', 11, 1002,  75000.000000, NULL, NULL, now()),
        (DATE '2025-08-19', 11, 2002,  30000.000000, NULL, NULL, now());
    """)

    # 3a) Quotes (as timestamps, all bonds)
    conn.execute("""
        INSERT INTO md_quote_bond VALUES
        (TIMESTAMP '2025-08-19 16:00:00', 1001, 'Trader',  'CleanPrice', 100.25),
        (TIMESTAMP '2025-08-19 16:00:00', 1001, 'Trader',  'Yield',       0.0460),
        (TIMESTAMP '2025-08-19 16:00:00', 2001, 'TRACE',   'CleanPrice',  99.85),
        (TIMESTAMP '2025-08-19 16:00:00', 2001, 'TRACE',   'Yield',       0.0615),
        (TIMESTAMP '2025-08-19 16:00:00', 2001, 'Model',   'ZSpread',     0.0012),
        (TIMESTAMP '2025-08-19 16:00:00', 1002, 'Trader',  'CleanPrice', 100.10),
        (TIMESTAMP '2025-08-19 16:00:00', 1002, 'Trader',  'Yield',       0.0455),
        (TIMESTAMP '2025-08-19 16:00:00', 2002, 'TRACE',   'CleanPrice',  98.95),
        (TIMESTAMP '2025-08-19 16:00:00', 2002, 'TRACE',   'Yield',       0.0555),
        (TIMESTAMP '2025-08-19 16:00:00', 2002, 'Model',   'ZSpread',     0.0010);
    """)

    # 3b) Curve and nodes (add another curve)
    conn.execute("""
        INSERT INTO md_curve VALUES
        (9001, 'usd_govt', 'Zero', 'Act/Act', 'Compounded', 'Semiannual', 'flat', DATE '2025-08-19', 'USD Govvies flat-ish'),
        (9002, 'usd_corp', 'Zero', '30/360', 'Compounded', 'Semiannual', 'bootstrap', DATE '2025-08-19', 'USD Corporates curve');
        INSERT INTO md_curve_node VALUES
        (9001, '6M',  0.0430, NULL),
        (9001, '1Y',  0.0440, NULL),
        (9001, '2Y',  0.0450, NULL),
        (9001, '3Y',  0.0460, NULL),
        (9002, '6M',  0.0550, NULL),
        (9002, '1Y',  0.0560, NULL),
        (9002, '2Y',  0.0570, NULL),
        (9002, '3Y',  0.0580, NULL);
    """)

    # 3c) FX (identity for USD, add EUR/USD)
    conn.execute("""
        INSERT INTO md_fx_rate VALUES
        (TIMESTAMP '2025-08-19 16:00:00', 'USD', 'USD', 1.0000000000, 'ECB'),
        (TIMESTAMP '2025-08-19 16:00:00', 'EUR', 'USD', 1.0850000000, 'ECB');
    """)

    # 4) Valuation run (using both curves)
    conn.execute("""
        INSERT INTO val_run (
            run_id, as_of_date, portfolio_id, curve_id, fx_source, pricing_dc, compounding, frequency, ql_version, model_tag, created_ts
        ) VALUES
        (
            7001, DATE '2025-08-19', 10, 9001, 'ECB', 'Act/Act', 'Compounded', 'Semiannual', '1.33', 'DiscountingBondEngine', now()
        ),
        (
            7002, DATE '2025-08-19', 11, 9002, 'ECB', '30/360', 'Compounded', 'Semiannual', '1.33', 'DiscountingBondEngine', now()
        );
    """)

    # 4b) Results for all bonds
    conn.execute("""
        INSERT INTO val_result_bond VALUES
        (
            7001, 10, 1001,
            100000.000000, 100.20000000, 0.35000000, 100.55000000,
            0.0460000000, NULL, 2.85, 2.90, 14.20, 85.00,
            100550.000000, 100550.000000
        ),
        (
            7001, 10, 2001,
            50000.000000, 99.80000000, 0.75000000, 100.55000000,
            0.0615000000, 0.0012000000, 3.80, 3.95, 18.30, 92.00,
            50275.000000,  50275.000000
        ),
        (
            7002, 11, 1002,
            75000.000000, 100.10000000, 0.30000000, 100.40000000,
            0.0455000000, NULL, 2.60, 2.65, 13.00, 80.00,
            75300.000000, 75300.000000
        ),
        (
            7002, 11, 2002,
            30000.000000, 98.90000000, 0.65000000, 99.55000000,
            0.0555000000, 0.0010000000, 3.60, 3.75, 17.00, 90.00,
            29865.000000,  29865.000000
        );
    """)

    # 4c) Cash flows (a couple per bond as example)
    conn.execute("""
        INSERT INTO val_cash_flow_bond VALUES
        (7001, 10, 1001, 1, DATE '2025-07-15', 'Coupon',     2.500000, 0.9975000000000000, 2.4937500000),
        (7001, 10, 1001, 2, DATE '2026-01-15', 'Coupon',     2.500000, 0.9920000000000000, 2.4800000000),
        (7001, 10, 1001, 3, DATE '2027-01-15', 'Redemption', 100.000000,0.9600000000000000,96.0000000000),
        (7001, 10, 2001, 1, DATE '2025-03-15', 'Coupon',     3.000000, 0.9950000000000000, 2.9850000000),
        (7001, 10, 2001, 2, DATE '2025-09-15', 'Coupon',     3.000000, 0.9850000000000000, 2.9550000000),
        (7001, 10, 2001, 3, DATE '2028-09-15', 'Redemption', 100.000000,0.9150000000000000,91.5000000000),
        (7002, 11, 1002, 1, DATE '2024-07-15', 'Coupon',     2.250000, 0.9960000000000000, 2.2410000000),
        (7002, 11, 1002, 2, DATE '2025-07-15', 'Coupon',     2.250000, 0.9910000000000000, 2.2297500000),
        (7002, 11, 1002, 3, DATE '2026-07-15', 'Redemption', 100.000000,0.9500000000000000,95.0000000000),
        (7002, 11, 2002, 1, DATE '2023-09-15', 'Coupon',     2.750000, 0.9940000000000000, 2.7335000000),
        (7002, 11, 2002, 2, DATE '2024-09-15', 'Coupon',     2.750000, 0.9840000000000000, 2.7060000000),
        (7002, 11, 2002, 3, DATE '2027-03-15', 'Redemption', 100.000000,0.9100000000000000,91.0000000000);
    """)


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


def main():
    config = load_config()
    conn = create_database(
        db_path=config["database"]["connection_string"],
        overwrite=True
    )
    insert_sample_data(conn)
    
    print(conn.sql("SELECT * FROM dim_security_bond").show())
    print(conn.sql("SELECT * FROM fact_position_lot").show())
    print(conn.sql("SELECT * FROM val_result_bond").show())

if __name__ == "__main__":
    main()
