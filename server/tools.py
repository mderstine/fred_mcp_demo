# server/tools.py
from __future__ import annotations
import json
import hashlib
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import duckdb
import QuantLib as ql
from fastmcp import FastMCP

DB_PATH = Path(__file__).resolve().parents[1] / "markets.duckdb"
server = FastMCP("pricing")

# ---------------- DuckDB Helpers ----------------

def _connect():
    return duckdb.connect(DB_PATH.as_posix(), read_only=False)

def _ensure_prices_table():
    with _connect() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                "asof" DATE,
                market TEXT,                 -- nullable when pricing with explicit curve only
                instrument_key TEXT,         -- stable hash of bond terms
                clean_price DOUBLE,
                dirty_price DOUBLE,
                accrued DOUBLE,
                ytm DOUBLE,
                PRIMARY KEY ("asof", market, instrument_key)
            );
        """)

def _ensure_curves_table():
    with _connect() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS curves (
                market TEXT,
                t DOUBLE,          -- time in years
                rate DOUBLE,       -- zero rate (decimal)
                PRIMARY KEY (market, t)
            );
        """)

# call this at module import so tables exist before any tool runs
_ensure_curves_table()
_ensure_prices_table()

def _get_curve_points(market: str) -> List[Tuple[float, float]]:
    """Return [(t_years, rate_decimal), ...] sorted by t for a market."""
    with _connect() as con:
        rows = con.execute(
            "SELECT t, rate FROM curves WHERE lower(market)=lower(?) ORDER BY t",
            [market],
        ).fetchall()
    return [(float(t), float(r)) for (t, r) in rows]

@server.tool()
def get_curve(market: str) -> List[Dict[str, float]]:
    """Return zero curve for `market` as [{t, rate}] (t in years, rate as decimal)."""
    pts = _get_curve_points(market)
    return [{"t": t, "rate": r} for (t, r) in pts]

@server.tool()
def put_curve(
    market: str,
    curve: list,                     # list of {"t": float, "rate": float} OR [ [t, rate], ... ]
    mode: str = "replace",           # "replace" or "append"
) -> dict:
    """
    Insert or refresh a market zero curve in DuckDB.

    - mode="replace": delete existing rows for `market`, then insert new points.
    - mode="append": insert new points; if a (market, t) exists, it is replaced.

    Returns: {"market":..., "points": N, "mode": "..."}
    """
    if not isinstance(market, str) or not market.strip():
        raise ValueError("market must be a non-empty string")
    if not isinstance(curve, list) or not curve:
        raise ValueError("curve must be a non-empty list")

    # normalize points
    pts = []
    for p in curve:
        if isinstance(p, dict):
            t = float(p["t"])
            r = float(p["rate"])
        elif isinstance(p, (list, tuple)) and len(p) == 2:
            t = float(p[0]); r = float(p[1])
        else:
            raise ValueError("each curve point must be {'t':..., 'rate':...} or [t, rate]")
        if not (t >= 0.0):
            raise ValueError(f"t must be >= 0, got {t}")
        pts.append((t, r))

    _ensure_curves_table()

    # transaction for atomic replace/upsert
    with _connect() as con:
        con.execute("BEGIN;")
        try:
            if mode.lower() == "replace":
                con.execute("DELETE FROM curves WHERE lower(market)=lower(?);", [market])
            # upsert semantics: replace on conflict of (market, t)
            con.executemany(
                "INSERT OR REPLACE INTO curves(market, t, rate) VALUES (?, ?, ?);",
                [(market, t, r) for (t, r) in pts]
            )
            con.execute("COMMIT;")
        except Exception:
            con.execute("ROLLBACK;")
            raise

    return {"market": market, "points": len(pts), "mode": mode.lower()}

# ---------------- QuantLib mapping helpers ----------------

_CALENDARS = {
    "TARGET": ql.TARGET(),
    "UnitedStates": ql.UnitedStates(ql.UnitedStates.GovernmentBond),
    "UnitedStates/Settlement": ql.UnitedStates(ql.UnitedStates.Settlement),
    "UnitedStates/GovernmentBond": ql.UnitedStates(ql.UnitedStates.GovernmentBond),
    "UnitedStates/NYSE": ql.UnitedStates(ql.UnitedStates.NYSE),
}
_DAYCOUNTS = {
    "Actual365Fixed": ql.Actual365Fixed(),
    "Actual360": ql.Actual360(),
    "Thirty360": ql.Thirty360(ql.Thirty360.BondBasis),
    "ActualActual": ql.ActualActual(ql.ActualActual.Bond),
}
_FREQ = {
    "Annual": ql.Annual,
    "Semiannual": ql.Semiannual,
    "Quarterly": ql.Quarterly,
    "Monthly": ql.Monthly,
}
_BDC = {
    "Following": ql.Following,
    "ModifiedFollowing": ql.ModifiedFollowing,
    "Preceding": ql.Preceding,
    "Unadjusted": ql.Unadjusted,
}

def _parse_date(iso: str) -> ql.Date:
    y, m, d = map(int, iso.split("-"))
    return ql.Date(d, m, y)

def _to_dates_from_years(eval_date: ql.Date, years: List[float], cal: ql.Calendar) -> List[ql.Date]:
    """Map year fractions to dates relative to eval_date (approx t*365 days)."""
    out = []
    for t in years:
        days = int(round(t * 365))
        out.append(cal.advance(eval_date, ql.Period(days, ql.Days)))
    return out

def _build_zero_curve(
    points: List[Tuple[float, float]],
    calendar: ql.Calendar,
    day_count: ql.DayCounter,
    evaluation_date: ql.Date,
) -> ql.YieldTermStructureHandle:
    if not points:
        raise ValueError("No curve points provided.")
    eval_adj = calendar.adjust(evaluation_date)
    ql.Settings.instance().evaluationDate = eval_adj

    ts = sorted(points, key=lambda x: x[0])
    years = [t for (t, r) in ts]
    rates = [r for (t, r) in ts]
    dates = _to_dates_from_years(eval_adj, years, calendar)

    zero = ql.ZeroCurve(dates, rates, day_count, calendar)
    return ql.YieldTermStructureHandle(zero)

def _make_engine(curve: ql.YieldTermStructureHandle) -> ql.DiscountingBondEngine:
    return ql.DiscountingBondEngine(curve)

def _fixed_rate_bond_price(
    face: float,
    coupon: float,
    frequency: str,
    issue_date: str,
    maturity_date: str,
    calendar: str,
    day_count: str,
    bdc: str,
    settlement_days: int,
    curve: ql.YieldTermStructureHandle,
    evaluation_date: ql.Date,
) -> Dict[str, float]:
    cal = _CALENDARS.get(calendar, ql.UnitedStates())
    dc = _DAYCOUNTS.get(day_count, ql.Actual365Fixed())
    freq = _FREQ.get(frequency, ql.Semiannual)
    conv = _BDC.get(bdc, ql.Following)

    ql.Settings.instance().evaluationDate = cal.adjust(evaluation_date)

    issue = _parse_date(issue_date)
    maturity = _parse_date(maturity_date)

    schedule = ql.Schedule(
        issue, maturity, ql.Period(freq),
        cal, conv, conv,
        ql.DateGeneration.Backward, False,
    )

    bond = ql.FixedRateBond(settlement_days, face, schedule, [coupon], dc)
    bond.setPricingEngine(_make_engine(curve))

    clean = bond.cleanPrice()
    dirty = bond.dirtyPrice()
    accrued = bond.accruedAmount()
    try:
        ytm = bond.bondYield(dc, ql.Compounded, freq)
    except Exception:
        ytm = float("nan")

    return {"clean_price": clean, "dirty_price": dirty, "accrued": accrued, "ytm": ytm}

# ---------------- Caching (prices table) ----------------

def _instrument_key(
    *,
    face: float,
    coupon: float,
    frequency: str,
    issue_date: str,
    maturity_date: str,
    calendar: str,
    day_count: str,
    business_day_convention: str,
    settlement_days: int,
) -> str:
    """
    Create a stable key from bond terms so the same instrument hashes identically.
    """
    payload = {
        "face": round(float(face), 10),
        "coupon": round(float(coupon), 10),
        "frequency": str(frequency),
        "issue_date": str(issue_date),
        "maturity_date": str(maturity_date),
        "calendar": str(calendar),
        "day_count": str(day_count),
        "bdc": str(business_day_convention),
        "settlement_days": int(settlement_days),
    }
    raw = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def _lookup_cached_price(asof: str, market: Optional[str], instrument_key: str) -> Optional[Dict[str, float]]:
    _ensure_prices_table()
    with _connect() as con:
        if market is None:
            rows = con.execute(
                "SELECT clean_price, dirty_price, accrued, ytm "
                "FROM prices WHERE asof = ? AND market IS NULL AND instrument_key = ?",
                [asof, instrument_key],
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT clean_price, dirty_price, accrued, ytm "
                "FROM prices WHERE asof = ? AND market = ? AND instrument_key = ?",
                [asof, market, instrument_key],
            ).fetchall()
    if not rows:
        return None
    cp, dp, ac, ytm = rows[0]
    return {"clean_price": float(cp), "dirty_price": float(dp), "accrued": float(ac), "ytm": float(ytm)}

def _persist_price(asof: str, market: Optional[str], instrument_key: str, result: Dict[str, float]) -> None:
    _ensure_prices_table()
    with _connect() as con:
        con.execute(
            "INSERT OR REPLACE INTO prices(asof, market, instrument_key, clean_price, dirty_price, accrued, ytm) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                asof, market, instrument_key,
                float(result["clean_price"]),
                float(result["dirty_price"]),
                float(result["accrued"]),
                float(result.get("ytm", float("nan"))),
            ],
        )

# ---------------- Tool: price_bond with cache ----------------

@server.tool()
def price_bond(
    # Curve source
    market: Optional[str] = None,
    curve: Optional[List[Dict[str, float]]] = None,   # [{"t":..., "rate":...}]
    # Bond params
    face: float = 100.0,
    coupon: float = 0.05,                # decimal (e.g., 0.05 = 5%)
    frequency: str = "Semiannual",       # Annual|Semiannual|Quarterly|Monthly
    issue_date: str = "2024-01-15",
    maturity_date: str = "2027-01-15",
    calendar: str = "UnitedStates",
    day_count: str = "Actual365Fixed",
    business_day_convention: str = "Following",
    settlement_days: int = 2,
    # Valuation / cache
    valuation_date: Optional[str] = None,  # YYYY-MM-DD; default = today
    persist: bool = False,                  # if True, save computed price into prices table
) -> Dict[str, float]:
    """
    Price a FixedRateBond using a zero curve, with DB cache:
      1) Look in DuckDB 'prices' by (asof=valuation_date, market, instrument_key).
      2) If found -> return cached price (source="db").
      3) Else -> build curve and compute, return (source="computed"). If persist=True, upsert into prices.

    Provide either market=<name> (reads a curve from DuckDB) OR curve=[{t,rate},...].
    coupon is annual (decimal). frequency controls schedule. Dates must be YYYY-MM-DD.
    """
    # 0) evaluation date
    if valuation_date:
        eval_date = _parse_date(valuation_date)
        asof = valuation_date
    else:
        eval_date = ql.Date.todaysDate()
        asof = f"{eval_date.year()}-{int(eval_date.month()):02d}-{int(eval_date.dayOfMonth()):02d}"

    # 1) build instrument key from bond terms
    ikey = _instrument_key(
        face=face,
        coupon=coupon,
        frequency=frequency,
        issue_date=issue_date,
        maturity_date=maturity_date,
        calendar=calendar,
        day_count=day_count,
        business_day_convention=business_day_convention,
        settlement_days=settlement_days,
    )

    # 2) try cache first (only if market is provided or we accept market None)
    cached = _lookup_cached_price(asof=asof, market=market, instrument_key=ikey)
    if cached:
        cached["source"] = "db"
        cached["asof"] = asof
        cached["market"] = market
        cached["instrument_key"] = ikey
        return cached

    # 3) load curve points
    cal = _CALENDARS.get(calendar, ql.UnitedStates())
    dc = _DAYCOUNTS.get(day_count, ql.Actual365Fixed())

    if curve:
        pts = [(float(p["t"]), float(p["rate"])) for p in curve]
    elif market:
        pts = _get_curve_points(market)
        if not pts:
            raise ValueError(f"No curve found for market={market}")
    else:
        raise ValueError("Provide either market or explicit curve points.")

    # 4) build term structure at valuation date & price
    yts = _build_zero_curve(pts, cal, dc, evaluation_date=eval_date)
    result = _fixed_rate_bond_price(
        face=face,
        coupon=coupon,
        frequency=frequency,
        issue_date=issue_date,
        maturity_date=maturity_date,
        calendar=calendar,
        day_count=day_count,
        bdc=business_day_convention,
        settlement_days=settlement_days,
        curve=yts,
        evaluation_date=eval_date,
    )

    result.update(
        {
            "source": "computed",
            "asof": asof,
            "market": market,
            "instrument_key": ikey,
        }
    )

    # 5) optionally persist
    if persist:
        _persist_price(asof=asof, market=market, instrument_key=ikey, result=result)

    return result
