import QuantLib as ql
import datetime as dt
from typing import Union


def interpret_date(date: Union[str, dt.datetime, dt.date]) -> ql.Date:
    """
    Convert a date string or datetime object to a QuantLib Date.
    
    Args:
        date (Union[str, dt.datetime, dt.date]): The date to convert.
    
    Returns:
        ql.Date: The corresponding QuantLib Date.
    """
    if isinstance(date, (dt.datetime, dt.date)):
        return ql.Date(date.day, date.month, date.year)
    elif isinstance(date, str):
        return ql.DateParser.parseISO(date)
    else:
        raise ValueError("date must be a string, datetime, or date object.")
    

def imply_calendar_from_currency(currency: str) -> ql.Calendar:
    """
    Get the appropriate QuantLib calendar based on the currency.
    
    Args:
        currency (str): The currency code (e.g., "USD", "EUR").
    
    Returns:
        ql.Calendar: The corresponding QuantLib Calendar.
    """
    if currency == "USD":
        return ql.UnitedStates(ql.UnitedStates.GovernmentBond)
    elif currency == "EUR":
        return ql.TARGET()
    else:
        raise ValueError(f"Unsupported currency: {currency}. Supported currencies are 'USD' and 'EUR'.")
    

def create_bond_schedule(
    issue_date: dt.date,
    maturity_years: int,
    frequency: int,
    calendar: ql.Calendar
) -> ql.Schedule:
    """
    Create a QuantLib Schedule for a bond.
    
    Args:
        issue_date (dt.date): The issue date of the bond.
        maturity_years (int): Years until maturity.
        frequency (int): Frequency of coupon payments (1 for annual, 2 for semi-annual, etc.).
        calendar (ql.Calendar): The calendar to use for scheduling.
    
    Returns:
        ql.Schedule: The created bond schedule.
    """
    issue_date_ql = ql.Date(issue_date.day, issue_date.month, issue_date.year)
    maturity_date = calendar.advance(issue_date_ql, ql.Period(maturity_years, ql.Years))
    
    return ql.Schedule(
        issue_date_ql,
        maturity_date,
        ql.Period(frequency),
        calendar,
        ql.Following,
        ql.Following,
        ql.DateGeneration.Backward,
        False
    )


def create_fixed_rate_bond(
    face_value: float,
    coupon_rate: float,
    schedule: ql.Schedule,
    settlement_days: int
) -> ql.FixedRateBond:
    """
    Create a QuantLib FixedRateBond.
    
    Args:
        face_value (float): The nominal value of the bond.
        coupon_rate (float): Annual coupon rate (e.g., 0.05 for 5%).
        schedule (ql.Schedule): The bond's payment schedule.
        settlement_days (int): Settlement days.
    
    Returns:
        ql.FixedRateBond: The created fixed-rate bond.
    """
    return ql.FixedRateBond(
        settlement_days,
        face_value,
        schedule,
        [coupon_rate],
        ql.ActualActual()
    )


def create_discounting_bond_engine(
    dates: list,
    rates: list,
    frequency: int,
    evaluation_date: dt.date
) -> ql.DiscountingBondEngine:
    """
    Create a QuantLib DiscountingBondEngine using a non-flat term structure.

    Args:
        dates (list): List of dates (str, dt.date, or dt.datetime) for the term structure.
        rates (list): List of zero rates corresponding to the dates.
        frequency (int): Frequency of coupon payments (1 for annual, 2 for semi-annual, etc.).
        evaluation_date (dt.date): The evaluation date.

    Returns:
        ql.DiscountingBondEngine: The created discounting bond engine.
    """
    ql_dates = [interpret_date(d) for d in dates]
    ql_rates = [ql.SimpleQuote(r) for r in rates]
    rate_helpers = [
        ql.DepositRateHelper(
            ql.QuoteHandle(q),
            ql.Period(ql_dates[i] - ql_dates[0], ql.Days),
            frequency,
            ql.TARGET(),
            ql.Following,
            False,
            ql.ActualActual()
        ) for i, q in enumerate(ql_rates)
    ]
    term_structure = ql.PiecewiseLogCubicDiscount(
        interpret_date(evaluation_date),
        rate_helpers,
        ql.ActualActual()
    )
    return ql.DiscountingBondEngine(ql.YieldTermStructureHandle(term_structure))


def evaluate_fixed_rate_bond(
    evaluation_date: Union[str, dt.datetime, dt.date],
    face_value: float,
    issue_date: Union[str, dt.datetime, dt.date],
    coupon_rate: float,
    maturity: int,
    frequency: int,
    settlement_days: int,
    currency: str
) -> float:
    """
    Evaluate a fixed-rate bond using QuantLib pricing.
    
    """

    ql.Settings.instance().evaluationDate = interpret_date(evaluation_date)
    calendar = imply_calendar_from_currency(currency)

    

        
    issue_date = today
    maturity_date = calendar.advance(issue_date, ql.Period(maturity, ql.Years))

    schedule = ql.Schedule(issue_date, maturity_date, ql.Period(frequency),
                           calendar, ql.Following, ql.Following,
                           ql.DateGeneration.Backward, False)

    bond = ql.FixedRateBond(settlement_days, face_value, schedule, [coupon_rate], ql.ActualActual())

    yield_curve = ql.FlatForward(today, yield_rate, ql.ActualActual(), ql.Compounded, frequency)
    discount_curve = ql.YieldTermStructureHandle(yield_curve)

    bond_engine = ql.DiscountingBondEngine(discount_curve)
    bond.setPricingEngine(bond_engine)

    return bond.cleanPrice()