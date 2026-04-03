import calendar
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from db_handler import connect_db, get_trades
from utils import (
    FILETIME_EPOCH_OFFSET,
    FILETIME_TICKS_PER_SECOND,
    calculate_metrics,
    convert_to_human_readable,
)

logger = logging.getLogger(__name__)


def to_filetime(dt: datetime) -> int:
    """Convert a datetime to a Windows FILETIME integer (100-ns ticks since 1601-01-01)."""
    posix_time = (dt - datetime(1970, 1, 1)).total_seconds()
    return int(posix_time * FILETIME_TICKS_PER_SECOND + FILETIME_EPOCH_OFFSET)


def calculate_premium_captured_over_range(
    start_date: datetime, end_date: datetime, connection: sqlite3.Connection
) -> float:
    """
    Sum the premium captured for every calendar day in [start_date, end_date].
    Uses an existing open *connection*; does not manage connection lifecycle.
    """
    total = 0.0
    current = start_date
    while current <= end_date:
        df = get_trades(connection, current.year, current.month, current.day)
        _, premium_captured, *_ = calculate_metrics(df)
        total += premium_captured
        current += timedelta(days=1)
    return total


def calculate_total_PL(
    start_date_str: str, end_date_str: Optional[str] = None
) -> Optional[float]:
    """
    Sum the last DailyLog PL entry for each day between *start_date_str* and *end_date_str*.

    Dates must be in YYYYMMDD format.  If *end_date_str* is omitted the full
    starting month is used.  Returns None on invalid input or database error.
    """
    if not start_date_str:
        raise ValueError("A start date must be provided in the format YYYYMMDD.")

    try:
        start_date = datetime.strptime(start_date_str, "%Y%m%d")
    except ValueError:
        logger.error("Invalid start date '%s'. Expected YYYYMMDD.", start_date_str)
        return None

    if end_date_str:
        try:
            end_date = datetime.strptime(end_date_str, "%Y%m%d")
        except ValueError:
            logger.error("Invalid end date '%s'. Expected YYYYMMDD.", end_date_str)
            return None
    else:
        end_date = start_date.replace(
            day=calendar.monthrange(start_date.year, start_date.month)[1]
        )

    start_filetime = to_filetime(start_date)
    end_filetime = to_filetime(end_date + timedelta(days=1)) - 1

    try:
        with connect_db() as connection:
            query = """
                SELECT DailyLogID, LogDate, PL, SPX
                FROM DailyLog
                WHERE LogDate BETWEEN ? AND ?;
            """
            df_daily_log = pd.read_sql_query(
                query, connection, params=(start_filetime, end_filetime)
            )
    except (sqlite3.Error, ConnectionError) as e:
        logger.error("Database error in calculate_total_PL: %s", e)
        return None

    if df_daily_log.empty:
        logger.info("No DailyLog data found between %s and %s.", start_date.date(), end_date.date())
        return 0.0

    df_daily_log['LogDate'] = df_daily_log['LogDate'].apply(convert_to_human_readable)
    df_last_of_day = df_daily_log.groupby(df_daily_log['LogDate'].dt.date).tail(1)

    logger.debug(
        "PL values summed (%s → %s):\n%s",
        start_date.date(), end_date.date(), df_last_of_day[['LogDate', 'PL']].to_string(),
    )

    total = float(df_last_of_day['PL'].sum())
    logger.info("Total PL (%s → %s): %.2f", start_date.date(), end_date.date(), total)
    return total
