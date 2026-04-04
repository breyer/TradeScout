import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import Generator, Optional, Union

import pandas as pd

from utils import convert_filetime_series, convert_to_human_readable, load_yaml_config

logger = logging.getLogger(__name__)


def load_config() -> dict:
    return load_yaml_config()


@contextmanager
def connect_db(retries: int = 5, delay: int = 1) -> Generator[sqlite3.Connection, None, None]:
    """
    Context manager that yields an open SQLite connection to the TAT database.
    Retries up to *retries* times with *delay* seconds between attempts.
    """
    config = load_config()
    db_path = config.get('db_path', 'data/data.db3')

    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found at {db_path}")

    connection: sqlite3.Connection = None
    for attempt in range(retries):
        try:
            connection = sqlite3.connect(db_path)
            logger.info("Connected to database at %s (attempt %d).", db_path, attempt + 1)
            break
        except sqlite3.OperationalError as e:
            logger.warning("Connection attempt %d/%d failed: %s", attempt + 1, retries, e)
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise ConnectionError(
                    f"Failed to connect to {db_path} after {retries} attempts"
                ) from e

    try:
        yield connection
    finally:
        if connection:
            connection.close()
            logger.info("Database connection closed.")


def get_trades(
    connection: sqlite3.Connection, year: int, month: int, day: int
) -> pd.DataFrame:
    """
    Return all TAT trades for a single calendar day using parameterized SQL.
    Timestamps are converted from Windows FILETIME to datetime.
    """
    query = """
        SELECT
            TradeID, DateOpened, DateClosed, TradeType,
            ShortPut, LongPut, ShortCall, LongCall,
            Qty, StopType, PriceOpen, PriceStopTarget,
            ProfitLoss, PriceClose, ClosingProcessed,
            TotalPremium, Commission, CommissionClose
        FROM Trade
        WHERE Year  = ?
          AND Month = ?
          AND Day   = ?
          AND TATTradeID IS NOT NULL;
    """
    df = pd.read_sql_query(query, connection, params=(year, month, day))
    df['DateOpened'] = df['DateOpened'].apply(convert_to_human_readable)
    df['DateClosed'] = df['DateClosed'].apply(convert_to_human_readable)

    return df[[
        "TradeID", "DateOpened", "TradeType", "ShortPut", "LongPut",
        "ShortCall", "LongCall", "Qty", "StopType", "PriceOpen",
        "PriceStopTarget", "ProfitLoss", "PriceClose", "DateClosed",
        "ClosingProcessed", "TotalPremium", "Commission", "CommissionClose",
    ]]


def get_trades_by_type(
    trade_type: str,
    start_date: Union[date, datetime],
    connection: Optional[sqlite3.Connection] = None,
) -> pd.DataFrame:
    """
    Return all trades of *trade_type* opened on or after *start_date*.

    Column aliases (PL, OpenDate, CloseDate, Contracts) match the expectations
    of Trade_Scout.py's calculate_trade_stats().

    Pass *connection* to reuse an existing DB connection; otherwise a new one
    is opened and closed automatically.
    """
    start_int = start_date.year * 10_000 + start_date.month * 100 + start_date.day
    query = """
        SELECT
            TradeID,
            DateOpened   AS OpenDate,
            DateClosed   AS CloseDate,
            TradeType,
            Qty          AS Contracts,
            PriceOpen,
            PriceStopTarget,
            ProfitLoss   AS PL,
            PriceClose,
            ClosingProcessed,
            TotalPremium
        FROM Trade
        WHERE TradeType    = ?
          AND TATTradeID   IS NOT NULL
          AND (Year * 10000 + Month * 100 + Day) >= ?;
    """
    if connection is not None:
        df = pd.read_sql_query(query, connection, params=(trade_type, start_int))
    else:
        with connect_db() as conn:
            df = pd.read_sql_query(query, conn, params=(trade_type, start_int))

    df['OpenDate'] = convert_filetime_series(df['OpenDate'])
    df['CloseDate'] = convert_filetime_series(df['CloseDate'])
    return df


def get_recent_trading_days(
    connection: sqlite3.Connection, target_date: datetime, n: int
) -> list:
    """
    Return the N most recent calendar dates (as datetime) that had at least one
    valid trade on or before *target_date*, sorted ascending (oldest first).

    Uses Trade.Year/Month/Day integer columns — no FILETIME conversion needed.
    If fewer than N trading days exist, returns all available.
    """
    target_int = target_date.year * 10_000 + target_date.month * 100 + target_date.day
    query = """
        SELECT DISTINCT Year, Month, Day
        FROM Trade
        WHERE TATTradeID IS NOT NULL
          AND (Year * 10000 + Month * 100 + Day) <= ?
        ORDER BY (Year * 10000 + Month * 100 + Day) DESC
        LIMIT ?
    """
    rows = connection.execute(query, (target_int, n)).fetchall()
    days = [datetime(int(r[0]), int(r[1]), int(r[2])) for r in rows]
    days.sort()
    return days


def has_dailylog_rows(
    connection: sqlite3.Connection, target_date: datetime
) -> bool:
    """
    Return True if DailyLog has any rows for *target_date*, False otherwise.

    DailyLog.LogDate stores .NET DateTime ticks (100-ns intervals since
    0001-01-01). The conversion reuses the same constants and formula as
    get_last_spx_value() so that the tick range is consistent.
    """
    from utils import NET_EPOCH_OFFSET_SECONDS, NET_TICKS_PER_SECOND  # avoid circular at import time
    epoch = datetime(1970, 1, 1)
    start_ticks = int(
        (
            (datetime(target_date.year, target_date.month, target_date.day) - epoch).total_seconds()
            + NET_EPOCH_OFFSET_SECONDS
        )
        * NET_TICKS_PER_SECOND
    )
    end_ticks = int(
        (
            (datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59, 999999) - epoch).total_seconds()
            + NET_EPOCH_OFFSET_SECONDS
        )
        * NET_TICKS_PER_SECOND
    )
    cursor = connection.execute(
        "SELECT COUNT(*) FROM DailyLog WHERE LogDate BETWEEN ? AND ?",
        (start_ticks, end_ticks),
    )
    return cursor.fetchone()[0] > 0


def is_trading_day(connection: sqlite3.Connection, date: datetime) -> bool:
    """
    Return True if the market was open on *date* — i.e. the Trade table has
    at least one valid row OR DailyLog has rows for that date.
    """
    trades = get_trades(connection, date.year, date.month, date.day)
    if not trades.empty:
        return True
    return has_dailylog_rows(connection, date)


def get_trading_days_in_range(
    connection: sqlite3.Connection, start_date: datetime, end_date: datetime
) -> list:
    """
    Return all calendar dates (as datetime) in [start_date, end_date] inclusive
    that had at least one valid trade in the Trade table, sorted ascending.
    """
    start_int = start_date.year * 10_000 + start_date.month * 100 + start_date.day
    end_int = end_date.year * 10_000 + end_date.month * 100 + end_date.day
    query = """
        SELECT DISTINCT Year, Month, Day
        FROM Trade
        WHERE TATTradeID IS NOT NULL
          AND (Year * 10000 + Month * 100 + Day) >= ?
          AND (Year * 10000 + Month * 100 + Day) <= ?
        ORDER BY (Year * 10000 + Month * 100 + Day) ASC
    """
    rows = connection.execute(query, (start_int, end_int)).fetchall()
    return [datetime(int(r[0]), int(r[1]), int(r[2])) for r in rows]


def get_trades_range(
    connection: sqlite3.Connection, start_date: datetime, end_date: datetime
) -> pd.DataFrame:
    """
    Return trades for all days in [start_date, end_date] inclusive.
    Columns: Year, Month, Day, ProfitLoss, TotalPremium, ClosingProcessed.
    Used by equity curve and rolling benchmarks.
    """
    start_int = start_date.year * 10_000 + start_date.month * 100 + start_date.day
    end_int = end_date.year * 10_000 + end_date.month * 100 + end_date.day
    query = """
        SELECT Year, Month, Day, ProfitLoss, TotalPremium, ClosingProcessed
        FROM Trade
        WHERE (Year * 10000 + Month * 100 + Day) BETWEEN ? AND ?
          AND TATTradeID IS NOT NULL;
    """
    return pd.read_sql_query(query, connection, params=(start_int, end_int))


def get_dailylog_for_date(
    connection: sqlite3.Connection, target_date: datetime
) -> pd.DataFrame:
    """
    Return all DailyLog rows for *target_date* with non-zero SPX, sorted ascending.

    Columns: time (datetime), PL, PremiumSold, SPX.
    LogDate is stored as .NET DateTime ticks (100-ns since 0001-01-01) in local time.
    """
    from utils import NET_EPOCH_OFFSET_SECONDS, NET_TICKS_PER_SECOND  # avoid circular at import
    epoch = datetime(1970, 1, 1)
    start_ticks = int(
        (
            (datetime(target_date.year, target_date.month, target_date.day) - epoch).total_seconds()
            + NET_EPOCH_OFFSET_SECONDS
        )
        * NET_TICKS_PER_SECOND
    )
    end_ticks = int(
        (
            (
                datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59, 999999)
                - epoch
            ).total_seconds()
            + NET_EPOCH_OFFSET_SECONDS
        )
        * NET_TICKS_PER_SECOND
    )
    query = """
        SELECT LogDate, PL, PremiumSold, SPX
        FROM DailyLog
        WHERE LogDate BETWEEN ? AND ?
          AND SPX IS NOT NULL AND SPX != 0
        ORDER BY LogDate ASC
    """
    df = pd.read_sql_query(query, connection, params=(start_ticks, end_ticks))
    if df.empty:
        return df
    df['time'] = df['LogDate'].apply(
        lambda t: epoch + timedelta(seconds=t / NET_TICKS_PER_SECOND - NET_EPOCH_OFFSET_SECONDS)
    )
    return df[['time', 'PL', 'PremiumSold', 'SPX']]


def get_spx_data_from_db(
    connection: Optional[sqlite3.Connection] = None,
) -> pd.DataFrame:
    """
    Return all DailyLog rows that contain an SPX value, sorted by LogDate.
    Column SPX is aliased to SPX_Close.

    Pass *connection* to reuse an existing DB connection; otherwise a new one
    is opened and closed automatically.
    """
    query = """
        SELECT DailyLogID, LogDate, PL, SPX AS SPX_Close
        FROM DailyLog
        WHERE LogDate IS NOT NULL;
    """
    if connection is not None:
        df = pd.read_sql_query(query, connection)
    else:
        with connect_db() as conn:
            df = pd.read_sql_query(query, conn)

    df['LogDate'] = convert_filetime_series(df['LogDate'])
    return df.sort_values('LogDate').reset_index(drop=True)
