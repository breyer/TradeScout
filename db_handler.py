import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import date, datetime
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
