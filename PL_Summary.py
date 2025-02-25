import pandas as pd
from datetime import datetime, timedelta
from db_handler import connect_db, get_trades
from utils import calculate_metrics, convert_to_human_readable

def calculate_premium_captured_over_range(start_date, end_date, connection):
    """
    Calculate the total premium captured for every day in [start_date, end_date].
    """
    current_date = start_date
    total_premium_captured = 0

    while current_date <= end_date:
        df_trades_ordered = get_trades(connection, current_date.year, current_date.month, current_date.day)
        # calculate_metrics returns multiple values; we only need premium_captured here
        _, premium_captured, _, _, _, _, _, _, _ = calculate_metrics(df_trades_ordered)
        total_premium_captured += premium_captured
        current_date += timedelta(days=1)
    
    return total_premium_captured

def calculate_total_PL(start_date_str, end_date_str=None):
    """
    Calculate the total PL sum from the last trade of each day between start_date_str and end_date_str.
    If end_date_str is omitted, default to the latest date in DailyLog.
    """
    if not start_date_str:
        raise ValueError("A start date must be provided in YYYYMMDD format.")

    try:
        with connect_db() as connection:
            # Read entire DailyLog table
            query = "SELECT DailyLogID, LogDate, PL, SPX FROM DailyLog;"
            df_daily_log = pd.read_sql_query(query, connection)

            # Convert 'LogDate' to human-readable
            df_daily_log['LogDate'] = df_daily_log['LogDate'].apply(convert_to_human_readable)

            # Parse start_date
            try:
                start_date = datetime.strptime(start_date_str, "%Y%m%d")
            except ValueError:
                print("Invalid start date format. Use YYYYMMDD (e.g., 20240917).")
                return None

            # Parse end_date or set to max if not given
            if end_date_str:
                try:
                    end_date = datetime.strptime(end_date_str, "%Y%m%d")
                except ValueError:
                    print("Invalid end date format. Use YYYYMMDD (e.g., 20240917).")
                    return None
            else:
                end_date = df_daily_log['LogDate'].max()

            # Filter rows to [start_date, end_date]
            df_filtered = df_daily_log[
                (df_daily_log['LogDate'] >= start_date) &
                (df_daily_log['LogDate'] <= end_date)
            ]
            if df_filtered.empty:
                print("No data found in the specified range.")
                return 0

            # Group by date, pick last row of each day
            df_last_of_day = df_filtered.groupby(df_filtered['LogDate'].dt.date).tail(1)

            print(f"Calculating total PL from {start_date} to {end_date}")
            print(f"Filtered DataFrame:\n{df_filtered[['LogDate','PL']]}")
            print(f"PL values being summed:\n{df_last_of_day[['LogDate','PL']]}")

            total_pl_sum = df_last_of_day['PL'].sum()
            print(f"Total PL sum: {total_pl_sum}")
            return total_pl_sum

    except Exception as e:
        print(f"Error while querying the database: {e}")
        return None
