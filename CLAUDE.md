# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# OpenWolf

@.wolf/OPENWOLF.md

This project uses OpenWolf for context management. Read and follow .wolf/OPENWOLF.md every session. Check .wolf/cerebrum.md before generating code. Check .wolf/anatomy.md before reading files.

---

## What TradeScout Does

TradeScout reads trade data from a local SQLite database (Trade Automation Toolbox / TAT), computes daily performance metrics (premium sold/captured, PCR, win rate, slippage, WTD/MTD P&L), and posts a formatted summary to one or more Discord webhooks. It also screenshots the running TAT application to attach to the Discord message.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run for today's date
python Trade_Scout.py

# Run for a specific date
python Trade_Scout.py --date 20240920 --win restore

# Run tests
pytest tests/

# Run a single test file
pytest tests/test_calculate_total_PL.py -v
```

`--win` accepts `restore` (default) or `max` — controls the TAT window state before screenshotting.

## Configuration

Copy `config/config.demo.yaml` to `config/config.yaml` and set:
- `db_path` — path to the TAT SQLite database (use forward slashes even on Windows)
- `webhooks` — list of Discord webhook URLs with optional `thread_id`

## Architecture

The entry point is `Trade_Scout.py`. Data flows as:

1. **`db_handler.py`** — opens the TAT SQLite DB (`connect_db` context manager with retries), queries the `Trade` table (`get_trades`) and `DailyLog` table (`get_spx_data_from_db`). All timestamps in the DB are Windows FILETIME integers; `convert_to_human_readable()` in `utils.py` converts them to Python datetimes, forcing the year to the current system year (workaround for TAT storing dates with a fixed year).

2. **`utils.py`** — stateless helpers: `load_yaml_config`, `calculate_metrics` (computes all trade stats from a DataFrame), `format_message` (builds the fixed-width Discord code block), `get_last_spx_value`, `get_most_recent_monday`, `take_screenshot_of_app` (uses `pygetwindow` + `pyautogui`).

3. **`PL_Summary.py`** — standalone utility for calculating total P&L over a date range using filetime-bounded queries on `DailyLog`. Uses `to_filetime()` to convert date boundaries for efficient SQL filtering.

4. **`discord_messenger.py`** — `send_message_to_discord` posts the message (with optional screenshot attachment) to all configured webhooks; `delete_messages` removes a previously sent message.

### Key Data Model Notes

- The `Trade` table is queried by `Year`, `Month`, `Day` integer columns (not a timestamp column).
- `ClosingProcessed = 0` means the trade expired; `= 1` means it was stopped out.
- Bad slippage: `abs(PriceClose) - PriceStopTarget >= 0.50`.
- WTD P&L uses `get_most_recent_monday(date)` as the start; MTD uses the 1st of the current month.
- SPX last value comes from the `DailyLog.SPX` column, filtered by the forced-year datetime.
