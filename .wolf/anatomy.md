# anatomy.md

> Auto-maintained by OpenWolf. Last scanned: 2026-04-04T13:35:14.493Z
> Files: 25 tracked | Anatomy hits: 0 | Misses: 0

## ./

- `.gitignore` — Git ignore rules (~99 tok)
- `chart_generator.py` — generate_equity_curve: queries Trade range, cumulative P&L matplotlib dark-theme PNG, returns temp file path or None (~993 tok)
- `CLAUDE.md` — CLAUDE.md (~750 tok)
- `db_handler.py` — load_config, connect_db, get_trades, get_trades_by_type, get_trades_range (date-range query for equity curve / rolling metrics) (~1679 tok)
- `discord_messenger.py` — load_webhooks, send_message_to_discord, delete_messages (~1198 tok)
- `PL_Summary.py` — to_filetime, calculate_premium_captured_over_range, calculate_total_PL (~1004 tok)
- `README.md` — Project documentation (~1163 tok)
- `requirements.txt` — Python dependencies (~34 tok)
- `Trade_Scout.py` — load_dotenv, calculate_trade_stats, create_trade_scout_message (~2324 tok)
- `utils.py` — load_yaml_config, take_screenshot_of_app, convert_to_human_readable, convert_filetime_series, calculate_rolling_metrics, format_rolling_section (~4066 tok)

## .claude/

- `settings.json` (~441 tok)

## .claude/rules/

- `openwolf.md` (~313 tok)

## .github/workflows/

- `release.yml` — CI: Build and Release (~1146 tok)

## config/

- `config.demo.yaml` — config.yaml (~421 tok)

## tests/

- `conftest.py` — pytest_configure (~124 tok)
- `data.DB3` (~0 tok)
- `test_calculate_total_PL.py` — TestCalculateTotalPL: test_sums_three_days_correctly, test_empty_database_returns_zero, test_invalid (~778 tok)
- `test_db_handler.py` — Unit tests for db_handler.py using in-memory SQLite — no real TAT DB required. (~2753 tok)
- `test_discord_messenger.py` — Unit tests for discord_messenger.py — all HTTP calls are mocked. (~2222 tok)
- `test_get_most_recent_monday.py` — TestGetMostRecentMonday: test_known_cases, test_no_arg_returns_a_monday (~451 tok)
- `test_integration.py` — TestGetTradesReal: setUpModule, setUp, tearDown, test_known_date_returns_exact_row_count + 23 more (~2604 tok)
- `test_pl_summary.py` — Unit tests for PL_Summary.py using in-memory SQLite. (~2353 tok)
- `test_spx_values.py` — Add parent directory to sys.path so we can import modules (~559 tok)
- `test_trade_scout.py` — Unit tests for Trade_Scout.py — database and Discord calls are mocked. (~2016 tok)
- `test_utils.py` — Unit tests for utils.py — no database or network required. (~4016 tok)
