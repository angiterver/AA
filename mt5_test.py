"""
mt5_test.py  -  MT5 Connection & Data Availability Diagnostic
==============================================================
Tests your MT5 connection and checks what historical data
is actually available on your account.

Run:
    python mt5_test.py
"""

import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv
import MetaTrader5 as mt5

load_dotenv(Path(__file__).parent / ".env")

ACCOUNT  = int(os.getenv("MT5_ACCOUNT", "0"))
PASSWORD = os.getenv("MT5_PASSWORD", "")
SERVER   = os.getenv("MT5_SERVER", "")

print("=" * 60)
print("  MT5 CONNECTION DIAGNOSTIC")
print("=" * 60)
print("  Account : {}".format(ACCOUNT))
print("  Server  : {}".format(SERVER))
print("=" * 60)
print("")

# ------------------------------------------------------------------------------
# 1. Connect
# ------------------------------------------------------------------------------
print("[1] Connecting to MT5...")
if not mt5.initialize(login=ACCOUNT, password=PASSWORD, server=SERVER):
    print("    ERROR: mt5.initialize() failed: {}".format(mt5.last_error()))
    quit()
print("    Connected OK")
print("")

# ------------------------------------------------------------------------------
# 2. Account Info
# ------------------------------------------------------------------------------
print("[2] Account Info:")
info = mt5.account_info()
if info:
    print("    Login       : {}".format(info.login))
    print("    Name        : {}".format(info.name))
    print("    Server      : {}".format(info.server))
    print("    Balance     : {:.2f} {}".format(info.balance, info.currency))
    print("    Equity      : {:.2f} {}".format(info.equity, info.currency))
    print("    Margin free : {:.2f} {}".format(info.margin_free, info.currency))
    print("    Leverage    : 1:{}".format(info.leverage))
print("")

# ------------------------------------------------------------------------------
# 3. Test XAUUSD with all fetch methods
# ------------------------------------------------------------------------------
SYMBOL = "XAUUSD"
print("[3] Testing data fetch methods for {}...".format(SYMBOL))
print("")

# Make sure symbol is selected/visible
mt5.symbol_select(SYMBOL, True)

# Method A: copy_rates_range with 2024 range
print("    Method A: copy_rates_range (2024-01-01 to 2024-01-08)")
start = datetime(2024, 1, 1, tzinfo=timezone.utc)
end   = datetime(2024, 1, 8, tzinfo=timezone.utc)
rates = mt5.copy_rates_range(SYMBOL, mt5.TIMEFRAME_M5, start, end)
if rates is not None and len(rates) > 0:
    print("    -> {} bars  (FIRST: {}  LAST: {})".format(
        len(rates),
        datetime.utcfromtimestamp(rates[0][0]).strftime("%Y-%m-%d %H:%M"),
        datetime.utcfromtimestamp(rates[-1][0]).strftime("%Y-%m-%d %H:%M"),
    ))
else:
    print("    -> FAILED or 0 bars  error={}".format(mt5.last_error()))

# Method B: copy_rates_from_pos — last 500 bars from current position
print("")
print("    Method B: copy_rates_from_pos (last 500 M5 bars from now)")
rates_b = mt5.copy_rates_from_pos(SYMBOL, mt5.TIMEFRAME_M5, 0, 500)
if rates_b is not None and len(rates_b) > 0:
    print("    -> {} bars  (FIRST: {}  LAST: {})".format(
        len(rates_b),
        datetime.utcfromtimestamp(rates_b[0][0]).strftime("%Y-%m-%d %H:%M"),
        datetime.utcfromtimestamp(rates_b[-1][0]).strftime("%Y-%m-%d %H:%M"),
    ))
else:
    print("    -> FAILED or 0 bars  error={}".format(mt5.last_error()))

# Method C: copy_rates_from_pos — last 100,000 bars (max history)
print("")
print("    Method C: copy_rates_from_pos (last 100,000 M5 bars = max history)")
rates_c = mt5.copy_rates_from_pos(SYMBOL, mt5.TIMEFRAME_M5, 0, 100000)
if rates_c is not None and len(rates_c) > 0:
    print("    -> {} bars  (EARLIEST: {}  LATEST: {})".format(
        len(rates_c),
        datetime.utcfromtimestamp(rates_c[0][0]).strftime("%Y-%m-%d %H:%M"),
        datetime.utcfromtimestamp(rates_c[-1][0]).strftime("%Y-%m-%d %H:%M"),
    ))
else:
    print("    -> FAILED or 0 bars  error={}".format(mt5.last_error()))

# Method D: copy_rates_from a recent date
print("")
print("    Method D: copy_rates_from (from 2025-01-01, 50000 bars)")
start_2025 = datetime(2025, 1, 1, tzinfo=timezone.utc)
rates_d = mt5.copy_rates_from(SYMBOL, mt5.TIMEFRAME_M5, start_2025, 50000)
if rates_d is not None and len(rates_d) > 0:
    print("    -> {} bars  (FIRST: {}  LAST: {})".format(
        len(rates_d),
        datetime.utcfromtimestamp(rates_d[0][0]).strftime("%Y-%m-%d %H:%M"),
        datetime.utcfromtimestamp(rates_d[-1][0]).strftime("%Y-%m-%d %H:%M"),
    ))
else:
    print("    -> FAILED or 0 bars  error={}".format(mt5.last_error()))

# Method E: very recent range (last 30 days)
print("")
now        = datetime.now(timezone.utc)
days30_ago = now - timedelta(days=30)
print("    Method E: copy_rates_range (last 30 days: {} to {})".format(
    days30_ago.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d")))
rates_e = mt5.copy_rates_range(SYMBOL, mt5.TIMEFRAME_M5, days30_ago, now)
if rates_e is not None and len(rates_e) > 0:
    print("    -> {} bars  (FIRST: {}  LAST: {})".format(
        len(rates_e),
        datetime.utcfromtimestamp(rates_e[0][0]).strftime("%Y-%m-%d %H:%M"),
        datetime.utcfromtimestamp(rates_e[-1][0]).strftime("%Y-%m-%d %H:%M"),
    ))
else:
    print("    -> FAILED or 0 bars  error={}".format(mt5.last_error()))

print("")

# ------------------------------------------------------------------------------
# 4. Summary & recommendation
# ------------------------------------------------------------------------------
print("[4] Summary:")
best_rates = None
best_method = ""
for label, r in [("C (100k pos)", rates_c), ("D (2025 range)", rates_d),
                 ("E (30 days)", rates_e), ("B (500 pos)", rates_b)]:
    if r is not None and len(r) > 1:
        if best_rates is None or len(r) > len(best_rates):
            best_rates  = r
            best_method = label

if best_rates is not None and len(best_rates) > 1:
    earliest = datetime.utcfromtimestamp(best_rates[0][0])
    latest   = datetime.utcfromtimestamp(best_rates[-1][0])
    print("    Best method : {}".format(best_method))
    print("    Total bars  : {:,}".format(len(best_rates)))
    print("    Date range  : {} -> {}".format(earliest.strftime("%Y-%m-%d"), latest.strftime("%Y-%m-%d")))
    print("")
    print("    Your account HAS usable data.")
    print("    Update backtest.py BT_START/BT_END to match this range:")
    print("")
    print("        BT_START = datetime({}, {}, {}, tzinfo=timezone.utc)".format(
        earliest.year, earliest.month, earliest.day))
    print("        BT_END   = datetime({}, {}, {}, tzinfo=timezone.utc)".format(
        latest.year, latest.month, latest.day))
else:
    print("    Your Trial account has NO usable historical bar data.")
    print("")
    print("    Options:")
    print("    1. Log in to MT5 terminal with your REAL/PRO Exness account")
    print("       and update .env with those credentials.")
    print("    2. Or manually scroll back in MT5 chart for XAUUSD M5 to")
    print("       load history, then re-run this test.")
    print("    3. Or use a free data source like Yahoo Finance for XAUUSD")
    print("       (we can build a downloader for that if needed).")

print("")
mt5.shutdown()
print("Done.")
print("=" * 60)