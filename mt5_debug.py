"""
mt5_debug.py  -  Find exactly which MT5 call works on this machine
Run:  python mt5_debug.py
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
import MetaTrader5 as mt5

load_dotenv(Path(__file__).parent / ".env")

ACCOUNT  = int(os.getenv("MT5_ACCOUNT", "0"))
PASSWORD = os.getenv("MT5_PASSWORD", "")
SERVER   = os.getenv("MT5_SERVER", "")
SYMBOL   = os.getenv("MT5_SYMBOLS", "XAUUSD").split(",")[0].strip()

print(f"Connecting to MT5 ({SERVER}) ...")
if not mt5.initialize(login=ACCOUNT, password=PASSWORD, server=SERVER):
    print("FAILED:", mt5.last_error())
    quit()
print("Connected OK\n")

mt5.symbol_select(SYMBOL, True)

results = {}

# -----------------------------------------------------------
# Test every combination that mt5_test.py used (Method D worked)
# -----------------------------------------------------------

print("=== Testing copy_rates_from variants ===\n")

# A: naive datetime, 50000 bars (exactly what mt5_test.py Method D did)
dt_naive_2025 = datetime(2025, 1, 1)   # no tzinfo
print(f"A) copy_rates_from(naive 2025-01-01, 50000) ...")
r = mt5.copy_rates_from(SYMBOL, mt5.TIMEFRAME_M5, dt_naive_2025, 50000)
if r is not None and len(r) > 0:
    print(f"   SUCCESS: {len(r)} bars, first={datetime.utcfromtimestamp(r[0][0])}, last={datetime.utcfromtimestamp(r[-1][0])}")
    results["A"] = r
else:
    print(f"   FAILED: {mt5.last_error()}")

# B: naive datetime, oct 2024, 50000 bars
dt_naive_oct = datetime(2024, 10, 25)  # no tzinfo
print(f"B) copy_rates_from(naive 2024-10-25, 50000) ...")
r = mt5.copy_rates_from(SYMBOL, mt5.TIMEFRAME_M5, dt_naive_oct, 50000)
if r is not None and len(r) > 0:
    print(f"   SUCCESS: {len(r)} bars, first={datetime.utcfromtimestamp(r[0][0])}, last={datetime.utcfromtimestamp(r[-1][0])}")
    results["B"] = r
else:
    print(f"   FAILED: {mt5.last_error()}")

# C: aware datetime, 2025
dt_aware_2025 = datetime(2025, 1, 1, tzinfo=timezone.utc)
print(f"C) copy_rates_from(aware 2025-01-01 UTC, 50000) ...")
r = mt5.copy_rates_from(SYMBOL, mt5.TIMEFRAME_M5, dt_aware_2025, 50000)
if r is not None and len(r) > 0:
    print(f"   SUCCESS: {len(r)} bars, first={datetime.utcfromtimestamp(r[0][0])}, last={datetime.utcfromtimestamp(r[-1][0])}")
    results["C"] = r
else:
    print(f"   FAILED: {mt5.last_error()}")

# D: copy_rates_from_pos, last 20000
print(f"D) copy_rates_from_pos(0, 20000) ...")
r = mt5.copy_rates_from_pos(SYMBOL, mt5.TIMEFRAME_M5, 0, 20000)
if r is not None and len(r) > 0:
    print(f"   SUCCESS: {len(r)} bars, first={datetime.utcfromtimestamp(r[0][0])}, last={datetime.utcfromtimestamp(r[-1][0])}")
    results["D"] = r
else:
    print(f"   FAILED: {mt5.last_error()}")

# E: copy_rates_from_pos, smaller batch
print(f"E) copy_rates_from_pos(0, 5000) ...")
r = mt5.copy_rates_from_pos(SYMBOL, mt5.TIMEFRAME_M5, 0, 5000)
if r is not None and len(r) > 0:
    print(f"   SUCCESS: {len(r)} bars, first={datetime.utcfromtimestamp(r[0][0])}, last={datetime.utcfromtimestamp(r[-1][0])}")
    results["E"] = r
else:
    print(f"   FAILED: {mt5.last_error()}")

# F: copy_rates_range, naive datetimes
dt_start = datetime(2024, 10, 25)
dt_end   = datetime(2026,  3, 27)
print(f"F) copy_rates_range(naive 2024-10-25, naive 2026-03-27) ...")
r = mt5.copy_rates_range(SYMBOL, mt5.TIMEFRAME_M5, dt_start, dt_end)
if r is not None and len(r) > 0:
    print(f"   SUCCESS: {len(r)} bars, first={datetime.utcfromtimestamp(r[0][0])}, last={datetime.utcfromtimestamp(r[-1][0])}")
    results["F"] = r
else:
    print(f"   FAILED: {mt5.last_error()}")

# G: copy_rates_range, aware datetimes
dt_start_aw = datetime(2024, 10, 25, tzinfo=timezone.utc)
dt_end_aw   = datetime(2026,  3, 27, tzinfo=timezone.utc)
print(f"G) copy_rates_range(aware 2024-10-25, aware 2026-03-27) ...")
r = mt5.copy_rates_range(SYMBOL, mt5.TIMEFRAME_M5, dt_start_aw, dt_end_aw)
if r is not None and len(r) > 0:
    print(f"   SUCCESS: {len(r)} bars, first={datetime.utcfromtimestamp(r[0][0])}, last={datetime.utcfromtimestamp(r[-1][0])}")
    results["G"] = r
else:
    print(f"   FAILED: {mt5.last_error()}")

# H: H1 bars with copy_rates_from_pos
print(f"H) H1 copy_rates_from_pos(0, 5000) ...")
r = mt5.copy_rates_from_pos(SYMBOL, mt5.TIMEFRAME_H1, 0, 5000)
if r is not None and len(r) > 0:
    print(f"   SUCCESS: {len(r)} bars, first={datetime.utcfromtimestamp(r[0][0])}, last={datetime.utcfromtimestamp(r[-1][0])}")
    results["H"] = r
else:
    print(f"   FAILED: {mt5.last_error()}")

mt5.shutdown()

print("\n=== SUMMARY ===")
if results:
    print(f"Working methods: {list(results.keys())}")
    best = max(results.items(), key=lambda x: len(x[1]))
    print(f"Best method    : {best[0]}  ({len(best[1])} bars)")
    print(f"\nPaste this output and I will fix the downloader to use the right method.")
else:
    print("NO methods worked. MT5 terminal may not be running or symbol not available.")
