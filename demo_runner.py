"""
demo_runner.py  -  2-Candle Momentum Strategy (with H1 Ranging Filter) - DEMO
==============================================================================
Runs the CandleStrategy against your Exness DEMO / TRIAL MT5 account.
All credentials and parameters come from .env in the same folder.

.env example
------------
    MT5_ACCOUNT=12345678
    MT5_PASSWORD=your_demo_password
    MT5_SERVER=Exness-Trial
    MT5_SYMBOLS=XAUUSDm
    TRADE_SIZE=0.01
    BAR_TIMEFRAME=5-MINUTE

    TP_POINTS=6000
    SL_POINTS=2000
    BE_TRIGGER_PTS=1100
    BE_BUFFER_PTS=150
    TRAIL_PTS=1500
    DAILY_PROFIT=500.0
    DAILY_LOSS_PCT=0.50
    MAX_TRADES=500
    MIN_BODY_PCT=0.50

    # H1 ranging filter
    H1_RANGE_BODY_PCT=0.35
    H1_RANGE_CANDLES=2

    # Session filters (1=on, 0=off)
    SESSION_ASIA=1
    SESSION_LONDON=1
    SESSION_NEWYORK=1

Run:
    python demo_runner.py

Stop:
    Ctrl+C
==============================================================================
"""

import os
import signal
import sys
import time
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv
from nautilus_trader.live.node import TradingNode

from mt5connect.config import MT5Config
from mt5connect.factories import (
    build_mt5_node_config,
    MT5LiveDataClientFactory,
    MT5LiveExecClientFactory,
)
from strategy import CandleStrategy, CandleConfig

# ------------------------------------------------------------------------------
# CREDENTIALS
# ------------------------------------------------------------------------------

load_dotenv(Path(__file__).parent / ".env")


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        sys.exit(f"ERROR: '{key}' is not set in .env")
    return val


MT5_ACCOUNT  = int(_require("MT5_ACCOUNT"))
MT5_PASSWORD = _require("MT5_PASSWORD")
MT5_SERVER   = _require("MT5_SERVER")
MT5_SYMBOLS  = [s.strip() for s in _require("MT5_SYMBOLS").split(",")]
TRADE_SIZE   = Decimal(os.getenv("TRADE_SIZE",    "0.01"))
TIMEFRAME    = os.getenv("BAR_TIMEFRAME",          "5-MINUTE")

# ------------------------------------------------------------------------------
# STRATEGY PARAMETERS
# ------------------------------------------------------------------------------

TP_POINTS      = int(os.getenv("TP_POINTS",          "6000"))
SL_POINTS      = int(os.getenv("SL_POINTS",          "2000"))
BE_TRIGGER     = int(os.getenv("BE_TRIGGER_PTS",     "1100"))
BE_BUFFER      = int(os.getenv("BE_BUFFER_PTS",      "150"))
TRAIL_PTS      = int(os.getenv("TRAIL_PTS",          "1500"))
DAILY_PROFIT   = float(os.getenv("DAILY_PROFIT",     "500.0"))
DAILY_LOSS_PCT = float(os.getenv("DAILY_LOSS_PCT",   "0.50"))
MAX_TRADES     = int(os.getenv("MAX_TRADES",          "500"))
MIN_BODY_PCT   = float(os.getenv("MIN_BODY_PCT",      "0.50"))

H1_RANGE_BODY_PCT = float(os.getenv("H1_RANGE_BODY_PCT", "0.35"))
H1_RANGE_CANDLES  = int(os.getenv("H1_RANGE_CANDLES",    "2"))

SESSION_ASIA    = os.getenv("SESSION_ASIA",    "1") == "1"
SESSION_LONDON  = os.getenv("SESSION_LONDON",  "1") == "1"
SESSION_NEWYORK = os.getenv("SESSION_NEWYORK", "1") == "1"

# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------

def main():
    symbol = MT5_SYMBOLS[0]

    print(f"\n{'=' * 62}")
    print(f"  2-CANDLE MOMENTUM STRATEGY  -  DEMO MODE")
    print(f"{'=' * 62}")
    print(f"  Account      : {MT5_ACCOUNT}  ({MT5_SERVER})")
    print(f"  Symbol       : {symbol}")
    print(f"  Timeframe    : {TIMEFRAME}  (H1 used for ranging filter only)")
    print(f"  Lot size     : {TRADE_SIZE}")
    print(f"  TP / SL      : {TP_POINTS} / {SL_POINTS} pts")
    print(f"  BE trigger   : +{BE_TRIGGER} pts  ->  SL to entry +/-{BE_BUFFER} pts")
    print(f"  Trailing     : {TRAIL_PTS} pts  (after BE only)")
    print(f"  Daily cap    : ${DAILY_PROFIT:,.0f} profit  |  {DAILY_LOSS_PCT*100:.0f}% loss")
    print(f"  Max trades   : {MAX_TRADES}/day")
    print(f"  H1 range     : body < {H1_RANGE_BODY_PCT:.0%}, "
          f"{H1_RANGE_CANDLES} consecutive candles = block")
    print(f"  Sessions     : Asia={'ON' if SESSION_ASIA else 'OFF'}  "
          f"London={'ON' if SESSION_LONDON else 'OFF'}  "
          f"NY={'ON' if SESSION_NEWYORK else 'OFF'}")
    print(f"  Ctrl+C to stop cleanly")
    print(f"{'=' * 62}\n")

    mt5_config = MT5Config(
        account               = MT5_ACCOUNT,
        password              = MT5_PASSWORD,
        server                = MT5_SERVER,
        symbols               = MT5_SYMBOLS,
        poll_interval_ms      = 100,
        exec_poll_interval_ms = 250,
    )

    instrument_id  = f"{symbol}.MT5"
    bar_type_5m    = f"{instrument_id}-{TIMEFRAME}-LAST-INTERNAL"
    bar_type_h1    = f"{instrument_id}-1-HOUR-LAST-INTERNAL"

    strategy_config = CandleConfig(
        instrument_id      = instrument_id,
        bar_type           = bar_type_5m,
        h1_bar_type        = bar_type_h1,
        trade_size         = TRADE_SIZE,
        tp_points          = TP_POINTS,
        sl_points          = SL_POINTS,
        be_trigger_pts     = BE_TRIGGER,
        be_buffer_pts      = BE_BUFFER,
        trail_pts          = TRAIL_PTS,
        daily_profit_usd   = DAILY_PROFIT,
        daily_loss_pct     = DAILY_LOSS_PCT,
        max_trades_per_day = MAX_TRADES,
        min_body_pct       = MIN_BODY_PCT,
        h1_range_body_pct  = H1_RANGE_BODY_PCT,
        h1_range_candles   = H1_RANGE_CANDLES,
        session_asia_on    = SESSION_ASIA,
        session_london_on  = SESSION_LONDON,
        session_newyork_on = SESSION_NEWYORK,
    )

    node_config = build_mt5_node_config(mt5_config=mt5_config)
    node        = TradingNode(config=node_config)

    node.add_data_client_factory("MT5", MT5LiveDataClientFactory)
    node.add_exec_client_factory("MT5", MT5LiveExecClientFactory)
    node.trader.add_strategy(CandleStrategy(config=strategy_config))

    def _shutdown(sig, frame):
        print("\nShutdown signal received. Stopping strategy...")
        node.stop()
        time.sleep(2)
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    MAX_RECONNECT_DELAY = 5

    while True:
        try:
            print("Connecting to MT5 demo account...\n")
            node.build()
            node.run()
            break
        except (ConnectionError, TimeoutError, OSError) as exc:
            print(f"\nConnection error: {exc}\nReconnecting in {MAX_RECONNECT_DELAY}s ...")
            time.sleep(MAX_RECONNECT_DELAY)
        except KeyboardInterrupt:
            _shutdown(None, None)


if __name__ == "__main__":
    main()