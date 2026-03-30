"""
backtest.py  -  Backtest the 2-Candle Momentum Strategy (with H1 Ranging Filter)
==============================================================================
Usage
-----
Step 1 - Download XAUUSD data from MT5 (both M5 and H1):
    python backtest.py --download

Step 2 - Run the backtest:
    python backtest.py

CHANGING THE DATE RANGE
-----------------------
Edit BT_START and BT_END in the CONFIG section below.

NOTE: Your Exness Trial account holds history from ~Oct 2024.
      If you need data older than that, use a real/live Exness account
      in your .env, or run mt5_test.py first to check what is available.
==============================================================================
"""

import os
import sys
import argparse
import pathlib
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from nautilus_trader.backtest.engine import BacktestEngine, BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import Bar, BarType, BarSpecification
from nautilus_trader.model.enums import (
    AccountType, OmsType, AggregationSource, BarAggregation, PriceType,
    AssetClass,
)
from nautilus_trader.model.identifiers import Venue, InstrumentId, Symbol
from nautilus_trader.model.instruments import Cfd
from nautilus_trader.model.objects import Money, Price, Quantity

from strategy import CandleStrategy, CandleConfig

# ==============================================================================
# CONFIG  -  edit BT_START / BT_END to change the download/backtest window
# ==============================================================================

load_dotenv(dotenv_path=Path(__file__).parent / ".env")

SYMBOL       = os.getenv("MT5_SYMBOLS", "XAUUSD").split(",")[0].strip()
CATALOG_PATH = pathlib.Path("./catalog")

INSTRUMENT_ID_STR = f"{SYMBOL}.MT5"
BAR_TYPE_5M_STR   = f"{INSTRUMENT_ID_STR}-5-MINUTE-LAST-EXTERNAL"
BAR_TYPE_H1_STR   = f"{INSTRUMENT_ID_STR}-1-HOUR-LAST-EXTERNAL"

# ------------------------------------------------------------------------------
# DATE RANGE  <-- EDIT THESE TWO LINES TO CHANGE THE PERIOD
# ------------------------------------------------------------------------------
BT_START = datetime(2024, 10, 25, tzinfo=timezone.utc)   # start date (inclusive)
BT_END   = datetime(2026,  3, 27, tzinfo=timezone.utc)   # end date   (set None for latest bar)
# ------------------------------------------------------------------------------


# ==============================================================================
# STEP 1 - DOWNLOAD
# ==============================================================================

def download_data():
    import MetaTrader5 as mt5

    account  = os.getenv("MT5_ACCOUNT")
    password = os.getenv("MT5_PASSWORD")
    server   = os.getenv("MT5_SERVER")

    if not account:
        print("ERROR: MT5_ACCOUNT not set in .env")
        return

    print(f"Connecting to MT5 ({server}) ...")
    if not mt5.initialize(login=int(account), password=password, server=server):
        print("ERROR: mt5.initialize() failed:", mt5.last_error())
        return
    print("Connected. Account:", mt5.account_info().login)

    mt5.symbol_select(SYMBOL, True)
    CATALOG_PATH.mkdir(parents=True, exist_ok=True)

    # Resolve end date
    end_dt   = BT_END if BT_END is not None else datetime.now(timezone.utc)
    start_ts = int(BT_START.timestamp())
    end_ts   = int(end_dt.timestamp())

    print(f"\n  Date range : {BT_START.date()} -> {end_dt.date()}")
    print(f"  Symbol     : {SYMBOL}\n")

    # --------------------------------------------------------------------------
    # Fetch using copy_rates_from_pos (confirmed working on this terminal).
    # Pull a large batch from position 0 (most recent bar backwards),
    # then trim to the requested BT_START / BT_END window.
    # --------------------------------------------------------------------------
    MAX_BARS = 99_000   # stay safely under MT5's hard limit

    def _fetch_rates(timeframe, label):
        print(f"  Fetching {label} bars via copy_rates_from_pos ...")
        rates = mt5.copy_rates_from_pos(SYMBOL, timeframe, 0, MAX_BARS)
        if rates is None or len(rates) == 0:
            print(f"  ERROR: copy_rates_from_pos returned nothing for {label}.")
            print(f"         Last MT5 error: {mt5.last_error()}")
            return None

        print(f"  Raw fetch  : {len(rates):,} {label} bars  "
              f"({datetime.fromtimestamp(rates[0][0], tz=timezone.utc).date()} -> "
              f"{datetime.fromtimestamp(rates[-1][0], tz=timezone.utc).date()})")

        # Trim to BT_START / BT_END
        rates = rates[(rates["time"] >= start_ts) & (rates["time"] <= end_ts)]

        if len(rates) == 0:
            print(f"  ERROR: No {label} bars remain after trimming to requested date range.")
            print(f"         Your account history may not cover {BT_START.date()} -> {end_dt.date()}.")
            return None

        print(f"  After trim : {len(rates):,} {label} bars  "
              f"({datetime.fromtimestamp(rates[0][0], tz=timezone.utc).date()} -> "
              f"{datetime.fromtimestamp(rates[-1][0], tz=timezone.utc).date()})")
        return rates

    # --------------------------------------------------------------------------
    # Download M5
    # --------------------------------------------------------------------------
    print(f"Downloading {SYMBOL} M5 bars ...")
    rates_m5 = _fetch_rates(mt5.TIMEFRAME_M5, "M5")
    if rates_m5 is None:
        mt5.shutdown()
        return

    # --------------------------------------------------------------------------
    # Download H1
    # --------------------------------------------------------------------------
    print(f"\nDownloading {SYMBOL} H1 bars ...")
    rates_h1 = _fetch_rates(mt5.TIMEFRAME_H1, "H1")
    if rates_h1 is None:
        mt5.shutdown()
        return

    mt5.shutdown()

    # --------------------------------------------------------------------------
    # Save to parquet
    # --------------------------------------------------------------------------
    def _save(rates, filename: str) -> None:
        df = pd.DataFrame(rates)
        df["ts_event"] = (df["time"] * 1_000_000_000).astype("int64")
        df["ts_init"]  = df["ts_event"]
        df = df.rename(columns={"tick_volume": "volume"})
        for col in ["open", "high", "low", "close"]:
            df[col] = (df[col] * 1_000_000_000).astype("int64")
        df["volume"] = (df["volume"] * 1_000_000_000).astype("int64")
        df = df[["ts_event", "ts_init", "open", "high", "low", "close", "volume"]]
        df = df.sort_values("ts_event").reset_index(drop=True)
        out = CATALOG_PATH / filename
        df.to_parquet(out, index=False)
        first = datetime.fromtimestamp(df["ts_event"].iloc[0]  / 1e9, tz=timezone.utc)
        last  = datetime.fromtimestamp(df["ts_event"].iloc[-1] / 1e9, tz=timezone.utc)
        print(f"  Saved {len(df):,} rows -> {out}  ({first.date()} to {last.date()})")

    print()
    _save(rates_m5, "raw_bars_m5.parquet")
    _save(rates_h1, "raw_bars_h1.parquet")
    print("\nDone. Now run:  python backtest.py")


# ==============================================================================
# BUILD INSTRUMENT
# ==============================================================================

def _build_instrument() -> Cfd:
    """XAUUSD CFD on the simulated MT5 venue."""
    venue         = Venue("MT5")
    instrument_id = InstrumentId(Symbol(SYMBOL), venue)
    return Cfd(
        instrument_id   = instrument_id,
        raw_symbol      = Symbol(SYMBOL),
        asset_class     = AssetClass.COMMODITY,
        quote_currency  = USD,
        price_precision = 2,
        size_precision  = 2,
        price_increment = Price.from_str("0.01"),
        size_increment  = Quantity.from_str("0.01"),
        max_quantity    = Quantity.from_str("1000.00"),
        min_quantity    = Quantity.from_str("0.01"),
        max_notional    = None,
        min_notional    = None,
        max_price       = Price.from_str("99999.99"),
        min_price       = Price.from_str("0.01"),
        margin_init     = Decimal("0.01"),
        margin_maint    = Decimal("0.005"),
        maker_fee       = Decimal("0.0"),
        taker_fee       = Decimal("0.0"),
        ts_event        = 0,
        ts_init         = 0,
    )


# ==============================================================================
# CONVERT PARQUET -> Bar objects
# ==============================================================================

def _df_to_bars(df: pd.DataFrame, instrument: Cfd, bar_type: BarType) -> list:
    prec = instrument.price_precision
    bars = []
    for row in df.itertuples(index=False):
        bars.append(
            Bar(
                bar_type = bar_type,
                open     = Price(row.open  / 1e9, prec),
                high     = Price(row.high  / 1e9, prec),
                low      = Price(row.low   / 1e9, prec),
                close    = Price(row.close / 1e9, prec),
                volume   = Quantity(row.volume / 1e9, instrument.size_precision),
                ts_event = int(row.ts_event),
                ts_init  = int(row.ts_init),
            )
        )
    return bars


# ==============================================================================
# STEP 2 - BACKTEST
# ==============================================================================

def run_backtest():
    m5_file = CATALOG_PATH / "raw_bars_m5.parquet"
    h1_file = CATALOG_PATH / "raw_bars_h1.parquet"

    for f in (m5_file, h1_file):
        if not f.exists():
            print(f"ERROR: {f} not found.")
            print("       Run first:  python backtest.py --download")
            sys.exit(1)

    print("Loading M5 parquet ...")
    df_m5    = pd.read_parquet(m5_file)
    first_ts = datetime.fromtimestamp(int(df_m5["ts_event"].iloc[0])  / 1e9, tz=timezone.utc)
    last_ts  = datetime.fromtimestamp(int(df_m5["ts_event"].iloc[-1]) / 1e9, tz=timezone.utc)
    print(f"  {len(df_m5):,} M5 rows  |  {first_ts.date()} -> {last_ts.date()}")

    print("Loading H1 parquet ...")
    df_h1 = pd.read_parquet(h1_file)
    print(f"  {len(df_h1):,} H1 rows")

    print("Building instrument ...")
    instrument = _build_instrument()
    print(f"  {instrument.id}")

    bar_type_m5 = BarType(
        instrument_id      = instrument.id,
        bar_spec           = BarSpecification(5, BarAggregation.MINUTE, PriceType.LAST),
        aggregation_source = AggregationSource.EXTERNAL,
    )
    bar_type_h1 = BarType(
        instrument_id      = instrument.id,
        bar_spec           = BarSpecification(1, BarAggregation.HOUR, PriceType.LAST),
        aggregation_source = AggregationSource.EXTERNAL,
    )

    print("Converting M5 bars ...")
    bars_m5 = _df_to_bars(df_m5, instrument, bar_type_m5)
    print(f"  {len(bars_m5):,} M5 Bar objects")

    print("Converting H1 bars ...")
    bars_h1 = _df_to_bars(df_h1, instrument, bar_type_h1)
    print(f"  {len(bars_h1):,} H1 Bar objects")

    # Read strategy parameters from .env
    session_asia    = os.getenv("SESSION_ASIA",    "1") == "1"
    session_london  = os.getenv("SESSION_LONDON",  "1") == "1"
    session_newyork = os.getenv("SESSION_NEWYORK", "1") == "1"

    strategy_cfg = CandleConfig(
        instrument_id      = INSTRUMENT_ID_STR,
        bar_type           = BAR_TYPE_5M_STR,
        h1_bar_type        = BAR_TYPE_H1_STR,
        trade_size         = Decimal(os.getenv("TRADE_SIZE",         "0.01")),
        tp_points          = int(os.getenv("TP_POINTS",              "6000")),
        sl_points          = int(os.getenv("SL_POINTS",              "2000")),
        be_trigger_pts     = int(os.getenv("BE_TRIGGER_PTS",         "1100")),
        be_buffer_pts      = int(os.getenv("BE_BUFFER_PTS",          "150")),
        trail_pts          = int(os.getenv("TRAIL_PTS",              "1500")),
        daily_profit_usd   = float(os.getenv("DAILY_PROFIT",         "500.0")),
        daily_loss_pct     = float(os.getenv("DAILY_LOSS_PCT",       "0.50")),
        max_trades_per_day = int(os.getenv("MAX_TRADES",              "500")),
        min_body_pct       = float(os.getenv("MIN_BODY_PCT",          "0.50")),
        h1_range_body_pct  = float(os.getenv("H1_RANGE_BODY_PCT",    "0.35")),
        h1_range_candles   = int(os.getenv("H1_RANGE_CANDLES",       "2")),
        session_asia_on    = session_asia,
        session_london_on  = session_london,
        session_newyork_on = session_newyork,
    )

    engine = BacktestEngine(
        config=BacktestEngineConfig(
            logging=LoggingConfig(log_level="WARNING"),
        )
    )

    engine.add_venue(
        venue             = Venue("MT5"),
        oms_type          = OmsType.NETTING,
        account_type      = AccountType.MARGIN,
        base_currency     = USD,
        starting_balances = [Money(10_000, USD)],
    )

    engine.add_instrument(instrument)

    # Add H1 bars first so ranging state is warm before 5M entries are processed
    engine.add_data(bars_h1)
    engine.add_data(bars_m5)

    engine.add_strategy(CandleStrategy(config=strategy_cfg))

    sessions_str = "  |  ".join(filter(None, [
        "Asia"     if session_asia    else "",
        "London"   if session_london  else "",
        "New York" if session_newyork else "",
    ])) or "NONE"

    print()
    print("=" * 62)
    print(f"  BACKTEST: 2-Candle Momentum on {SYMBOL}")
    print(f"  Data    : {first_ts.date()} -> {last_ts.date()}  ({len(bars_m5):,} M5 bars)")
    print(f"  Capital : $10,000")
    print(f"  TP/SL   : {strategy_cfg.tp_points} / {strategy_cfg.sl_points} pts")
    print(f"  BE      : +{strategy_cfg.be_trigger_pts} pts -> "
          f"+/-{strategy_cfg.be_buffer_pts} pts buffer")
    print(f"  Trail   : {strategy_cfg.trail_pts} pts (after BE)")
    print(f"  H1 range: body < {strategy_cfg.h1_range_body_pct:.0%}, "
          f"{strategy_cfg.h1_range_candles} candles = block")
    print(f"  Sessions: {sessions_str}")
    print("=" * 62)
    print()

    engine.run()

    print()
    print("=" * 62)
    print("  BACKTEST RESULTS")
    print("=" * 62)

    result = engine.get_result()
    print(f"  Iterations      : {result.iterations:,}")
    print(f"  Total events    : {result.total_events:,}")
    print(f"  Total orders    : {result.total_orders:,}")
    print(f"  Total positions : {result.total_positions:,}")
    print()

    for currency, stats in result.stats_pnls.items():
        print(f"  -- PnL ({currency}) --")
        for k, v in stats.items():
            if v is not None and str(v) not in ("nan", "None"):
                print(f"     {k:<35}: {v}")
    print()

    print("  -- Returns --")
    for k, v in result.stats_returns.items():
        if v is not None and str(v) not in ("nan", "None"):
            print(f"     {k:<35}: {v}")

    print()
    print("=" * 62)
    print("  Account Report")
    print("=" * 62)
    engine.trader.generate_account_report(Venue("MT5"))

    engine.dispose()
    print("\nBacktest complete.")


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="2-Candle Momentum Backtester with H1 Ranging Filter"
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download M5 and H1 bar data from MT5 for the BT_START/BT_END range",
    )
    args = parser.parse_args()

    if args.download:
        download_data()
    else:
        run_backtest()