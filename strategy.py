"""
strategy.py  -  2-Candle Momentum Strategy with H1 Ranging Filter
==============================================================================
Instrument : XAUUSD / XAUUSDm  (Gold)
Broker     : Exness via MetaTrader 5
Framework  : NautilusTrader + mt5connect adapter

STRATEGY RULES
--------------
- Body Filter  : BodyPct = |Close - Open| / (High - Low)  -> must be 0.50-1.00
- BUY  signal  : 2 consecutive BULLISH candles passing the filter (5M)
- SELL signal  : 2 consecutive BEARISH candles passing the filter (5M)
- Execution    : Market order IMMEDIATELY on close of Candle 2 (no delay)
- One trade    : Only 1 open position at any time

H1 RANGING FILTER
-----------------
The H1 chart is used ONLY to detect a ranging (consolidating) market.
When H1 is ranging, all new entries are blocked regardless of 5M signal.
H1 is never used for trade direction or execution - only to suppress entries.

Ranging is defined as: the H1 candle body is less than range_body_pct of
the H1 high-low range AND price is within the ATR band of the recent H1 high/low.
Two consecutive H1 candles with small bodies -> ranging flag ON.
One H1 candle with a strong body (>= range_body_pct) -> ranging flag OFF.

RISK MANAGEMENT
---------------
- TP  = 6000 points   |  SL = 2000 points
- Break-even : profit >= 1100 pts -> SL moves to entry +/- 150 pts
- Trailing SL (1500 pts) activates ONLY after BE+Buffer fires - NEVER before
- Daily profit cap : $500   |  Daily loss cap : 50% of starting balance
- Max 500 trades / day

SESSION FILTER  (UTC - Exness server time = UTC+0)
--------------------------------------------------
- Asia    : 00:00 - 09:00 UTC
- London  : 07:00 - 16:00 UTC
- New York: 12:00 - 21:00 UTC

==============================================================================
"""

import datetime
from decimal import Decimal
from typing import Optional, List

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide, PositionSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId, ClientOrderId
from nautilus_trader.model.objects import Quantity, Price
from nautilus_trader.trading.strategy import Strategy


# ==============================================================================
# CONFIGURATION
# ==============================================================================

class CandleConfig(StrategyConfig, frozen=True):
    """All tuneable parameters for the 2-Candle Momentum Strategy."""

    # -- Instrument ------------------------------------------------------------
    instrument_id: str      # e.g. "XAUUSDm.MT5"
    bar_type:      str      # e.g. "XAUUSDm.MT5-5-MINUTE-LAST-INTERNAL"
    h1_bar_type:   str      # e.g. "XAUUSDm.MT5-1-HOUR-LAST-INTERNAL"

    # -- Position sizing -------------------------------------------------------
    trade_size: Decimal = Decimal("0.01")

    # -- 5M Candle body filter -------------------------------------------------
    min_body_pct: float = 0.50   # Minimum body / range ratio (50%)
    max_body_pct: float = 1.00   # Maximum body / range ratio (100%)

    # -- H1 Ranging filter -----------------------------------------------------
    # H1 candles with body < h1_range_body_pct are considered "ranging" candles.
    h1_range_body_pct:    float = 0.35   # H1 body < 35% of H1 range = weak/ranging candle
    # How many consecutive weak H1 candles trigger the ranging flag
    h1_range_candles:     int   = 2
    # How many H1 candles to keep in the lookback buffer
    h1_lookback:          int   = 10

    # -- Risk management (points; 1 point = price_increment) ------------------
    tp_points:      int = 6000   # Take-profit distance in points
    sl_points:      int = 2000   # Stop-loss distance in points
    be_trigger_pts: int = 1100   # Profit points that fire break-even
    be_buffer_pts:  int = 150    # SL offset above/below entry after BE fires
    trail_pts:      int = 1500   # Trailing SL distance - AFTER BE only

    # -- Daily risk limits -----------------------------------------------------
    daily_profit_usd:   float = 500.0    # Hard-stop on day profit ($)
    daily_loss_pct:     float = 0.50     # Hard-stop on day loss (50% of balance)
    max_trades_per_day: int   = 500

    # -- Session filters (UTC) -------------------------------------------------
    session_asia_on:    bool = True    # 00:00 - 09:00 UTC
    session_london_on:  bool = True    # 07:00 - 16:00 UTC
    session_newyork_on: bool = True    # 12:00 - 21:00 UTC

    # -- Heartbeat -------------------------------------------------------------
    heartbeat_secs: int = 600


# ==============================================================================
# STRATEGY
# ==============================================================================

class CandleStrategy(Strategy):
    """
    2-Candle Momentum Strategy with H1 Ranging Filter.

    5M signal logic
    ---------------
    consecutive_bull / consecutive_bear count valid same-direction 5M candles.
    Both reset on direction change or failed body filter.
    When either counter reaches exactly 2, a market order fires IMMEDIATELY
    on that bar close - no third-candle wait, no confirmation.

    H1 ranging detection
    --------------------
    A rolling buffer of recent H1 candles is maintained.
    A candle is "weak" if its body is < h1_range_body_pct of its range.
    If the last h1_range_candles consecutive H1 candles are all weak,
    is_ranging = True and all new 5M entries are blocked.
    is_ranging = False as soon as one strong H1 candle closes.

    Risk management
    ---------------
    TP and SL bracket orders placed on entry fill.
    BE fires at be_trigger_pts profit; SL moves to entry +/- be_buffer_pts.
    Trailing (trail_pts) starts ONLY after be_active is True - hard guard.
    On close, remaining bracket order cancelled and all state reset.
    """

    def __init__(self, config: CandleConfig) -> None:
        super().__init__(config)

        self.instrument_id = InstrumentId.from_str(config.instrument_id)
        self.bar_type      = BarType.from_str(config.bar_type)
        self.h1_bar_type   = BarType.from_str(config.h1_bar_type)

        # 5M streak counters
        self.consecutive_bull: int = 0
        self.consecutive_bear: int = 0

        # H1 ranging state
        self.h1_bars:   List[Bar] = []    # rolling buffer of recent H1 bars
        self.is_ranging: bool     = False  # True = H1 is ranging, block entries

        # Daily accounting
        self.trades_today:     int   = 0
        self.daily_pnl_usd:    float = 0.0
        self.starting_balance: float = 0.0
        self.day_reset_date:   Optional[datetime.date] = None

        # Break-even / trailing state
        self.be_active:       bool  = False
        self.trailing_active: bool  = False
        self.trailing_sl:     float = 0.0

        # Bracket order IDs
        self.sl_order_id: Optional[ClientOrderId] = None
        self.tp_order_id: Optional[ClientOrderId] = None

        # Heartbeat
        self._last_heartbeat_ts: int = 0

    # --------------------------------------------------------------------------
    # LIFECYCLE
    # --------------------------------------------------------------------------

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.instrument_id)
        if self.instrument is None:
            self.log.error(
                f"Instrument {self.instrument_id} not found in cache. "
                "Verify MT5 data client is connected and symbol is correct."
            )
            return

        # Subscribe to both 5M and H1 bar feeds
        self.subscribe_bars(self.bar_type)
        self.subscribe_bars(self.h1_bar_type)

        account = self.portfolio.account(self.instrument_id.venue)
        if account:
            self.starting_balance = self._read_balance(account)
        self.log.info(f"Starting balance : ${self.starting_balance:,.2f}")

        # State recovery after restart
        open_positions = self.cache.positions_open(instrument_id=self.instrument_id)
        if open_positions:
            pos = open_positions[0]
            self.log.warning(
                f"STATE RECOVERY: Found existing {pos.side.name} position "
                f"(entry={pos.avg_px_open:.3f}). Re-attaching risk logic."
            )
        else:
            self.log.info("No existing position. Ready for fresh entries.")

        sessions = []
        if self.config.session_asia_on:    sessions.append("Asia (00:00-09:00 UTC)")
        if self.config.session_london_on:  sessions.append("London (07:00-16:00 UTC)")
        if self.config.session_newyork_on: sessions.append("New York (12:00-21:00 UTC)")
        session_str = ", ".join(sessions) if sessions else "NONE"

        self.log.info(
            f"=== 2-CANDLE MOMENTUM STRATEGY ACTIVE ===\n"
            f"    Symbol    : {self.instrument_id}\n"
            f"    5M bars   : {self.bar_type}\n"
            f"    H1 bars   : {self.h1_bar_type}\n"
            f"    Lot size  : {self.config.trade_size}\n"
            f"    TP / SL   : {self.config.tp_points} / {self.config.sl_points} pts\n"
            f"    BE trigger: +{self.config.be_trigger_pts} pts -> "
            f"SL to entry +/-{self.config.be_buffer_pts} pts\n"
            f"    Trail     : {self.config.trail_pts} pts (AFTER BE only)\n"
            f"    Daily cap : ${self.config.daily_profit_usd:,.0f} profit | "
            f"{self.config.daily_loss_pct * 100:.0f}% loss\n"
            f"    Sessions  : {session_str}\n"
            f"    H1 range  : body < {self.config.h1_range_body_pct:.0%} "
            f"x {self.config.h1_range_candles} candles -> block entries\n"
        )

    def on_stop(self) -> None:
        self.close_all_positions(instrument_id=self.instrument_id)
        self.log.info("Strategy stopped. All positions closed.")

    # --------------------------------------------------------------------------
    # H1 BAR HANDLER  (ranging detection only - never used for execution)
    # --------------------------------------------------------------------------

    def on_bar(self, bar: Bar) -> None:
        """
        Routes incoming bars to the correct handler based on bar type.
        H1 bars -> ranging detection only.
        5M bars -> entry logic + risk management.
        """
        if bar.bar_type == self.h1_bar_type:
            self._process_h1_bar(bar)
        elif bar.bar_type == self.bar_type:
            self._process_5m_bar(bar)

    def _process_h1_bar(self, bar: Bar) -> None:
        """
        Updates the H1 ranging flag.
        A candle is 'weak' (ranging) if its body < h1_range_body_pct of range.
        If the last h1_range_candles consecutive candles are all weak -> ranging.
        One strong candle clears the ranging flag immediately.
        """
        high   = float(bar.high)
        low    = float(bar.low)
        open_  = float(bar.open)
        close  = float(bar.close)
        range_ = high - low

        body_pct = abs(close - open_) / range_ if range_ > 0 else 0.0
        is_weak  = body_pct < self.config.h1_range_body_pct

        # Maintain rolling buffer
        self.h1_bars.append(bar)
        if len(self.h1_bars) > self.config.h1_lookback:
            self.h1_bars.pop(0)

        # Check the last N candles
        n = self.config.h1_range_candles
        if len(self.h1_bars) >= n:
            recent = self.h1_bars[-n:]
            all_weak = all(self._is_h1_weak(b) for b in recent)
            prev_ranging = self.is_ranging
            self.is_ranging = all_weak

            if self.is_ranging and not prev_ranging:
                self.log.info(
                    f"H1 RANGING DETECTED | H1 body={body_pct:.2%} "
                    f"(< {self.config.h1_range_body_pct:.0%} threshold) | "
                    f"Entries BLOCKED until H1 shows a strong candle."
                )
            elif not self.is_ranging and prev_ranging:
                self.log.info(
                    f"H1 TRENDING RESUMED | H1 body={body_pct:.2%} "
                    f"(>= {self.config.h1_range_body_pct:.0%} threshold) | "
                    f"Entries ALLOWED again."
                )

    def _is_h1_weak(self, bar: Bar) -> bool:
        """Returns True if this H1 candle's body is below the ranging threshold."""
        high   = float(bar.high)
        low    = float(bar.low)
        open_  = float(bar.open)
        close  = float(bar.close)
        range_ = high - low
        body_pct = abs(close - open_) / range_ if range_ > 0 else 0.0
        return body_pct < self.config.h1_range_body_pct

    # --------------------------------------------------------------------------
    # 5M BAR HANDLER  (entry logic + risk management)
    # --------------------------------------------------------------------------

    def _process_5m_bar(self, bar: Bar) -> None:
        """
        Called on every closed 5M candle. Execution order:
          1. Day-reset check
          2. Heartbeat
          3. Daily safety limits
          4. Body-percentage filter -> update streak counters
          5. Risk management (BE + trailing SL) for open position
          6. H1 ranging check + session filter + entry
        """

        # 1. Daily reset
        self._check_day_reset()

        # 2. Heartbeat
        now_ts = int(self.clock.utc_now().timestamp())
        if now_ts - self._last_heartbeat_ts >= self.config.heartbeat_secs:
            self.log.info(
                f"HEARTBEAT | Price={float(bar.close):.3f} | "
                f"Trades={self.trades_today} | PnL=${self.daily_pnl_usd:+.2f} | "
                f"Streak Bull={self.consecutive_bull} Bear={self.consecutive_bear} | "
                f"H1 Ranging={self.is_ranging}"
            )
            self._last_heartbeat_ts = now_ts

        # 3. Daily safety limits
        if self._daily_limits_hit():
            return

        # 4. 5M candle body filter and streak update
        high   = float(bar.high)
        low    = float(bar.low)
        open_  = float(bar.open)
        close  = float(bar.close)
        range_ = high - low

        body_pct = abs(close - open_) / range_ if range_ > 0 else 0.0
        is_valid = self.config.min_body_pct <= body_pct <= self.config.max_body_pct
        is_bull  = close > open_
        is_bear  = close < open_

        if is_valid and is_bull:
            self.consecutive_bull += 1
            self.consecutive_bear  = 0
            self.log.debug(
                f"Valid BULL 5M | body={body_pct:.2%} | streak={self.consecutive_bull}"
            )
        elif is_valid and is_bear:
            self.consecutive_bear += 1
            self.consecutive_bull  = 0
            self.log.debug(
                f"Valid BEAR 5M | body={body_pct:.2%} | streak={self.consecutive_bear}"
            )
        else:
            if self.consecutive_bull > 0 or self.consecutive_bear > 0:
                self.log.debug(
                    f"5M candle rejected | body={body_pct:.2%} "
                    f"(need {self.config.min_body_pct:.0%}-{self.config.max_body_pct:.0%}) "
                    f"| streaks reset (bull={self.consecutive_bull} bear={self.consecutive_bear})"
                )
            self.consecutive_bull = 0
            self.consecutive_bear = 0

        # 5. Risk management for any open position
        self._manage_open_position(close)

        # 6. Entry checks
        if self.cache.positions_open(instrument_id=self.instrument_id):
            return   # Already in a trade

        if self.trades_today >= self.config.max_trades_per_day:
            return   # Daily trade cap

        # H1 ranging check - block entries during ranging market
        if self.is_ranging:
            self.log.debug(
                "Entry blocked: H1 market is ranging. "
                "Waiting for H1 to show a strong directional candle."
            )
            return

        # Session filter
        if not self._in_active_session():
            return

        # Fire IMMEDIATELY when streak reaches exactly 2
        if self.consecutive_bull == 2:
            self.log.info(
                f"BUY SIGNAL | 2 consecutive bull 5M candles | "
                f"close={close:.3f} | H1 trending (not ranging)"
            )
            self._submit_market_order(OrderSide.BUY)
            self.consecutive_bull = 0   # Reset - same pair must not re-trigger

        elif self.consecutive_bear == 2:
            self.log.info(
                f"SELL SIGNAL | 2 consecutive bear 5M candles | "
                f"close={close:.3f} | H1 trending (not ranging)"
            )
            self._submit_market_order(OrderSide.SELL)
            self.consecutive_bear = 0   # Reset - same pair must not re-trigger

    # --------------------------------------------------------------------------
    # SESSION FILTER
    # --------------------------------------------------------------------------

    def _in_active_session(self) -> bool:
        """
        Returns True if current UTC time is inside at least one enabled session.
          Asia    : 00:00 - 09:00 UTC
          London  : 07:00 - 16:00 UTC
          New York: 12:00 - 21:00 UTC
        """
        utc_now = self.clock.utc_now()
        hm      = utc_now.hour * 60 + utc_now.minute

        def mins(h: int, m: int = 0) -> int:
            return h * 60 + m

        if self.config.session_asia_on    and mins(0)  <= hm < mins(9):
            return True
        if self.config.session_london_on  and mins(7)  <= hm < mins(16):
            return True
        if self.config.session_newyork_on and mins(12) <= hm < mins(21):
            return True

        self.log.debug(
            f"Outside all active sessions @ {utc_now.strftime('%H:%M')} UTC"
        )
        return False

    # --------------------------------------------------------------------------
    # TRADE SUBMISSION
    # --------------------------------------------------------------------------

    def _submit_market_order(self, side: OrderSide) -> None:
        """Submit a market order immediately (IOC - fastest MT5 fill)."""
        self.be_active       = False
        self.trailing_active = False
        self.trailing_sl     = 0.0
        self.sl_order_id     = None
        self.tp_order_id     = None

        qty = Quantity.from_str(str(self.config.trade_size))

        order = self.order_factory.market(
            instrument_id = self.instrument_id,
            order_side    = side,
            quantity      = qty,
            time_in_force = TimeInForce.IOC,
        )

        self.submit_order(order)
        self.trades_today += 1

        self.log.info(
            f"ORDER SUBMITTED | {side.name} {qty} {self.instrument_id} | "
            f"Trade #{self.trades_today} today"
        )

    # --------------------------------------------------------------------------
    # RISK MANAGEMENT  (called every 5M bar while position is open)
    # --------------------------------------------------------------------------

    def _manage_open_position(self, current_price: float) -> None:
        """
        Step 1 - Break-even + buffer
            Fires when unrealised profit >= be_trigger_pts.
            SL moves to entry +/- be_buffer_pts.
            trailing_sl is primed at the new SL level.

        Step 2 - Trailing SL (STRICTLY after be_active is True)
            Hard guard prevents any trailing before BE fires.
            LONG : trailing_sl rises when price rises.
            SHORT: trailing_sl falls when price falls.
        """
        positions = self.cache.positions_open(instrument_id=self.instrument_id)
        if not positions:
            return

        pos   = positions[0]
        entry = float(pos.avg_px_open)
        point = float(self.instrument.price_increment)

        if pos.side == PositionSide.LONG:
            profit_pts = (current_price - entry) / point
        else:
            profit_pts = (entry - current_price) / point

        # Update daily PnL with unrealised mark-to-market
        try:
            unreal = self.portfolio.unrealized_pnl(self.instrument_id)
            if unreal is not None:
                self.daily_pnl_usd = float(unreal.as_double())
        except Exception:
            pass

        # Step 1 - Break-even + buffer
        if not self.be_active and profit_pts >= self.config.be_trigger_pts:
            if pos.side == PositionSide.LONG:
                new_sl = entry + self.config.be_buffer_pts * point
            else:
                new_sl = entry - self.config.be_buffer_pts * point

            self.be_active   = True
            self.trailing_sl = new_sl

            self.log.info(
                f"BREAK-EVEN TRIGGERED | Profit={profit_pts:.0f} pts | "
                f"SL -> {new_sl:.5f} "
                f"({'entry+' if pos.side == PositionSide.LONG else 'entry-'}"
                f"{self.config.be_buffer_pts} pts)"
            )
            self._update_sl_price(new_sl, pos.side)

        # Step 2 - Trailing SL (ONLY after BE is active - hard guard)
        if not self.be_active:
            return

        trail_dist = self.config.trail_pts * point

        if pos.side == PositionSide.LONG:
            candidate = current_price - trail_dist
            if candidate > self.trailing_sl:
                self.trailing_sl     = candidate
                self.trailing_active = True
                self.log.info(
                    f"TRAILING SL raised -> {self.trailing_sl:.5f} "
                    f"(price={current_price:.5f})"
                )
                self._update_sl_price(self.trailing_sl, pos.side)

        else:  # SHORT
            candidate = current_price + trail_dist
            # trailing_sl == 0.0 means not yet initialised for this trade
            if self.trailing_sl == 0.0 or candidate < self.trailing_sl:
                self.trailing_sl     = candidate
                self.trailing_active = True
                self.log.info(
                    f"TRAILING SL lowered -> {self.trailing_sl:.5f} "
                    f"(price={current_price:.5f})"
                )
                self._update_sl_price(self.trailing_sl, pos.side)

    def _update_sl_price(self, new_sl: float, side: PositionSide) -> None:
        """
        Modify the trigger price on the open SL stop-market order.
        Falls back to a full cache scan if sl_order_id is not yet stored.
        """
        self.log.info(f"SL update requested -> {new_sl:.5f} [{side.name}]")

        sl_order = None

        if self.sl_order_id is not None:
            sl_order = self.cache.order(self.sl_order_id)

        if sl_order is None or not sl_order.is_open:
            for o in self.cache.orders_open(instrument_id=self.instrument_id):
                if o.order_type.name in ("STOP_MARKET", "STOP_LIMIT"):
                    sl_order         = o
                    self.sl_order_id = o.client_order_id
                    self.log.info(
                        f"SL order located via cache scan: {o.client_order_id}"
                    )
                    break

        if sl_order is not None and sl_order.is_open:
            try:
                self.modify_order(
                    order         = sl_order,
                    trigger_price = Price(new_sl, self.instrument.price_precision),
                )
                self.log.info(f"SL modified -> {new_sl:.5f}")
            except Exception as exc:
                self.log.error(f"SL modify failed: {exc}")

    # --------------------------------------------------------------------------
    # ORDER / POSITION EVENT HOOKS
    # --------------------------------------------------------------------------

    def on_order_filled(self, event) -> None:
        """Place TP and SL bracket orders immediately on entry fill."""
        order = self.cache.order(event.client_order_id)
        if order is None:
            return

        # Only act on the entry market fill
        if order.order_type.name != "MARKET":
            return

        positions = self.cache.positions_open(instrument_id=self.instrument_id)
        if not positions:
            return

        pos   = positions[0]
        entry = float(pos.avg_px_open)
        point = float(self.instrument.price_increment)
        prec  = self.instrument.price_precision

        if pos.side == PositionSide.LONG:
            tp_price = entry + self.config.tp_points * point
            sl_price = entry - self.config.sl_points * point
        else:
            tp_price = entry - self.config.tp_points * point
            sl_price = entry + self.config.sl_points * point

        qty = pos.quantity

        self.log.info(
            f"ENTRY FILLED | {pos.side.name} @ {entry:.5f} | "
            f"TP={tp_price:.5f}  SL={sl_price:.5f}"
        )

        # Stop-loss order
        sl_side  = OrderSide.SELL if pos.side == PositionSide.LONG else OrderSide.BUY
        sl_order = self.order_factory.stop_market(
            instrument_id = self.instrument_id,
            order_side    = sl_side,
            quantity      = qty,
            trigger_price = Price(sl_price, prec),
            time_in_force = TimeInForce.GTC,
        )
        self.sl_order_id = sl_order.client_order_id
        self.submit_order(sl_order)

        # Take-profit order
        tp_side  = OrderSide.SELL if pos.side == PositionSide.LONG else OrderSide.BUY
        tp_order = self.order_factory.limit(
            instrument_id = self.instrument_id,
            order_side    = tp_side,
            quantity      = qty,
            price         = Price(tp_price, prec),
            time_in_force = TimeInForce.GTC,
        )
        self.tp_order_id = tp_order.client_order_id
        self.submit_order(tp_order)

    def on_position_closed(self, event) -> None:
        """Cancel surviving bracket order and reset all state on position close."""
        self.log.info(f"POSITION CLOSED | PnL: {event.realized_pnl}")

        if event.realized_pnl is not None:
            self.daily_pnl_usd += float(event.realized_pnl.as_double())

        for oid in (self.sl_order_id, self.tp_order_id):
            if oid is not None:
                o = self.cache.order(oid)
                if o is not None and o.is_open:
                    try:
                        self.cancel_order(o)
                    except Exception as exc:
                        self.log.warning(
                            f"Could not cancel bracket order {oid}: {exc}"
                        )

        self.be_active       = False
        self.trailing_active = False
        self.trailing_sl     = 0.0
        self.sl_order_id     = None
        self.tp_order_id     = None

        self.log.info(f"Daily PnL so far: ${self.daily_pnl_usd:+.2f}")

    # --------------------------------------------------------------------------
    # DAILY HELPERS
    # --------------------------------------------------------------------------

    def _read_balance(self, account) -> float:
        """
        Safely read the total account balance for Exness MT5 accounts.

        Exness uses a multi-currency margin account that has no single
        base currency, so account.balance_total() requires an explicit
        Currency argument.  We iterate over all available balances and
        return the USD total (or the first available currency if USD is
        not present).  Falls back to 0.0 if nothing is available.
        """
        try:
            from nautilus_trader.model.currencies import USD
            # Try USD first (standard for Exness Gold accounts)
            total = account.balance_total(USD)
            if total is not None:
                return float(total.as_double())
        except Exception:
            pass

        # Fallback: sum all reported balances regardless of currency
        try:
            balances = account.balances()
            if balances:
                # balances() returns a dict {Currency: AccountBalance}
                # or a list of AccountBalance objects depending on version
                if isinstance(balances, dict):
                    values = list(balances.values())
                else:
                    values = list(balances)
                if values:
                    return float(values[0].total.as_double())
        except Exception:
            pass

        self.log.warning(
            "Could not read account balance - defaulting to 0.0. "
            "Daily loss % guard will be inactive until balance is sampled."
        )
        return 0.0

    def _check_day_reset(self) -> None:
        """Reset counters and re-sample balance at midnight UTC."""
        today = self.clock.utc_now().date()
        if self.day_reset_date != today:
            self.day_reset_date = today
            self.trades_today   = 0
            self.daily_pnl_usd  = 0.0
            account = self.portfolio.account(self.instrument_id.venue)
            if account:
                self.starting_balance = self._read_balance(account)
            self.log.info(
                f"NEW TRADING DAY: {today}  "
                f"Balance=${self.starting_balance:,.2f}  Counters reset."
            )

    def _daily_limits_hit(self) -> bool:
        """
        Returns True if any daily guard has fired:
          1. Profit  >= daily_profit_usd
          2. Loss    >= daily_loss_pct x starting_balance  (closes open position)
          3. Trades  >= max_trades_per_day
        """
        if self.daily_pnl_usd >= self.config.daily_profit_usd:
            self.log.warning(
                f"DAILY PROFIT CAP HIT: ${self.daily_pnl_usd:+.2f} >= "
                f"${self.config.daily_profit_usd:,.0f}. No new entries today."
            )
            return True

        if self.starting_balance > 0:
            max_loss = self.starting_balance * self.config.daily_loss_pct
            if self.daily_pnl_usd <= -max_loss:
                self.log.error(
                    f"DAILY LOSS CAP HIT: ${self.daily_pnl_usd:+.2f} <= "
                    f"-${max_loss:.2f} ({self.config.daily_loss_pct * 100:.0f}% "
                    f"of ${self.starting_balance:,.2f}). Halting for the day."
                )
                if self.cache.positions_open(instrument_id=self.instrument_id):
                    self.close_all_positions(instrument_id=self.instrument_id)
                return True

        if self.trades_today >= self.config.max_trades_per_day:
            self.log.warning(
                f"MAX TRADES HIT: {self.trades_today} trades today. "
                "No new entries until midnight UTC."
            )
            return True

        return False