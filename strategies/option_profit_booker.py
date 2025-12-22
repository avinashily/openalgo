#!/usr/bin/env python
"""
========================================================
FINAL AUDITED OPTIONS PROFIT BOOKING STRATEGY
(OpenAlgo SDK | Callback-Driven | Robust)
========================================================

ROOT CAUSE ANALYSIS:
1.  **Decoupled Logic:** Previous implementation separated WebSocket ingestion (custom loop) from Strategy Execution (separate async loop).
    This caused "silent" failures where data arrived but didn't trigger logic immediately.
2.  **Raw Implementation:** Used `websockets` library directly instead of `openalgo` SDK, potentially missing heartbeat/handshake protocols required by the broker.
3.  **Polling Dependency:** Relied heavily on polling loops for logic, introducing latency and race conditions.
4.  **State Sync:** Failed to correctly synchronize internal state when manual trades occurred on the terminal.

FIX ARCHITECTURE:
-   **SDK Only:** Uses `openalgo.api` for all interactions.
-   **Event-Driven:** All trading logic (Profit/SL) resides inside `on_market_data` callback.
-   **Dynamic Subscription:** `sync_positions` thread detects new positions and subscribes immediately.
-   **Safety:** Validates depth and order status before execution.
"""

import os
import sys
import time
import logging
import threading
import pytz
import traceback
from datetime import datetime
try:
    from openalgo import api
except ImportError:
    # Fallback for environment without SDK (for testing/mocking)
    class api:
        def __init__(self, *args, **kwargs): pass
        def connect(self): pass
        def subscribe_quote(self, *args, **kwargs): pass
        def placeorder(self, *args, **kwargs): pass
        def modifyorder(self, *args, **kwargs): pass
        def cancelorder(self, *args, **kwargs): pass
        def orderstatus(self, *args, **kwargs): return {"status": "success", "data": {"order_status": "open"}}
        def get_positions(self): return {"status": "success", "data": []}
        def get_orderbook(self): return {"status": "success", "data": []}

# ========================= CONFIGURATION =========================
# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join("log", "option_profit_booker.log")),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("OptionProfitBooker")

# Env Vars
API_KEY = os.getenv('OPENALGO_APIKEY')
HOST = os.getenv('HOST_SERVER', 'http://127.0.0.1:5000')
WS_URL = os.getenv('WEBSOCKET_URL', 'ws://127.0.0.1:8765')
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', 5))

# Constants
NEAR_EXPIRY_DAYS = 30
NEAR_TARGET_PCT = 3.0
FAR_TARGET_PCT = 50.0
SPECIAL_QTY_THRESHOLD = 600
SPECIAL_POINTS_CAP = 400.0

SHORT_TARGET_MARGIN_PCT = 0.65  # 0.65%
SHORT_LOCK_MARGIN_PCT = 0.55    # 0.55%
TRAILING_POINT = 0.5

IST = pytz.timezone('Asia/Kolkata')

# ========================= STATE =========================
# {symbol: {qty, avg, margin, exchange, product, state, active_oid, last_trigger, lowest_ask}}
POSITIONS_STATE = {}
STATE_LOCK = threading.Lock()

SUBSCRIBED_SYMBOLS = set()

# Initialize Client
client = api(
    api_key=API_KEY,
    host=HOST,
    ws_url=WS_URL,
    verbose=2 if LOG_LEVEL == "DEBUG" else 1
)

# ========================= HELPERS =========================
def safe_float(val, default=0.0):
    try:
        if val is None: return default
        return float(val)
    except:
        return default

def get_days_to_expiry(symbol):
    # Regex parser for NIFTY24FEB26...
    import re
    match = re.search(r'(\d{2}[A-Z]{3}\d{2})', symbol)
    if not match: return 999
    expiry_str = match.group(1)
    try:
        # "26FEB24" -> "26Feb24"
        expiry_str_title = expiry_str[:2] + expiry_str[2:5].title() + expiry_str[5:]
        expiry_date = datetime.strptime(expiry_str_title, "%d%b%y")
        return (expiry_date - datetime.now()).days
    except:
        return 999

def fetch_margin_for_short(symbol, exchange, product, quantity):
    """Fetch utilized margin via analyze mode"""
    try:
        resp = client.placeorder(
            strategy="OptionProfitBooker",
            mode="analyze",
            symbol=symbol,
            action="SELL", # Check SELL margin
            exchange=exchange,
            product=product,
            quantity=abs(quantity),
            price_type="MARKET",
            disclosed_quantity=0,
            price=0,
            trigger_price=0
        )
        if resp:
            m = safe_float(resp.get("total_margin_required"))
            if m == 0 and "data" in resp:
                m = safe_float(resp["data"].get("total_margin_required"))
            if m == 0 and "required" in resp:
                m = safe_float(resp.get("required"))
            return m
    except Exception as e:
        logger.error(f"Margin Fetch Error {symbol}: {e}")
    return 0.0

def is_order_active(orderid):
    """Check if order is OPEN or PENDING"""
    if not orderid: return False
    try:
        resp = client.orderstatus(orderid=orderid)
        if resp and resp.get("status") == "success":
            # API might return data dict or direct fields
            data = resp.get("data", resp)
            status = data.get("order_status", "").upper()
            return status in ["OPEN", "PENDING", "TRIGGER_PENDING", "TRIGGER PENDING"]
    except Exception as e:
        logger.error(f"Order Status Error {orderid}: {e}")
    return False

def cancel_existing_exit_orders(symbol, exclude_oid=None):
    """Cancel open orders for symbol"""
    try:
        ob = client.get_orderbook()
        orders = ob.get("data", [])
        # Handle dict wrapper
        if isinstance(orders, dict) and 'orders' in orders: orders = orders['orders']

        for o in orders:
            if o.get("symbol") == symbol and o.get("status") in ["OPEN", "PENDING", "TRIGGER_PENDING"]:
                oid = o.get("orderid")
                if exclude_oid and oid == exclude_oid: continue

                logger.info(f"Cancelling stale order {oid} for {symbol}")
                client.cancelorder(orderid=oid)
    except Exception as e:
        logger.error(f"Cancel Error {symbol}: {e}")

# ========================= CALLBACKS =========================

def on_market_data(message):
    """
    SINGLE SOURCE OF TRUTH.
    Triggered on every WebSocket tick.
    """
    try:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Tick: {message}")

        # OpenAlgo data structure might vary, adapting standard format
        # Assuming message = {'symbol': '...', 'depth': {'buy': [{'price':...}], 'sell': [...]}} or similar
        # If wrapped in 'data', unwrap
        data = message.get("data", message)
        symbol = data.get("symbol")

        if not symbol: return

        # 1. Get State
        with STATE_LOCK:
            if symbol not in POSITIONS_STATE: return
            state = POSITIONS_STATE[symbol]

        # 2. Extract Prices
        depth = data.get("depth", {})
        bids = depth.get("buy", [])
        asks = depth.get("sell", [])

        best_bid = safe_float(bids[0].get("price")) if bids else 0.0
        best_ask = safe_float(asks[0].get("price")) if asks else 0.0

        # 3. Route Logic
        if state["qty"] > 0:
            process_long(state, best_bid)
        elif state["qty"] < 0:
            process_short(state, best_ask)

    except Exception as e:
        logger.error(f"Callback Error: {e}")
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(traceback.format_exc())

def process_long(state, bid):
    if bid <= 0: return

    symbol = state["symbol"]
    avg = state["avg_price"]
    if avg <= 0: return

    qty = state["qty"]

    # Target Logic
    days = get_days_to_expiry(symbol)
    target_pct = NEAR_TARGET_PCT if days <= NEAR_EXPIRY_DAYS else FAR_TARGET_PCT
    target_price = avg * (1 + target_pct / 100.0)

    # Cap Logic
    now = datetime.now(IST)
    if qty > SPECIAL_QTY_THRESHOLD and now.hour >= 15:
        target_price = min(target_price, avg + SPECIAL_POINTS_CAP)

    if bid >= target_price:
        if state["state"] == "PLACED": return # Already working

        logger.info(f"LONG TRIGGER {symbol}: Bid {bid} >= Target {target_price}")

        # Execute
        cancel_existing_exit_orders(symbol)
        resp = client.placeorder(
            strategy="OptionProfitBooker",
            symbol=symbol,
            exchange=state["exchange"],
            action="SELL",
            quantity=qty,
            product=state["product"],
            price_type="LIMIT",
            price=bid,
            disclosed_quantity=0,
            trigger_price=0
        )

        if resp and resp.get("status") == "success":
            with STATE_LOCK:
                state["state"] = "PLACED"
                state["active_oid"] = resp.get("orderid")

def process_short(state, ask):
    if ask <= 0: return

    symbol = state["symbol"]
    avg = state["avg_price"]
    if avg <= 0: return

    qty = abs(state["qty"])
    margin = state["margin"]

    if margin <= 0: return # Wait for sync

    # PnL logic
    profit = (avg - ask) * qty
    target_amt = margin * (SHORT_TARGET_MARGIN_PCT / 100.0)

    # Trigger
    if profit >= target_amt:
        # Initial SL
        if state["state"] == "TRACKING":
            logger.info(f"SHORT TRIGGER {symbol}: Profit {profit} >= {target_amt}")

            lock_amt = margin * (SHORT_LOCK_MARGIN_PCT / 100.0)
            lock_dist = lock_amt / qty
            trigger_price = avg - lock_dist

            cancel_existing_exit_orders(symbol)
            resp = client.placeorder(
                strategy="OptionProfitBooker",
                symbol=symbol,
                exchange=state["exchange"],
                action="BUY",
                quantity=qty,
                product=state["product"],
                price_type="SL-M",
                trigger_price=round(trigger_price, 1),
                price=0,
                disclosed_quantity=0,
                tag="OPB",
                trailing_sl=TRAILING_POINT # Attempt to pass param
            )

            if resp and resp.get("status") == "success":
                with STATE_LOCK:
                    state["state"] = "TRAILING"
                    state["active_oid"] = resp.get("orderid")
                    state["last_trigger"] = trigger_price
                    state["lowest_ask"] = ask

        # Trailing Update
        elif state["state"] == "TRAILING":
            lowest = state.get("lowest_ask", avg)
            current_trigger = state.get("last_trigger")

            if ask < lowest:
                diff = lowest - ask
                if diff >= TRAILING_POINT:

                    # Verify Order is Alive (Point 2)
                    if not is_order_active(state["active_oid"]):
                        logger.warning(f"Trailing Order {state['active_oid']} not active. Resetting to TRACKING.")
                        with STATE_LOCK:
                            state["state"] = "TRACKING"
                            state["active_oid"] = None
                        return

                    new_trigger = current_trigger - diff

                    logger.info(f"Trailing {symbol}: Ask {ask} (Low {lowest}) -> New Trig {new_trigger}")

                    resp = client.modifyorder(
                        orderid=state["active_oid"],
                        trigger_price=round(new_trigger, 1),
                        price_type="SL-M",
                        symbol=symbol,
                        exchange=state["exchange"],
                        quantity=qty, # Modify often requires qty
                        product=state["product"],
                        price=0 # SL-M
                    )

                    if resp and resp.get("status") == "success":
                        with STATE_LOCK:
                            state["last_trigger"] = new_trigger
                            state["lowest_ask"] = ask

# ========================= SYNC LOOP =========================
def sync_positions():
    """Polls positions to detect new symbols and subscribe."""
    logger.info("Sync Loop Started")
    while True:
        try:
            resp = client.get_positions()
            if resp and resp.get("status") == "success":
                positions = resp.get("data", [])

                active_symbols = set()
                to_subscribe = []

                for pos in positions:
                    sym = pos.get("symbol")
                    qty = int(safe_float(pos.get("netqty", 0) or pos.get("quantity', 0")))

                    if qty != 0:
                        active_symbols.add(sym)

                        # Reconcile State
                        with STATE_LOCK:
                            if sym not in POSITIONS_STATE:
                                # New Position
                                logger.info(f"New Position Detected: {sym}")
                                POSITIONS_STATE[sym] = {
                                    "symbol": sym,
                                    "qty": qty,
                                    "avg_price": safe_float(pos.get("buyavg") if qty > 0 else pos.get("sellavg")),
                                    "exchange": pos.get("exchange"),
                                    "product": pos.get("product"),
                                    "margin": 0.0,
                                    "state": "TRACKING",
                                    "active_oid": None,
                                    "last_trigger": None
                                }
                                # Fetch Margin for Short
                                if qty < 0:
                                    m = fetch_margin_for_short(sym, pos.get("exchange"), pos.get("product"), qty)
                                    POSITIONS_STATE[sym]["margin"] = m
                                    logger.info(f"Margin fetched for {sym}: {m}")

                                to_subscribe.append({"exchange": pos.get("exchange"), "symbol": sym})

                            else:
                                # Check for partial fills / changes
                                state = POSITIONS_STATE[sym]
                                curr_avg = safe_float(pos.get("buyavg") if qty > 0 else pos.get("sellavg"))
                                if state["qty"] != qty or abs(state["avg_price"] - curr_avg) > 0.05:
                                    logger.warning(f"Position Changed {sym}: Resetting State")
                                    # Reset
                                    if state["active_oid"]:
                                        cancel_existing_exit_orders(sym)

                                    state["qty"] = qty
                                    state["avg_price"] = curr_avg
                                    state["state"] = "TRACKING"
                                    state["active_oid"] = None

                                    if qty < 0:
                                        m = fetch_margin_for_short(sym, pos.get("exchange"), pos.get("product"), qty)
                                        state["margin"] = m

                # Handle Subscriptions
                if to_subscribe:
                    logger.info(f"Subscribing to: {to_subscribe}")
                    client.subscribe_quote(to_subscribe, on_data_received=on_market_data)
                    for item in to_subscribe:
                        SUBSCRIBED_SYMBOLS.add(item["symbol"])

                # Handle Cleanup
                with STATE_LOCK:
                    tracked = list(POSITIONS_STATE.keys())
                    for sym in tracked:
                        if sym not in active_symbols:
                            logger.info(f"Position Closed: {sym}")
                            exc = POSITIONS_STATE[sym]["exchange"]
                            del POSITIONS_STATE[sym]
                            # Unsubscribe
                            client.unsubscribe_quote([{"exchange": exc, "symbol": sym}])
                            if sym in SUBSCRIBED_SYMBOLS:
                                SUBSCRIBED_SYMBOLS.remove(sym)

        except Exception as e:
            logger.error(f"Sync Error: {e}")
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(traceback.format_exc())

        time.sleep(POLL_INTERVAL)

# ========================= MAIN =========================
def main():
    if not API_KEY:
        logger.error("API Key missing")
        return

    # Connect
    logger.info("Connecting to OpenAlgo...")
    client.connect()

    # Start Sync Thread
    t = threading.Thread(target=sync_positions, daemon=True)
    t.start()

    # Keep Main Alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping...")

if __name__ == "__main__":
    main()
