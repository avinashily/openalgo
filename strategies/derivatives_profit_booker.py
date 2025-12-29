#!/usr/bin/env python
"""
Derivatives Profit Booker Strategy (Asyncio | REST | Custom Client)

WHAT THIS STRATEGY DOES:
------------------------
This strategy is an automated "Profit Booker" for Options and Futures trading.
It monitors your open positions in real-time and automatically places "Exit Orders" (Sell for Longs, Buy for Shorts)
when a specific profit target is reached.

KEY FEATURES:
1. **Hybrid Architecture**: It uses a custom "Asynchronous" design. This means it can do multiple things at once
   (like checking prices and fetching order status) without freezing or slowing down.
2. **REST API Polling**: Instead of keeping a permanent connection open (WebSocket), it asks the server for
   market prices every few seconds. This is often more stable and easier to manage.
3. **Pre-calculated Targets**: It calculates the exact "Target Price" for every position in advance.
   This makes the reaction time very fast when the market moves.
4. **Safety First**: It checks for "Zombie Orders" (old orders that were left open) and cancels them before placing new ones.
   It also handles manual intervention (if you change quantity manually, it resets itself).

HOW TO USE:
-----------
1. Run this script alongside your OpenAlgo server.
2. Ensure your API Key is set in the environment variables.
3. The script will automatically find all your open positions and start tracking them.

AUTHOR: Jules (AI Assistant)
"""
import os
import sys
import asyncio
import logging
import json
import traceback
import re
import pytz
import requests
from datetime import datetime
from functools import partial

# ==============================================================================
# 1. CONFIGURATION & SETTINGS
#    These values control how the strategy behaves. You can change them here.
# ==============================================================================

STRATEGY_NAME = "DerivativesProfitBooker"

# Logging Setup: Helps us see what the bot is doing in the console and log file.
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
if os.getenv("DEBUG", "").lower() in ("true", "1", "yes"):
    LOG_LEVEL = "DEBUG"

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join("log", "derivatives_profit_booker.log")),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(STRATEGY_NAME)

# Connection Settings: Where is the OpenAlgo server?
API_KEY = os.getenv('OPENALGO_APIKEY')
HOST = os.getenv('HOST_SERVER', 'http://127.0.0.1:5000')

# Polling Intervals (in seconds):
# How often to check for new/closed positions (e.g., every 5 seconds)
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', 5))
# How often to check market prices (e.g., every 15 seconds)
MARKET_POLL_INTERVAL = int(os.getenv('MARKET_POLL_INTERVAL', 15))

# Profit Targets:
# ----------------
# NEAR_EXPIRY_DAYS: If an option expires in less than this many days, it's "Near Term".
NEAR_EXPIRY_DAYS = 45

# NEAR_TARGET_PCT: Profit target % for Near Term options and Futures (e.g., 10%).
NEAR_TARGET_PCT = 10.0

# FAR_TARGET_PCT: Profit target % for Far Term options (e.g., 50%).
FAR_TARGET_PCT = 50.0

# Special Logic for Large Quantities (3 PM Rule):
# If quantity > 600 AND time is after 3:00 PM, cap the profit points to 400.
SPECIAL_QTY_THRESHOLD = 600
SPECIAL_POINTS_CAP = 400.0

# Short Selling Targets (based on Margin Utilized):
SHORT_TARGET_MARGIN_PCT = 0.65  # Target 0.65% return on margin
SHORT_LOCK_MARGIN_PCT = 0.55    # Lock 0.55% return when trailing
TRAILING_POINT = 0.5            # Trailing Stop-Loss step size

# Timezone: We use Indian Standard Time (IST) for all time checks.
IST = pytz.timezone('Asia/Kolkata')

# Regex: Patterns to identify symbols (e.g., detecting if "NIFTY23DEC..." is a Call/Put or Future)
OPTION_REGEX = re.compile(r'^([A-Z0-9]+)(\d{2}[A-Z]{3}\d{2})(\d+(\.\d+)?)(CE|PE)$')
FUTURE_REGEX = re.compile(r'^([A-Z0-9]+)(\d{2}[A-Z]{3}\d{2})FUT$')

# ==============================================================================
# 2. HELPER FUNCTIONS
#    Small utilities to parse data or perform safe calculations.
# ==============================================================================

def safe_float(val, default=0.0):
    """Safely convert a value to a decimal number (float). Returns 0.0 if it fails."""
    try:
        if val is None: return default
        return float(val)
    except:
        return default

def get_days_to_expiry(symbol):
    """
    Calculates how many days are left until the option expires.
    It parses the date from the symbol (e.g., '09DEC25').
    """
    match = OPTION_REGEX.match(symbol)
    if not match: match = FUTURE_REGEX.match(symbol)
    if not match: return 999

    expiry_str = match.group(2) # e.g., '09DEC25'
    try:
        # Convert '09DEC25' -> '09Dec25' for parsing
        expiry_str_title = expiry_str[:2] + expiry_str[2:5].title() + expiry_str[5:]
        expiry_date = datetime.strptime(expiry_str_title, "%d%b%y")
        return (expiry_date - datetime.now()).days
    except:
        return 999

def is_future_symbol(symbol):
    """Checks if the symbol ends with 'FUT', indicating it's a Future contract."""
    return bool(FUTURE_REGEX.match(symbol))

def update_targets(state):
    """
    CORE LOGIC: Pre-calculates the exact 'Target Price' for exit.

    Why do we do this here?
    Calculating the target price involves math (percentages, margin checks, time checks).
    Doing this computation 100 times a second inside the high-speed loop is wasteful.
    Instead, we calculate it ONCE whenever the position changes (or every sync cycle),
    store it in 'state', and the high-speed loop just checks: "Is Price >= Target?".
    """
    symbol = state["symbol"]
    avg = state["avg_price"]
    qty = state["qty"]
    margin = state.get("margin", 0.0)
    is_fut = state.get("is_future", False)

    state["target_price"] = 0.0

    if avg <= 0: return

    # Case 1: Long Options (Non-Future, Qty > 0)
    if not is_fut and qty > 0:
        days = get_days_to_expiry(symbol)
        # Choose target % based on expiry duration
        target_pct = NEAR_TARGET_PCT if days <= NEAR_EXPIRY_DAYS else FAR_TARGET_PCT
        target_price = avg * (1 + target_pct / 100.0)

        # Apply Time-Based Cap (Special Rule for > 600 qty after 3 PM)
        now = datetime.now(IST)
        if qty > SPECIAL_QTY_THRESHOLD and now.hour >= 15:
            target_price = min(target_price, avg + SPECIAL_POINTS_CAP)

        state["target_price"] = target_price

    # Case 2: Futures (Long or Short)
    elif is_fut:
        if margin > 0:
            # Target is a % of the Margin used
            target_amt = margin * (NEAR_TARGET_PCT / 100.0)
            if qty > 0:
                # Long Future: Exit Higher
                state["target_price"] = avg + (target_amt / abs(qty))
            else:
                # Short Future: Exit Lower
                state["target_price"] = avg - (target_amt / abs(qty))

    # Case 3: Short Options (Non-Future, Qty < 0)
    elif not is_fut and qty < 0:
        if margin > 0:
            # Target is a % of Margin (0.65%)
            target_amt = margin * (SHORT_TARGET_MARGIN_PCT / 100.0)
            # Short Option: We want price to DROP. Exit Lower.
            state["target_price"] = avg - (target_amt / abs(qty))

# ==============================================================================
# 3. ASYNC API CLIENT
#    A custom tool to talk to the OpenAlgo API without blocking the script.
# ==============================================================================
class AsyncApiClient:
    def __init__(self):
        self.headers = {'Content-Type': 'application/json'}

    async def _post(self, endpoint, payload):
        """
        Sends a request to the server in the background.
        It uses 'run_in_executor' so the main bot loop doesn't freeze while waiting for a response.
        """
        loop = asyncio.get_running_loop()

        def do_req():
            try:
                url = f"{HOST.rstrip('/')}/api/v1/{endpoint}"
                # Ensure authentication
                if "apikey" not in payload: payload["apikey"] = API_KEY

                headers = self.headers.copy()
                if API_KEY:
                     headers["Authorization"] = f"Bearer {API_KEY}"

                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"API REQ {endpoint}: {json.dumps(payload)}")

                # Standard blocking request (wrapped in executor)
                resp = requests.post(url, json=payload, headers=headers, timeout=5)

                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"API RESP {endpoint} [{resp.status_code}]: {resp.text}")

                if resp.status_code == 200:
                    return resp.json()
                return None
            except Exception as e:
                logger.error(f"API Error {endpoint}: {e}")
                return None

        return await loop.run_in_executor(None, do_req)

    async def get_positions(self):
        """Fetches all open positions (including manual trades)."""
        data = await self._post("positionbook", {})
        if data and data.get("status") == "success":
            return data.get("data", [])
        return []

    async def get_orders(self):
        """Fetches all open/pending orders."""
        data = await self._post("orderbook", {})
        if data and data.get("status") == "success":
            orders = data.get("data", [])
            # Handle potential nested 'orders' key
            if isinstance(orders, dict) and 'orders' in orders:
                return orders['orders']
            return orders
        return []

    async def cancel_order(self, orderid):
        """Cancels a specific order by ID."""
        return await self._post("cancelorder", {"orderid": orderid})

    async def place_order(self, **kwargs):
        """Places a new order with the broker."""
        payload = {
            "strategy": STRATEGY_NAME, # Tag order with our strategy name
            "disclosed_quantity": 0,
            "price": 0,
            "trigger_price": 0
        }
        payload.update(kwargs)
        return await self._post("placeorder", payload)

    async def modify_order(self, **kwargs):
        """Modifies an existing order (e.g., changing Stop Loss price)."""
        return await self._post("modifyorder", kwargs)

    async def get_depth(self, symbol, exchange):
        """Fetches Market Depth (Bid/Ask prices) for a symbol."""
        payload = {"symbol": symbol, "exchange": exchange}
        data = await self._post("depth", payload)
        if data and data.get("status") == "success":
            return data.get("data")
        return None

    async def order_status(self, orderid):
        """Checks the status of a specific order."""
        return await self._post("orderstatus", {"orderid": orderid, "strategy": STRATEGY_NAME})

    async def fetch_margin_for_short(self, symbol, exchange, product, quantity):
        """
        Calculates how much margin is required for a trade.
        Uses 'analyze' mode which simulates an order without placing it.
        """
        payload = {
            "mode": "analyze",
            "symbol": symbol,
            "action": "SELL",
            "exchange": exchange,
            "product": product,
            "quantity": abs(quantity),
            "price_type": "MARKET"
        }
        resp = await self.place_order(**payload)
        if resp:
            # Look for margin field in various possible response locations
            m = safe_float(resp.get("total_margin_required"))
            if m == 0 and "data" in resp:
                m = safe_float(resp["data"].get("total_margin_required"))
            if m == 0 and "required" in resp:
                m = safe_float(resp.get("required"))
            return m
        return 0.0

# ==============================================================================
# 4. GLOBAL STATE
#    Keeps track of all active positions and their profit targets.
# ==============================================================================

POSITIONS_STATE = {} # Dictionary: { Symbol -> { qty, avg, target_price, ... } }
STATE_LOCK = asyncio.Lock() # Lock: Prevents two threads from editing state at the same time
api = AsyncApiClient()

# ==============================================================================
# 5. CORE LOGIC FUNCTIONS
# ==============================================================================

async def is_order_active(orderid):
    """Returns True if the order is still OPEN or PENDING."""
    if not orderid: return False
    try:
        resp = await api.order_status(orderid)
        if resp and resp.get("status") == "success":
            data = resp.get("data", resp)
            status = data.get("order_status", "").upper()
            return status in ["OPEN", "PENDING", "TRIGGER_PENDING", "TRIGGER PENDING"]
    except:
        pass
    return False

async def cancel_existing_exit_orders(symbol, exclude_oid=None):
    """
    Finds and cancels any existing 'Exit Orders' for this symbol.
    We do this before placing a new exit to avoid having duplicate sell orders.
    """
    try:
        orders = await api.get_orders()
        to_cancel = []
        for order in orders:
            if order.get("symbol") != symbol: continue

            # Check if order is active
            status = order.get("order_status", "").upper()
            if status not in ["OPEN", "PENDING", "TRIGGER_PENDING", "TRIGGER PENDING"]:
                continue

            oid = order.get("orderid")
            if oid == exclude_oid: continue
            to_cancel.append(oid)

        if to_cancel:
            logger.info(f"Cancelling {to_cancel} for {symbol}")
            for oid in to_cancel:
                await api.cancel_order(oid)
    except Exception as e:
        logger.error(f"Cancel Error {symbol}: {e}")

async def process_future(state, bid, ask):
    """Checks Profit Target for FUTURES."""
    qty = state["qty"]
    symbol = state["symbol"]
    target_price = state.get("target_price", 0.0)

    if target_price <= 0: return

    triggered = False

    # Check Price vs Target
    if qty > 0: # Long Future
        if bid <= 0: return
        current_price = bid
        exit_action = "SELL"
        if bid >= target_price: triggered = True
    else: # Short Future
        if ask <= 0: return
        current_price = ask
        exit_action = "BUY"
        if ask <= target_price: triggered = True

    if triggered:
        # Don't place duplicates
        if state["state"] == "PLACED": return
        if state["active_oid"] and await is_order_active(state["active_oid"]): return

        logger.info(f"FUTURE TRIGGER {symbol}: Price {current_price} hit Target {target_price}")

        # Cancel old exits & Place new Limit Order
        await cancel_existing_exit_orders(symbol)

        resp = await api.place_order(
            symbol=symbol,
            exchange=state["exchange"],
            action=exit_action,
            quantity=abs(qty),
            product=state["product"],
            price_type="LIMIT",
            price=current_price
        )

        if resp and resp.get("status") == "success":
            async with STATE_LOCK:
                state["state"] = "PLACED"
                state["active_oid"] = resp.get("orderid")

async def process_long(state, bid):
    """Checks Profit Target for LONG OPTIONS."""
    if bid <= 0: return
    symbol = state["symbol"]
    qty = state["qty"]
    target_price = state.get("target_price", 0.0)

    if target_price <= 0: return

    if bid >= target_price:
        if state["state"] == "PLACED": return
        if state["active_oid"] and await is_order_active(state["active_oid"]): return

        logger.info(f"LONG TRIGGER {symbol}: Bid {bid} >= Target {target_price}")
        await cancel_existing_exit_orders(symbol)

        resp = await api.place_order(
            symbol=symbol,
            exchange=state["exchange"],
            action="SELL",
            quantity=qty,
            product=state["product"],
            price_type="LIMIT",
            price=bid
        )
        if resp and resp.get("status") == "success":
            async with STATE_LOCK:
                state["state"] = "PLACED"
                state["active_oid"] = resp.get("orderid")

async def process_short(state, ask):
    """Checks Profit Target for SHORT OPTIONS (uses Trailing SL)."""
    if ask <= 0: return
    symbol = state["symbol"]
    avg = state["avg_price"]
    qty = abs(state["qty"])
    margin = state["margin"]
    target_price = state.get("target_price", 0.0)

    if target_price <= 0: return

    if ask <= target_price:
        # Phase 1: Initial Trigger -> Place Stop Loss Market (SL-M) order
        if state["state"] == "TRACKING":
            logger.info(f"SHORT TRIGGER {symbol}: Ask {ask} <= Target {target_price}")

            # Calculate Initial Lock Price
            lock_amt = margin * (SHORT_LOCK_MARGIN_PCT / 100.0)
            lock_dist = lock_amt / qty
            trigger_price = avg - lock_dist

            await cancel_existing_exit_orders(symbol)
            resp = await api.place_order(
                symbol=symbol,
                exchange=state["exchange"],
                action="BUY",
                quantity=qty,
                product=state["product"],
                price_type="SL-M",
                trigger_price=round(trigger_price, 1),
                tag="OPB"
            )
            if resp and resp.get("status") == "success":
                async with STATE_LOCK:
                    state["state"] = "TRAILING"
                    state["active_oid"] = resp.get("orderid")
                    state["last_trigger"] = trigger_price
                    state["lowest_ask"] = ask

        # Phase 2: Trailing Logic -> Move SL down if price drops
        elif state["state"] == "TRAILING":
            lowest = state.get("lowest_ask", avg)
            current_trigger = state.get("last_trigger")

            if ask < lowest:
                diff = lowest - ask
                # Only modify if moved enough points
                if diff >= TRAILING_POINT:
                    if not await is_order_active(state["active_oid"]):
                        # Order died? Reset to tracking
                        async with STATE_LOCK:
                            state["state"] = "TRACKING"
                            state["active_oid"] = None
                        return

                    new_trigger = current_trigger - diff
                    logger.info(f"Trailing {symbol}: New Trig {new_trigger}")
                    resp = await api.modify_order(
                        orderid=state["active_oid"],
                        trigger_price=round(new_trigger, 1),
                        price_type="SL-M",
                        symbol=symbol,
                        exchange=state["exchange"],
                        quantity=qty,
                        product=state["product"]
                    )
                    if resp and resp.get("status") == "success":
                        async with STATE_LOCK:
                            state["last_trigger"] = new_trigger
                            state["lowest_ask"] = ask

async def process_market_data(symbol, depth_data):
    """
    Main entry point for Market Data updates.
    Routes data to the correct processor (Long, Short, Future).
    """
    try:
        async with STATE_LOCK:
            if symbol not in POSITIONS_STATE: return
            state = POSITIONS_STATE[symbol]

        # Parse depth
        depth = depth_data.get("depth", {})
        bids = depth.get("buy", [])
        asks = depth.get("sell", [])

        best_bid = safe_float(bids[0].get("price")) if bids else 0.0
        best_ask = safe_float(asks[0].get("price")) if asks else 0.0

        # Route to logic
        if state.get("is_future"):
            await process_future(state, best_bid, best_ask)
        elif state["qty"] > 0:
            await process_long(state, best_bid)
        elif state["qty"] < 0:
            await process_short(state, best_ask)
    except Exception as e:
        logger.error(f"Process Error {symbol}: {e}")

# ==============================================================================
# 6. MAIN LOOPS
# ==============================================================================

async def sync_positions_loop():
    """
    Loop 1: Synchronize Positions (Discovery).
    - Fetches all open positions from Broker.
    - Adds new positions to tracking.
    - Updates quantity/avg if changed (re-entry).
    - Removes closed positions.
    - Refreshes Target Prices (important for time-based logic).
    """
    logger.info("Sync Loop Started")
    while True:
        try:
            positions = await api.get_positions()
            active_symbols = set()

            for pos in positions:
                sym = pos.get("symbol")
                if not sym: continue
                # Parse qty (support both 'netqty' and 'quantity')
                qty = int(safe_float(pos.get("netqty", 0) or pos.get("quantity", 0)))

                if qty != 0:
                    active_symbols.add(sym)
                    await reconcile_position_state(pos)

            # Cleanup closed positions & Refresh Targets
            async with STATE_LOCK:
                tracked = list(POSITIONS_STATE.keys())
                for sym in tracked:
                    if sym not in active_symbols:
                        logger.info(f"Closed: {sym}")
                        del POSITIONS_STATE[sym]
                    else:
                        # ALWAYS refresh targets here.
                        # Why? Because some logic (like the 3 PM Cap) depends on TIME.
                        # Even if position hasn't changed, the Target Price might need to change due to time.
                        update_targets(POSITIONS_STATE[sym])

        except Exception as e:
            logger.error(f"Sync Error: {e}")
        await asyncio.sleep(POLL_INTERVAL)

async def reconcile_position_state(pos):
    """
    Compares Broker Position vs Internal State.
    Updates internal state if there's a mismatch (Manual Entry/Exit).
    """
    symbol = pos.get('symbol')
    qty = int(safe_float(pos.get('netqty', 0) or pos.get('quantity', 0)))
    avg_price = safe_float(pos.get('buyavg') or pos.get('sellavg') or pos.get('average_price'))
    exchange = pos.get('exchange')
    product = pos.get('product')

    async with STATE_LOCK:
        # New Position
        if symbol not in POSITIONS_STATE:
            is_fut = is_future_symbol(symbol)
            state = {
                'symbol': symbol, 'exchange': exchange, 'product': product,
                'qty': qty, 'avg_price': avg_price, 'margin': 0.0,
                'active_oid': None, 'state': 'TRACKING', 'is_future': is_fut
            }
            update_targets(state)
            if qty < 0 or is_fut:
                pass # Margin needed, handled below
            POSITIONS_STATE[symbol] = state

        # Existing Position Update
        else:
            state = POSITIONS_STATE[symbol]
            qty_changed = state['qty'] != qty
            avg_changed = abs(state['avg_price'] - avg_price) > 0.05

            if qty_changed or avg_changed:
                state['qty'] = qty
                state['avg_price'] = avg_price
                state['active_oid'] = None # Reset orders since position changed
                state['state'] = 'TRACKING'
                if qty_changed:
                     state['margin'] = 0.0 # Force margin re-fetch if size changed

            # Always update targets
            update_targets(state)

    # Fetch Margin (Async, outside lock to allow concurrency)
    need_margin = False
    async with STATE_LOCK:
        s = POSITIONS_STATE.get(symbol)
        # We need margin for Shorts and Futures
        if s and (s['qty'] < 0 or s.get('is_future')) and s['margin'] == 0.0:
            need_margin = True

    if need_margin:
        m = await api.fetch_margin_for_short(symbol, exchange, product, qty)
        async with STATE_LOCK:
            if symbol in POSITIONS_STATE:
                POSITIONS_STATE[symbol]['margin'] = m
                logger.info(f"Margin {symbol}: {m}")
                # Recalculate target now that we have margin
                update_targets(POSITIONS_STATE[symbol])

async def poll_market_data_loop():
    """
    Loop 2: Market Data Polling.
    - Iterates over all tracked symbols.
    - Asks server for 'Market Depth' (Price).
    - Triggers profit check logic.
    """
    logger.info("Market Poll Loop Started")
    while True:
        try:
            async with STATE_LOCK:
                # Copy list to iterate safely
                symbols = [(k, v['exchange']) for k, v in POSITIONS_STATE.items()]

            for sym, exc in symbols:
                depth = await api.get_depth(sym, exc)
                if depth:
                    await process_market_data(sym, depth)
        except Exception as e:
            logger.error(f"Poll Error: {e}")
        await asyncio.sleep(MARKET_POLL_INTERVAL)

async def run_strategy():
    """Main Async Entry Point: Starts both background loops."""
    if not API_KEY:
        logger.error("API Key missing")
        return

    # Run both loops concurrently
    await asyncio.gather(
        sync_positions_loop(),
        poll_market_data_loop()
    )

def main():
    """Script Entry Point."""
    try:
        asyncio.run(run_strategy())
    except KeyboardInterrupt:
        logger.info("Stopping...")

if __name__ == "__main__":
    main()
