#!/usr/bin/env python
"""
Derivatives Profit Booker Strategy (Asyncio | REST | Custom Client)
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

# ========================= CONFIGURATION =========================
STRATEGY_NAME = "DerivativesProfitBooker"

# Logging
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

# Env Vars
API_KEY = os.getenv('OPENALGO_APIKEY')
HOST = os.getenv('HOST_SERVER', 'http://127.0.0.1:5000')
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', 5))
MARKET_POLL_INTERVAL = int(os.getenv('MARKET_POLL_INTERVAL', 15))

# Constants
NEAR_EXPIRY_DAYS = 45
NEAR_TARGET_PCT = 10.0
FAR_TARGET_PCT = 50.0
SPECIAL_QTY_THRESHOLD = 600
SPECIAL_POINTS_CAP = 400.0

SHORT_TARGET_MARGIN_PCT = 0.65
SHORT_LOCK_MARGIN_PCT = 0.55
TRAILING_POINT = 0.5

IST = pytz.timezone('Asia/Kolkata')
OPTION_REGEX = re.compile(r'^([A-Z0-9]+)(\d{2}[A-Z]{3}\d{2})(\d+(\.\d+)?)(CE|PE)$')
FUTURE_REGEX = re.compile(r'^([A-Z0-9]+)(\d{2}[A-Z]{3}\d{2})FUT$')

# ========================= HELPERS =========================
def safe_float(val, default=0.0):
    try:
        if val is None: return default
        return float(val)
    except:
        return default

def get_days_to_expiry(symbol):
    match = OPTION_REGEX.match(symbol)
    if not match: match = FUTURE_REGEX.match(symbol)
    if not match: return 999

    expiry_str = match.group(2)
    try:
        expiry_str_title = expiry_str[:2] + expiry_str[2:5].title() + expiry_str[5:]
        expiry_date = datetime.strptime(expiry_str_title, "%d%b%y")
        return (expiry_date - datetime.now()).days
    except:
        return 999

def is_future_symbol(symbol):
    return bool(FUTURE_REGEX.match(symbol))

def update_targets(state):
    """Pre-calculate static target PRICE based on current state."""
    symbol = state["symbol"]
    avg = state["avg_price"]
    qty = state["qty"]
    margin = state.get("margin", 0.0)
    is_fut = state.get("is_future", False)

    # Defaults
    state["base_target_price"] = 0.0 # For Long Options (before cap)
    state["target_price"] = 0.0      # For Shorts/Futures (Trigger Price)

    if avg <= 0: return

    # 1. Long Options (Non-Future, Qty > 0)
    if not is_fut and qty > 0:
        days = get_days_to_expiry(symbol)
        target_pct = NEAR_TARGET_PCT if days <= NEAR_EXPIRY_DAYS else FAR_TARGET_PCT
        state["base_target_price"] = avg * (1 + target_pct / 100.0)

    # 2. Futures (Any Qty)
    elif is_fut:
        if margin > 0:
            target_amt = margin * (NEAR_TARGET_PCT / 100.0)
            if qty > 0:
                state["target_price"] = avg + (target_amt / abs(qty))
            else:
                state["target_price"] = avg - (target_amt / abs(qty))

    # 3. Short Options (Non-Future, Qty < 0)
    elif not is_fut and qty < 0:
        if margin > 0:
            target_amt = margin * (SHORT_TARGET_MARGIN_PCT / 100.0)
            state["target_price"] = avg - (target_amt / abs(qty))

# ========================= ASYNC API CLIENT =========================
class AsyncApiClient:
    def __init__(self):
        self.headers = {'Content-Type': 'application/json'}

    async def _post(self, endpoint, payload):
        loop = asyncio.get_running_loop()

        def do_req():
            try:
                url = f"{HOST.rstrip('/')}/api/v1/{endpoint}"
                if "apikey" not in payload: payload["apikey"] = API_KEY

                headers = self.headers.copy()
                if API_KEY:
                     headers["Authorization"] = f"Bearer {API_KEY}"

                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"API REQ {endpoint}: {json.dumps(payload)}")

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
        data = await self._post("positionbook", {})
        if data and data.get("status") == "success":
            return data.get("data", [])
        return []

    async def get_orders(self):
        data = await self._post("orderbook", {})
        if data and data.get("status") == "success":
            orders = data.get("data", [])
            if isinstance(orders, dict) and 'orders' in orders:
                return orders['orders']
            return orders
        return []

    async def cancel_order(self, orderid):
        return await self._post("cancelorder", {"orderid": orderid})

    async def place_order(self, **kwargs):
        payload = {
            "strategy": STRATEGY_NAME,
            "disclosed_quantity": 0,
            "price": 0,
            "trigger_price": 0
        }
        payload.update(kwargs)
        return await self._post("placeorder", payload)

    async def modify_order(self, **kwargs):
        return await self._post("modifyorder", kwargs)

    async def get_depth(self, symbol, exchange):
        payload = {"symbol": symbol, "exchange": exchange}
        data = await self._post("depth", payload)
        if data and data.get("status") == "success":
            return data.get("data")
        return None

    async def order_status(self, orderid):
        return await self._post("orderstatus", {"orderid": orderid, "strategy": STRATEGY_NAME})

    async def fetch_margin_for_short(self, symbol, exchange, product, quantity):
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
            m = safe_float(resp.get("total_margin_required"))
            if m == 0 and "data" in resp:
                m = safe_float(resp["data"].get("total_margin_required"))
            if m == 0 and "required" in resp:
                m = safe_float(resp.get("required"))
            return m
        return 0.0

# ========================= STATE =========================
POSITIONS_STATE = {}
STATE_LOCK = asyncio.Lock()
api = AsyncApiClient()

# ========================= LOGIC =========================

async def is_order_active(orderid):
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
    try:
        orders = await api.get_orders()
        to_cancel = []
        for order in orders:
            if order.get("symbol") != symbol: continue
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
    qty = state["qty"]
    symbol = state["symbol"]
    target_price = state.get("target_price", 0.0)

    if target_price <= 0: return

    triggered = False
    if qty > 0:
        if bid <= 0: return
        current_price = bid
        exit_action = "SELL"
        if bid >= target_price: triggered = True
    else:
        if ask <= 0: return
        current_price = ask
        exit_action = "BUY"
        if ask <= target_price: triggered = True

    if triggered:
        if state["state"] == "PLACED": return
        if state["active_oid"] and await is_order_active(state["active_oid"]): return

        logger.info(f"FUTURE TRIGGER {symbol}: Price {current_price} hit Target {target_price}")
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
    if bid <= 0: return
    symbol = state["symbol"]
    avg = state["avg_price"]
    qty = state["qty"]

    target_price = state.get("base_target_price", 0.0)
    if target_price <= 0: return

    # Cap Logic (Dynamic)
    now = datetime.now(IST)
    if qty > SPECIAL_QTY_THRESHOLD and now.hour >= 15:
        target_price = min(target_price, avg + SPECIAL_POINTS_CAP)

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
    if ask <= 0: return
    symbol = state["symbol"]
    avg = state["avg_price"]
    qty = abs(state["qty"])
    margin = state["margin"]
    target_price = state.get("target_price", 0.0)

    if target_price <= 0: return

    if ask <= target_price:
        if state["state"] == "TRACKING":
            logger.info(f"SHORT TRIGGER {symbol}: Ask {ask} <= Target {target_price}")

            # Recalculate lock trigger dynamically based on margin for now
            # Or should we pre-calculate lock trigger too?
            # User asked for "target price" for checking if we can place order.
            # The order placement needs 'trigger_price' for SL-M.
            # Initial SL Trigger = Avg - Lock_Dist
            # Lock_Dist = (Margin * 0.55%) / Qty.

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

        elif state["state"] == "TRAILING":
            lowest = state.get("lowest_ask", avg)
            current_trigger = state.get("last_trigger")
            if ask < lowest:
                diff = lowest - ask
                if diff >= TRAILING_POINT:
                    if not await is_order_active(state["active_oid"]):
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
    try:
        async with STATE_LOCK:
            if symbol not in POSITIONS_STATE: return
            state = POSITIONS_STATE[symbol]

        depth = depth_data.get("depth", {})
        bids = depth.get("buy", [])
        asks = depth.get("sell", [])

        best_bid = safe_float(bids[0].get("price")) if bids else 0.0
        best_ask = safe_float(asks[0].get("price")) if asks else 0.0

        if state.get("is_future"):
            await process_future(state, best_bid, best_ask)
        elif state["qty"] > 0:
            await process_long(state, best_bid)
        elif state["qty"] < 0:
            await process_short(state, best_ask)
    except Exception as e:
        logger.error(f"Process Error {symbol}: {e}")

async def sync_positions_loop():
    logger.info("Sync Loop Started")
    while True:
        try:
            positions = await api.get_positions()
            active_symbols = set()

            for pos in positions:
                sym = pos.get("symbol")
                if not sym: continue
                qty = int(safe_float(pos.get("netqty", 0) or pos.get("quantity", 0)))

                if qty != 0:
                    active_symbols.add(sym)
                    await reconcile_position_state(pos)

            async with STATE_LOCK:
                tracked = list(POSITIONS_STATE.keys())
                for sym in tracked:
                    if sym not in active_symbols:
                        logger.info(f"Closed: {sym}")
                        del POSITIONS_STATE[sym]
        except Exception as e:
            logger.error(f"Sync Error: {e}")
        await asyncio.sleep(POLL_INTERVAL)

async def reconcile_position_state(pos):
    symbol = pos.get('symbol')
    qty = int(safe_float(pos.get('netqty', 0) or pos.get('quantity', 0)))
    avg_price = safe_float(pos.get('buyavg') or pos.get('sellavg') or pos.get('average_price'))
    exchange = pos.get('exchange')
    product = pos.get('product')

    async with STATE_LOCK:
        if symbol not in POSITIONS_STATE:
            is_fut = is_future_symbol(symbol)
            state = {
                'symbol': symbol, 'exchange': exchange, 'product': product,
                'qty': qty, 'avg_price': avg_price, 'margin': 0.0,
                'active_oid': None, 'state': 'TRACKING', 'is_future': is_fut
            }
            update_targets(state)
            if qty < 0 or is_fut:
                pass
            POSITIONS_STATE[symbol] = state

        else:
            state = POSITIONS_STATE[symbol]
            qty_changed = state['qty'] != qty
            avg_changed = abs(state['avg_price'] - avg_price) > 0.05

            if qty_changed or avg_changed:
                state['qty'] = qty
                state['avg_price'] = avg_price
                state['active_oid'] = None
                state['state'] = 'TRACKING'
                if qty_changed:
                     state['margin'] = 0.0 # Force re-fetch
                update_targets(state)

    # Handle Margin Fetch outside lock to allow concurrency
    need_margin = False
    async with STATE_LOCK:
        s = POSITIONS_STATE.get(symbol)
        if s and (s['qty'] < 0 or s.get('is_future')) and s['margin'] == 0.0:
            need_margin = True

    if need_margin:
        m = await api.fetch_margin_for_short(symbol, exchange, product, qty)
        async with STATE_LOCK:
            if symbol in POSITIONS_STATE:
                POSITIONS_STATE[symbol]['margin'] = m
                logger.info(f"Margin {symbol}: {m}")
                update_targets(POSITIONS_STATE[symbol])

async def poll_market_data_loop():
    logger.info("Market Poll Loop Started")
    while True:
        try:
            async with STATE_LOCK:
                symbols = [(k, v['exchange']) for k, v in POSITIONS_STATE.items()]

            for sym, exc in symbols:
                depth = await api.get_depth(sym, exc)
                if depth:
                    await process_market_data(sym, depth)
        except Exception as e:
            logger.error(f"Poll Error: {e}")
        await asyncio.sleep(MARKET_POLL_INTERVAL)

async def run_strategy():
    if not API_KEY:
        logger.error("API Key missing")
        return

    await asyncio.gather(
        sync_positions_loop(),
        poll_market_data_loop()
    )

def main():
    try:
        asyncio.run(run_strategy())
    except KeyboardInterrupt:
        logger.info("Stopping...")

if __name__ == "__main__":
    main()
