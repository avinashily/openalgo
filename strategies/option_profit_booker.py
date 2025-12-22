#!/usr/bin/env python
"""
Option Profit Booker Strategy (Async/Robust)
- Asyncio-based architecture with concurrent Polling, WebSocket, and Strategy Engine.
- Handles edge cases: Partial fills, Re-entries (Avg Price change), Stale Orders.
- Longs: Expiry-based Targets (10% Near / 28% Far).
- Shorts: Margin-based Targets (0.65% Profit -> Lock 0.55% -> Trail).
- Supports DEBUG mode for verbose logging.
"""
import os
import sys
import json
import asyncio
import logging
import requests
import websockets
import re
import pytz
import functools
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# ========================= CONFIGURATION =========================
# Logging Setup
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
if os.getenv("DEBUG", "").lower() in ("true", "1", "yes"):
    LOG_LEVEL = "DEBUG"

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
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', 5)) # Faster polling (5s) for sync

# Strategy Constants (Reference Script Values)
NEAR_EXPIRY_DAYS = 30
NEAR_TARGET_PCT = 3.0
FAR_TARGET_PCT = 50.0
SPECIAL_QTY_THRESHOLD = 600
SPECIAL_POINTS_CAP = 400.0

SHORT_TARGET_MARGIN_PCT = 0.65  # 0.65%
SHORT_LOCK_MARGIN_PCT = 0.55    # 0.55%
TRAILING_POINT = 0.5

IST = pytz.timezone('Asia/Kolkata')
SYMBOL_REGEX = re.compile(r'^([A-Z0-9]+)(\d{2}[A-Z]{3}\d{2})(\d+)(CE|PE)$')

# ========================= SHARED STATE =========================
DEPTH_CACHE = {}          # {symbol: {bid: float, ask: float}}
DEPTH_LOCK = asyncio.Lock()

POSITIONS_STATE = {}      # {symbol: PositionStateObj}
STATE_LOCK = asyncio.Lock()

OPEN_ORDERS_CACHE = {}    # {orderid: order_dict}
ORDERS_LOCK = asyncio.Lock()

# Executor for blocking API calls
API_EXECUTOR = ThreadPoolExecutor(max_workers=5)

# ========================= HELPERS =========================
def safe_float(val, default=0.0):
    try:
        if val is None: return default
        return float(val)
    except:
        return default

def get_days_to_expiry(symbol):
    match = SYMBOL_REGEX.match(symbol)
    if not match: return 999
    expiry_str = match.group(2)
    try:
        # "31JUL25" -> "31Jul25"
        expiry_str_title = expiry_str[:2] + expiry_str[2:5].title() + expiry_str[5:]
        expiry_date = datetime.strptime(expiry_str_title, "%d%b%y")
        return (expiry_date - datetime.now()).days
    except:
        return 999

# ========================= API CLIENT (Sync -> Async Wrapper) =========================
class ApiClient:
    def __init__(self):
        self.headers = {'Content-Type': 'application/json'}

    def _post(self, endpoint, payload):
        try:
            url = f"{HOST}/api/v1/{endpoint}"
            if "apikey" not in payload: payload["apikey"] = API_KEY

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"API REQ {endpoint}: {json.dumps(payload)}")

            resp = requests.post(url, json=payload, headers=self.headers, timeout=5)

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"API RESP {endpoint} [{resp.status_code}]: {resp.text}")

            if resp.status_code == 200:
                return resp.json()
            return None
        except Exception as e:
            logger.error(f"API Error {endpoint}: {e}")
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(traceback.format_exc())
            return None

    def get_positions(self):
        data = self._post("positionbook", {})
        if data and data.get("status") == "success":
            return data.get("data", [])
        return []

    def get_orders(self):
        data = self._post("orderbook", {})
        if data and data.get("status") == "success":
            orders = data.get("data", [])
            if isinstance(orders, dict) and 'orders' in orders:
                return orders['orders']
            return orders
        return []

    def cancel_order(self, orderid):
        return self._post("cancelorder", {"orderid": orderid})

    def place_order(self, **kwargs):
        payload = {
            "strategy": "OptionProfitBooker",
            "disclosed_quantity": 0,
            "price": 0,
            "trigger_price": 0
        }
        payload.update(kwargs)
        return self._post("placeorder", payload)

    def modify_order(self, **kwargs):
        return self._post("modifyorder", kwargs)

    def fetch_margin_for_short(self, symbol, exchange, product, quantity):
        # Simulate SELL order to get utilized margin
        payload = {
            "mode": "analyze",
            "symbol": symbol,
            "action": "SELL",
            "exchange": exchange,
            "product": product,
            "quantity": abs(quantity),
            "price_type": "MARKET"
        }
        resp = self.place_order(**payload)
        if resp:
            # Look for total_margin_required
            m = safe_float(resp.get("total_margin_required"))
            if m == 0 and "data" in resp:
                m = safe_float(resp["data"].get("total_margin_required"))
            if m == 0 and "required" in resp:
                m = safe_float(resp.get("required"))
            return m
        return 0.0

api = ApiClient()

# ========================= CORE LOGIC =========================

async def cancel_existing_exit_orders(symbol, current_active_oid=None):
    """Cancel all OPEN orders for this symbol"""
    async with ORDERS_LOCK:
        orders_to_cancel = []
        for oid, order in OPEN_ORDERS_CACHE.items():
            if order['symbol'] == symbol and order['status'] in ['OPEN', 'PENDING', 'TRIGGER_PENDING']:
                if current_active_oid and oid == current_active_oid:
                    continue
                orders_to_cancel.append(oid)

    if orders_to_cancel:
        logger.info(f"Cancelling existing orders for {symbol}: {orders_to_cancel}")
        loop = asyncio.get_running_loop()
        futures = [loop.run_in_executor(API_EXECUTOR, api.cancel_order, oid) for oid in orders_to_cancel]
        await asyncio.gather(*futures)

async def reconcile_position_state(pos):
    """Sync API position with Internal State"""
    symbol = pos['symbol']
    qty = int(safe_float(pos.get('netqty', 0) or pos.get('quantity', 0)))
    avg_price = safe_float(pos.get('buyavg') if qty > 0 else pos.get('sellavg'))

    async with STATE_LOCK:
        if symbol not in POSITIONS_STATE:
            POSITIONS_STATE[symbol] = {
                'symbol': symbol,
                'exchange': pos.get('exchange'),
                'product': pos.get('product'),
                'qty': qty,
                'avg_price': avg_price,
                'margin': 0.0,
                'active_oid': None,
                'last_trigger': None,
                'lowest_ask': None,
                'state': 'TRACKING'
            }
            if qty < 0:
                loop = asyncio.get_running_loop()
                # Use partial if needed, but fetch_margin_for_short takes positional args in wrapper
                call = functools.partial(api.fetch_margin_for_short, symbol, pos['exchange'], pos['product'], qty)
                margin = await loop.run_in_executor(API_EXECUTOR, call)
                POSITIONS_STATE[symbol]['margin'] = margin
                logger.info(f"New Short {symbol}: Margin {margin}")

        else:
            state = POSITIONS_STATE[symbol]
            qty_changed = state['qty'] != qty
            avg_changed = abs(state['avg_price'] - avg_price) > 0.05

            if qty_changed or avg_changed:
                logger.warning(f"Position Changed {symbol}: Qty {state['qty']}->{qty}, Avg {state['avg_price']}->{avg_price}")

                if state['active_oid']:
                    await cancel_existing_exit_orders(symbol)

                state['qty'] = qty
                state['avg_price'] = avg_price
                state['active_oid'] = None
                state['last_trigger'] = None
                state['state'] = 'TRACKING'

                if qty < 0:
                     loop = asyncio.get_running_loop()
                     call = functools.partial(api.fetch_margin_for_short, symbol, pos['exchange'], pos['product'], qty)
                     margin = await loop.run_in_executor(API_EXECUTOR, call)
                     state['margin'] = margin

async def strategy_engine():
    """Main Logic Loop"""
    logger.info("Strategy Engine Started")
    while True:
        try:
            async with STATE_LOCK:
                symbols = list(POSITIONS_STATE.keys())

            for symbol in symbols:
                async with STATE_LOCK:
                    if symbol not in POSITIONS_STATE: continue
                    state = POSITIONS_STATE[symbol]

                async with DEPTH_LOCK:
                    depth = DEPTH_CACHE.get(symbol)

                if not depth: continue

                if state['qty'] > 0:
                    await handle_long(state, depth)
                elif state['qty'] < 0:
                    await handle_short(state, depth)

        except Exception as e:
            logger.error(f"Strategy Engine Error: {e}")
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(traceback.format_exc())

        await asyncio.sleep(1)

async def handle_long(state, depth):
    bid = depth.get('bid', 0)
    if bid <= 0: return

    symbol = state['symbol']
    avg = state['avg_price']

    if avg <= 0: return

    qty = state['qty']

    days = get_days_to_expiry(symbol)
    target_pct = NEAR_TARGET_PCT if days <= NEAR_EXPIRY_DAYS else FAR_TARGET_PCT
    target_price = avg * (1 + target_pct / 100.0)

    now_ist = datetime.now(IST)
    if qty > SPECIAL_QTY_THRESHOLD and now_ist.hour >= 15:
        target_pts = avg + SPECIAL_POINTS_CAP
        target_price = min(target_price, target_pts)

    if bid >= target_price:
        if state['state'] == 'PLACED': return

        logger.info(f"LONG TARGET {symbol}: Bid {bid} >= Target {target_price}")

        await cancel_existing_exit_orders(symbol)

        loop = asyncio.get_running_loop()
        call = functools.partial(api.place_order,
            symbol=symbol, exchange=state['exchange'], action="SELL",
            quantity=qty, product=state['product'], price_type="LIMIT", price=bid)

        resp = await loop.run_in_executor(API_EXECUTOR, call)

        if resp and resp.get('status') == 'success':
            async with STATE_LOCK:
                state['state'] = 'PLACED'
                state['active_oid'] = resp.get('orderid')

async def handle_short(state, depth):
    ask = depth.get('ask', 0)
    if ask <= 0: return

    symbol = state['symbol']
    avg = state['avg_price']

    if avg <= 0: return

    qty = abs(state['qty'])
    margin = state['margin']

    if margin <= 0: return

    profit = (avg - ask) * qty
    target_amt = margin * (SHORT_TARGET_MARGIN_PCT / 100.0)

    if profit >= target_amt:
        if state['state'] == 'TRACKING':
            logger.info(f"SHORT TARGET {symbol}: Profit {profit} >= {target_amt}")

            lock_amt = margin * (SHORT_LOCK_MARGIN_PCT / 100.0)
            lock_dist = lock_amt / qty
            trigger_price = avg - lock_dist

            await cancel_existing_exit_orders(symbol)
            loop = asyncio.get_running_loop()

            call = functools.partial(api.place_order,
                symbol=symbol, exchange=state['exchange'], action="BUY",
                quantity=qty, product=state['product'], price_type="SL-M",
                trigger_price=round(trigger_price, 1),
                tag="OPB", trailing_sl=TRAILING_POINT)

            resp = await loop.run_in_executor(API_EXECUTOR, call)

            if resp and resp.get('status') == 'success':
                async with STATE_LOCK:
                    state['state'] = 'TRAILING'
                    state['active_oid'] = resp.get('orderid')
                    state['last_trigger'] = trigger_price
                    state['lowest_ask'] = ask

        elif state['state'] == 'TRAILING':

            lowest = state.get('lowest_ask', avg)
            current_trigger = state['last_trigger']

            if ask < lowest:
                diff = lowest - ask
                if diff >= TRAILING_POINT:
                    new_trigger = current_trigger - diff

                    logger.info(f"Trailing {symbol}: Ask {ask} (Low {lowest}) -> New Trig {new_trigger}")

                    loop = asyncio.get_running_loop()
                    call = functools.partial(api.modify_order,
                        orderid=state['active_oid'],
                        trigger_price=round(new_trigger, 1),
                        price_type="SL-M", symbol=symbol, exchange=state['exchange'])

                    resp = await loop.run_in_executor(API_EXECUTOR, call)

                    if resp and resp.get('status') == 'success':
                        async with STATE_LOCK:
                            state['last_trigger'] = new_trigger
                            state['lowest_ask'] = ask

# ========================= POLLER =========================
async def data_poller():
    """Polls Positions and Orders"""
    logger.info("Data Poller Started")
    loop = asyncio.get_running_loop()

    while True:
        try:
            orders = await loop.run_in_executor(API_EXECUTOR, api.get_orders)
            async with ORDERS_LOCK:
                OPEN_ORDERS_CACHE.clear()
                for o in orders:
                    OPEN_ORDERS_CACHE[o.get('orderid')] = o

            positions = await loop.run_in_executor(API_EXECUTOR, api.get_positions)

            active_symbols = set()
            for pos in positions:
                sym = pos.get('symbol')
                qty = int(safe_float(pos.get('netqty', 0) or pos.get('quantity', 0)))

                if qty != 0:
                    active_symbols.add(sym)
                    await reconcile_position_state(pos)

            async with STATE_LOCK:
                tracked = list(POSITIONS_STATE.keys())
                for sym in tracked:
                    if sym not in active_symbols:
                        logger.info(f"Position Closed: {sym}")
                        if POSITIONS_STATE[sym]['active_oid']:
                            await cancel_existing_exit_orders(sym, None)
                        del POSITIONS_STATE[sym]

        except Exception as e:
            logger.error(f"Poller Error: {e}")
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(traceback.format_exc())

        await asyncio.sleep(POLL_INTERVAL)

# ========================= WEBSOCKET =========================
async def websocket_listener():
    """Maintains WS Connection and updates DEPTH_CACHE"""
    logger.info(f"WS Listener connecting to {WS_URL}")
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                await ws.send(json.dumps({"action": "authenticate", "api_key": API_KEY}))

                sub_task = asyncio.create_task(manage_subscriptions(ws))

                while True:
                    msg = await ws.recv()
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"WS IN: {msg[:200]}...") # Truncate large msgs

                    data = json.loads(msg)

                    if data.get('type') == 'market_data' and data.get('mode') == 3:
                        mdata = data.get('data', {})
                        sym = mdata.get('symbol')
                        if sym:
                            depth = mdata.get('depth', {})
                            bids = depth.get('buy', [])
                            asks = depth.get('sell', [])

                            best_bid = safe_float(bids[0]['price']) if bids else 0
                            best_ask = safe_float(asks[0]['price']) if asks else 0

                            async with DEPTH_LOCK:
                                DEPTH_CACHE[sym] = {'bid': best_bid, 'ask': best_ask}

        except Exception as e:
            logger.error(f"WS Error: {e}")
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(traceback.format_exc())
            await asyncio.sleep(5)

async def _sync_subscriptions_step(ws, subscribed):
    """Sync subscribed list with POSITIONS_STATE"""
    async with STATE_LOCK:
        current_map = {sym: data['exchange'] for sym, data in POSITIONS_STATE.items()}

    # Subscribe New
    to_sub = set(current_map.keys()) - set(subscribed.keys())
    for sym in to_sub:
        exc = current_map[sym]
        msg = {"action": "subscribe", "symbol": sym, "exchange": exc, "mode": 3}
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"WS OUT: {json.dumps(msg)}")

        await ws.send(json.dumps(msg))
        subscribed[sym] = exc
        logger.info(f"Subscribed {sym}")

    # Unsubscribe Old
    to_unsub = set(subscribed.keys()) - set(current_map.keys())
    for sym in to_unsub:
        exc = subscribed[sym]
        msg = {"action": "unsubscribe", "symbol": sym, "exchange": exc, "mode": 3}
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"WS OUT: {json.dumps(msg)}")

        await ws.send(json.dumps(msg))
        del subscribed[sym]
        logger.info(f"Unsubscribed {sym}")

async def manage_subscriptions(ws):
    """Periodically subscribe to new symbols in State"""
    subscribed = {} # {symbol: exchange}
    while True:
        try:
            await _sync_subscriptions_step(ws, subscribed)
        except Exception as e:
            logger.error(f"Sub Manager Error: {e}")

        await asyncio.sleep(2)

# ========================= ENTRY POINT =========================
def main():
    if not API_KEY:
        logger.error("API Key missing")
        return

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.create_task(websocket_listener())
    loop.create_task(data_poller())
    loop.create_task(strategy_engine())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
