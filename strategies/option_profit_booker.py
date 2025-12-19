#!/usr/bin/env python
"""
Option Profit Booker Strategy
Fetches open positions and books profit based on configurable logic.
- Long Options: Profit > Target% (based on expiry). Books via Limit Order.
- Short Options: Profit > 0.7% of Margin. Triggers Trailing SL (Lock 0.55%, Trail 0.5 pts).
  (Margin fetched via analyze mode).
"""
import os
import sys
import json
import time
import asyncio
import threading
import requests
import logging
import websockets
import queue
import re
from datetime import datetime
import pytz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join("log", "option_profit_booker.log")),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("OptionProfitBooker")

# --- Configuration Constants (Environment Variables) ---
API_KEY = os.getenv('OPENALGO_APIKEY')
HOST = os.getenv('HOST_SERVER', 'http://127.0.0.1:5000')
WS_URL = os.getenv('WEBSOCKET_URL', 'ws://127.0.0.1:8765')

# Long Strategy Config
try:
    PROFIT_PERCENTAGE_NEAR = float(os.getenv('PROFIT_PERCENTAGE_NEAR', 4.0))
except ValueError:
    logger.warning("Invalid PROFIT_PERCENTAGE_NEAR, defaulting to 4.0")
    PROFIT_PERCENTAGE_NEAR = 4.0

try:
    PROFIT_PERCENTAGE_FAR = float(os.getenv('PROFIT_PERCENTAGE_FAR', 10.0))
except ValueError:
    logger.warning("Invalid PROFIT_PERCENTAGE_FAR, defaulting to 10.0")
    PROFIT_PERCENTAGE_FAR = 10.0

try:
    QUANTITY_THRESHOLD = int(os.getenv('QUANTITY_THRESHOLD', 300))
except ValueError:
    logger.warning("Invalid QUANTITY_THRESHOLD, defaulting to 300")
    QUANTITY_THRESHOLD = 300

try:
    PROFIT_POINTS_THRESHOLD = float(os.getenv('PROFIT_POINTS_THRESHOLD', 200.0))
except ValueError:
    logger.warning("Invalid PROFIT_POINTS_THRESHOLD, defaulting to 200.0")
    PROFIT_POINTS_THRESHOLD = 200.0

# Short Strategy Config
try:
    PROFIT_PERCENTAGE_SHORT = float(os.getenv('PROFIT_PERCENTAGE_SHORT', 0.7)) # 0.7% of Margin
except ValueError:
    logger.warning("Invalid PROFIT_PERCENTAGE_SHORT, defaulting to 0.7")
    PROFIT_PERCENTAGE_SHORT = 0.7

try:
    PROFIT_LOCKED_PERCENTAGE = float(os.getenv('PROFIT_LOCKED_PERCENTAGE', 0.55)) # Lock 0.55%
except ValueError:
    logger.warning("Invalid PROFIT_LOCKED_PERCENTAGE, defaulting to 0.55")
    PROFIT_LOCKED_PERCENTAGE = 0.55

try:
    TRAILING_POINT = float(os.getenv('TRAILING_POINT', 0.5)) # Trail 0.5 pts
except ValueError:
    logger.warning("Invalid TRAILING_POINT, defaulting to 0.5")
    TRAILING_POINT = 0.5

try:
    POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', 30))
except ValueError:
    logger.warning("Invalid POLL_INTERVAL, defaulting to 30")
    POLL_INTERVAL = 30

# -------------------------------------------------------

class OptionProfitBooker:
    def __init__(self):
        if not API_KEY:
            logger.error("OPENALGO_APIKEY environment variable not set")
            sys.exit(1)

        # Config (Instance variables for testability)
        self.profit_percentage_near = PROFIT_PERCENTAGE_NEAR
        self.profit_percentage_far = PROFIT_PERCENTAGE_FAR
        self.quantity_threshold = QUANTITY_THRESHOLD
        self.profit_points_threshold = PROFIT_POINTS_THRESHOLD
        self.profit_percentage_short = PROFIT_PERCENTAGE_SHORT
        self.profit_locked_percentage = PROFIT_LOCKED_PERCENTAGE
        self.trailing_point = TRAILING_POINT
        self.poll_interval = POLL_INTERVAL

        # State
        self.tracked_positions = {}  # {symbol: position_data}
        self.running = True
        self.ws_connected = False
        self.lock = threading.Lock()
        self.sub_queue = queue.Queue() # Queue for WS commands

        # REST API headers
        self.headers = {
            'Content-Type': 'application/json'
        }

        # Symbol parser regex (DDMMMYY)
        self.symbol_regex = re.compile(r'^([A-Z0-9]+)(\d{2}[A-Z]{3}\d{2})(\d+)(CE|PE)$')

        # Timezone
        self.tz_ist = pytz.timezone('Asia/Kolkata')

    def safe_float(self, value, default=0.0):
        """Safely convert value to float, handling None"""
        try:
            if value is None:
                return default
            return float(value)
        except (ValueError, TypeError):
            return default

    def get_effective_target_price(self, position):
        """
        Calculate effective target price based on expiry, time, and quantity (Long positions).
        """
        symbol = position['symbol']
        avg_price = self.safe_float(position.get('buy_avg')) or self.safe_float(position.get('avg_price'))
        qty = position.get('quantity', 0)

        if avg_price <= 0:
            return None

        # Parse expiry
        match = self.symbol_regex.match(symbol)
        if not match:
            # Fallback to near target
            return avg_price * (1 + self.profit_percentage_near / 100)

        expiry_str = match.group(2)
        try:
            # Convert to Title Case for strptime %b (e.g., "31JUL25" -> "31Jul25")
            expiry_str_title = expiry_str[:2] + expiry_str[2:5].title() + expiry_str[5:]
            expiry_date = datetime.strptime(expiry_str_title, "%d%b%y")
            days_to_expiry = (expiry_date - datetime.now()).days
        except ValueError:
            return avg_price * (1 + self.profit_percentage_near / 100)

        # Calculate standard percentage-based target
        if days_to_expiry <= 30:
            target_price = avg_price * (1 + self.profit_percentage_near / 100)
        else:
            # Far expiry logic
            target_pct_price = avg_price * (1 + self.profit_percentage_far / 100)

            # Check for special condition: Qty > 300 AND Time >= 15:00 IST
            now = datetime.now(self.tz_ist)
            if qty > self.quantity_threshold and now.hour >= 15:
                target_pts_price = avg_price + self.profit_points_threshold
                # Take the lower of the two targets (conservative booking)
                target_price = min(target_pct_price, target_pts_price)
                logger.debug(f"Special condition for {symbol}: Min({target_pct_price}, {target_pts_price}) = {target_price}")
            else:
                target_price = target_pct_price

        return target_price

    def fetch_margin(self, position):
        """
        Fetch margin requirement for a position using placeorder(mode='analyze').
        Simulates the SELL order (for Short) to get utilized margin.
        """
        try:
            url = f"{HOST}/api/v1/placeorder"

            # Construct payload to simulate the Short Order (SELL)
            payload = {
                "apikey": API_KEY,
                "strategy": "OptionProfitBooker",
                "mode": "analyze", # Analyze mode for margin check
                "symbol": position['symbol'],
                "action": "SELL", # Check margin for SELLING
                "exchange": position['exchange'],
                "price_type": "MARKET",
                "product": position['product'],
                "quantity": abs(position['quantity']),
                "price": 0,
                "trigger_price": 0,
                "disclosed_quantity": 0
            }

            response = requests.post(url, json=payload, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                # Check for total_margin_required in root or data
                # Logic: User said "look for total_margin_required in the response"

                margin = self.safe_float(data.get('total_margin_required'))
                if margin == 0 and 'data' in data:
                     margin = self.safe_float(data['data'].get('total_margin_required'))

                if margin > 0:
                    return margin

                # Fallback: check if 'required' field exists (common in some APIs)
                if 'required' in data:
                     return self.safe_float(data.get('required'))

            logger.warning(f"Margin not found in response for {position['symbol']}: {response.text}")
            return 0.0
        except Exception as e:
            logger.error(f"Error fetching margin for {position['symbol']}: {e}")
            return 0.0

    def get_positions(self):
        """Fetch current positions from API"""
        try:
            url = f"{HOST}/api/v1/positionbook"
            payload = {"apikey": API_KEY}
            response = requests.post(url, json=payload, headers=self.headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    return data.get('data', [])
                else:
                    logger.error(f"Error in positionbook response: {data.get('message')}")
            else:
                logger.error(f"Failed to fetch positions. Status: {response.status_code}, Body: {response.text}")
            return []
        except Exception as e:
            logger.error(f"Exception fetching positions: {e}")
            return []

    def get_open_orders(self):
        """Fetch open orders from API"""
        try:
            url = f"{HOST}/api/v1/orderbook"
            payload = {"apikey": API_KEY}
            response = requests.post(url, json=payload, headers=self.headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    # Filter for OPEN or PENDING orders
                    all_orders = data.get('data', [])
                    # Handle if data is wrapped in 'orders' key
                    if isinstance(all_orders, dict) and 'orders' in all_orders:
                        all_orders = all_orders['orders']

                    open_orders = [o for o in all_orders if o.get('status') in ['OPEN', 'PENDING', 'TRIGGER_PENDING', 'open', 'pending', 'trigger_pending']]
                    return open_orders
            return []
        except Exception as e:
            logger.error(f"Exception fetching orderbook: {e}")
            return []

    def cancel_order(self, order_id):
        """Cancel a specific order"""
        try:
            url = f"{HOST}/api/v1/cancelorder"
            payload = {
                "apikey": API_KEY,
                "orderid": order_id
            }
            response = requests.post(url, json=payload, headers=self.headers, timeout=10)
            if response.status_code == 200:
                logger.info(f"Cancelled order {order_id}")
                return True
            else:
                logger.error(f"Failed to cancel order {order_id}: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Exception cancelling order {order_id}: {e}")
            return False

    def place_order(self, symbol, exchange, quantity, product, transaction_type, price_type="MARKET", price=0, trigger_price=0, **kwargs):
        """Place an order with optional extra fields (tag, trailing_sl)"""
        try:
            url = f"{HOST}/api/v1/placeorder"
            payload = {
                "apikey": API_KEY,
                "strategy": "OptionProfitBooker",
                "symbol": symbol,
                "action": transaction_type,
                "exchange": exchange,
                "price_type": price_type,
                "product": product,
                "quantity": abs(quantity),
                "price": price,
                "trigger_price": trigger_price,
                "disclosed_quantity": 0
            }
            # Add extra fields (e.g., tag, trailing_sl)
            payload.update(kwargs)

            response = requests.post(url, json=payload, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                order_id = data.get('orderid')
                logger.info(f"Placed {transaction_type} {price_type} order for {quantity} {symbol}. Order ID: {order_id}")
                return order_id
            else:
                logger.error(f"Failed to place order for {symbol}: {response.text}")
                return None
        except Exception as e:
            logger.error(f"Exception placing order for {symbol}: {e}")
            return None

    def modify_order(self, order_id, new_trigger_price, symbol):
        """Modify an existing SL order (Synthetic Trailing)"""
        try:
            url = f"{HOST}/api/v1/modifyorder"
            payload = {
                "apikey": API_KEY,
                "orderid": order_id,
                "trigger_price": new_trigger_price,
                "price": 0,
                "price_type": "SL-M",
                "quantity": 0, # Rely on orderid
                "exchange": "NFO",
                "symbol": symbol
            }

            response = requests.post(url, json=payload, headers=self.headers, timeout=10)
            if response.status_code == 200:
                logger.info(f"Modified SL order {order_id} to trigger {new_trigger_price}")
                return True
            else:
                logger.error(f"Failed to modify order {order_id}: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Exception modifying order {order_id}: {e}")
            return False

    async def ws_handler(self):
        """Websocket client handler"""
        while self.running:
            try:
                logger.info(f"Connecting to WebSocket: {WS_URL}")
                async with websockets.connect(WS_URL) as websocket:
                    self.ws_connected = True
                    logger.info("WebSocket Connected")

                    # Authenticate
                    auth_msg = {
                        "action": "authenticate",
                        "api_key": API_KEY
                    }
                    await websocket.send(json.dumps(auth_msg))

                    # Subscribe to existing tracked positions
                    with self.lock:
                        for symbol, pos in list(self.tracked_positions.items()):
                            if not pos.get('processing', False):
                                await self.subscribe(websocket, symbol, pos['exchange'])

                    # Listen loop
                    while self.running:
                        # Process outgoing subscriptions
                        while not self.sub_queue.empty():
                            try:
                                req = self.sub_queue.get_nowait()
                                action = req['action']
                                if action == 'subscribe':
                                    await self.subscribe(websocket, req['symbol'], req['exchange'])
                                elif action == 'unsubscribe':
                                    await self.unsubscribe(websocket, req['symbol'], req['exchange'])
                            except queue.Empty:
                                break

                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                            await self.process_message(message)
                        except asyncio.TimeoutError:
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning("WebSocket connection closed")
                            break

            except Exception as e:
                self.ws_connected = False
                logger.error(f"WebSocket error: {e}")
                await asyncio.sleep(5)  # Reconnect delay

    async def subscribe(self, websocket, symbol, exchange):
        """Send subscribe message"""
        msg = {
            "action": "subscribe",
            "symbol": symbol,
            "exchange": exchange,
            "mode": 3,  # Depth
            "depth_level": 5
        }
        await websocket.send(json.dumps(msg))
        logger.info(f"Subscribed to {symbol} ({exchange})")

    async def unsubscribe(self, websocket, symbol, exchange):
        """Send unsubscribe message"""
        msg = {
            "action": "unsubscribe",
            "symbol": symbol,
            "exchange": exchange,
            "mode": 3
        }
        await websocket.send(json.dumps(msg))
        logger.info(f"Unsubscribed from {symbol}")

    async def process_message(self, message):
        """Process incoming WS message"""
        try:
            data = json.loads(message)

            if data.get('type') == 'market_data' and data.get('mode') == 3:
                market_data = data.get('data', {})
                symbol = market_data.get('symbol')
                depth = market_data.get('depth', {})
                buy_depth = depth.get('buy', [])
                sell_depth = depth.get('sell', [])

                if not symbol:
                    return

                with self.lock:
                    position = self.tracked_positions.get(symbol)

                if not position or position.get('processing', False):
                    return

                # --- LONG POSITION LOGIC ---
                if position['quantity'] > 0:
                    if not buy_depth: return
                    best_bid = self.safe_float(buy_depth[0].get('price'))
                    avg_price = self.safe_float(position.get('buy_avg')) or self.safe_float(position.get('avg_price'))

                    if avg_price <= 0: return

                    profit_pct = ((best_bid - avg_price) / avg_price) * 100
                    target_profit = position.get('target_profit', self.profit_percentage_near)

                    if profit_pct >= target_profit:
                        logger.info(f"LONG PROFIT TARGET {symbol}: {profit_pct:.2f}% (Target: {target_profit}%)")
                        position['processing'] = True
                        await asyncio.to_thread(self.book_profit_long, position, best_bid)

                # --- SHORT POSITION LOGIC ---
                elif position['quantity'] < 0:
                    if not sell_depth: return
                    # For shorts, we buy back at ASK.
                    best_ask = self.safe_float(sell_depth[0].get('price'))
                    avg_price = self.safe_float(position.get('sell_avg')) or self.safe_float(position.get('avg_price'))
                    margin = self.safe_float(position.get('margin_used', 0))

                    if avg_price <= 0 or margin <= 0: return

                    # Profit = (Entry - Current) * Qty
                    # Since Qty is negative for short, Abs(Qty) is size.
                    # Profit = (AvgPrice - BestAsk) * Abs(Qty)
                    unrealized_profit = (avg_price - best_ask) * abs(position['quantity'])

                    # Target: 0.7% of Margin
                    target_profit_amt = margin * (self.profit_percentage_short / 100.0)

                    # Check for Triggering Trailing SL
                    if unrealized_profit >= target_profit_amt:
                        # Logic: Start Trailing SL if not already started
                        if not position.get('trailing_active', False):
                            logger.info(f"SHORT PROFIT TARGET {symbol}: Profit {unrealized_profit} >= {target_profit_amt} ({self.profit_percentage_short}% of Margin)")
                            position['processing'] = True # Temp lock while placing initial SL
                            await asyncio.to_thread(self.start_trailing_sl, position, best_ask, avg_price, margin)
                        else:
                            # Logic: Update existing Trailing SL
                            # "Trailing 0.5 point" -> If best_ask drops 0.5 from lowest seen, drop trigger 0.5.

                            current_trigger = position.get('sl_trigger_price')
                            lowest_ask = position.get('lowest_ask', avg_price)

                            # Update Lowest Ask if current is lower
                            if best_ask < lowest_ask:
                                diff = lowest_ask - best_ask
                                if diff >= self.trailing_point:
                                    # Move SL down by diff
                                    new_trigger = current_trigger - diff
                                    # Ensure we don't move SL *up* (though diff > 0 implies move down for trigger)
                                    # Wait. Trigger Price for BUY SL-M is ABOVE market price.
                                    # If Price Goes Down (Profit), SL should Go Down (Follow).
                                    # Yes. New Trigger < Old Trigger.

                                    if not position.get('modifying', False):
                                        position['modifying'] = True
                                        await asyncio.to_thread(self.update_trailing_sl, position, new_trigger, best_ask)

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def book_profit_long(self, position, limit_price):
        """Execute profit booking for Long positions (Limit Sell)"""
        symbol = position['symbol']
        exchange = position['exchange']
        quantity = position['quantity']
        product = position['product']

        try:
            # 1. Cancel open orders
            open_orders = self.get_open_orders()
            for order in open_orders:
                if (order.get('symbol') == symbol and
                    order.get('transaction_type') == 'SELL' and
                    order.get('status') in ['OPEN', 'PENDING', 'TRIGGER_PENDING']):
                    self.cancel_order(order.get('orderid'))
                    time.sleep(0.5)

            # 2. Place SELL LIMIT order
            if quantity > 0:
                success = self.place_order(symbol, exchange, quantity, product,
                                         "SELL", "LIMIT", price=limit_price)
                if success:
                    logger.info(f"Profit booked for {symbol}. Removed from tracking.")
                    with self.lock:
                        if symbol in self.tracked_positions:
                            del self.tracked_positions[symbol]
                    self.sub_queue.put({'action': 'unsubscribe', 'symbol': symbol, 'exchange': exchange})
                else:
                    with self.lock:
                        if symbol in self.tracked_positions:
                            self.tracked_positions[symbol]['processing'] = False
        except Exception as e:
            logger.error(f"Exception booking long profit: {e}")
            with self.lock:
                if symbol in self.tracked_positions:
                    self.tracked_positions[symbol]['processing'] = False

    def start_trailing_sl(self, position, current_ask, avg_price, margin):
        """Initialize Trailing SL for Short positions"""
        symbol = position['symbol']
        exchange = position['exchange']
        quantity = abs(position['quantity']) # Order qty is positive
        product = position['product']

        try:
            # 1. Cancel open orders
            open_orders = self.get_open_orders()
            for order in open_orders:
                if (order.get('symbol') == symbol and
                    order.get('status') in ['OPEN', 'PENDING', 'TRIGGER_PENDING']):
                    self.cancel_order(order.get('orderid'))
                    time.sleep(0.5)

            # 2. Calculate Initial SL Trigger
            # Lock 0.55% Profit of Margin.
            # Locked Profit = Margin * 0.0055
            # Locked Profit = (Entry - SL) * Qty
            # SL = Entry - (Margin * 0.0055 / Qty)

            sl_gap_per_unit = (margin * (self.profit_locked_percentage / 100.0)) / quantity
            initial_trigger = avg_price - sl_gap_per_unit

            # Place SL-M Order (Transaction: BUY)
            # Send 'tag' and 'trailing_sl' as requested, even if API might strip them.
            # Also enabling synthetic trailing logic via 'trailing_active'.

            order_id = self.place_order(symbol, exchange, quantity, product,
                                      "BUY", "SL-M",
                                      trigger_price=round(initial_trigger, 1),
                                      tag="OPB",
                                      trailing_sl=self.trailing_point)

            if order_id:
                with self.lock:
                    if symbol in self.tracked_positions:
                        self.tracked_positions[symbol]['processing'] = False
                        self.tracked_positions[symbol]['trailing_active'] = True
                        self.tracked_positions[symbol]['sl_order_id'] = order_id
                        self.tracked_positions[symbol]['sl_trigger_price'] = initial_trigger
                        self.tracked_positions[symbol]['lowest_ask'] = current_ask
                logger.info(f"Started Trailing SL for {symbol} at {initial_trigger} (Lock {self.profit_locked_percentage}%)")
            else:
                # Failed, reset to try again
                with self.lock:
                    if symbol in self.tracked_positions:
                        self.tracked_positions[symbol]['processing'] = False

        except Exception as e:
            logger.error(f"Exception starting trailing SL: {e}")
            with self.lock:
                if symbol in self.tracked_positions:
                    self.tracked_positions[symbol]['processing'] = False

    def update_trailing_sl(self, position, new_trigger, current_ask):
        """Modify Trailing SL order"""
        symbol = position['symbol']
        order_id = position['sl_order_id']

        try:
            # Synthetic Trailing: Update the existing SL-M order
            success = self.modify_order(order_id, round(new_trigger, 1), symbol)

            with self.lock:
                if symbol in self.tracked_positions:
                    self.tracked_positions[symbol]['modifying'] = False
                    if success:
                        self.tracked_positions[symbol]['sl_trigger_price'] = new_trigger
                        self.tracked_positions[symbol]['lowest_ask'] = current_ask
                        logger.info(f"Updated Trailing SL for {symbol} to {new_trigger}")
                    else:
                        # If modify fails, we might want to reset lowest_ask to retry later?
                        # Or just leave it.
                        pass

        except Exception as e:
            logger.error(f"Exception updating trailing SL: {e}")
            with self.lock:
                if symbol in self.tracked_positions:
                    self.tracked_positions[symbol]['modifying'] = False

    def position_poller(self):
        """Periodically check for positions"""
        while self.running:
            try:
                positions = self.get_positions()
                current_symbols = set()

                for pos in positions:
                    # Generic quantity extraction
                    qty = int(float(pos.get('netqty', 0) or pos.get('quantity', 0)))
                    symbol = pos.get('symbol')

                    if qty != 0 and symbol:
                        current_symbols.add(symbol)

                        with self.lock:
                            # Update existing or add new
                            if symbol in self.tracked_positions:
                                # Retry margin fetch if missing/zero for short positions
                                if qty < 0 and self.tracked_positions[symbol].get('margin_used', 0) == 0:
                                    margin = self.fetch_margin({
                                        'exchange': pos.get('exchange'),
                                        'symbol': symbol,
                                        'product': pos.get('product'),
                                        'quantity': qty
                                    })
                                    if margin > 0:
                                        self.tracked_positions[symbol]['margin_used'] = margin
                                        logger.info(f"Updated Margin for {symbol}: {margin}")
                            else:
                                # Identify Long vs Short
                                is_short = qty < 0
                                margin = 0.0
                                target_profit = 0.0

                                if is_short:
                                    # Fetch Margin for short
                                    # We construct a temp dict for fetch_margin
                                    temp_pos = {
                                        'exchange': pos.get('exchange'),
                                        'symbol': symbol,
                                        'product': pos.get('product'),
                                        'quantity': qty
                                    }
                                    margin = self.fetch_margin(temp_pos)
                                    logger.info(f"Tracking SHORT: {symbol} (Margin: {margin})")
                                else:
                                    target_profit = self.get_effective_target_price({'symbol': symbol, 'buy_avg': pos.get('buyavg'), 'quantity': qty})
                                    logger.info(f"Tracking LONG: {symbol} (Target: {target_profit})")

                                self.tracked_positions[symbol] = {
                                    'symbol': symbol,
                                    'exchange': pos.get('exchange'),
                                    'product': pos.get('product'),
                                    'quantity': qty,
                                    'buy_avg': pos.get('buyavg') or pos.get('buy_avg'),
                                    'sell_avg': pos.get('sellavg') or pos.get('sell_avg'),
                                    'margin_used': margin,
                                    'target_profit': target_profit,
                                    'processing': False,
                                    'trailing_active': False
                                }
                                self.sub_queue.put({
                                    'action': 'subscribe',
                                    'symbol': symbol,
                                    'exchange': pos.get('exchange')
                                })

                # Cleanup closed
                with self.lock:
                    for sym in list(self.tracked_positions.keys()):
                        if sym not in current_symbols:
                            logger.info(f"Position closed: {sym}")
                            pos = self.tracked_positions.pop(sym)
                            self.sub_queue.put({'action': 'unsubscribe', 'symbol': sym, 'exchange': pos['exchange']})

            except Exception as e:
                logger.error(f"Error in position poller: {e}")

            time.sleep(self.poll_interval)

    def start(self):
        """Start threads"""
        logger.info("Starting Option Profit Booker Strategy")

        # Start Position Poller in a separate thread
        t_poller = threading.Thread(target=self.position_poller, daemon=True)
        t_poller.start()

        # Start WebSocket Handler in the main thread
        try:
            asyncio.run(self.ws_handler())
        except KeyboardInterrupt:
            logger.info("Stopping strategy...")
            self.running = False

if __name__ == "__main__":
    strategy = OptionProfitBooker()
    strategy.start()
