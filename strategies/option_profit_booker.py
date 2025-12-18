#!/usr/bin/env python
"""
Option Profit Booker Strategy
Fetches option buying open positions and books profit if > target% based on market depth.
Target % depends on expiry: <= 30 days (4%), > 30 days (10%).
Also supports profit booking based on points for large quantities post 3:00 PM.
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

class OptionProfitBooker:
    def __init__(self):
        # Configuration
        self.api_key = os.getenv('OPENALGO_APIKEY')
        self.host = os.getenv('HOST_SERVER', 'http://127.0.0.1:5000')
        self.ws_url = os.getenv('WEBSOCKET_URL', 'ws://127.0.0.1:8765')

        # Configurable parameters
        try:
            self.profit_percentage_near = float(os.getenv('PROFIT_PERCENTAGE_NEAR', 4.0))
        except ValueError:
            logger.warning("Invalid PROFIT_PERCENTAGE_NEAR, defaulting to 4.0")
            self.profit_percentage_near = 4.0

        try:
            self.profit_percentage_far = float(os.getenv('PROFIT_PERCENTAGE_FAR', 10.0))
        except ValueError:
            logger.warning("Invalid PROFIT_PERCENTAGE_FAR, defaulting to 10.0")
            self.profit_percentage_far = 10.0

        try:
            self.poll_interval = int(os.getenv('POLL_INTERVAL', 30))
        except ValueError:
            logger.warning("Invalid POLL_INTERVAL, defaulting to 30")
            self.poll_interval = 30

        try:
            self.quantity_threshold = int(os.getenv('QUANTITY_THRESHOLD', 300))
        except ValueError:
            logger.warning("Invalid QUANTITY_THRESHOLD, defaulting to 300")
            self.quantity_threshold = 300

        try:
            self.profit_points_threshold = float(os.getenv('PROFIT_POINTS_THRESHOLD', 200.0))
        except ValueError:
            logger.warning("Invalid PROFIT_POINTS_THRESHOLD, defaulting to 200.0")
            self.profit_points_threshold = 200.0

        if not self.api_key:
            logger.error("OPENALGO_APIKEY environment variable not set")
            sys.exit(1)

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

    def get_effective_target_price(self, position):
        """
        Calculate effective target price based on expiry, time, and quantity.
        """
        symbol = position['symbol']
        avg_price = float(position.get('buy_avg', 0)) or float(position.get('avg_price', 0))
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
            expiry_date = datetime.strptime(expiry_str, "%d%b%y")
            days_to_expiry = (expiry_date - datetime.now()).days
        except ValueError:
            return avg_price * (1 + self.profit_percentage_near / 100)

        # Calculate standard percentage-based target
        if days_to_expiry <= 30:
            target_price = avg_price * (1 + self.profit_percentage_near / 100)
        else:
            # Far expiry logic
            target_pct_price = avg_price * (1 + self.profit_percentage_far / 100)

            # Check for special condition: Qty > 300 AND Time >= 15:00
            now = datetime.now()
            if qty > self.quantity_threshold and now.hour >= 15:
                target_pts_price = avg_price + self.profit_points_threshold
                # Take the lower of the two targets (conservative booking)
                target_price = min(target_pct_price, target_pts_price)
                logger.debug(f"Special condition for {symbol}: Min({target_pct_price}, {target_pts_price}) = {target_price}")
            else:
                target_price = target_pct_price

        return target_price

    def get_positions(self):
        """Fetch current positions from API"""
        try:
            url = f"{self.host}/api/v1/positionbook"
            payload = {"apikey": self.api_key}
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
            url = f"{self.host}/api/v1/orderbook"
            payload = {"apikey": self.api_key}
            response = requests.post(url, json=payload, headers=self.headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    # Filter for OPEN or PENDING orders
                    all_orders = data.get('data', [])
                    open_orders = [o for o in all_orders if o.get('status') in ['OPEN', 'PENDING', 'TRIGGER_PENDING']]
                    return open_orders
            return []
        except Exception as e:
            logger.error(f"Exception fetching orderbook: {e}")
            return []

    def cancel_order(self, order_id):
        """Cancel a specific order"""
        try:
            url = f"{self.host}/api/v1/cancelorder"
            payload = {
                "apikey": self.api_key,
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

    def place_sell_order(self, symbol, exchange, quantity, product, price_type="MARKET", price=0):
        """Place a SELL order (Market or Limit)"""
        try:
            url = f"{self.host}/api/v1/placeorder"
            # Standard OpenAlgo API payload
            payload = {
                "apikey": self.api_key,
                "strategy": "OptionProfitBooker",
                "symbol": symbol,
                "action": "SELL",
                "exchange": exchange,
                "price_type": price_type,
                "product": product,
                "quantity": quantity,
                "price": price,
                "trigger_price": 0,
                "disclosed_quantity": 0
            }

            response = requests.post(url, json=payload, headers=self.headers, timeout=10)
            if response.status_code == 200:
                logger.info(f"Placed SELL {price_type} order for {quantity} {symbol} at {price}")
                return True
            else:
                logger.error(f"Failed to place SELL order for {symbol}: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Exception placing order for {symbol}: {e}")
            return False

    async def ws_handler(self):
        """Websocket client handler"""
        while self.running:
            try:
                logger.info(f"Connecting to WebSocket: {self.ws_url}")
                async with websockets.connect(self.ws_url) as websocket:
                    self.ws_connected = True
                    logger.info("WebSocket Connected")

                    # Authenticate
                    auth_msg = {
                        "action": "authenticate",
                        "api_key": self.api_key
                    }
                    await websocket.send(json.dumps(auth_msg))

                    # Subscribe to existing tracked positions
                    with self.lock:
                        # Copy keys to avoid iteration issues if modified elsewhere
                        # (Though modification happens in poller thread or main thread)
                        for symbol, pos in list(self.tracked_positions.items()):
                            if not pos.get('processing', False): # Only subscribe if not processing exit
                                await self.subscribe(websocket, symbol, pos['exchange'])

                    # Listen loop
                    while self.running:
                        # Process outgoing subscriptions (from position poller)
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
                            # Just continue to check queue
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

            # Check for depth data
            if data.get('type') == 'market_data' and data.get('mode') == 3:
                market_data = data.get('data', {})
                symbol = market_data.get('symbol')
                depth = market_data.get('depth', {})
                buy_depth = depth.get('buy', [])

                if not buy_depth or not symbol:
                    return

                # Get Best Bid (Highest Buy Price)
                best_bid = float(buy_depth[0].get('price', 0))

                with self.lock:
                    position = self.tracked_positions.get(symbol)

                if position and not position.get('processing', False):
                    # Calculate dynamic target price
                    target_price = self.get_effective_target_price(position)

                    if target_price and best_bid >= target_price:
                        # Use Limit order logic if using the "points" condition or standard limit booking
                        # User requested limit order booking for this condition.
                        # We will use LIMIT order at Best Bid to ensure we book at this price or better.

                        logger.info(f"PROFIT TARGET REACHED for {symbol}: Bid {best_bid} >= Target {target_price}")
                        # Mark as processing IMMEDIATELY to prevent double triggering
                        position['processing'] = True

                        # Run blocking call in thread executor
                        await asyncio.to_thread(self.book_profit, position, best_bid)

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def book_profit(self, position, limit_price):
        """Execute profit booking logic (Runs in thread)"""
        symbol = position['symbol']
        exchange = position['exchange']
        quantity = position['quantity'] # Net quantity
        product = position['product']

        try:
            # 1. Check for open exit orders
            open_orders = self.get_open_orders()
            for order in open_orders:
                if (order.get('symbol') == symbol and
                    order.get('transaction_type') == 'SELL' and
                    order.get('status') in ['OPEN', 'PENDING', 'TRIGGER_PENDING']):

                    logger.info(f"Cancelling pending exit order {order.get('orderid')} for {symbol}")
                    self.cancel_order(order.get('orderid'))
                    time.sleep(0.5) # Wait for cancellation

            # 2. Place SELL LIMIT order
            if quantity > 0:
                success = self.place_sell_order(symbol, exchange, quantity, product,
                                              price_type="LIMIT", price=limit_price)
                if success:
                    logger.info(f"Profit booked for {symbol}. Removed from tracking.")
                    # Remove from tracking immediately after success
                    with self.lock:
                        if symbol in self.tracked_positions:
                            del self.tracked_positions[symbol]

                    # Unsubscribe via queue
                    self.sub_queue.put({
                        'action': 'unsubscribe',
                        'symbol': symbol,
                        'exchange': exchange
                    })
                else:
                    # Failed to place order, reset processing flag to retry
                    logger.error(f"Failed to book profit for {symbol}, resetting processing flag")
                    with self.lock:
                        if symbol in self.tracked_positions:
                            self.tracked_positions[symbol]['processing'] = False

        except Exception as e:
            logger.error(f"Exception booking profit for {symbol}: {e}")
            # Reset processing flag on error
            with self.lock:
                if symbol in self.tracked_positions:
                    self.tracked_positions[symbol]['processing'] = False

    def position_poller(self):
        """Periodically check for new positions"""
        while self.running:
            try:
                positions = self.get_positions()

                current_symbols = set()

                for pos in positions:
                    # Logic: Option Buying Open Positions (Quantity > 0)
                    qty = int(float(pos.get('netqty', 0) or pos.get('quantity', 0)))
                    symbol = pos.get('symbol')

                    if qty > 0 and symbol:
                        current_symbols.add(symbol)

                        with self.lock:
                            # If already tracked, just update quantity/price if not processing exit
                            if symbol in self.tracked_positions:
                                if not self.tracked_positions[symbol].get('processing', False):
                                    self.tracked_positions[symbol]['quantity'] = qty
                                    self.tracked_positions[symbol]['buy_avg'] = pos.get('buyavg') or pos.get('buy_avg')
                            else:
                                # New position
                                self.tracked_positions[symbol] = {
                                    'symbol': symbol,
                                    'exchange': pos.get('exchange'),
                                    'product': pos.get('product'),
                                    'quantity': qty,
                                    'buy_avg': pos.get('buyavg') or pos.get('buy_avg'),
                                    'processing': False
                                }
                                logger.info(f"New position detected: {symbol} (Qty: {qty})")
                                self.sub_queue.put({
                                    'action': 'subscribe',
                                    'symbol': symbol,
                                    'exchange': pos.get('exchange')
                                })

                # Check for closed positions
                with self.lock:
                    # Iterate over copy
                    tracked_symbols = list(self.tracked_positions.keys())
                    for sym in tracked_symbols:
                        # If position is no longer in API response AND we are not currently processing an exit
                        if sym not in current_symbols:
                            if not self.tracked_positions[sym].get('processing', False):
                                logger.info(f"Position closed: {sym}")
                                pos = self.tracked_positions.pop(sym)
                                self.sub_queue.put({
                                    'action': 'unsubscribe',
                                    'symbol': sym,
                                    'exchange': pos['exchange']
                                })

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
