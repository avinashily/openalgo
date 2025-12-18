#!/usr/bin/env python
"""
Option Profit Booker Strategy
Fetches option buying open positions and books profit if > 10% based on market depth.
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
            self.profit_percentage = float(os.getenv('PROFIT_PERCENTAGE', 10.0))
        except ValueError:
            logger.warning("Invalid PROFIT_PERCENTAGE, defaulting to 10.0")
            self.profit_percentage = 10.0

        try:
            self.poll_interval = int(os.getenv('POLL_INTERVAL', 30))
        except ValueError:
            logger.warning("Invalid POLL_INTERVAL, defaulting to 30")
            self.poll_interval = 30

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

    def place_sell_order(self, symbol, exchange, quantity, product):
        """Place a SELL MARKET order"""
        try:
            url = f"{self.host}/api/v1/placeorder"
            # Standard OpenAlgo API payload
            payload = {
                "apikey": self.api_key,
                "strategy": "OptionProfitBooker",
                "symbol": symbol,
                "action": "SELL",
                "exchange": exchange,
                "price_type": "MARKET",
                "product": product,
                "quantity": quantity,
                "price": 0,
                "trigger_price": 0,
                "disclosed_quantity": 0
            }

            response = requests.post(url, json=payload, headers=self.headers, timeout=10)
            if response.status_code == 200:
                logger.info(f"Placed SELL order for {quantity} {symbol}")
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
                    avg_price = float(position.get('buy_avg', 0)) or float(position.get('avg_price', 0))

                    if avg_price <= 0:
                        return

                    profit_pct = ((best_bid - avg_price) / avg_price) * 100

                    if profit_pct >= self.profit_percentage:
                        logger.info(f"PROFIT TARGET REACHED for {symbol}: {profit_pct:.2f}% (Bid: {best_bid}, Avg: {avg_price})")
                        # Mark as processing IMMEDIATELY to prevent double triggering
                        position['processing'] = True

                        # Run blocking call in thread executor
                        await asyncio.to_thread(self.book_profit, position)

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def book_profit(self, position):
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

            # 2. Place SELL MARKET order
            if quantity > 0:
                success = self.place_sell_order(symbol, exchange, quantity, product)
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
