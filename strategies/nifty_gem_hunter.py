#!/usr/bin/env python
"""
Nifty Gem Hunter Cloud Scanner

This strategy scans the Nifty Option Chain for "Gems" matching strict Deep Value,
Liquidity, and Spread filters. It runs asynchronously and sends a Telegram alert
when a matching option is found, without executing any trades automatically.
"""

import os
import time
import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Set

import aiohttp
import pytz

# --- Configuration Constants ---

# Discovery Filters
MIN_DAYS_TO_EXPIRY = 30
STRIKE_RANGE = 1000
DEEP_VALUE_THRESHOLD = 0.01  # 1% of Spot Price

# Liquidity Filters
MAX_SPREAD_PCT = 0.02  # 2%
MIN_VOLUME = 5

# Execution / State
POLL_INTERVAL = 30  # seconds
TELEGRAM_USERNAME = os.getenv("TELEGRAM_USERNAME", "admin")  # User to notify

# API Setup
API_KEY = os.getenv("OPENALGO_APIKEY", "")
HOST = os.getenv("OPENALGO_HOST", "http://127.0.0.1:5000")
SYMBOL = "NIFTY"
EXCHANGE = "NSE"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("NiftyGemHunter")

# Setup default timezone
ist = pytz.timezone('Asia/Kolkata')


class AsyncApiClient:
    """Async wrapper for OpenAlgo REST APIs."""

    def __init__(self, host: str, api_key: str):
        self.host = host.rstrip('/')
        self.api_key = api_key
        self.headers = {
            "Content-Type": "application/json",
            "X-API-KEY": api_key
        }

    async def _post(self, endpoint: str, payload: dict) -> dict:
        url = f"{self.host}{endpoint}"

        # OpenAlgo APIs typically expect apikey in payload if not purely in headers
        if "apikey" not in payload:
            payload["apikey"] = self.api_key

        async with aiohttp.ClientSession(headers=self.headers) as session:
            try:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    text = await response.text()
                    if response.status != 200:
                        logger.error(f"API Error POST {endpoint}: HTTP {response.status} - {text}")
                        return {}
                    try:
                        return json.loads(text)
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON from {endpoint}: {text}")
                        return {}
            except Exception as e:
                logger.error(f"Request failed for {endpoint}: {e}")
                return {}

    async def get_quotes(self, symbol: str, exchange: str = "NSE") -> dict:
        """Fetch real-time quotes."""
        return await self._post("/api/v1/quotes", {
            "symbol": symbol,
            "exchange": exchange
        })

    async def get_expiries(self, symbol: str, exchange: str = "NSE") -> dict:
        """Fetch available option expiries."""
        return await self._post("/api/v1/expiry", {
            "symbol": symbol,
            "exchange": exchange
        })

    async def get_option_chain(self, symbol: str, expiry_date: str, exchange: str = "NSE") -> dict:
        """Fetch full option chain for a specific expiry."""
        return await self._post("/api/v1/optionchain", {
            "symbol": symbol,
            "exchange": exchange,
            "expiry_date": expiry_date
        })

    async def send_telegram_alert(self, username: str, message: str) -> bool:
        """Send a notification via Telegram."""
        # Uses /api/v1/telegram/notify
        response = await self._post("/api/v1/telegram/notify", {
            "username": username,
            "message": message,
            "priority": 10
        })
        return response.get("status") == "success"

class GemHunterScanner:
    """Core logic for Nifty Gem Hunter Strategy."""

    def __init__(self, client: AsyncApiClient):
        self.client = client
        self.alerted_gems_today: Set[str] = set()
        self.current_day: str = ""

    def _is_market_open(self) -> bool:
        """Check if market is currently open (09:15 to 15:30 IST)."""
        now = datetime.now(ist)

        # Weekend check
        if now.weekday() >= 5: # 5=Sat, 6=Sun
            return False

        # Time check
        market_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_end = now.replace(hour=15, minute=30, second=0, microsecond=0)
        return market_start <= now <= market_end

    def _reset_daily_state(self):
        """Reset state if it's a new day."""
        today = datetime.now(ist).strftime("%Y-%m-%d")
        if self.current_day != today:
            logger.info(f"New day detected: {today}. Resetting state.")
            self.alerted_gems_today.clear()
            self.current_day = today

    def _safe_float(self, val: Any, default: float = 0.0) -> float:
        try:
            return float(val) if val is not None else default
        except (ValueError, TypeError):
            return default

    def _safe_int(self, val: Any, default: int = 0) -> int:
        try:
            return int(val) if val is not None else default
        except (ValueError, TypeError):
            return default

    async def find_target_expiry(self) -> Optional[str]:
        """Find the nearest expiry date that is >= MIN_DAYS_TO_EXPIRY days away."""
        expiries_resp = await self.client.get_expiries(SYMBOL, EXCHANGE)

        if not expiries_resp or "data" not in expiries_resp:
            logger.warning("Failed to fetch expiries")
            return None

        exp_list = expiries_resp.get("data", [])
        if not exp_list:
            logger.warning("Empty expiry list received")
            return None

        now = datetime.now(ist)
        target_expiry = None
        min_diff = float('inf')

        for exp_str in exp_list:
            try:
                # Openalgo standard format typically DD-MMM-YYYY or YYYY-MM-DD
                # We attempt both formats safely
                try:
                    exp_date = datetime.strptime(exp_str, "%d-%b-%Y")
                except ValueError:
                    exp_date = datetime.strptime(exp_str, "%Y-%m-%d")

                exp_date = ist.localize(exp_date)
                days_to_expiry = (exp_date - now).days

                if days_to_expiry >= MIN_DAYS_TO_EXPIRY and days_to_expiry < min_diff:
                    target_expiry = exp_str
                    min_diff = days_to_expiry
            except Exception as e:
                logger.error(f"Error parsing expiry {exp_str}: {e}")

        return target_expiry

    async def run_scan_cycle(self):
        """Execute one complete cycle of the scanning logic."""
        self._reset_daily_state()

        if not self._is_market_open():
            logger.info("Market is closed. Waiting...")
            return

        # 1. Fetch Nifty Spot Price
        quotes_resp = await self.client.get_quotes(SYMBOL, EXCHANGE)
        if not quotes_resp or "data" not in quotes_resp:
            logger.error("Failed to fetch spot quotes")
            return

        spot_price = self._safe_float(quotes_resp["data"].get("ltp"))
        if spot_price <= 0:
            logger.error(f"Invalid spot price: {spot_price}")
            return

        logger.info(f"Current Nifty Spot: {spot_price}")

        # 2. Find target expiry
        target_expiry = await self.find_target_expiry()
        if not target_expiry:
            logger.warning(f"No expiry found matching >= {MIN_DAYS_TO_EXPIRY} days criteria")
            return

        logger.info(f"Target Expiry: {target_expiry}")

        # 3. Fetch Option Chain
        chain_resp = await self.client.get_option_chain(SYMBOL, target_expiry, "NFO")
        if not chain_resp or "data" not in chain_resp:
            logger.error(f"Failed to fetch option chain for {target_expiry}")
            return

        chain_data = chain_resp.get("data", [])
        gems_found = 0

        # 4. Scan the chain for Gems
        for strike_data in chain_data:
            strike = self._safe_float(strike_data.get("strike_price"))

            # STRIKE RANGE Filter
            if abs(strike - spot_price) > STRIKE_RANGE:
                continue

            # Process Call (CE) and Put (PE) options for this strike
            for opt_type in ["CE", "PE"]:
                opt_info = strike_data.get(opt_type, {})
                if not opt_info:
                    continue

                bid = self._safe_float(opt_info.get("bid_price"))
                ask = self._safe_float(opt_info.get("ask_price"))
                volume = self._safe_int(opt_info.get("volume"))
                symbol = opt_info.get("tradingsymbol", f"{SYMBOL}{target_expiry}{int(strike)}{opt_type}")

                if ask <= 0:
                    continue # Cannot evaluate without valid ask

                # VOLUME Filter
                if volume < MIN_VOLUME:
                    continue

                # SPREAD Filter
                spread_pct = (ask - bid) / ask if ask > 0 else 1.0
                if spread_pct > MAX_SPREAD_PCT:
                    continue

                # DEEP VALUE Filter
                if opt_type == "CE":
                    intrinsic = max(0.0, spot_price - strike)
                else: # PE
                    intrinsic = max(0.0, strike - spot_price)

                extrinsic = ask - intrinsic

                if extrinsic <= (DEEP_VALUE_THRESHOLD * spot_price):
                    # WE FOUND A GEM!
                    extrinsic_pct = (extrinsic / spot_price) * 100
                    gems_found += 1

                    if symbol not in self.alerted_gems_today:
                        alert_msg = (
                            f"💎 GEM ALERT: {symbol}\n"
                            f"Type: {opt_type} | Strike: {strike}\n"
                            f"Spot: {spot_price:.2f} | Ask: {ask:.2f}\n"
                            f"Extrinsic: {extrinsic:.2f} ({extrinsic_pct:.2f}%)\n"
                            f"Spread: {spread_pct*100:.2f}% | Vol: {volume}\n\n"
                            f"[BUY 2 LOTS]"
                        )
                        logger.info(f"New Gem Alert! {symbol}")

                        success = await self.client.send_telegram_alert(TELEGRAM_USERNAME, alert_msg)
                        if success:
                            self.alerted_gems_today.add(symbol)
                        else:
                            logger.error(f"Failed to send alert for {symbol}")

        logger.info(f"Scan complete. Gems found: {gems_found}")


async def main():
    if not API_KEY:
        logger.error("OPENALGO_APIKEY is not set in environment.")
        return

    client = AsyncApiClient(HOST, API_KEY)
    scanner = GemHunterScanner(client)

    logger.info("Nifty Gem Hunter Scanner started.")

    while True:
        try:
            await scanner.run_scan_cycle()
        except asyncio.CancelledError:
            logger.info("Scanner stopped by system.")
            break
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)

        await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Scanner manually stopped.")
