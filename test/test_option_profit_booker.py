import unittest
from unittest.mock import MagicMock, patch
import os
import sys
import json
import threading
from datetime import datetime
import pytz

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock Env
os.environ['OPENALGO_APIKEY'] = 'test_key'

import strategies.option_profit_booker as strategy

class TestSDKStrategy(unittest.TestCase):
    def setUp(self):
        # Reset State
        strategy.POSITIONS_STATE.clear()
        strategy.SUBSCRIBED_SYMBOLS.clear()

        # Mock Client methods
        strategy.client.placeorder = MagicMock(return_value={'status': 'success', 'orderid': '123'})
        strategy.client.modifyorder = MagicMock(return_value={'status': 'success'})
        strategy.client.cancelorder = MagicMock(return_value={'status': 'success'})
        strategy.client.get_orderbook = MagicMock(return_value={'status': 'success', 'data': []})
        strategy.client.orderstatus = MagicMock(return_value={'status': 'success', 'data': {'order_status': 'OPEN'}})

    def test_long_trigger(self):
        """Test Long Position Target Trigger via Callback"""
        symbol = "TEST_CE"

        strategy.POSITIONS_STATE[symbol] = {
            "symbol": symbol, "qty": 100, "avg_price": 100.0,
            "exchange": "NFO", "product": "MIS", "state": "TRACKING",
            "active_oid": None
        }

        # Far Target 50% -> 150.0

        msg_high = {"data": {"symbol": symbol, "depth": {"buy": [{"price": 151.0}], "sell": []}}}
        strategy.on_market_data(msg_high)

        strategy.client.placeorder.assert_called_once()
        args, kwargs = strategy.client.placeorder.call_args
        self.assertEqual(kwargs['action'], 'SELL')
        self.assertEqual(kwargs['price'], 151.0)

    def test_short_trigger_and_trail(self):
        """Test Short Logic: Trigger -> SL Placement -> Trailing"""
        symbol = "TEST_PE"
        strategy.POSITIONS_STATE[symbol] = {
            "symbol": symbol, "qty": -50, "avg_price": 100.0,
            "exchange": "NFO", "product": "MIS", "state": "TRACKING",
            "margin": 100000.0, "active_oid": None
        }

        # Target 0.65% -> 650.
        # Trigger Price 80.0 (Huge Profit)

        # 1. Trigger
        msg_trigger = {"data": {"symbol": symbol, "depth": {"buy": [], "sell": [{"price": 80.0}]}}}
        strategy.on_market_data(msg_trigger)

        strategy.client.placeorder.assert_called_once()
        _, kwargs = strategy.client.placeorder.call_args
        self.assertEqual(kwargs['action'], 'BUY')
        self.assertEqual(kwargs['price_type'], 'SL-M')

        # Lock 0.55% (550 -> 11 pts). Entry 100 -> Trigger 89.0
        self.assertEqual(kwargs['trigger_price'], 89.0)

        # 2. Trailing Update
        # Move Ask down to 79.5 (Diff 0.5 from 80.0 lowest)
        msg_trail = {"data": {"symbol": symbol, "depth": {"buy": [], "sell": [{"price": 79.5}]}}}
        strategy.on_market_data(msg_trail)

        strategy.client.modifyorder.assert_called_once()
        _, kwargs = strategy.client.modifyorder.call_args
        # Old Trig 89.0 -> New 88.5
        self.assertEqual(kwargs['trigger_price'], 88.5)

    def test_dead_order_reset(self):
        """Test race condition: Order died externally, trailing logic should reset state"""
        symbol = "TEST_PE"
        strategy.POSITIONS_STATE[symbol] = {
            "symbol": symbol, "qty": -50, "avg_price": 100.0,
            "exchange": "NFO", "product": "MIS", "state": "TRAILING",
            "margin": 100000.0, "active_oid": "DEAD_OID",
            "last_trigger": 89.0, "lowest_ask": 80.0
        }

        # Mock status as COMPLETE (Dead)
        strategy.client.orderstatus.return_value = {'status': 'success', 'data': {'order_status': 'COMPLETE'}}

        # Trigger trail update (Ask 79.5 < 80.0)
        msg = {"data": {"symbol": symbol, "depth": {"buy": [], "sell": [{"price": 79.5}]}}}
        strategy.on_market_data(msg)

        # Should verify status
        strategy.client.orderstatus.assert_called_with(orderid="DEAD_OID")

        # Should NOT modify
        strategy.client.modifyorder.assert_not_called()

        # Should Reset State
        self.assertEqual(strategy.POSITIONS_STATE[symbol]['state'], 'TRACKING')
        self.assertIsNone(strategy.POSITIONS_STATE[symbol]['active_oid'])

if __name__ == '__main__':
    unittest.main()
