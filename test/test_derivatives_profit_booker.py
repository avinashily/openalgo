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

import strategies.derivatives_profit_booker as strategy

class TestSDKStrategy(unittest.TestCase):
    def setUp(self):
        # Reset State
        strategy.POSITIONS_STATE.clear()
        strategy.SUBSCRIBED_SYMBOLS.clear()

        # Mock Client methods
        strategy.client.placeorder = MagicMock(return_value={
            'status': 'success', 'orderid': '123', 'total_margin_required': 100000.0
        })
        strategy.client.modifyorder = MagicMock(return_value={'status': 'success'})
        strategy.client.cancelorder = MagicMock(return_value={'status': 'success'})
        strategy.client.get_orderbook = MagicMock(return_value={'status': 'success', 'data': []})
        strategy.client.orderstatus = MagicMock(return_value={'status': 'success', 'data': {'order_status': 'OPEN'}})

    def test_reconcile_new_short(self):
        """Test reconciling a new short position fetches margin (Sync)"""
        pos = {
            'symbol': 'NIFTYSHORT',
            'netqty': -50,
            'sellavg': 100.0,
            'exchange': 'NFO',
            'product': 'MIS'
        }

        strategy.reconcile_position_state(pos)

        self.assertIn('NIFTYSHORT', strategy.POSITIONS_STATE)
        state = strategy.POSITIONS_STATE['NIFTYSHORT']
        self.assertEqual(state['qty'], -50)
        self.assertEqual(state['margin'], 100000.0)
        strategy.client.placeorder.assert_called()

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

        strategy.client.placeorder.assert_called()
        # Verify args if needed, but existence implies trigger

    def test_short_trigger_and_trail(self):
        """Test Short Logic: Trigger -> SL Placement -> Trailing"""
        symbol = "TEST_PE"
        strategy.POSITIONS_STATE[symbol] = {
            "symbol": symbol, "qty": -50, "avg_price": 100.0,
            "exchange": "NFO", "product": "MIS", "state": "TRACKING",
            "margin": 100000.0, "active_oid": None
        }

        # Target 0.65% -> 650. Trigger Price <= 87.0

        # 1. Trigger
        msg_trigger = {"data": {"symbol": symbol, "depth": {"buy": [], "sell": [{"price": 80.0}]}}}
        strategy.on_market_data(msg_trigger)

        strategy.client.placeorder.assert_called()

        # 2. Trailing Update
        strategy.POSITIONS_STATE[symbol]['state'] = 'TRAILING'
        strategy.POSITIONS_STATE[symbol]['active_oid'] = '999'
        strategy.POSITIONS_STATE[symbol]['last_trigger'] = 89.0
        strategy.POSITIONS_STATE[symbol]['lowest_ask'] = 80.0

        msg_trail = {"data": {"symbol": symbol, "depth": {"buy": [], "sell": [{"price": 79.5}]}}}
        strategy.on_market_data(msg_trail)

        strategy.client.modifyorder.assert_called_once()

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

    def test_future_cancels_manual_order(self):
        """Test Future logic cancels manual order"""
        symbol = "TEST26FEB24FUT"
        strategy.POSITIONS_STATE[symbol] = {
            'symbol': symbol, 'exchange': 'NFO', 'product': 'MIS',
            'qty': 100, 'avg_price': 100.0, 'margin': 100000.0,
            'active_oid': None, 'state': 'TRACKING', 'is_future': True
        }

        # Mock existing manual order in Broker Response
        strategy.client.get_orderbook.return_value = {
            'status': 'success',
            'data': [{'orderid': 'MANUAL_OID', 'symbol': symbol, 'status': 'OPEN'}]
        }

        # Future Target 3% of Margin 100k = 3000.
        # Profit = (131 - 100) * 100 = 3100. Trigger!
        msg = {"data": {"symbol": symbol, "depth": {"buy": [{"price": 131.0}], "sell": []}}}

        strategy.on_market_data(msg)

        # Assert Cancel called for MANUAL_OID
        strategy.client.cancelorder.assert_called_with(orderid="MANUAL_OID")

        # Assert Place called
        strategy.client.placeorder.assert_called()

if __name__ == '__main__':
    unittest.main()
