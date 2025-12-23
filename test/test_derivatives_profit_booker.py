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
        strategy.client.cancelallorder = MagicMock(return_value={'status': 'success'})
        strategy.client.get_orderbook = MagicMock(return_value={'status': 'success', 'data': []})
        strategy.client.orderstatus = MagicMock(return_value={'status': 'success', 'data': {'order_status': 'OPEN'}})
        # Mock openposition for sync logic tests if any
        strategy.client.openposition = MagicMock(return_value={'status': 'success', 'data': []})

    @patch('strategies.derivatives_profit_booker.requests.get')
    def test_fetch_position_book_rest(self, mock_get):
        """Test REST Position Fetch"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "success",
            "data": [{"symbol": "TEST", "netqty": 50}]
        }
        mock_get.return_value = mock_response

        positions = strategy.fetch_position_book_rest()
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]['symbol'], "TEST")

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

    @patch('strategies.derivatives_profit_booker.requests.get')
    def test_long_trigger(self, mock_get):
        """Test Long Position Target Trigger via Callback"""
        symbol = "TEST_CE"

        # Mock Orderbook for REST call
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "success",
            "data": [
                {"symbol": symbol, "orderid": "EXIT_OID", "order_status": "OPEN"}
            ]
        }
        mock_get.return_value = mock_response

        strategy.POSITIONS_STATE[symbol] = {
            "symbol": symbol, "qty": 100, "avg_price": 100.0,
            "exchange": "NFO", "product": "MIS", "state": "TRACKING",
            "active_oid": None
        }

        # Far Target 50% -> 150.0

        msg_high = {"data": {"symbol": symbol, "depth": {"buy": [{"price": 151.0}], "sell": []}}}
        strategy.on_market_data(msg_high)

        strategy.client.placeorder.assert_called()

        # Verify REST-based cancel logic
        strategy.client.cancelorder.assert_called_with(orderid="EXIT_OID", strategy=strategy.STRATEGY_NAME)

    @patch('strategies.derivatives_profit_booker.requests.get')
    def test_short_trigger_and_trail(self, mock_get):
        """Test Short Logic: Trigger -> SL Placement -> Trailing"""
        # Mock Orderbook (Empty)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "success", "data": []}
        mock_get.return_value = mock_response

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
        strategy.client.orderstatus.assert_called_with(orderid="DEAD_OID", strategy=strategy.STRATEGY_NAME)

        # Should NOT modify
        strategy.client.modifyorder.assert_not_called()

        # Should Reset State
        self.assertEqual(strategy.POSITIONS_STATE[symbol]['state'], 'TRACKING')
        self.assertIsNone(strategy.POSITIONS_STATE[symbol]['active_oid'])

    @patch('strategies.derivatives_profit_booker.requests.get')
    def test_future_cancels_manual_order(self, mock_get):
        """Test Future logic cancels manual/strategy orders"""
        symbol = "TEST26FEB24FUT"

        # Mock Orderbook with a Manual Order (no strategy tag needed to find it via REST)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "success",
            "data": [
                {"symbol": symbol, "orderid": "MANUAL_OID", "order_status": "OPEN"}
            ]
        }
        mock_get.return_value = mock_response

        strategy.POSITIONS_STATE[symbol] = {
            'symbol': symbol, 'exchange': 'NFO', 'product': 'MIS',
            'qty': 100, 'avg_price': 100.0, 'margin': 100000.0,
            'active_oid': None, 'state': 'TRACKING', 'is_future': True
        }

        # Future Target 10% of Margin 100k = 10,000.
        # Price 250 -> Profit (250 - 100) * 100 = 15,000 > 10,000.

        msg = {"data": {"symbol": symbol, "depth": {"buy": [{"price": 250.0}], "sell": []}}}

        strategy.on_market_data(msg)

        # Assert Cancel called for specific OID (REST Logic)
        strategy.client.cancelorder.assert_called_with(orderid="MANUAL_OID", strategy=strategy.STRATEGY_NAME)

        # Assert Place called
        strategy.client.placeorder.assert_called()

if __name__ == '__main__':
    unittest.main()
