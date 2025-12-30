import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import os
import sys
import asyncio

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock Env
os.environ['OPENALGO_APIKEY'] = 'test_key'

import strategies.derivatives_profit_booker as strategy

class TestAsyncStrategy(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        strategy.POSITIONS_STATE.clear()

        # Mock API methods on the global api instance
        strategy.api.place_order = AsyncMock(return_value={
            'status': 'success', 'orderid': '123', 'total_margin_required': 100000.0, 'data': {'total_margin_required': 100000.0}
        })
        strategy.api.modify_order = AsyncMock(return_value={'status': 'success'})
        strategy.api.cancel_order = AsyncMock(return_value={'status': 'success'})
        strategy.api.get_orders = AsyncMock(return_value=[])
        strategy.api.order_status = AsyncMock(return_value={'status': 'success', 'data': {'order_status': 'OPEN'}})
        strategy.api.get_depth = AsyncMock()
        strategy.api.get_positions = AsyncMock(return_value=[])
        strategy.api.fetch_margin_for_short = AsyncMock(return_value=100000.0)

    async def test_reconcile_new_short(self):
        """Test reconciling a new short position fetches margin and calc targets"""
        pos = {
            'symbol': 'NIFTYSHORT',
            'netqty': -50,
            'sellavg': 100.0,
            'exchange': 'NFO',
            'product': 'MIS'
        }

        await strategy.reconcile_position_state(pos)

        self.assertIn('NIFTYSHORT', strategy.POSITIONS_STATE)
        state = strategy.POSITIONS_STATE['NIFTYSHORT']
        self.assertEqual(state['qty'], -50)
        self.assertEqual(state['margin'], 100000.0)

        # Verify Target Calculation (0.65% of 100k)
        # Target Price = 100 - (650 / 50) = 87.0
        self.assertAlmostEqual(state['target_price'], 87.0)

        strategy.api.fetch_margin_for_short.assert_called()

    async def test_long_trigger(self):
        """Test Long Position Target Trigger using Pre-calc"""
        symbol = "TEST_CE"
        # Manually seed state with pre-calculated target
        # For test simplicity, we simulate what reconcile does
        state = {
            "symbol": symbol, "qty": 100, "avg_price": 100.0,
            "exchange": "NFO", "product": "MIS", "state": "TRACKING",
            "active_oid": None
        }
        # Run update_targets
        strategy.update_targets(state)
        # Verify target (Far Target 50% since no date match)
        self.assertEqual(state['target_price'], 150.0)

        strategy.POSITIONS_STATE[symbol] = state

        # Mock Orderbook for REST call (finding exit orders)
        strategy.api.get_orders.return_value = [
            {"symbol": symbol, "orderid": "EXIT_OID", "order_status": "OPEN"}
        ]

        # Price 151.0 -> Trigger.
        depth_data = {"bids": [{"price": 151.0}], "asks": []}
        await strategy.process_market_data(symbol, depth_data)

        strategy.api.place_order.assert_called()
        strategy.api.cancel_order.assert_called_with("EXIT_OID")

    async def test_short_trigger_and_trail(self):
        """Test Short Logic: Trigger -> SL Placement -> Trailing"""
        symbol = "TEST_PE"
        state = {
            "symbol": symbol, "qty": -50, "avg_price": 100.0,
            "exchange": "NFO", "product": "MIS", "state": "TRACKING",
            "margin": 100000.0, "active_oid": None
        }
        strategy.update_targets(state) # Calc target_profit_amt
        strategy.POSITIONS_STATE[symbol] = state

        # Trigger
        depth_data = {"bids": [], "asks": [{"price": 80.0}]}
        await strategy.process_market_data(symbol, depth_data)

        strategy.api.place_order.assert_called()

        # Trailing Update
        strategy.POSITIONS_STATE[symbol]['state'] = 'TRAILING'
        strategy.POSITIONS_STATE[symbol]['active_oid'] = '999'
        strategy.POSITIONS_STATE[symbol]['last_trigger'] = 89.0
        strategy.POSITIONS_STATE[symbol]['lowest_ask'] = 80.0

        depth_trail = {"bids": [], "asks": [{"price": 79.5}]}
        await strategy.process_market_data(symbol, depth_trail)

        strategy.api.modify_order.assert_called_once()

    async def test_dead_order_reset(self):
        """Test race condition: Order died externally, trailing logic should reset state"""
        symbol = "TEST_PE"
        strategy.POSITIONS_STATE[symbol] = {
            "symbol": symbol, "qty": -50, "avg_price": 100.0,
            "exchange": "NFO", "product": "MIS", "state": "TRAILING",
            "margin": 100000.0, "active_oid": "DEAD_OID",
            "last_trigger": 89.0, "lowest_ask": 80.0
        }
        # Pre-calc not strictly needed for this test as logic bypasses it if state=TRAILING
        # But good to have
        strategy.update_targets(strategy.POSITIONS_STATE[symbol])

        # Mock status as COMPLETE (Dead)
        strategy.api.order_status.return_value = {'status': 'success', 'data': {'order_status': 'COMPLETE'}}

        depth_data = {"bids": [], "asks": [{"price": 79.5}]}
        await strategy.process_market_data(symbol, depth_data)

        strategy.api.order_status.assert_called_with("DEAD_OID")
        strategy.api.modify_order.assert_not_called()
        self.assertEqual(strategy.POSITIONS_STATE[symbol]['state'], 'TRACKING')

    async def test_future_cancels_manual_order(self):
        """Test Future logic cancels manual/strategy orders"""
        symbol = "TEST26FEB24FUT"
        state = {
            'symbol': symbol, 'exchange': 'NFO', 'product': 'MIS',
            'qty': 100, 'avg_price': 100.0, 'margin': 100000.0,
            'active_oid': None, 'state': 'TRACKING', 'is_future': True
        }
        strategy.update_targets(state)
        # Futures Target 10% of 100k = 10,000.
        # Target Price = 100 + (10000 / 100) = 200.0
        self.assertEqual(state['target_price'], 200.0)

        strategy.POSITIONS_STATE[symbol] = state

        strategy.api.get_orders.return_value = [
            {"symbol": symbol, "orderid": "MANUAL_OID", "order_status": "OPEN"}
        ]

        # Profit (250-100)*100 = 15,000 > 10,000.
        depth_data = {"bids": [{"price": 250.0}], "asks": []}
        await strategy.process_market_data(symbol, depth_data)

        strategy.api.cancel_order.assert_called_with("MANUAL_OID")
        strategy.api.place_order.assert_called()

if __name__ == '__main__':
    unittest.main()
