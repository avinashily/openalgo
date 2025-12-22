import unittest
from unittest.mock import MagicMock, patch, AsyncMock, call
import os
import sys
import json
import asyncio
from datetime import datetime
import pytz
import functools

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock Env
os.environ['OPENALGO_APIKEY'] = 'test_key'

import strategies.option_profit_booker as strategy

class TestAsyncOptionStrategy(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Reset State
        strategy.POSITIONS_STATE.clear()
        strategy.DEPTH_CACHE.clear()
        strategy.OPEN_ORDERS_CACHE.clear()

        # Mock API
        strategy.api.place_order = MagicMock(return_value={
            'status': 'success', 'orderid': '123', 'total_margin_required': 100000.0
        })
        strategy.api.cancel_order = MagicMock(return_value={'status': 'success'})
        strategy.api.modify_order = MagicMock(return_value={'status': 'success'})
        strategy.api.orderstatus = MagicMock(return_value={'status': 'success', 'data': {'order_status': 'OPEN'}})

    def mock_run_in_executor(self, executor, func, *args, **kwargs):
        if kwargs:
            raise TypeError("run_in_executor does not accept keyword arguments")
        return func(*args)

    async def test_reconcile_new_short(self):
        """Test reconciling a new short position fetches margin"""
        pos = {
            'symbol': 'NIFTYSHORT',
            'netqty': -50,
            'sellavg': 100.0,
            'exchange': 'NFO',
            'product': 'MIS'
        }

        with patch('asyncio.get_running_loop') as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=self.mock_run_in_executor)

            await strategy.reconcile_position_state(pos)

            self.assertIn('NIFTYSHORT', strategy.POSITIONS_STATE)
            state = strategy.POSITIONS_STATE['NIFTYSHORT']
            self.assertEqual(state['qty'], -50)
            self.assertEqual(state['margin'], 100000.0)
            strategy.api.place_order.assert_called()

    async def test_short_trigger_logic(self):
        """Test Short Logic Trigger"""
        symbol = 'TEST_PE'
        strategy.POSITIONS_STATE[symbol] = {
            'symbol': symbol, 'exchange': 'NFO', 'product': 'MIS',
            'qty': -50, 'avg_price': 100.0, 'margin': 100000.0,
            'active_oid': None, 'last_trigger': None, 'state': 'TRACKING'
        }

        depth_no = {'ask': 90.0, 'bid': 89.0}
        strategy.DEPTH_CACHE[symbol] = depth_no

        with patch('asyncio.get_running_loop') as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=self.mock_run_in_executor)

            await strategy.handle_short(strategy.POSITIONS_STATE[symbol], depth_no)
            # No trigger

            # Trigger Case
            depth_yes = {'ask': 80.0, 'bid': 79.0}
            strategy.DEPTH_CACHE[symbol] = depth_yes

            await strategy.handle_short(strategy.POSITIONS_STATE[symbol], depth_yes)

            strategy.api.place_order.assert_called()
            # We can verify args if needed, but existence checks flow

    async def test_trailing_update(self):
        """Test Trailing Update Logic"""
        symbol = 'TEST_PE'
        strategy.POSITIONS_STATE[symbol] = {
            'symbol': symbol, 'exchange': 'NFO', 'product': 'MIS',
            'qty': -50, 'avg_price': 100.0, 'margin': 100000.0,
            'active_oid': '999', 'last_trigger': 89.0, 'lowest_ask': 87.0,
            'state': 'TRAILING'
        }

        depth = {'ask': 86.5, 'bid': 86.0}
        strategy.DEPTH_CACHE[symbol] = depth

        with patch('asyncio.get_running_loop') as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=self.mock_run_in_executor)

            await strategy.handle_short(strategy.POSITIONS_STATE[symbol], depth)

            strategy.api.modify_order.assert_called_once()

    async def test_subscription_management(self):
        """Test Subscribe/Unsubscribe logic"""
        mock_ws = AsyncMock()
        subscribed = {}

        # 1. New Position -> Subscribe
        strategy.POSITIONS_STATE['NEW_SYM'] = {'exchange': 'NFO'}

        await strategy._sync_subscriptions_step(mock_ws, subscribed)

        expected_sub = {"action": "subscribe", "symbol": "NEW_SYM", "exchange": "NFO", "mode": 3}
        mock_ws.send.assert_called_with(json.dumps(expected_sub))
        self.assertIn("NEW_SYM", subscribed)
        self.assertEqual(subscribed["NEW_SYM"], "NFO")

        mock_ws.send.reset_mock()

        # 2. Position Closed -> Unsubscribe
        del strategy.POSITIONS_STATE['NEW_SYM']

        await strategy._sync_subscriptions_step(mock_ws, subscribed)

        expected_unsub = {"action": "unsubscribe", "symbol": "NEW_SYM", "exchange": "NFO", "mode": 3}
        mock_ws.send.assert_called_with(json.dumps(expected_unsub))
        self.assertNotIn("NEW_SYM", subscribed)

    async def test_future_cancels_manual_order(self):
        """Test Future logic cancels manual order"""
        symbol = "TESTFUT"
        strategy.POSITIONS_STATE[symbol] = {
            'symbol': symbol, 'exchange': 'NFO', 'product': 'MIS',
            'qty': 100, 'avg_price': 100.0, 'margin': 100000.0,
            'active_oid': None, 'state': 'TRACKING', 'is_future': True
        }
        strategy.OPEN_ORDERS_CACHE["MANUAL_OID"] = {
            "orderid": "MANUAL_OID", "symbol": symbol, "status": "OPEN"
        }

        depth = {'bid': 131.0, 'ask': 132.0} # Profit 3100 > 3000
        strategy.DEPTH_CACHE[symbol] = depth

        with patch('asyncio.get_running_loop') as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=self.mock_run_in_executor)

            # Need to call handle_future. Currently strategy.handle_future exists.
            await strategy.handle_future(strategy.POSITIONS_STATE[symbol], depth)

            # Verify Cancel
            strategy.api.cancel_order.assert_called_with("MANUAL_OID")

            # Verify Place
            strategy.api.place_order.assert_called()

if __name__ == '__main__':
    unittest.main()
