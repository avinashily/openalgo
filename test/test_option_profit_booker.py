import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import os
import sys
import json
import asyncio
from datetime import datetime
import pytz

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
        strategy.api.place_order = MagicMock(return_value={'status': 'success', 'orderid': '123'})
        strategy.api.cancel_order = MagicMock(return_value={'status': 'success'})
        strategy.api.modify_order = MagicMock(return_value={'status': 'success'})
        strategy.api.fetch_margin_for_short = MagicMock(return_value=100000.0)

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
            # Mock executor to call the function
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=lambda exec, func, *args, **kwargs: func(*args, **kwargs))

            await strategy.reconcile_position_state(pos)

            self.assertIn('NIFTYSHORT', strategy.POSITIONS_STATE)
            state = strategy.POSITIONS_STATE['NIFTYSHORT']
            self.assertEqual(state['qty'], -50)
            self.assertEqual(state['margin'], 100000.0)
            strategy.api.fetch_margin_for_short.assert_called_once()

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
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=lambda exec, func, *args, **kwargs: func(*args, **kwargs))

            await strategy.handle_short(strategy.POSITIONS_STATE[symbol], depth_no)
            strategy.api.place_order.assert_not_called()

            # Trigger Case: Ask 80.0 (Huge profit to guarantee trigger)
            depth_yes = {'ask': 80.0, 'bid': 79.0}
            strategy.DEPTH_CACHE[symbol] = depth_yes

            await strategy.handle_short(strategy.POSITIONS_STATE[symbol], depth_yes)

            # Verify call
            strategy.api.place_order.assert_called_once()
            args, kwargs = strategy.api.place_order.call_args
            self.assertEqual(kwargs['action'], 'BUY')
            self.assertEqual(kwargs['price_type'], 'SL-M')

            # Lock Calc: Margin 100k * 0.55% = 550.
            # Qty 50. Dist = 11.
            # Trigger = 100 - 11 = 89.0
            self.assertEqual(kwargs['trigger_price'], 89.0)

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
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=lambda exec, func, *args, **kwargs: func(*args, **kwargs))

            await strategy.handle_short(strategy.POSITIONS_STATE[symbol], depth)

            strategy.api.modify_order.assert_called_once()
            _, kwargs = strategy.api.modify_order.call_args
            self.assertEqual(kwargs['trigger_price'], 88.5)

    async def test_reconcile_change_resets_state(self):
        """Test if changing quantity resets the state"""
        symbol = 'TEST_PE'
        strategy.POSITIONS_STATE[symbol] = {
            'symbol': symbol, 'exchange': 'NFO', 'product': 'MIS',
            'qty': -50, 'avg_price': 100.0, 'margin': 100000.0,
            'active_oid': '999', 'state': 'TRAILING'
        }
        strategy.OPEN_ORDERS_CACHE['999'] = {'symbol': symbol, 'status': 'OPEN'}

        new_pos = {
            'symbol': symbol, 'netqty': -25, 'sellavg': 100.0,
            'exchange': 'NFO', 'product': 'MIS'
        }

        with patch('asyncio.get_running_loop') as mock_loop:
             mock_loop.return_value.run_in_executor = AsyncMock(side_effect=lambda exec, func, *args, **kwargs: func(*args, **kwargs))

             await strategy.reconcile_position_state(new_pos)

             strategy.api.cancel_order.assert_called_with('999')
             self.assertEqual(strategy.POSITIONS_STATE[symbol]['qty'], -25)
             self.assertEqual(strategy.POSITIONS_STATE[symbol]['state'], 'TRACKING')

if __name__ == '__main__':
    unittest.main()
