import unittest
from unittest.mock import MagicMock, patch
import os
import sys
import json
import asyncio
from datetime import datetime

# Add project root to path to import the strategy
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set required env vars before importing to avoid sys.exit(1)
os.environ['OPENALGO_APIKEY'] = 'test_key'
from strategies.option_profit_booker import OptionProfitBooker

class TestOptionProfitBooker(unittest.TestCase):
    def setUp(self):
        self.strategy = OptionProfitBooker()
        # Reset defaults for testing
        self.strategy.profit_percentage_near = 4.0
        self.strategy.profit_percentage_far = 10.0
        self.strategy.quantity_threshold = 300
        self.strategy.profit_points_threshold = 200.0

        # New Short defaults (Updated)
        self.strategy.profit_percentage_short = 0.7  # 0.7%
        self.strategy.profit_locked_percentage = 0.55 # 0.55%
        self.strategy.trailing_point = 0.5

    def test_safe_float(self):
        """Test the safe_float helper method"""
        self.assertEqual(self.strategy.safe_float(10.5), 10.5)
        self.assertEqual(self.strategy.safe_float("10.5"), 10.5)
        self.assertEqual(self.strategy.safe_float(None), 0.0)
        self.assertEqual(self.strategy.safe_float("invalid"), 0.0)
        self.assertEqual(self.strategy.safe_float(None, default=1.0), 1.0)

    def test_fetch_margin(self):
        """Test fetch_margin uses placeorder with analyze mode"""
        with patch('requests.post') as mock_post:
            # Mock success response
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "status": "success",
                "total_margin_required": 12345.67
            }
            mock_post.return_value = mock_response

            position = {
                'symbol': 'TESTSYM',
                'exchange': 'NFO',
                'product': 'MIS',
                'quantity': -50
            }

            margin = self.strategy.fetch_margin(position)

            self.assertEqual(margin, 12345.67)

            # Verify call args
            args, kwargs = mock_post.call_args
            payload = kwargs['json']
            self.assertEqual(payload['mode'], 'analyze')
            self.assertEqual(payload['action'], 'SELL') # Should check SELL margin for Short
            self.assertEqual(payload['quantity'], 50) # Abs qty

    def test_process_message_short_position_logic(self):
        """Test profit trigger for Short Position (0.7% Target)"""
        symbol = "NIFTY27FEB2518000PE"
        margin = 100000.0
        entry_price = 100.0
        qty = -50

        # Target Profit = 0.7% of Margin = 700
        # Profit = (Entry - Current) * Abs(Qty)
        # 700 = (100 - X) * 50 => 14 = 100 - X => X = 86.0
        # If Ask <= 86.0, it should trigger.

        with patch('strategies.option_profit_booker.datetime') as mock_date:
            mock_date.now.return_value = datetime(2025, 1, 1)

            self.strategy.tracked_positions[symbol] = {
                "symbol": symbol,
                "sell_avg": entry_price,
                "quantity": qty,
                "exchange": "NFO",
                "margin_used": margin,
                "processing": False,
                "trailing_active": False,
                "product": "MIS"
            }

            self.strategy.start_trailing_sl = MagicMock()

            # 1. Ask = 90 (Profit = (100-90)*50 = 500 < 700) -> No Trigger
            message_no = {
                "type": "market_data", "mode": 3,
                "data": {"symbol": symbol, "depth": {"sell": [{"price": 90}]}}
            }
            asyncio.run(self.strategy.process_message(json.dumps(message_no)))
            self.strategy.start_trailing_sl.assert_not_called()

            # 2. Ask = 86 (Profit = (100-86)*50 = 700 == 700) -> Trigger
            message_yes = {
                "type": "market_data", "mode": 3,
                "data": {"symbol": symbol, "depth": {"sell": [{"price": 86.0}]}}
            }
            asyncio.run(self.strategy.process_message(json.dumps(message_yes)))
            self.strategy.start_trailing_sl.assert_called_once()

    def test_start_trailing_sl_logic(self):
        """Test start_trailing_sl calculates correct trigger"""
        # Lock 0.55%
        # Margin 100k -> Lock Amt 550.
        # Qty 50. Lock Pts = 550/50 = 11.
        # Entry 100.
        # Initial Trigger = Entry - 11 = 89.

        symbol = "TEST_PE"
        position = {
            "symbol": symbol,
            "exchange": "NFO",
            "quantity": -50,
            "product": "MIS"
        }

        self.strategy.place_order = MagicMock(return_value="12345")
        self.strategy.cancel_order = MagicMock()
        self.strategy.get_open_orders = MagicMock(return_value=[])

        # Call start_trailing_sl
        # args: position, current_ask, avg_price, margin
        self.strategy.start_trailing_sl(position, 86.0, 100.0, 100000.0)

        # Verify place_order called with correct trigger
        # We expect trigger_price around 89.0
        args, kwargs = self.strategy.place_order.call_args

        # Verify args: symbol, exchange, quantity, product, transaction, price_type
        self.assertEqual(args[0], symbol)
        self.assertEqual(args[4], "BUY") # Exit Short is Buy
        self.assertEqual(args[5], "SL-M")

        # Verify kwargs: trigger_price, tag, trailing_sl
        self.assertAlmostEqual(kwargs['trigger_price'], 89.0)
        self.assertEqual(kwargs['tag'], "OPB")
        self.assertEqual(kwargs['trailing_sl'], 0.5)

    def test_update_trailing_sl(self):
        """Test synthetic trailing logic"""
        symbol = "NIFTYSHORT"

        self.strategy.tracked_positions[symbol] = {
            "symbol": symbol,
            "sl_order_id": "123",
            "sl_trigger_price": 110.0,
            "lowest_ask": 100.0,
            "modifying": False,
            "quantity": -50,
            "exchange": "NFO",
            "trailing_active": True,
            "margin_used": 100000,
            "sell_avg": 120,
            "processing": False
        }

        self.strategy.update_trailing_sl = MagicMock()

        # Old Lowest Ask = 100.
        # New Ask = 99.0 (Diff 1.0 >= 0.5) -> Update
        message = {
            "type": "market_data", "mode": 3,
            "data": {"symbol": symbol, "depth": {"sell": [{"price": 99.0}]}}
        }

        asyncio.run(self.strategy.process_message(json.dumps(message)))
        self.strategy.update_trailing_sl.assert_called()

if __name__ == '__main__':
    unittest.main()
