# distutils: language=c++
# distutils: sources=hummingbot/core/cpp/OrderBookEntry.cpp

import logging
import numpy as np
from decimal import Decimal
from typing import Dict

from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book_row import OrderBookRow

_rraot_logger = None
s_empty_diff = np.ndarray(shape=(0, 4), dtype="float64")

SwitcheoOrderBookTrackingDictionary = Dict[Decimal, Dict[str, Dict[str, any]]]


cdef class SwitcheoActiveOrderTracker:
    def __init__(self,
                 active_asks: SwitcheoOrderBookTrackingDictionary = None,
                 active_bids: SwitcheoOrderBookTrackingDictionary = None):
        super().__init__()
        self._active_asks = active_asks or {}
        self._active_bids = active_bids or {}

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _rraot_logger
        if _rraot_logger is None:
            _rraot_logger = logging.getLogger(__name__)
        return _rraot_logger

    @property
    def active_asks(self) -> SwitcheoOrderBookTrackingDictionary:
        return self._active_asks

    @property
    def active_bids(self) -> SwitcheoOrderBookTrackingDictionary:
        return self._active_bids

    def volume_for_ask_price(self, price) -> float:
        return sum([float(msg["amount"]) for msg in self._active_asks[price].values()])

    def volume_for_bid_price(self, price) -> float:
        return sum([float(msg["amount"]) for msg in self._active_bids[price].values()])

    cdef tuple c_convert_diff_message_to_np_arrays(self, object message):
        pass

    cdef tuple c_convert_snapshot_message_to_np_arrays(self, object message):
        cdef:
            object price
            str amount

        # Refresh all order tracking.
        self._active_bids.clear()
        self._active_asks.clear()
        for snapshot_orders, active_orders in [(message.content["buys"], self._active_bids),
                                               (message.content["sells"], self._active_asks)]:
            for order in snapshot_orders:
                price = Decimal(order["price"])
                amount = order["amount"]
                active_orders[price] = amount

        # Return the sorted snapshot tables.
        cdef:
            np.ndarray[np.float64_t, ndim=2] bids = np.array(
                [[message.timestamp,
                  float(price),
                  sum([float(amount)
                       for amount in self._active_bids[price]]),
                  message.update_id]
                 for price in sorted(self._active_bids.keys(), reverse=True)], dtype="float64", ndmin=2)
            np.ndarray[np.float64_t, ndim=2] asks = np.array(
                [[message.timestamp,
                  float(price),
                  sum([float(amount)
                       for amount in self._active_asks[price]]),
                  message.update_id]
                 for price in sorted(self._active_asks.keys(), reverse=True)], dtype="float64", ndmin=2)

        # If there're no rows, the shape would become (1, 0) and not (0, 4).
        # Reshape to fix that.
        if bids.shape[1] != 4:
            bids = bids.reshape((0, 4))
        if asks.shape[1] != 4:
            asks = asks.reshape((0, 4))

        return bids, asks

    cdef np.ndarray[np.float64_t, ndim=1] c_convert_trade_message_to_np_array(self, object message):
        cdef:
            str order_id = message.content["event"]["order"]["orderHash"]
            object price = Decimal(message.content["event"]["order"]["price"])
            double trade_type_value = 1.0 if message.content["event"]["type"] == "ASK" else 2.0
            double filled_base_amount = Decimal(message.content["event"]["filledBaseTokenAmount"])

        return np.array([message.timestamp, trade_type_value, float(price), float(filled_base_amount)],
                        dtype="float64")

    def convert_diff_message_to_order_book_row(self, message):
        pass

    def convert_snapshot_message_to_order_book_row(self, message):
        np_bids, np_asks = self.c_convert_snapshot_message_to_np_arrays(message)
        bids_row = [OrderBookRow(price, qty, update_id) for ts, price, qty, update_id in np_bids]
        asks_row = [OrderBookRow(price, qty, update_id) for ts, price, qty, update_id in np_asks]
        return bids_row, asks_row
