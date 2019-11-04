#!/usr/bin/env python

import ujson
import logging
from typing import (
    Dict,
    List,
    Optional,
)

from sqlalchemy.engine import RowProxy

from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_message import (
    SwitcheoOrderBookMessage,
    OrderBookMessage,
    OrderBookMessageType
)

_rrob_logger = None


cdef class SwitcheoOrderBook(OrderBook):

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _rrob_logger
        if _rrob_logger is None:
            _rrob_logger = logging.getLogger(__name__)
        return _rrob_logger

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        return SwitcheoOrderBookMessage(OrderBookMessageType.SNAPSHOT, msg, timestamp)

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        return SwitcheoOrderBookMessage(OrderBookMessageType.DIFF, msg, timestamp)

    @classmethod
    def snapshot_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
        msg = record.json if type(record.json)==dict else ujson.loads(record.json)
        return SwitcheoOrderBookMessage(OrderBookMessageType.SNAPSHOT, msg,
                                        timestamp=record.timestamp * 1e-3)

    @classmethod
    def diff_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
        return SwitcheoOrderBookMessage(OrderBookMessageType.DIFF, record.json, timestamp=record.timestamp * 1e-3)

    @classmethod
    def trade_receive_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
        return SwitcheoOrderBookMessage(OrderBookMessageType.TRADE, record.json)

    @classmethod
    def from_snapshot(cls, snapshot: OrderBookMessage):
        raise NotImplementedError("Switcheo order book needs to retain individual order data.")

    @classmethod
    def restore_from_snapshot_and_diffs(self, snapshot: OrderBookMessage, diffs: List[OrderBookMessage]):
        raise NotImplementedError("Switcheo order book needs to retain individual order data.")
