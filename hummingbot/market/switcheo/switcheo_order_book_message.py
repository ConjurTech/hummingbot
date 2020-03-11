#!/usr/bin/env python

from typing import (
    Dict,
    List,
    Optional,
)

from hummingbot.core.data_type.order_book_row import OrderBookRow
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage,
    OrderBookMessageType,
)


class SwitcheoOrderBookMessage(OrderBookMessage):
    def __new__(cls, message_type: OrderBookMessageType, content: Dict[str, any], timestamp: Optional[float] = None,
                *args, **kwargs):
        if message_type is OrderBookMessageType.SNAPSHOT and timestamp is None:
            raise ValueError("timestamp must not be None when initializing snapshot messages.")
        elif message_type is OrderBookMessageType.DIFF and timestamp is None:
            raise ValueError("timestamp must not be None when defining diff messages.")
        elif message_type is OrderBookMessageType.TRADE:
            timestamp = content["event"]["timestamp"]
        elif timestamp is None:
            raise ValueError("timestamp field required for this message.")

        return super(SwitcheoOrderBookMessage, cls).__new__(
            cls, message_type, content, timestamp=timestamp, *args, **kwargs)

    @property
    def update_id(self) -> int:
        return int(self.timestamp * 1e3)

    @property
    def trade_id(self) -> int:
        return int(self.timestamp * 1e3)

    @property
    def symbol(self) -> str:
        return self.content["symbol"]

    @property
    def asks(self) -> List[OrderBookRow]:
        raise NotImplementedError("Switcheo order book messages have different semantics.")

    @property
    def bids(self) -> List[OrderBookRow]:
        raise NotImplementedError("Switcheo order book messages have different semantics.")

    @property
    def has_update_id(self) -> bool:
        return True

    @property
    def has_trade_id(self) -> bool:
        return True

    def __eq__(self, other) -> bool:
        return self.type == other.type and self.timestamp == other.timestamp

    def __lt__(self, other) -> bool:
        if self.timestamp != other.timestamp:
            return self.timestamp < other.timestamp
        else:
            """
            If timestamp is the same, the ordering is snapshot < diff < trade
            """
            return self.type.value < other.type.value
