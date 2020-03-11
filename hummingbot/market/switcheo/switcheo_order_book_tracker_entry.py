from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.market.switcheo.switcheo_active_order_tracker import SwitcheoActiveOrderTracker


class SwitcheoOrderBookTrackerEntry(OrderBookTrackerEntry):
    def __init__(self, symbol: str, timestamp: float, order_book: OrderBook,
                 active_order_tracker: SwitcheoActiveOrderTracker):

        self._active_order_tracker = active_order_tracker
        super(SwitcheoOrderBookTrackerEntry, self).__init__(symbol, timestamp, order_book)

    def __repr__(self) -> str:
        return f"SwitcheoOrderBookTrackerEntry(symbol='{self._symbol}', timestamp='{self._timestamp}', " \
            f"order_book='{self._order_book}')"

    @property
    def active_order_tracker(self) -> SwitcheoActiveOrderTracker:
        return self._active_order_tracker
