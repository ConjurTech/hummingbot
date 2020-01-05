import logging
from operator import itemgetter
from socketio import ClientNamespace as SocketIOClientNamespace
from switcheo.utils import stringify_message, sha1_hash_digest
import threading
import time
from typing import Optional

from hummingbot.core.data_type.order_book_message import SwitcheoOrderBookMessage
from hummingbot.logger import HummingbotLogger
from hummingbot.market.switcheo.switcheo_order_book import SwitcheoOrderBook


class OrderBooksNamespace(SocketIOClientNamespace):

    _rraobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._rraobds_logger is None:
            cls._rraobds_logger = logging.getLogger(__name__)
        return cls._rraobds_logger

    def __init__(self, order_book_snapshot_stream, order_book_diff_stream):
        self.lock = threading.Lock()
        self.namespace = '/v2/books'
        self.order_book = {}
        self.order_book_snapshot_stream = order_book_snapshot_stream
        self.order_book_diff_stream = order_book_diff_stream
        SocketIOClientNamespace.__init__(self, namespace=self.namespace)

    def on_all(self, data):
        symbol = data["room"]["pair"]
        self.lock.acquire()
        self.order_book[symbol] = data
        self.lock.release()
        digest_hash = data["digest"]
        book = data["book"]
        book_digest_hash = sha1_hash_digest(stringify_message(book))
        if digest_hash != book_digest_hash:
            self.emit(event="leave", data=data["room"], namespace='/v2/books')
            self.emit(event="join", data=data["room"], namespace='/v2/books')
        else:
            snapshot_timestamp: float = time.time()
            snapshot_msg: SwitcheoOrderBookMessage = SwitcheoOrderBook.snapshot_message_from_exchange(
                msg=book,
                timestamp=snapshot_timestamp,
                metadata={"symbol": symbol}
            )
            self.order_book_snapshot_stream.put_nowait(snapshot_msg)

    def on_updates(self, data):
        update_digest = data["digest"]
        update_pair = data["room"]["pair"]
        update_events = data["events"]
        if "symbol" in self.order_book[update_pair]["book"]:
            del self.order_book[update_pair]["book"]["symbol"]
        self.lock.acquire()
        for event in update_events:
            price_match = False
            buy_event = False
            sell_event = False
            event_iteration = 0
            if event["side"] == "buy":
                event_side = "buys"
                buy_event = True
            elif event["side"] == "sell":
                event_side = "sells"
                sell_event = True
            event_price = event["price"]
            event_change = event["delta"]
            for side in self.order_book[update_pair]["book"][event_side]:
                if side["price"] == event_price:
                    price_match = True
                    updated_amount = int(side["amount"]) + int(event_change)
                    if updated_amount == 0:
                        self.order_book[update_pair]["book"][event_side].remove(side)
                    else:
                        updated_book = {}
                        updated_book["amount"] = str(updated_amount)
                        updated_book["price"] = str(event_price)
                        self.order_book[update_pair]["book"][event_side][event_iteration] = updated_book
                    break
                event_iteration += 1
            if not price_match:
                new_book = {}
                new_book["amount"] = event_change
                new_book["price"] = event_price
                self.order_book[update_pair]["book"][event_side].append(new_book)
        if buy_event and sell_event:
            self.order_book[update_pair]["book"]["buys"] = sorted(
                self.order_book[update_pair]["book"]["buys"], key=itemgetter("price"), reverse=True)
            self.order_book[update_pair]["book"]["sells"] = sorted(
                self.order_book[update_pair]["book"]["sells"], key=itemgetter("price"), reverse=True)
        elif buy_event:
            self.order_book[update_pair]["book"]["buys"] = sorted(
                self.order_book[update_pair]["book"]["buys"], key=itemgetter("price"), reverse=True)
        elif sell_event:
            self.order_book[update_pair]["book"]["sells"] = sorted(
                self.order_book[update_pair]["book"]["sells"], key=itemgetter("price"), reverse=True)
        book = self.order_book[update_pair]["book"]
        self.lock.release()
        book_digest_hash = sha1_hash_digest(stringify_message(book))
        if update_digest != book_digest_hash:
            self.emit(event="leave", data=data["room"], namespace='/v2/books')
            self.emit(event="join", data=data["room"], namespace='/v2/books')
        else:
            update_book_timestamp: float = time.time()
            update_book_msg: SwitcheoOrderBookMessage = SwitcheoOrderBook.snapshot_message_from_exchange(
                msg=book,
                timestamp=update_book_timestamp,
                metadata={"symbol": update_pair}
            )
            self.order_book_snapshot_stream.put_nowait(update_book_msg)
