#!/usr/bin/env python

import asyncio
import aiohttp
import logging
import pandas as pd
from typing import (
    Dict,
    List,
    Optional,
)
import re
import time
from socketio import Client as SocketIOClient

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.market.switcheo.switcheo_order_book import SwitcheoOrderBook
from hummingbot.market.switcheo.switcheo_active_order_tracker import SwitcheoActiveOrderTracker
from hummingbot.market.switcheo.switcheo_socketio_namespace import OrderBooksNamespace
from hummingbot.core.utils import async_ttl_cache
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry, SwitcheoOrderBookTrackerEntry
from hummingbot.core.data_type.order_book_message import SwitcheoOrderBookMessage

TRADING_PAIR_FILTER = re.compile(r"(ETH|DAI|USDT|WBTC)$")

REST_BASE_URL = "https://api.switcheo.network/v2"
TOKENS_URL = f"{REST_BASE_URL}/exchange/tokens"
MARKETS_URL = f"{REST_BASE_URL}/exchange/pairs"
TICKER_PRICE_CHANGE_URL = f"{REST_BASE_URL}/tickers/last_24_hours"
WS_URL = "https://ws.switcheo.io"


class SwitcheoAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _rraobds_logger: Optional[HummingbotLogger] = None
    _client: Optional[aiohttp.ClientSession] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._rraobds_logger is None:
            cls._rraobds_logger = logging.getLogger(__name__)
        return cls._rraobds_logger

    def __init__(self,
                 socketio_client: SocketIOClient,
                 symbols: Optional[List[str]] = None):
        super().__init__()
        self._symbols: Optional[List[str]] = symbols
        self._sio: SocketIOClient = socketio_client

    @classmethod
    def http_client(cls) -> aiohttp.ClientSession:
        if cls._client is None:
            if not asyncio.get_event_loop().is_running():
                raise EnvironmentError("Event loop must be running to start HTTP client session.")
            cls._client = aiohttp.ClientSession()
        return cls._client

    @classmethod
    async def get_all_token_info(cls) -> Dict[str, any]:
        """
        Returns all token information
        """
        client: aiohttp.ClientSession = cls.http_client()
        async with client.get(TOKENS_URL) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching token info. HTTP status is {response.status}.")
            data = await response.json()
            return {value["hash"]: value for key, value in data.items()}

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        """
        Returned data frame should have symbol as index and include usd volume, baseAsset and quoteAsset
        """
        client: aiohttp.ClientSession = cls.http_client()
        async with client.get(f"{MARKETS_URL}?show_details=1") as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching active Switcheo markets. HTTP status is {response.status}.")
            data = await response.json()
            data: List[Dict[str, any]] = [
                {**item, **{"baseAsset": item["baseAssetSymbol"], "quoteAsset": item["quoteAssetSymbol"]}}
                for item in data
            ]
            # active_markets: pd.DataFrame = pd.DataFrame.from_records(data=data, index="name")

        async with client.get(f"{TICKER_PRICE_CHANGE_URL}") as response:
            response: aiohttp.ClientResponse = response

            if response.status != 200:
                raise IOError(f"Error fetching active Switcheo markets. HTTP status is {response.status}.")
            data = await response.json()

            all_markets: pd.DataFrame = pd.DataFrame.from_records(data=data, index="pair")

            wbtc_price: float = float(all_markets.loc["WBTC_DAI"].close)
            eth_price: float = float(all_markets.loc["ETH_DAI"].close)
            # neo_price: float = float(all_markets.loc["NEO_DAI"].close)
            # eos_price: float = float(all_markets.loc["EOS_CUSD"].close)
            # usdt_price: float = float(all_markets.loc["USDT_DAI"].close)
            # eth_dai_price: float = float(all_markets.loc["ETH_DAI"]["volume"])
            # print(eth_dai_price)
            # dai_usd_price: float = ExchangeRateConversion.get_instance().adjust_token_rate("DAI", eth_dai_price)
            # print(dai_usd_price)
            # usd_volume: List[float] = []
            # quote_volume: List[float] = []
            # for row in all_markets.itertuples():
            #     product_name: str = row.Index
            #     base_volume: float = float(row.stats["volume"])
            #     quote_volume.append(base_volume)
            #     if product_name.endswith("WETH"):
            #         usd_volume.append(dai_usd_price * base_volume)
            #     else:
            #         usd_volume.append(base_volume)
            usd_volume: float = [
                (
                    volume * wbtc_price if symbol.endswith("WBTC") else
                    volume * eth_price if symbol.endswith("ETH") else
                    # volume * neo_price if symbol.endswith("NEO") else
                    # volume * eos_price if symbol.endswith("EOS") else
                    volume
                )
                for symbol, volume in zip(all_markets.index,
                                          all_markets.volume.astype("float"))]

            all_markets.loc[:, "USDVolume"] = usd_volume
            all_markets.loc[:, "volume"] = all_markets.volume
            return all_markets.sort_values("USDVolume", ascending=False)

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, any]:
        async with client.get(f"{REST_BASE_URL}/offers/book?pair={trading_pair}") as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching Switcheo market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")
            return await response.json()

    async def get_trading_pairs(self) -> List[str]:
        self.logger().debug(f"Get Trading Pairs: {self._symbols}")
        if not self._symbols:
            try:
                active_markets: pd.DataFrame = await self.get_active_exchange_markets()
                self.logger().debug(f"Active Markets: {active_markets}")
                self._symbols = active_markets.index.tolist()
            except Exception:
                self._symbols = []
                self.logger().network(
                    f"Error getting active exchange information.",
                    exc_info=True,
                    app_warning_msg=f"Error getting active exchange information. Check network connection."
                )
        return self._symbols

    async def get_tracking_pairs(self, snapshot_msg_stream, diff_msg_stream) -> Dict[str, OrderBookTrackerEntry]:
        # Get the currently active markets and start the SocketIO connection
        trading_pairs: List[str] = await self.get_trading_pairs()
        self.logger().debug(f"Trading Pairs: {trading_pairs}")
        retval: Dict[str, OrderBookTrackerEntry] = {}

        number_of_pairs: int = len(trading_pairs)
        for index, trading_pair in enumerate(trading_pairs):
            try:
                obn = OrderBooksNamespace(order_book_snapshot_stream=snapshot_msg_stream, order_book_diff_stream=diff_msg_stream)
                self._sio.register_namespace(obn)
                self._sio.connect(url=WS_URL)
                for trading_pair in trading_pairs:
                    request: Dict[str, str] = {
                        "contractHash": "0x7ee7ca6e75de79e618e88bdf80d0b1db136b22d0",
                        "pair": trading_pair
                    }
                self._sio.emit(event="join", data=request, namespace='/v2/books')
                self._sio.sleep(2)
                snapshot: Dict[str, any] = self._sio.namespace_handlers['/v2/books'].order_book[trading_pair]['book']
                snapshot_timestamp: float = time.time()
                snapshot_msg: SwitcheoOrderBookMessage = SwitcheoOrderBook.snapshot_message_from_exchange(
                    snapshot,
                    snapshot_timestamp,
                    metadata={"symbol": trading_pair}
                )

                switcheo_order_book: OrderBook = self.order_book_create_function()
                switcheo_active_order_tracker: SwitcheoActiveOrderTracker = SwitcheoActiveOrderTracker()
                bids, asks = switcheo_active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)
                switcheo_order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)

                retval[trading_pair] = SwitcheoOrderBookTrackerEntry(
                    trading_pair,
                    snapshot_timestamp,
                    switcheo_order_book,
                    switcheo_active_order_tracker
                )
                self.logger().info(f"Initialized order book for {trading_pair}. "
                                   f"{index+1}/{number_of_pairs} completed.")

                await asyncio.sleep(0.9)

            except Exception:
                self.logger().error(f"Error getting snapshot for {trading_pair}. ", exc_info=True)
                await asyncio.sleep(5.0)
        return retval

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        # Trade messages are received from the order book SocketIO web socket
        pass

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        # Order Book messages are received from the order book SocketIO web socket
        pass

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        # Order Book messages are received from the order book SocketIO web socket
        pass
