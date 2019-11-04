#!/usr/bin/env python

import asyncio
import aiohttp
import logging
import pandas as pd
from typing import (
    AsyncIterable,
    Dict,
    List,
    Optional,
)
import re
import time
import ujson
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.market.switcheo.switcheo_order_book import SwitcheoOrderBook
from hummingbot.market.switcheo.switcheo_active_order_tracker import SwitcheoActiveOrderTracker
from hummingbot.core.utils import async_ttl_cache
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry, SwitcheoOrderBookTrackerEntry
from hummingbot.core.data_type.order_book_message import OrderBookMessage, SwitcheoOrderBookMessage
# from hummingbot.core.utils.exchange_rate_conversion import ExchangeRateConversion

TRADING_PAIR_FILTER = re.compile(r"(ETH|DAI|USDT|WBTC|NEO|EOS)$")

REST_BASE_URL = "https://api.switcheo.network/v2"
TOKENS_URL = f"{REST_BASE_URL}/exchange/tokens"
MARKETS_URL = f"{REST_BASE_URL}/exchange/pairs"
TICKER_PRICE_CHANGE_URL = f"{REST_BASE_URL}/tickers/last_24_hours"
WS_URL = "wss://ws.switcheo.io/"


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

    def __init__(self, symbols: Optional[List[str]] = None):
        print("Switcheo API Order Book Data Source")
        super().__init__()
        self._symbols: Optional[List[str]] = symbols
        print(self._symbols)

    @classmethod
    def http_client(cls) -> aiohttp.ClientSession:
        print("http client")
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
        print("Switcheo get all token info")
        print(TOKENS_URL)
        client: aiohttp.ClientSession = cls.http_client()
        async with client.get(TOKENS_URL) as response:
            response: aiohttp.ClientResponse = response
            # print(response)
            if response.status != 200:
                raise IOError(f"Error fetching token info. HTTP status is {response.status}.")
            data = await response.json()
            # print(data)
            return {d["address"]: d for d in data}

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        """
        Returned data frame should have symbol as index and include usd volume, baseAsset and quoteAsset
        """
        print("get active exchange markets")
        client: aiohttp.ClientSession = cls.http_client()
        async with client.get(f"{MARKETS_URL}?show_details=1") as response:
            response: aiohttp.ClientResponse = response
            # print(response)
            if response.status != 200:
                raise IOError(f"Error fetching active Switcheo markets. HTTP status is {response.status}.")
            data = await response.json()
            print("Active Exchange Markets JSON")
            # print(data)
            data: List[Dict[str, any]] = [
                {**item, **{"baseAsset": item["baseAssetSymbol"], "quoteAsset": item["quoteAssetSymbol"]}}
                for item in data
            ]
            print("Active Echange Markets after Response with JSON manipulation")
            # print(data)
            # active_markets: pd.DataFrame = pd.DataFrame.from_records(data=data, index="name")
            print("Active Markets")
            # print(active_markets)

        async with client.get(f"{TICKER_PRICE_CHANGE_URL}") as response:
            print("Ticker Price Change URL")
            response: aiohttp.ClientResponse = response

            if response.status != 200:
                raise IOError(f"Error fetching active Switcheo markets. HTTP status is {response.status}.")
            data = await response.json()
            # print(data)

            all_markets: pd.DataFrame = pd.DataFrame.from_records(data=data, index="pair")
            print("All Markets")
            # print(all_markets)

            wbtc_price: float = float(all_markets.loc["WBTC_DAI"].close)
            print("WBTC DAI")
            print(wbtc_price)
            eth_price: float = float(all_markets.loc["ETH_DAI"].close)
            neo_price: float = float(all_markets.loc["NEO_DAI"].close)
            eos_price: float = float(all_markets.loc["EOS_CUSD"].close)
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
                    volume * neo_price if symbol.endswith("NEO") else
                    volume * eos_price if symbol.endswith("EOS") else
                    volume
                )
                for symbol, volume in zip(all_markets.index,
                                          all_markets.volume.astype("float"))]

            all_markets.loc[:, "USDVolume"] = usd_volume
            all_markets.loc[:, "volume"] = all_markets.volume
            # print(all_markets)
            return all_markets.sort_values("USDVolume", ascending=False)

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, any]:
        print(f"Get Snapshot: {trading_pair}")
        async with client.get(f"{REST_BASE_URL}/offers/book?pair={trading_pair}") as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                print("Incorrect Order Book Snapshot URL!")
                raise IOError(f"Error fetching Switcheo market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")
            return await response.json()

    async def get_trading_pairs(self) -> List[str]:
        print("Get Trading Pairs")
        print(self._symbols)
        # self._symbols = []
        if not self._symbols:
            try:
                print("Inside Try")
                active_markets: pd.DataFrame = await self.get_active_exchange_markets()
                print("Active Markets")
                print(active_markets)
                self._symbols = active_markets.index.tolist()
            except Exception:
                self._symbols = []
                print("Error")
                self.logger().network(
                    f"Error getting active exchange information.",
                    exc_info=True,
                    app_warning_msg=f"Error getting active exchange information. Check network connection."
                )
        # print(self._symbols)
        return self._symbols

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        # Get the currently active markets
        print("Get Tracking Pairs")
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.get_trading_pairs()
            print(trading_pairs)
            retval: Dict[str, OrderBookTrackerEntry] = {}

            number_of_pairs: int = len(trading_pairs)
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    print("Inside get Tracking Pairs to Get Snapshot")
                    snapshot: Dict[str, any] = await self.get_snapshot(client, trading_pair)
                    print(snapshot)
                    snapshot_timestamp: float = time.time()
                    snapshot_msg: SwitcheoOrderBookMessage = SwitcheoOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        snapshot_timestamp,
                        metadata={"symbol": trading_pair}
                    )
                    print("Snapshot Message")
                    print(snapshot_msg)

                    switcheo_order_book: OrderBook = self.order_book_create_function()
                    print("Switcheo Order Book")
                    print(switcheo_order_book)
                    switcheo_active_order_tracker: SwitcheoActiveOrderTracker = SwitcheoActiveOrderTracker()
                    print("Switcheo Active Order Tracker")
                    print(switcheo_active_order_tracker)
                    bids, asks = switcheo_active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)
                    print("Bids")
                    print(bids)
                    print("Asks")
                    print(asks)
                    switcheo_order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)
                    print("Switcheo Order Book Apply Snapshot")
                    print(switcheo_order_book)

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

    async def _inner_messages(self,
                              ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        print("Inner Messages")
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        # Trade messages are received from the order book web socket
        print("Listen for Trades")
        pass

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        print("Listen for order book diffs")
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                async with websockets.connect(WS_URL) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    for trading_pair in trading_pairs:
                        request: Dict[str, str] = {
                            "type": "SUBSCRIBE",
                            "topic": "BOOK",
                            "market": trading_pair
                        }
                        await ws.send(ujson.dumps(request))
                    async for raw_msg in self._inner_messages(ws):
                        msg = ujson.loads(raw_msg)
                        # Valid Diff messages from Switcheo have action key
                        if "action" in msg:
                            diff_msg: SwitcheoOrderBookMessage = SwitcheoOrderBook.diff_message_from_exchange(
                                msg, time.time())
                            output.put_nowait(diff_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        print("Listen for order book snapshots")
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                client: aiohttp.ClientSession = self.http_client()
                for trading_pair in trading_pairs:
                    try:
                        snapshot: Dict[str, any] = await self.get_snapshot(client, trading_pair)
                        snapshot_timestamp: float = time.time()
                        snapshot_msg: OrderBookMessage = SwitcheoOrderBook.snapshot_message_from_exchange(
                            snapshot,
                            snapshot_timestamp,
                            metadata={"symbol": trading_pair}
                        )
                        output.put_nowait(snapshot_msg)
                        self.logger().debug(f"Saved order book snapshot for {trading_pair}")

                        await asyncio.sleep(5.0)

                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.logger().error("Unexpected error.", exc_info=True)
                        await asyncio.sleep(5.0)
                this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                delta: float = next_hour.timestamp() - time.time()
                await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)
