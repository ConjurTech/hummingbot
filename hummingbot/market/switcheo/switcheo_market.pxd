from libc.stdint cimport int64_t

from hummingbot.market.market_base cimport MarketBase
from hummingbot.core.data_type.transaction_tracker cimport TransactionTracker
from collections import (
    deque,
    OrderedDict
)
from web3 import Web3
from hummingbot.wallet.ethereum.web3_wallet import Web3Wallet
import aiohttp
from cachetools import TTLCache
from hummingbot.core.utils.async_call_scheduler import AsyncCallScheduler

cdef class SwitcheoMarket(MarketBase):
    cdef:
        object _user_stream_tracker
        object _ev_loop
        object _poll_notifier
        double _last_timestamp
        double _poll_interval
        double _last_pull_timestamp
        double _last_update_balances_timestamp
        double _last_update_order_timestamp
        double _last_update_asset_info_timestamp
        double _last_update_contract_address_timestamp
        dict _in_flight_deposits
        dict _in_flight_orders
        object _in_flight_cancels
        dict _order_not_found_records
        TransactionTracker _tx_tracker
        dict _withdraw_rules
        dict _trading_rules
        dict _trade_fees
        double _last_update_trade_fees_timestamp
        object _data_source_type
        public object _status_polling_task
        public object _user_stream_event_listener_task
        public object _user_stream_tracker_task
        public object _order_tracker_task
        public object _trading_rules_polling_task
        object _async_scheduler
        object _set_server_time_offset_task
        object _order_expiry_queue
        object _order_expiry_set
        object _w3
        object _wallet
        object _shared_client
        object _api_response_records
        object _assets_info
        str _contract_address
        int _last_nonce
    cdef c_start_tracking_order(self,
                                str order_id,
                                str exchange_order_id,
                                str trading_pair,
                                object trade_type,
                                object price,
                                object amount,
                                object order_type)
    cdef c_expire_order(self, str order_id)
    cdef c_check_and_remove_expired_orders(self)
    cdef c_stop_tracking_order(self, str order_id)
