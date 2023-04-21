import configparser
import re
import sys
import time
import traceback
from typing import Dict, List

from clickhouse_driver import Client
from dateutil.parser import parse

from ccapi import (Event, EventHandler, Request, Session, SessionConfigs,
                   SessionOptions, Subscription, SubscriptionList)
from transform import EXCHANGE_MARKET_TO_UNIFIED_SYMBOL


def process_events(client, events: List[Event]):
    try:
        data = []
        for event in events:
            if event.getType() == Event.Type_SUBSCRIPTION_DATA:
                for message in event.getMessageList():
                    exchange, market = message.getCorrelationIdList()[0].split(",")
                    timestamp_start = parse(message.getTimeISO())

                    bid_element, ask_element = message.getElementList()
                    data.append(
                        (
                            timestamp_start,
                            market,
                            exchange,
                            float(bid_element.getValue("BID_PRICE")),
                            float(ask_element.getValue("ASK_PRICE")),
                            float(bid_element.getValue("BID_SIZE")),
                            float(ask_element.getValue("ASK_SIZE")),
                        )
                    )
            else:
                print(f"Received an event:\n{event.toString()}")

        if data:
            client.execute(
                "INSERT INTO quote_ticks (timestamp, symbol, exchange, bid_price, ask_price, bid_size, ask_size) VALUES",
                data,
            )
            print(f"Write {len(data)} lines to ck")
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)


class MarketHandler(EventHandler):
    def __init__(self, filter: str):
        super().__init__()
        self.symbol_map = {}
        self.filter = filter

    def processEvent(self, event: Event, session: Session) -> bool:
        for message in event.getMessageList():
            try:
                exchange = message.getCorrelationIdList()[0]
                if exchange not in self.symbol_map:
                    self.symbol_map[exchange] = {}
                for element in message.getElementList():
                    element_dict = element.getNameValueMap()
                    instrument = element_dict["INSTRUMENT"]
                    transformer = EXCHANGE_MARKET_TO_UNIFIED_SYMBOL[exchange]
                    unified_symbol = transformer(element_dict)
                    if (
                        unified_symbol
                        and re.match(self.filter, unified_symbol.upper())
                        and unified_symbol.upper() not in self.symbol_map[exchange]
                    ):
                        self.symbol_map[exchange][unified_symbol.upper()] = instrument

            except Exception:
                print(traceback.format_exc())
                sys.exit(1)
        return True


def filter_markets(exchanges: List[str], filter: str) -> Dict[str, Dict[str, str]]:
    eventHandler = MarketHandler(filter)
    option = SessionOptions()
    config = SessionConfigs()
    session = Session(option, config, eventHandler)
    for ex in exchanges:
        request = Request(Request.Operation_GET_INSTRUMENTS, ex, "", ex)
        if ex == "okx":
            request.appendParam(
                {
                    "instType": "SPOT",
                }
            )
        session.sendRequest(request)
    time.sleep(5)
    session.stop()
    return eventHandler.symbol_map


def store_clickhouse(
    exchange_markets: Dict[str, Dict[str, str]], collect_interval: int, ck_client
):
    print(f"Total exchanges: {len(exchange_markets)}")
    print(
        f"Total markets: {sum(len(markets) for markets in exchange_markets.values())}"
    )

    option = SessionOptions()
    session_config = SessionConfigs()
    session = Session(option, session_config)

    subscriptionList = SubscriptionList()
    for exchange, markets in exchange_markets.items():
        for unified_symbol, native_symbol in markets.items():
            correlation_id = ",".join([exchange, unified_symbol])
            print(exchange, unified_symbol, native_symbol)
            subscription = Subscription(
                exchange,
                native_symbol,
                "MARKET_DEPTH",
                "MARKET_DEPTH_MAX=1",
                correlation_id,
            )
            if exchange == "bybit-derivatives":
                subscription.setInstrumentType("usdt-contract")
            subscriptionList.append(subscription)

    session.subscribe(subscriptionList)
    try:
        while True:
            time.sleep(collect_interval)
            events = session.getEventQueue().purge()
            process_events(ck_client, events)
    except KeyboardInterrupt:
        session.stop()
        print("Bye")
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)


def main():
    config = configparser.ConfigParser()
    config.read("config.ini")

    general_config = config["general"]
    exchanges = general_config.get("exchanges", "binance-usds-futures").split(",")
    market_filter = general_config.get("market_filter", ".*")
    collect_interval = general_config.getint("collect_interval", 10)

    clickhouse_config = config["clickhouse"]
    host = clickhouse_config.get("host", "localhost")
    port = clickhouse_config.getint("port", 9000)
    user = clickhouse_config.get("user", "default")
    password = clickhouse_config.get("password", "")
    database = clickhouse_config.get("database", "default")

    client = Client(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        secure=True,
    )

    filtered_marktes = filter_markets(exchanges, market_filter)
    store_clickhouse(filtered_marktes, collect_interval, client)


if __name__ == "__main__":
    main()
