import configparser
import sys
import time
import traceback
from typing import List

from clickhouse_driver import Client
from dateutil.parser import parse

from ccapi import (Event, Session, SessionConfigs, SessionOptions,
                   Subscription, SubscriptionList)


def process_events(client, events: List[Event], exchange_market_pairs: List[tuple]):
    try:
        data = []

        for event in events:
            if event.getType() == Event.Type_SUBSCRIPTION_DATA:
                for message in event.getMessageList():
                    exchange, market = message.getCorrelationIdList()[0].split(",")
                    timestamp_start = parse(message.getTimeISO())

                    bid_element, ask_element = message.getElementList()
                    bid_element.getNameValueMap()
                    ask_element.getNameValueMap()

                    data.append(
                        (
                            timestamp_start,
                            market,
                            exchange,
                            bid_element.getValue("BID_PRICE"),
                            ask_element.getValue("ASK_PRICE"),
                            bid_element.getValue("BID_SIZE"),
                            ask_element.getValue("ASK_SIZE"),
                        )
                    )

        if data:
            client.execute(
                "INSERT INTO quote_ticks (timestamp, symbol, exchange, bid_price, ask_price, bid_size, ask_size) VALUES",
                data,
            )

    except Exception:
        print(traceback.format_exc())
        sys.exit(1)


def main():
    config = configparser.ConfigParser()
    config.read("config.ini")

    general_config = config["General"]
    exchanges = general_config.get("Exchanges", "binance-usds-futures").split(",")
    markets = general_config.get("Markets", "ethusdt,btcusdt,bnbusdt").split(",")
    collect_interval = general_config.getint("CollectInterval", 60)

    clickhouse_config = config["ClickHouse"]
    host = clickhouse_config.get("host", "localhost")
    port = clickhouse_config.getint("port", 9000)
    user = clickhouse_config.get("user", "default")
    password = clickhouse_config.get("password", "")
    database = clickhouse_config.get("database", "default")

    client = Client(
        host=host, port=port, user=user, password=password, database=database
    )

    option = SessionOptions()
    session_config = SessionConfigs()
    session = Session(option, session_config)

    subscriptionList = SubscriptionList()
    exchange_market_pairs = []
    for exchange in exchanges:
        for market in markets:
            correlation_id = ",".join([exchange, market])
            subscription = Subscription(
                exchange,
                market.upper(),
                "MARKET_DEPTH",
                "MARKET_DEPTH_MAX=1",
                correlation_id,
            )
            subscriptionList.append(subscription)
            exchange_market_pairs.append((exchange, market))

    session.subscribe(subscriptionList)
    try:
        while True:
            time.sleep(collect_interval)
            events = session.getEventQueue().purge()
            process_events(client, events, exchange_market_pairs)
    except KeyboardInterrupt:
        session.stop()
        print("Bye")


if __name__ == "__main__":
    main()

