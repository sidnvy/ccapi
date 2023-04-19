import configparser
import os
import sys
import time
from typing import List

import pyarrow as pa
from clickhouse_driver import Client
from dateutil.parser import parse

from ccapi import (Event, Session, SessionConfigs, SessionOptions,
                   Subscription, SubscriptionList)


def create_clickhouse_table(client):
    client.execute(
        """
        CREATE TABLE IF NOT EXISTS default.quote_ticks (
            timestamp DateTime64 CODEC(Delta, ZSTD(1)),
            symbol LowCardinality(String) CODEC(ZSTD(1)),
            exchange LowCardinality(String) CODEC(ZSTD(1)),
            bid_price Float64 CODEC(Delta, ZSTD(1)),
            ask_price Float64 CODEC(Delta, ZSTD(1)),
            bid_size Float64 CODEC(Delta, ZSTD(1)),
            ask_size Float64 CODEC(Delta, ZSTD(1))
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(timestamp)
        ORDER BY (timestamp)
        SETTINGS index_granularity = 8192
        """
    )


def process_events(
    events: List[Event], schema: pa.Schema, exchange_market_pairs: List[tuple]
):
    try:
        data_buffers = {
            pair: {col_name: [] for col_name in schema.names}
            for pair in exchange_market_pairs
        }

        for event in events:
            if event.getType() == Event.Type_SUBSCRIPTION_DATA:
                for message in event.getMessageList():
                    exchange, market = message.getCorrelationIdList()[0].split(",")
                    timestamp_start = parse(message.getTimeISO())

                    bid_element, ask_element = message.getElementList()
                    bid_element.getNameValueMap()
                    ask_element.getNameValueMap()

                    buffer = data_buffers[(exchange, market)]
                    buffer["timestamp"].append(int(timestamp_start.timestamp() * 1e9))
                    buffer["bid_price"].append(bid_element.getValue("BID_PRICE"))
                    buffer["ask_price"].append(ask_element.getValue("ASK_PRICE"))
                    buffer["bid_size"].append(bid_element.getValue("BID_SIZE"))
                    buffer["ask_size"].append(ask_element.getValue("ASK_SIZE"))

        for exchange, market in exchange_market_pairs:
            rows = []
            for index in range(len(data_buffers[(exchange, market)]["timestamp"])):
                row = (
                    data_buffers[(exchange, market)]["timestamp"][index],
                    market,
                    exchange,
                    data_buffers[(exchange, market)]["bid_price"][index],
                    data_buffers[(exchange, market)]["ask_price"][index],
                    data_buffers[(exchange, market)]["bid_size"][index],
                    data_buffers[(exchange, market)]["ask_size"][index],
                )
                rows.append(row)

            client.execute(
                """
                INSERT INTO quote_ticks (timestamp, symbol, exchange, bid_price, ask_price, bid_size, ask_size)
                VALUES
            """,
                rows,
            )

            print(f"Inserted rows for {exchange}/{market} at {time.time()}")

    except Exception as e:
        print(f"Error processing events: {e}")
        sys.exit(1)


def main():
    config = configparser.ConfigParser()
    config.read("config.ini")

    general_config = config['General']
    exchanges = general_config.get('Exchanges', 'binance-usds-futures').split(',')
    markets = general_config.get('Markets', 'ethusdt,btcusdt,bnbusdt').split(',')
    collect_interval = general_config.getint('CollectInterval', 60)

    clickhouse_config = config["ClickHouse"]
    client = Client(
        host=clickhouse_config.get("host", "localhost"),
        port=clickhouse_config.getint("port", 9000),
        user=clickhouse_config.get("user", "default"),
        password=clickhouse_config.get("password", ""),
        database=clickhouse_config.get("database", "default"),
    )

    create_clickhouse_table(client)


    option = SessionOptions()
    session_config = SessionConfigs()
    session = Session(option, session_config)

    subscription_list = SubscriptionList()
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
            subscription_list.append(subscription)
            exchange_market_pairs.append((exchange, market))

    schema = pa.schema(
        [
            ("timestamp", pa.int64()),
            ("bid_price", pa.float64()),
            ("ask_price", pa.float64()),
            ("bid_size", pa.float64()),
            ("ask_size", pa.float64()),
        ]
    )

    session.subscribe(subscription_list)


    try:
        while True:
            time.sleep(collect_interval)
            events = session.getEventQueue().purge()
            process_events(events, schema, exchange_market_pairs)
    except KeyboardInterrupt:
        session.stop()
        print("Bye")


if __name__ == "__main__":
    main()
