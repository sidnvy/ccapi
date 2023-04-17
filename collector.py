import os
import sys
import time
from typing import List
import traceback
from ccapi import (
    SessionOptions,
    SessionConfigs,
    Session,
    Subscription,
    Event,
    SubscriptionList,
)
from dateutil.parser import parse
import pyarrow as pa

import pyarrow.parquet as pq
from pyarrow.fs import S3FileSystem


def process_events(
    events: List[Event],
    data_dir: str,
    schema: pa.Schema,
    exchange_market_pairs: List[tuple],
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
            data_buffer_arrays = {
                k: pa.array(v) for k, v in data_buffers[(exchange, market)].items()
            }
            if not data_buffer_arrays["timestamp"]:
                print(f"No data collected for {exchange}/{market}")
                continue

            record_batch = pa.RecordBatch.from_arrays(
                list(data_buffer_arrays.values()), schema=schema
            )
            table = pa.Table.from_batches([record_batch], schema=schema)

            output_path = os.path.join(data_dir, exchange, market)
            if data_dir.startswith("s3://"):
                fs = S3FileSystem()
                fs.mkdir(output_path, recursive=True)
            else:
                os.makedirs(output_path, exist_ok=True)

            first_timestamp = data_buffer_arrays["timestamp"][0].as_py()
            output_file = os.path.join(output_path, f"{first_timestamp}.parquet")

            if data_dir.startswith("s3://"):
                with fs.open_output_stream(output_file) as out:
                    pq.write_table(table, out, compression="snappy")
            else:
                pq.write_table(table, output_file, compression="snappy")

            print(f"Table written for {exchange}/{market} at {time.time()}")

        human_readable_size = pa.format_size(pa.total_allocated_bytes())
        print(f"Total allocated bytes: {human_readable_size}")

    except Exception:
        print(traceback.format_exc())
        sys.exit(1)


def main():
    exchanges = os.getenv("EXCHANGES", "binance-usds-futures").split(",")
    markets = os.getenv("MARKETS", "ethusdt,btcusdt,bnbusdt").split(",")
    collect_interval = int(os.getenv("COLLECT_INTERVAL", 60 * 60))
    data_dir = os.getenv("DATA_DIR", "file:///data")

    option = SessionOptions()
    config = SessionConfigs()
    session = Session(option, config)

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

    schema = pa.schema(
        [
            ("timestamp", pa.int64()),
            ("bid_price", pa.float64()),
            ("ask_price", pa.float64()),
            ("bid_size", pa.float64()),
            ("ask_size", pa.float64()),
        ]
    )

    session.subscribe(subscriptionList)
    try:
        while True:
            time.sleep(collect_interval)
            events = session.getEventQueue().purge()
            process_events(events, data_dir, schema, exchange_market_pairs)
    except KeyboardInterrupt:
        session.stop()
        print("Bye")


if __name__ == "__main__":
    main()
