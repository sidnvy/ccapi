import os
import sys
import time
import traceback
from datetime import datetime
from typing import List
import pandas as pd
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


def get_current_utc_day():
    return datetime.utcnow().date()

def write_temp_arrow_file(record_batch: pa.RecordBatch, temp_folder: str) -> str:
    timestamp_ns = record_batch.column('timestamp')[0].as_py()
    temp_file = os.path.join(temp_folder, f"{timestamp_ns}.arrow")

    with pa.OSFile(temp_file, 'wb') as sink:
        with pa.ipc.new_file(sink, record_batch.schema) as writer:
            writer.write(record_batch)

    return temp_file


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

        current_day = get_current_utc_day()

        for exchange, market in exchange_market_pairs:
            temp_folder = os.path.join("temp_folder", exchange, market)
            os.makedirs(temp_folder, exist_ok=True)

            data_buffer_arrays = {
                k: pa.array(v) for k, v in data_buffers[(exchange, market)].items()
            }
            if not data_buffer_arrays["timestamp"]:
                print(f"No data collected for {exchange}/{market}")
                continue

            record_batch = pa.RecordBatch.from_arrays(
                list(data_buffer_arrays.values()), schema=schema
            )
            write_temp_arrow_file(record_batch, temp_folder)
            temp_files = [
                os.path.join(temp_folder, f)
                for f in os.listdir(temp_folder)
                if f.endswith(".arrow")
            ]

            last_timestamp = pd.to_datetime(
                data_buffer_arrays["timestamp"][-1].as_py()
            ).date()
            if last_timestamp > current_day:
                tables = []
                for file in temp_files:
                    with pa.memory_map(file, "rb") as source:
                        reader = pa.ipc.open_file(source)
                        table = reader.read_all()
                        tables.append(table)

                merged_table = pa.concat_tables(tables)

                output_path = os.path.join(data_dir, exchange, market)
                if data_dir.startswith("s3://"):
                    fs = S3FileSystem()
                    fs.mkdir(output_path, recursive=True)
                else:
                    os.makedirs(output_path, exist_ok=True)

                pq.write_table(
                    merged_table,
                    os.path.join(
                        output_path,
                        f'output_daily_{current_day.strftime("%Y%m%d")}.parquet',
                    ),
                    compression="snappy",
                )

                for file in temp_files:
                    os.remove(file)

                current_day = get_current_utc_day()

            print(f"Table written for {exchange}/{market} at {time.time()}")

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
