from typing import Dict


def kucoin_futures_transform(element: Dict):
    instrument = element["INSTRUMENT"].replace("XBT", "BTC")

    base = instrument
    quote = instrument
    if instrument.endswith("USDTM"):
        base = instrument.rstrip("USDTM")
        quote = "USDT"
    elif instrument.endswith("USDCM"):
        base = instrument.rstrip("USDC")
        quote = "USDC"
    elif instrument.endswith("USDM"):
        base = instrument.rstrip("USDM")
        quote = "USD"
    else:
        return

    return f"{base}-{quote}-PERP"


def okx_transform(element: Dict):
    instrument = element["INSTRUMENT"]
    if instrument.endswith("SWAP"):
        return instrument.replace("SWAP", "PERP")
    elif not instrument.endswith("PERP"):
        return instrument


def binance_usds_futures(e: Dict):
    if not e["INSTRUMENT"][-1].isdigit():
        return f"{e['UNDERLYING_SYMBOL'].rstrip(e['MARGIN_ASSET'])}-{e['MARGIN_ASSET']}-PERP"


EXCHANGE_MARKET_TO_UNIFIED_SYMBOL = {
    "binance-usds-futures": binance_usds_futures,
    "bybit": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-SPOT",
    "bybit-derivatives": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-PERP",
    "mexc-futures": lambda e: f"{e['INSTRUMENT'].rstrip(e['MARGIN_ASSET'])[:-1]}-{e['MARGIN_ASSET']}-PERP",
    # "bitfinex": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-SPOT", # how to distinguish spot and futures
    "okx": okx_transform,
    "huobi-coin-swap": lambda e: f"{e['INSTRUMENT']}-USD-PERP",
    "huobi-usdt-swap": lambda e: f"{e['INSTRUMENT']}-USDT-PERP",
    "ascendex": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-SPOT",
    "kucoin": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-SPOT",
    "bitget-futures": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-PERP",
    "kucoin-futures": kucoin_futures_transform,
    "bitget": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-SPOT",
    "bitmart": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-SPOT",
    "cryptocom": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-SPOT",
    "kraken": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-SPOT",
    "bitstamp": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-SPOT",
    # "gateio-perpetual-futures": lambda e: f"{e['ERROR_MESSAGE']['label']}",
    "coinbase": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-SPOT",
    "mexc": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-PERP",
    # "bitmex": lambda e: f"{e['ERROR_MESSAGE']}",
    "binance-coin-futures": lambda e: f"{e['MARGIN_ASSET']}-{e['UNDERLYING_SYMBOL'].lstrip(e['MARGIN_ASSET'])}-PERP",
    "deribit": lambda e: f"{e['UNDERLYING_SYMBOL']}-USDC-PERP",
    "huobi": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-SPOT",
    "gateio": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-SPOT",
    "binance": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-SPOT",
    # "kraken-futures": lambda e: f"{e['ERROR_MESSAGE']}",
    # "gemini": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-SPOT",  # hard to split
    # "binance-us": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-SPOT",  # not interested
    "whitebit": lambda e: f"{e['BASE_ASSET']}-{e['QUOTE_ASSET']}-PERP",
}
