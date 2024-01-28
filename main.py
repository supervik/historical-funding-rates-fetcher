import importlib

# Enter exchange name: binance, bybit, gate, htx, kucoin, mexc, dydx
EXCHANGE = "dydx"
# Enter the symbol in the format: BTC-USDT
SYMBOL = "ETH-USDT"

# Enter start and end dates (YYYY-MM-DD)
START_DATE = "2023-12-01"
END_DATE = "2024-01-01"


def main():
    try:
        exchange_module = importlib.import_module(f'exchanges.{EXCHANGE}')

        # Dynamically get the class from the module
        ExchangeClass = getattr(exchange_module, EXCHANGE.capitalize())

        # Instantiate the exchange class and fetch data
        exchange = ExchangeClass(SYMBOL, START_DATE, END_DATE)
        data = exchange.fetch_data()
        print(data)

        data.to_csv(f'data/{SYMBOL}_{EXCHANGE}_{START_DATE}_{END_DATE}_funding_history.csv', index=False)
    except (ImportError, AttributeError) as e:
        print(f"Failed to import class for exchange '{EXCHANGE}'. Error: {e}")


if __name__ == "__main__":
    main()