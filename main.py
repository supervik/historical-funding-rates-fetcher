import exchanges

# Enter exchange name: binance, bybit, gate, htx, kucoin, mexc, dydx
EXCHANGE = "dydx"
# Enter the symbol in the format: BTC-USDT
SYMBOL = "ETH-USDT"

# SYMBOL = "BTC/USDT:USDT"
# htx: BTC-USDT
# kucoin: XBTUSDTM
# gate: BTC_USDT
# bybit: BTCUSDT or BTCPERP or BTCUSD
# binance: BTCUSDT
# mexc: BTC_USDT
# dydx: BTC-USD

# Enter start and end dates (YYYY-MM-DD)
START_DATE = "2020-01-01"
END_DATE = "2024-01-01"


def main():
    fetch_data = exchanges.exchange_functions.get(EXCHANGE)

    if fetch_data:
        data = fetch_data(SYMBOL, START_DATE, END_DATE)
        print(data)
        data.to_csv(f'{SYMBOL}_{EXCHANGE}_{START_DATE}_{END_DATE}_funding_history.csv', index=False)
    else:
        print(f"Exchange {EXCHANGE} is not supported.")


if __name__ == "__main__":
    main()