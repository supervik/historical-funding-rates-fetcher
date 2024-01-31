# Historical Funding Rates Fetcher

This repository contains a Python script along with associated data for retrieving and analyzing historical funding rates from various cryptocurrency exchanges. 

Discover more about this project and explore the insights in the article: 
[My Insights into the Impact of Funding Rates](https://viktoriatsybko.substack.com/)

## Repository Contents
- **`main.py`**: A Python script to fetch historical funding rates. It includes the following features:
  - **Support for Multiple Exchanges**: Binance, Bybit, dYdX, Gate, HTX, Kucoin, MEXC.
  - **Customizable Fetching**: Users can specify parameters such as the exchange, trading pair, and the time range for data retrieval. Currently only USDT margined pairs are supported
  - **Data Export**: Results are saved to a CSV file for easy analysis.
- **`data/`**: Directory containing CSV files with funding rates for BTC and ETH (USDT or USD margined) covering the period from 2020 to 2023 for multiple exchanges. Each CSV file includes the following columns:

    | Column        | Description   |
    |---------------|---------------|
    | Symbol        | The trading pair. |
    | Date          | The date of the funding rate settlement. Typically every 8 hours or 24h for dYdX. |
    | Funding Rate  | The funding rate in decimal format. Multiply by 100 to get the percentage. |
