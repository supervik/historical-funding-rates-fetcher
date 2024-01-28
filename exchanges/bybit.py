from datetime import datetime
import requests
import pandas as pd
from exchanges.exchange_base import ExchangeBase


class Bybit(ExchangeBase):
    def fetch_data(self):
        print("Start processing Bybit funding rates. It can take some minutes. Please wait. ....")
        # Convert dates to timestamps (in milliseconds)
        start_ts, end_ts = self.convert_start_end_time()
        symbol_converted = self.symbol.replace('-', '')

        url = 'https://api.bybit.com/v5/market/funding/history'
        all_data = []
        print(f"start_ts = {start_ts}, end_ts = {end_ts}")
        while True:
            params = {
                'category': 'linear',  # 'linear' or 'inverse'
                'symbol': symbol_converted,
                'endTime': end_ts,
                'limit': 200  # API allows up to 200
            }

            response = requests.get(url, params=params)
            if response.status_code == 200:
                funding_rates = response.json()['result'].get('list', [])
                if not funding_rates:
                    break

                for rate in funding_rates:
                    funding_time = int(rate['fundingRateTimestamp'])
                    if funding_time < start_ts:
                        continue

                    all_data.append({'Symbol': self.symbol,
                                     'Date': datetime.fromtimestamp(funding_time / 1000),
                                     'Funding Rate': rate['fundingRate']})

                # Update end_ts for the next API request
                first_funding_time = int(funding_rates[-1]['fundingRateTimestamp'])
                end_ts = first_funding_time - 1

                # Break the loop if we have reached the start timestamp
                if first_funding_time <= start_ts:
                    break
            else:
                print("Failed to fetch data from Bybit")
                break

        all_data.reverse()
        return pd.DataFrame(all_data, columns=["Symbol", "Date", "Funding Rate"])

