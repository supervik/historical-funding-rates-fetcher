from datetime import datetime
import requests
import pandas as pd
from exchanges.exchange_base import ExchangeBase


class Binance(ExchangeBase):
    def fetch_data(self):
        print("Start processing Binance funding rates. It can take some minutes. Please wait. ....")
        start_ts, end_ts = self.convert_start_end_time()
        url = 'https://fapi.binance.com/fapi/v1/fundingRate'
        symbol_converted = self.symbol.replace('-', '')

        all_data = []
        while start_ts < end_ts:
            params = {'symbol': symbol_converted, 'startTime': start_ts, 'limit': 1000}
            response = requests.get(url, params=params)

            if response.status_code == 200:
                funding_rates = response.json()
                if not funding_rates:
                    break

                for rate in funding_rates:
                    all_data.append({'Symbol': self.symbol,
                                     'Date': datetime.fromtimestamp(rate['fundingTime'] / 1000),
                                     'Funding Rate': rate['fundingRate']})

                last_funding_time = funding_rates[-1]['fundingTime']
                if last_funding_time >= end_ts:
                    break  # Stop fetching if last funding time in batch is greater than or equal to end_ts

                start_ts = last_funding_time + 1
            else:
                print("Failed to fetch data from Binance")
                break

        # Convert to DataFrame and filter if 'Date' column exists
        df = pd.DataFrame(all_data)
        if 'Date' in df.columns:
            df = df[df['Date'] < datetime.fromtimestamp(end_ts / 1000)]
        else:
            print("No data fetched or 'Date' column not found in DataFrame.")

        return df

