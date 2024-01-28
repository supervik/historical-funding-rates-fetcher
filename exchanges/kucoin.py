from datetime import datetime
import requests
import pandas as pd
from exchanges.exchange_base import ExchangeBase


class Kucoin(ExchangeBase):
    def fetch_data(self):
        print("Start processing Kucoin funding rates. It can take some minutes. Please wait. ....")
        url = 'https://api-futures.kucoin.com/api/v1/contract/funding-rates'

        # Convert dates to timestamps (in milliseconds)
        start_ts, end_ts = self.convert_start_end_time()

        symbol_converted = self.symbol.replace('-', '')
        if 'BTC' in self.symbol:
            symbol_converted = symbol_converted.replace('BTC', 'XBT')
        symbol_converted += 'M'

        all_data = []

        while True:
            params = {
                'symbol': symbol_converted,
                'from': start_ts,
                'to': end_ts
            }

            response = requests.get(url, params=params)
            if response.status_code == 200:
                funding_rates = response.json().get('data', [])
                if not funding_rates:
                    break

                for rate in funding_rates:
                    funding_time = int(rate['timepoint'])
                    if funding_time < start_ts:
                        continue

                    all_data.append({
                        'Symbol': self.symbol,
                        'Date': datetime.fromtimestamp(funding_time / 1000),
                        'Funding Rate': rate['fundingRate']
                    })

                # Update end_ts for the next API request
                first_funding_time = int(funding_rates[-1]['timepoint'])
                end_ts = first_funding_time - 1

                # Break the loop if we have reached the start timestamp
                if first_funding_time <= start_ts:
                    break
            else:
                print("Failed to fetch data from Kucoin")
                break

        all_data.reverse()
        return pd.DataFrame(all_data, columns=["Symbol", "Date", "Funding Rate"])

