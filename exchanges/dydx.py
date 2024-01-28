from datetime import datetime, timedelta
import requests
import pandas as pd
from exchanges.exchange_base import ExchangeBase


class Dydx(ExchangeBase):
    def convert_start_end_time(self):
        """Convert dates to timestamps"""
        start = datetime.strptime(self.start_date, '%Y-%m-%d')
        end = datetime.strptime(self.end_date, '%Y-%m-%d')
        return start, end

    def fetch_data(self):
        print("Start processing dYdX funding rates. It can take some minutes. Please wait. ....")
        start, end = self.convert_start_end_time()

        all_data = []
        current_date = end
        symbol_converted = self.symbol.replace('USDT', 'USD')

        while current_date > start:
            effective_date = current_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            url = f"https://api.dydx.exchange/v3/historical-funding/{symbol_converted}?effectiveBeforeOrAt={effective_date}"

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json().get('historicalFunding', [])
                if not data or len(data) == 1:
                    print(f"No data found for dYdX")
                    break
                print(f"\rProcessing {current_date}", end="")
                for record in data:
                    effective_at = datetime.strptime(record['effectiveAt'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    if effective_at < start or effective_at >= end:
                        continue
                    all_data.append({
                        'Symbol': self.symbol,
                        'Date': effective_at,
                        'Funding Rate': record['rate']
                    })
            else:
                print(f"Failed to fetch data from dYdX: {response.status_code}")
                break

            # Update current_date based on the last record's timestamp
            last_record_time = datetime.strptime(data[-1]['effectiveAt'], '%Y-%m-%dT%H:%M:%S.%fZ')
            current_date = last_record_time - timedelta(hours=1)

        all_data.reverse()
        return pd.DataFrame(all_data, columns=["Symbol", "Date", "Funding Rate"])

