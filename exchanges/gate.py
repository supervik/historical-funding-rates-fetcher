from datetime import datetime, timedelta
import requests
import pandas as pd
import gzip
import io
from exchanges.exchange_base import ExchangeBase


class Gate(ExchangeBase):
    def convert_start_end_time(self):
        """Convert dates to timestamps"""
        start = datetime.strptime(self.start_date, '%Y-%m-%d')
        end = datetime.strptime(self.end_date, '%Y-%m-%d')
        return start, end

    def fetch_data(self):
        print("Start processing Gate funding rates. It can take some minutes. Please wait. ....")
        start, end = self.convert_start_end_time()
        current = start
        symbol_converted = self.symbol.replace('-', '_')

        all_data = pd.DataFrame(columns=['Symbol', 'Date', 'Funding Rate'])

        while current <= end:
            year_month = current.strftime("%Y%m")
            url = f"https://download.gatedata.org/futures_usdt/funding_applies/{year_month}/{symbol_converted}-{year_month}.csv.gz"

            # Download and extract the data
            response = requests.get(url)
            if response.status_code == 200:
                with gzip.open(io.BytesIO(response.content), 'rt') as f:
                    monthly_data = pd.read_csv(f, header=None, names=['timestamp', 'Funding Rate'])
                    monthly_data['Symbol'] = self.symbol
                    monthly_data['Date'] = pd.to_datetime(monthly_data['timestamp'], unit='s')
                    all_data = pd.concat([all_data, monthly_data[['Symbol', 'Date', 'Funding Rate']]])

            # Move to the next month
            print(f"\rProcessing {current}", end="")
            current += timedelta(days=32)
            current = current.replace(day=1)

        # Filter data
        all_data = all_data[(all_data['Date'] >= start) & (all_data['Date'] <= end)]

        return all_data

