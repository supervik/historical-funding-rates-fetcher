from datetime import datetime
import requests
import pandas as pd
from exchanges.exchange_base import ExchangeBase


class Htx(ExchangeBase):
    def fetch_data(self):
        print("Start processing HTX funding rates. It can take some minutes. Please wait. ....")
        start_ts, end_ts = self.convert_start_end_time()

        url = "https://api.hbdm.com/linear-swap-api/v1/swap_historical_funding_rate"
        all_data = []
        page_index = 1
        page_size = 50
        data_within_range = True

        while data_within_range:
            params = {
                'contract_code': self.symbol,
                'page_index': page_index,
                'page_size': page_size
            }

            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json().get('data', {})
                if data["current_page"] > data["total_page"]:
                    break
                records = data.get('data', [])

                if not records:
                    break

                for record in records:
                    funding_time = int(record['funding_time'])
                    if funding_time < start_ts:
                        # Break the loop if funding_time is earlier than start_ts
                        data_within_range = False
                        break

                    if start_ts <= funding_time <= end_ts:
                        all_data.append({
                            'Symbol': self.symbol,
                            'Date': datetime.fromtimestamp(funding_time / 1000),
                            'Funding Rate': float(record['funding_rate'])
                        })
                page_index += 1
            else:
                print(f"Failed to fetch data from HTX: {response.status_code}")
                break

        all_data.reverse()
        df = pd.DataFrame(all_data)

        return df

