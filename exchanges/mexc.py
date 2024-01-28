from datetime import datetime
import requests
import pandas as pd
from exchanges.exchange_base import ExchangeBase


class Mexc(ExchangeBase):
    def fetch_data(self):
        print("Start processing MEXC funding rates. It can take some minutes. Please wait. ....")
        start_ts, end_ts = self.convert_start_end_time()
        url = "https://contract.mexc.com/api/v1/contract/funding_rate/history"
        all_data = []
        page_num = 1
        page_size = 100
        data_within_range = True
        symbol_converted = self.symbol.replace('-', '_')

        while data_within_range:
            params = {
                'symbol': symbol_converted,
                'page_num': page_num,
                'page_size': page_size
            }

            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json().get('data', {})
                if data["currentPage"] > data["totalPage"]:
                    break
                records = data.get('resultList', [])
                if not records:
                    break

                for record in records:
                    funding_time = int(record['settleTime'])
                    if funding_time < start_ts:
                        # Break the loop if funding_time is earlier than start_ts
                        data_within_range = False
                        break

                    if start_ts <= funding_time <= end_ts:
                        all_data.append({
                            'Symbol': self.symbol,
                            'Date': datetime.fromtimestamp(funding_time / 1000),
                            'Funding Rate': float(record['fundingRate'])
                        })
                page_num += 1
            else:
                print(f"Failed to fetch data from MEXC: {response.status_code}")
                break

        all_data.reverse()
        df = pd.DataFrame(all_data)

        return df

