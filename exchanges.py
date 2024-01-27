import requests
import pandas as pd
import io
import gzip
from datetime import datetime, timedelta


def convert_start_end_time(start_date, end_date):
    """Convert dates to timestamps"""
    start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
    end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000)
    return start_ts, end_ts


def fetch_data_binance(symbol, start_date, end_date):
    """"""
    start_ts, end_ts = convert_start_end_time(start_date, end_date)
    url = 'https://fapi.binance.com/fapi/v1/fundingRate'
    symbol_converted = symbol.replace('-', '')

    all_data = []
    while start_ts < end_ts:
        params = {'symbol': symbol_converted, 'startTime': start_ts, 'limit': 1000}
        response = requests.get(url, params=params)

        if response.status_code == 200:
            funding_rates = response.json()
            if not funding_rates:
                break

            for rate in funding_rates:
                all_data.append({'Symbol': symbol,
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


def fetch_data_bybit(symbol, start_date, end_date, category='linear'):
    # Convert dates to timestamps (in milliseconds)
    start_ts, end_ts = convert_start_end_time(start_date, end_date)
    symbol_converted = symbol.replace('-', '')

    url = 'https://api.bybit.com/v5/market/funding/history'
    all_data = []
    print(f"start_ts = {start_ts}, end_ts = {end_ts}")
    while True:
        params = {
            'category': category,  # 'linear' or 'inverse'
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

                all_data.append({'Symbol': symbol,
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


def fetch_data_kucoin(symbol, start_date, end_date):
    # Kucoin API endpoint for funding rates
    url = 'https://api-futures.kucoin.com/api/v1/contract/funding-rates'

    # Convert dates to timestamps (in milliseconds)
    start_ts, end_ts = convert_start_end_time(start_date, end_date)

    symbol_converted = symbol.replace('-', '')
    if 'BTC' in symbol:
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
                    'Symbol': symbol,
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


def fetch_data_gate(symbol, start_date, end_date):
    # Prepare the date range
    print("Start processing gate.io funding rates. It can take some minutes. Please wait. ....")
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    current = start
    symbol_converted = symbol.replace('-', '_')

    all_data = pd.DataFrame(columns=['Symbol', 'Date', 'Funding Rate'])

    while current <= end:
        year_month = current.strftime("%Y%m")
        url = f"https://download.gatedata.org/futures_usdt/funding_applies/{year_month}/{symbol_converted}-{year_month}.csv.gz"

        # Download and extract the data
        response = requests.get(url)
        if response.status_code == 200:
            with gzip.open(io.BytesIO(response.content), 'rt') as f:
                monthly_data = pd.read_csv(f, header=None, names=['timestamp', 'Funding Rate'])
                monthly_data['Symbol'] = symbol
                monthly_data['Date'] = pd.to_datetime(monthly_data['timestamp'], unit='s')
                all_data = pd.concat([all_data, monthly_data[['Symbol', 'Date', 'Funding Rate']]])

        # Move to the next month
        print(f"\rProcessing {current}", end="")
        current += timedelta(days=32)
        current = current.replace(day=1)

    # Filter data
    all_data = all_data[(all_data['Date'] >= start) & (all_data['Date'] <= end)]

    return all_data


def fetch_data_htx(symbol, start_date, end_date):
    print("Start processing htx funding rates. It can take some minutes. Please wait. ....")
    start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
    end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000)

    url = "https://api.hbdm.com/linear-swap-api/v1/swap_historical_funding_rate"
    all_data = []
    page_index = 1
    page_size = 50
    data_within_range = True

    while data_within_range:
        params = {
            'contract_code': symbol,
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
                        'Symbol': symbol,
                        'Date': datetime.fromtimestamp(funding_time / 1000),
                        'Funding Rate': float(record['funding_rate'])
                    })
            page_index += 1
        else:
            print(f"Failed to fetch data from HTX: {response.status_code}")
            break

    all_data.reverse()
    return pd.DataFrame(all_data, columns=["Symbol", "Date", "Funding Rate"])


def fetch_data_mexc(symbol, start_date, end_date):
    print("Start processing MEXC funding rates. It can take some minutes. Please wait. ....")
    start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
    end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000)

    url = "https://contract.mexc.com/api/v1/contract/funding_rate/history"
    all_data = []
    page_num = 1
    page_size = 100
    data_within_range = True
    symbol_converted = symbol.replace('-', '_')

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
                        'Symbol': symbol,
                        'Date': datetime.fromtimestamp(funding_time / 1000),
                        'Funding Rate': float(record['fundingRate'])
                    })
            page_num += 1
        else:
            print(f"Failed to fetch data from MEXC: {response.status_code}")
            break

    all_data.reverse()
    return pd.DataFrame(all_data, columns=["Symbol", "Date", "Funding Rate"])


def fetch_data_dydx(symbol, start_date, end_date):
    print("Start processing DYDX funding rates. It can take some minutes. Please wait. ....")
    # Convert dates to ISO 8601 format
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    all_data = []
    current_date = end
    symbol_converted = symbol.replace('USDT', 'USD')

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
                    'Symbol': symbol,
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


# Dictionary mapping exchange names to functions
exchange_functions = {
    'binance': fetch_data_binance,
    'bybit': fetch_data_bybit,
    'kucoin': fetch_data_kucoin,
    'gate': fetch_data_gate,
    'htx': fetch_data_htx,
    'mexc': fetch_data_mexc,
    'dydx': fetch_data_dydx
    # Add more exchanges here as needed
}
