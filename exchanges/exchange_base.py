from datetime import datetime


class ExchangeBase:
    def __init__(self, symbol, start_date, end_date):
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date

    def convert_start_end_time(self):
        """Convert dates to timestamps"""
        start_ts = int(datetime.strptime(self.start_date, '%Y-%m-%d').timestamp() * 1000)
        end_ts = int(datetime.strptime(self.end_date, '%Y-%m-%d').timestamp() * 1000)
        return start_ts, end_ts

    def fetch_data(self):
        raise NotImplementedError("This method should be implemented by subclasses.")
