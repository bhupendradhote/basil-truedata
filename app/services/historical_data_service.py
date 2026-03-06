import logging
import pandas as pd
from datetime import datetime
from truedata.history import TD_hist
from app.config.settings import Settings

class HistoricalDataService:
    def __init__(self):
        self.username = Settings.TD_USERNAME
        self.password = Settings.TD_PASSWORD
        self.td_hist = None

    def connect(self):
        """Initializes the connection to TrueData if not already connected."""
        if not self.username or not self.password:
            raise ValueError("TrueData credentials are missing. Check your .env file.")
        
        if self.td_hist is None:
            self.td_hist = TD_hist(self.username, self.password, log_level=logging.WARNING)

    def get_history(self, symbol: str, duration: str = "1 D", bar_size: str = "1 min") -> list:
        """
        Fetches historical data by relative duration (e.g., '1 D', '5 D').
        """
        self.connect()
        try:
            df = self.td_hist.get_historic_data(symbol, duration=duration, bar_size=bar_size)
            return self._format_dataframe(df)
        except Exception as e:
            print(f"❌ Error fetching history for {symbol}: {e}")
            return []

    def get_history_by_date(self, symbol: str, start_date: datetime, end_date: datetime, bar_size: str = "EOD") -> list:
        """
        Fetches historical data between specific dates.
        """
        self.connect()
        try:
            df = self.td_hist.get_historic_data(symbol, start_time=start_date, end_time=end_date, bar_size=bar_size)
            return self._format_dataframe(df)
        except Exception as e:
            print(f"❌ Error fetching history by date for {symbol}: {e}")
            return []

    def _format_dataframe(self, df) -> list:
        """
        Converts TrueData's Pandas DataFrame into a clean list of dictionaries
        so FastAPI can easily send it as JSON to a mobile app.
        """
        if df is None or df.empty:
            return []
            
        if 'time' in df.columns:
            df['time'] = df['time'].astype(str)
        elif df.index.name == 'time':
            df.reset_index(inplace=True)
            df['time'] = df['time'].astype(str)
            
        df = df.where(pd.notnull(df), None)
        
        return df.to_dict(orient="records")

    def disconnect(self):
        """Cleans up the connection."""
        if self.td_hist:
            self.td_hist = None
            print("🔌 Disconnected Historical Service.")