import logging
import requests  
from truedata.history import TD_hist
from app.config.settings import Settings

class TrueDataService:
    def __init__(self):
        self.username = Settings.TD_USERNAME
        self.password = Settings.TD_PASSWORD
        self.td_hist = None

    def connect_history(self):
        if not self.username or not self.password:
            raise ValueError("TrueData credentials are missing. Check your .env file.")
        self.td_hist = TD_hist(self.username, self.password, log_level=logging.WARNING)


    def get_all_symbols(self, category: str = "NSE_EQ") -> list:
        """
        Fetches the master symbol lists directly from TrueData's official hosted files.
        """
        symbol_urls = {
            "NSE_EQ": "https://www.truedata.in/downloads/symbol_lists/5.ALL_NSE_EQ.txt",
            "NSE_INDICES": "https://www.truedata.in/downloads/symbol_lists/A.NSE_INDICES.txt",
            "NSE_FUT_CONT": "https://www.truedata.in/downloads/symbol_lists/8.NSE_FUTURES_CONTINUOUS-I.txt",
            "NSE_OPT_CONT": "https://www.truedata.in/downloads/symbol_lists/D.NSE_CONTINUOUS_OPTIONS.txt",
            "BSE_EQ": "https://www.truedata.in/downloads/symbol_lists/32.ALL_BSE_EQ.txt",
            "BSE_INDICES": "https://www.truedata.in/downloads/symbol_lists/C.BSE_INDICES.txt"
        }

        if category not in symbol_urls:
            print(f"⚠ Invalid category. Choose from: {', '.join(symbol_urls.keys())}")
            return []

        try:
            print(f"📥 Downloading {category} master list...")
            response = requests.get(symbol_urls[category], timeout=10)
            response.raise_for_status() 
            
            raw_symbols = response.text.strip().split('\n')
            
            clean_symbols = [sym.strip() for sym in raw_symbols if sym.strip()]
            
            print(f"✅ Successfully fetched {len(clean_symbols)} symbols for {category}.")
            return clean_symbols
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to fetch symbols for {category}: {e}")
            return []

    def disconnect(self):
        if self.td_hist:
            self.td_hist = None
            print("🔌 Disconnected.")