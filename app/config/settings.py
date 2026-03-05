import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    TD_USERNAME = os.getenv("TD_USERNAME")
    TD_PASSWORD = os.getenv("TD_PASSWORD")
    TD_REALTIME_PORT = int(os.getenv("TD_REALTIME_PORT", 8084))
    TD_URL = os.getenv("TD_URL")
    
    