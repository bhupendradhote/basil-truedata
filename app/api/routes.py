# app/api/routes.py
from fastapi import APIRouter, HTTPException, Query
from app.services.historical_data_service import HistoricalDataService
from app.services.symbols_service import TrueDataService

# Initialize services and router
router = APIRouter()

# We can initialize the services here  
td_service = TrueDataService()
history_service = HistoricalDataService()

# Route to get symbols based on category like EQ, INDICES
@router.get("/symbols/{category}")
async def get_symbols(category: str):
    """
    API Endpoint to fetch symbols based on category.
    Returns JSON format perfect for mobile app consumption.
    """
    symbols = td_service.get_all_symbols(category=category.upper())
    
    if not symbols:
        raise HTTPException(status_code=404, detail=f"Failed to fetch or invalid category: {category}")
        
    return {
        "category": category.upper(),
        "total_count": len(symbols),
        "symbols": symbols
    }
    
# Route to get historical data by duration
    
@router.get("/history/{symbol}")
async def get_symbol_history(
    symbol: str, 
    duration: str = Query("1 D", description="Duration like '1 D', '5 D'"), 
    bar_size: str = Query("1 min", description="Candle size like '1 min', '5 min', 'EOD'")
):
    """
    API Endpoint to fetch historical candlestick data for a specific symbol.
    """
    data = history_service.get_history(symbol=symbol.upper(), duration=duration, bar_size=bar_size)
    
    if not data:
        raise HTTPException(status_code=404, detail=f"No data found for {symbol}. Check if market is open or symbol is correct.")
        
    return {
        "symbol": symbol.upper(),
        "duration": duration,
        "bar_size": bar_size,
        "total_candles": len(data),
        "data": data
    }
    
# Route to get historical data by specific date range