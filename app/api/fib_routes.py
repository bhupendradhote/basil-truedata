from fastapi import APIRouter, HTTPException, Query
import pandas as pd
from app.services.historical_data_service import HistoricalDataService
from app.services.fibonacci_strategy_service import AutoFibonacciService

router = APIRouter()
history_service = HistoricalDataService()
fib_service = AutoFibonacciService(lookback_periods=200)

@router.get("/strategy/autofib/{symbol}")
async def run_autofib_strategy(
    symbol: str, 
    duration: str = Query(..., description="Duration string (e.g. '15 D', '1 Y')"), 
    bar_size: str = Query(..., description="Candle size (e.g. '5 min', 'EOD')")
):
    
    # Fetch data based on dynamic timeframe and duration
    raw_data = history_service.get_history(symbol=symbol.upper(), duration=duration, bar_size=bar_size)
    
    if not raw_data:
        raise HTTPException(status_code=404, detail=f"No data found for {symbol} on {bar_size} timeframe.")
        
    df = pd.DataFrame(raw_data)
    result = fib_service.run_strategy(df)
    
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])

    # Extract crossover signals for the chart
    signals = []
    for row in result["data"]:
        if row.get('signal_buy'):
            signals.append({
                "time": row['time'], "position": "belowBar", "color": "#2f81f7", 
                "shape": "arrowUp", "text": "9x21 Bull"
            })
        elif row.get('signal_sell'):
            signals.append({
                "time": row['time'], "position": "aboveBar", "color": "#f85149", 
                "shape": "arrowDown", "text": "9x21 Bear"
            })

    return {
        "symbol": symbol.upper(),
        "bar_size": bar_size,
        "total_candles": len(result["data"]),
        "data": result["data"],
        "fib_levels": result["fib_levels"],
        "signals": signals
    }