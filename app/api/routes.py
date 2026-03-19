from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
import pandas as pd
import asyncio
import json

from app.services.historical_data_service import HistoricalDataService
from app.services.symbols_service import TrueDataService
from app.services.strategy_service import InstitutionalStrategyService
from app.services.scanner_service import MarketScannerService, MarketDataAdapter

router = APIRouter()

td_service = TrueDataService()
history_service = HistoricalDataService()
strategy_service = InstitutionalStrategyService()
scanner_service = MarketScannerService(
    history_service=history_service,
    strategy_service=strategy_service,
    max_workers=8,
    scan_bull_threshold=12,
    scan_bear_threshold=12,
    chunk_size=20,
)


@router.get("/symbols/{category}")
async def get_symbols(category: str):
    symbols = td_service.get_all_symbols(category=category.upper())
    if not symbols:
        raise HTTPException(status_code=404, detail=f"Failed to fetch or invalid category: {category}")
    return {"category": category.upper(), "total_count": len(symbols), "symbols": symbols}


@router.get("/history/{symbol}")
async def get_symbol_history(
    symbol: str,
    duration: str = Query("1 D", description="Duration like '1 D', '5 D'"),
    bar_size: str = Query("1 min", description="Candle size like '1 min', '5 min', 'EOD'")
):
    data = history_service.get_history(symbol=symbol.upper(), duration=duration, bar_size=bar_size)
    if not data:
        raise HTTPException(status_code=404, detail=f"No data found for {symbol}.")
    return {
        "symbol": symbol.upper(),
        "duration": duration,
        "bar_size": bar_size,
        "total_candles": len(data),
        "data": data
    }


@router.get("/strategy/{symbol}")
async def run_strategy(symbol: str, duration: str = Query("1 Y")):
    raw_data = history_service.get_history(symbol=symbol.upper(), duration=duration, bar_size="5 min")

    if not raw_data:
        raise HTTPException(status_code=404, detail=f"No data found for {symbol}.")

    df = MarketDataAdapter.to_dataframe(raw_data)
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No valid candle data found for {symbol}.")

    processed_df = strategy_service.run_strategy(df)

    if processed_df is None or processed_df.empty:
        raise HTTPException(status_code=500, detail="Strategy processing failed.")

    signals = []
    for _, row in processed_df.iterrows():
        if pd.notna(row.get("is_bullish")) and row["is_bullish"] == True:
            score = int(row["bullish_score"])
            signals.append({
                "time": row["time"],
                "position": "belowBar",
                "color": "#3fb950",
                "shape": "arrowUp",
                "text": "BUY",
                "score": score
            })
        elif pd.notna(row.get("is_bearish")) and row["is_bearish"] == True:
            score = int(row["bearish_score"])
            signals.append({
                "time": row["time"],
                "position": "aboveBar",
                "color": "#f85149",
                "shape": "arrowDown",
                "text": "SELL",
                "score": score
            })

    final_data = processed_df[["time", "open", "high", "low", "close", "volume"]].to_dict(orient="records")

    if not processed_df.empty:
        last_row = processed_df.iloc[-1]
        current_status = {
            "bullish_score": int(last_row["bullish_score"]) if pd.notna(last_row["bullish_score"]) else 0,
            "bearish_score": int(last_row["bearish_score"]) if pd.notna(last_row["bearish_score"]) else 0,
            "is_bullish": bool(last_row["is_bullish"]),
            "is_bearish": bool(last_row["is_bearish"]),
            "matched_bullish": str(last_row["matched_bullish"]) if "matched_bullish" in last_row else "",
            "matched_bearish": str(last_row["matched_bearish"]) if "matched_bearish" in last_row else ""
        }
    else:
        current_status = {
            "bullish_score": 0,
            "bearish_score": 0,
            "is_bullish": False,
            "is_bearish": False,
            "matched_bullish": "",
            "matched_bearish": ""
        }

    return {
        "symbol": symbol.upper(),
        "total_candles": len(final_data),
        "total_signals": len(signals),
        "data": final_data,
        "signals": signals,
        "current_status": current_status
    }


@router.get("/scanner/nse-eq/current-signals")
async def scan_nse_eq_current_signals(
    duration: str = Query("2 Y", description="History window to scan"),
    bar_size: str = Query("5 min", description="Candle size to scan"),
    direction: str = Query("both", description="both | bullish | bearish"),
    chunk_size: int = Query(20, description="Number of symbols per chunk"),
):
    if direction not in {"both", "bullish", "bearish"}:
        raise HTTPException(status_code=400, detail="direction must be one of: both, bullish, bearish")

    symbols = td_service.get_all_symbols(category="NSE_EQ")
    if not symbols:
        raise HTTPException(status_code=404, detail="Could not load NSE_EQ symbols.")

    result = await asyncio.to_thread(
        scanner_service.scan_symbols_chunked,
        symbols,
        duration,
        bar_size,
        direction,
        chunk_size,
    )

    return {
        "category": "NSE_EQ",
        "duration": duration,
        "bar_size": bar_size,
        "direction": direction,
        **result
    }


@router.get("/scanner/nse-eq/current-signals/stream")
async def scan_nse_eq_current_signals_stream(
    duration: str = Query("2 Y", description="History window to scan"),
    bar_size: str = Query("5 min", description="Candle size to scan"),
    direction: str = Query("both", description="both | bullish | bearish"),
    chunk_size: int = Query(20, description="Number of symbols per chunk"),
):
    if direction not in {"both", "bullish", "bearish"}:
        raise HTTPException(status_code=400, detail="direction must be one of: both, bullish, bearish")

    symbols = td_service.get_all_symbols(category="NSE_EQ")
    if not symbols:
        raise HTTPException(status_code=404, detail="Could not load NSE_EQ symbols.")

    async def event_generator():
        async for payload in _stream_scan(symbols, duration, bar_size, direction, chunk_size):
            yield f"data: {json.dumps(payload, default=str)}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


async def _stream_scan(symbols, duration, bar_size, direction, chunk_size):
    async for payload in scanner_service.stream_scan_symbols(
        symbols=symbols,
        duration=duration,
        bar_size=bar_size,
        direction=direction,
        chunk_size=chunk_size,
    ):
        yield payload