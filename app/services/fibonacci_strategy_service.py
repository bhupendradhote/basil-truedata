import pandas as pd
import pandas_ta as ta
import numpy as np
import traceback

class AutoFibonacciService:
    def __init__(self, lookback_periods=200):
        # 200-candle depth auto-adjusts based on the timeframe of the incoming dataframe
        self.lookback = lookback_periods

    def run_strategy(self, df: pd.DataFrame) -> dict:
        if df is None or df.empty:
            return {"error": "Empty DataFrame"}

        try:
            # Standardize Time Column
            time_col = next((col for col in ['time', 'timestamp', 'date', 'Time', 'Date'] if col in df.columns), None)
            if not time_col:
                return {"error": f"Could not find time column. Available columns: {df.columns}"}

            df['time'] = pd.to_datetime(df[time_col])
            df.set_index('time', inplace=True)

            # Safely map and convert required columns
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col not in df.columns:
                    alt_col = next((c for c in df.columns if c.lower() == col or c.lower() == col[0]), None)
                    if alt_col:
                        df[col] = df[alt_col]
                    else:
                        if col == 'volume':
                            df['volume'] = 0
                        else:
                            return {"error": f"Missing required price column: '{col}'"}
                
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Drop rows where critical price data is NaN, but keep volume even if 0
            df.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)

            if len(df) < self.lookback:
                print(f"⚠️ Warning: DataFrame has {len(df)} rows, which is less than the lookback of {self.lookback}.")

            if df.empty:
                return {"error": "DataFrame became empty after dropping NaNs."}

            # 1. Calculate EMAs
            df.ta.ema(length=9, append=True)
            df.ta.ema(length=21, append=True)

            # 2. Calculate Auto Fibonacci Levels (Based on recent lookback window)
            recent_df = df.tail(self.lookback)
            swing_high = recent_df['high'].max()
            swing_low = recent_df['low'].min()
            diff = swing_high - swing_low

            # Standard Retracement Levels
            fib_levels = {
                "0.000 (Low)": swing_low,
                "0.236": swing_low + (0.236 * diff),
                "0.382": swing_low + (0.382 * diff),
                "0.500": swing_low + (0.500 * diff),
                "0.618": swing_low + (0.618 * diff),
                "0.786": swing_low + (0.786 * diff),
                "1.000 (High)": swing_high
            }

            # 3. Generate Basic Crossover Signals (9 EMA crossing 21 EMA)
            df['EMA_9'] = df.get('EMA_9', np.nan)
            df['EMA_21'] = df.get('EMA_21', np.nan)
            
            df['signal_buy'] = (df['EMA_9'] > df['EMA_21']) & (df['EMA_9'].shift(1) <= df['EMA_21'].shift(1))
            df['signal_sell'] = (df['EMA_9'] < df['EMA_21']) & (df['EMA_9'].shift(1) >= df['EMA_21'].shift(1))

            df.reset_index(inplace=True)
            df['time'] = df['time'].astype(str)
            df = df.replace([np.nan, np.inf, -np.inf], None)

            # Package output
            chart_data = df[['time', 'open', 'high', 'low', 'close', 'volume', 'EMA_9', 'EMA_21', 'signal_buy', 'signal_sell']].to_dict(orient="records")

            return {
                "data": chart_data,
                "fib_levels": fib_levels,
                "swing_high": swing_high,
                "swing_low": swing_low
            }

        except Exception as e:
            traceback.print_exc()
            return {"error": str(e)}