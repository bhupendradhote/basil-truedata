import pandas as pd
import pandas_ta as ta
import numpy as np
import traceback

class InstitutionalStrategyService:
    def __init__(self):
        # Strict 20/20 confluence for triggers
        self.MIN_CONFLUENCE = 20

    def run_strategy(self, df_5m: pd.DataFrame) -> pd.DataFrame:
        
        if df_5m is None or df_5m.empty:
            print("❌ Strategy Error: Empty DataFrame passed to strategy.")
            return df_5m

        try:
            print(f"⚙️ Running Strategy on {len(df_5m)} candles...")
            
            # 1. Standardize Time Column
            time_col = next((col for col in ['time', 'timestamp', 'date', 'Time', 'Date'] if col in df_5m.columns), None)
            if not time_col:
                print(f"❌ Strategy Error: Could not find time column. Available columns: {df_5m.columns}")
                return None

            df_5m['time'] = pd.to_datetime(df_5m[time_col])
            df_5m.set_index('time', inplace=True)
            
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col not in df_5m.columns:
                    alt_col = next((c for c in df_5m.columns if c.lower() == col or c.lower() == col[0]), None)
                    if alt_col:
                        df_5m[col] = df_5m[alt_col]
                    else:
                        print(f"❌ Strategy Error: Missing required column '{col}'.")
                        return None
                        
                df_5m[col] = pd.to_numeric(df_5m[col], errors='coerce')
                
            df_5m.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)

            # 2. Resample to Higher Timeframes
            agg_dict = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
            
            df_15m = df_5m.resample('15min').agg(agg_dict).dropna()
            df_1h = df_5m.resample('1h').agg(agg_dict).dropna()
            df_4h = df_5m.resample('4h').agg(agg_dict).dropna()
            df_1d = df_5m.resample('D').agg(agg_dict).dropna()
            df_1w = df_5m.resample('W-MON').agg(agg_dict).dropna()

            # 3. Calculate Indicators
            # --- 5 MINUTE ---
            df_5m.ta.adx(length=14, append=True) 
            df_5m.ta.ema(length=20, append=True) 
            df_5m.ta.ema(length=50, append=True) 
            df_5m.ta.vwap(anchor="D", append=True)           
            df_5m.ta.obv(append=True)             
            if 'OBV' in df_5m.columns:  
                df_5m['OBV_EMA_5'] = ta.ema(df_5m['OBV'], length=5)

            # First 5m High/Low for the day
            df_5m['date_only'] = df_5m.index.date
            try:
                opening_range = df_5m.between_time('09:15', '09:20')
                first_5m_highs = opening_range.groupby('date_only')['high'].max()
                first_5m_lows = opening_range.groupby('date_only')['low'].min()
                df_5m['first_5m_high'] = df_5m['date_only'].map(first_5m_highs)
                df_5m['first_5m_low'] = df_5m['date_only'].map(first_5m_lows)
            except Exception:
                df_5m['first_5m_high'] = df_5m['high']
                df_5m['first_5m_low'] = df_5m['low']

            # Compute additional derived indicators on HTFs
            if not df_1d.empty:
                df_1d['4day_max_close'] = df_1d['close'].shift(1).rolling(4).max()
                df_1d['prev_low'] = df_1d['low'].shift(1)
            if not df_1w.empty:
                df_1w['prev_high'] = df_1w['high'].shift(1)
                df_1w['prev_low'] = df_1w['low'].shift(1)

            # --- HIGHER TIMEFRAMES ---
            df_15m.ta.macd(fast=12, slow=26, signal=9, append=True) 
            ha_15m = ta.ha(df_15m['open'], df_15m['high'], df_15m['low'], df_15m['close'])
            df_15m['HA_EMA_5'] = ta.ema(ha_15m['HA_close'], length=5)
            df_15m['HA_EMA_9'] = ta.ema(ha_15m['HA_close'], length=9)

            df_1h.ta.rsi(length=14, append=True) 
            df_1h.ta.ema(length=9, append=True)  
            df_1h.ta.ema(length=50, append=True) 

            df_4h.ta.stoch(append=True)        
            df_4h.ta.ema(length=9, append=True)  
            df_4h.ta.ema(length=50, append=True) 

            df_1d.ta.ema(length=9, append=True)  
            df_1d.ta.ema(length=50, append=True) 

            df_1w.ta.ema(length=9, append=True)  
            df_1w.ta.ema(length=50, append=True) 

            # 4. Forward-Fill Higher Timeframes down to 5-Minute Index
            def align(htf_df, cols, prefix):
                for col in cols:
                    if col in htf_df.columns:
                        df_5m[f'{prefix}_{col}'] = htf_df[col].reindex(df_5m.index, method='ffill')
                    else:
                        df_5m[f'{prefix}_{col}'] = np.nan

            align(df_15m, ['MACD_12_26_9', 'MACDs_12_26_9', 'HA_EMA_5', 'HA_EMA_9'], '15m')
            align(df_1h, ['RSI_14', 'EMA_9', 'EMA_50'], '1h')
            align(df_4h, ['STOCHk_14_3_3', 'STOCHd_14_3_3', 'EMA_9', 'EMA_50'], '4h')
            align(df_1d, ['EMA_9', 'EMA_50', 'high', 'low', 'close', '4day_max_close', 'prev_low'], '1d')
            align(df_1w, ['EMA_9', 'EMA_50', 'high', 'low', 'prev_high', 'prev_low'], '1w')

            df_5m['18_candle_high'] = df_5m['high'].shift(1).rolling(18).max()
            df_5m['18_candle_low'] = df_5m['low'].shift(1).rolling(18).min()
            
            zero_series = pd.Series(0, index=df_5m.index)

            # 5. Bullish Conditions
            b1 = df_5m.get('ADX_14', zero_series) > 30
            b2 = df_5m['close'] > df_5m.get('1d_4day_max_close', df_5m['close'])
            b3 = df_5m['high'] > df_5m.get('first_5m_high', df_5m['high'])
            b4 = df_5m['close'] >= df_5m.get('18_candle_high', df_5m['close'])
            b5 = df_5m.get('4h_STOCHk_14_3_3', zero_series) > df_5m.get('4h_STOCHd_14_3_3', zero_series)
            b6 = df_5m['close'] > df_5m.get('4h_EMA_9', df_5m['close'])
            b7 = df_5m['close'].shift(1) > df_5m.get('4h_EMA_9', df_5m['close']).shift(1)
            b8 = df_5m['close'] > df_5m.get('4h_EMA_50', df_5m['close'])
            b9 = df_5m.get('1h_RSI_14', zero_series) > 60
            b10 = df_5m.get('1h_EMA_9', zero_series) > df_5m.get('1h_EMA_50', zero_series)
            b11 = df_5m.get('15m_MACD_12_26_9', zero_series) > df_5m.get('15m_MACDs_12_26_9', zero_series)
            b12 = df_5m.get('15m_HA_EMA_5', zero_series) > df_5m.get('15m_HA_EMA_9', zero_series)
            b13 = df_5m['close'] > df_5m.get('VWAP_D', df_5m['close'])
            b14 = df_5m.get('EMA_20', zero_series) > df_5m.get('EMA_50', zero_series)
            b15 = df_5m.get('OBV', zero_series) > df_5m.get('OBV_EMA_5', zero_series)
            
            hl_range = df_5m['high'] - df_5m['low']
            b16 = (df_5m['close'] - df_5m['open']).abs() > (0.60 * hl_range)
            
            b17 = df_5m.get('1w_EMA_9', zero_series) > df_5m.get('1w_EMA_50', zero_series)
            b18 = df_5m.get('1d_close', df_5m['close']) > df_5m.get('1d_EMA_50', df_5m['close'])
            b19 = df_5m.get('1d_close', df_5m['close']) > df_5m.get('1d_EMA_9', df_5m['close'])
            b20 = df_5m['close'] > df_5m.get('1w_prev_high', df_5m['close'])

            bullish_conditions = [b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18, b19, b20]
            
            df_5m['matched_bullish'] = ""
            for i, cond in enumerate(bullish_conditions):
                mask = cond.fillna(False).astype(bool)
                df_5m.loc[mask, 'matched_bullish'] = df_5m.loc[mask, 'matched_bullish'] + str(i + 1) + ","
            
            df_5m['matched_bullish'] = df_5m['matched_bullish'].str.rstrip(',')
            df_5m['bullish_score'] = sum(cond.fillna(False).astype(int) for cond in bullish_conditions)

            # 6. Bearish Conditions
            s1 = df_5m.get('ADX_14', zero_series) > 30
            s2 = df_5m['low'] < df_5m.get('first_5m_low', df_5m['low'])
            s3 = df_5m['close'] <= df_5m.get('18_candle_low', df_5m['close'])
            s4 = df_5m['close'] < df_5m.get('1d_prev_low', df_5m['close'])
            s5 = df_5m.get('4h_STOCHk_14_3_3', zero_series) < df_5m.get('4h_STOCHd_14_3_3', zero_series)
            s6 = df_5m['close'] < df_5m.get('4h_EMA_9', df_5m['close'])
            s7 = df_5m['close'].shift(1) < df_5m.get('4h_EMA_9', df_5m['close']).shift(1)
            s8 = df_5m['close'] < df_5m.get('4h_EMA_50', df_5m['close'])
            s9 = df_5m.get('1h_RSI_14', zero_series) < 40
            s10 = df_5m.get('1h_EMA_9', zero_series) < df_5m.get('1h_EMA_50', zero_series)
            s11 = df_5m.get('15m_MACD_12_26_9', zero_series) < df_5m.get('15m_MACDs_12_26_9', zero_series)
            s12 = df_5m.get('15m_HA_EMA_5', zero_series) < df_5m.get('15m_HA_EMA_9', zero_series)
            s13 = df_5m['close'] < df_5m.get('VWAP_D', df_5m['close'])
            s14 = df_5m.get('EMA_20', zero_series) < df_5m.get('EMA_50', zero_series)
            s15 = df_5m.get('OBV', zero_series) < df_5m.get('OBV_EMA_5', zero_series)
            
            s16 = (df_5m['open'] - df_5m['close']).abs() > (0.55 * hl_range)
            
            s17 = df_5m.get('1w_EMA_9', zero_series) < df_5m.get('1w_EMA_50', zero_series)
            s18 = df_5m.get('1d_close', df_5m['close']) < df_5m.get('1d_EMA_50', df_5m['close'])
            s19 = df_5m.get('1d_close', df_5m['close']) < df_5m.get('1d_EMA_9', df_5m['close'])
            s20 = df_5m['close'] < df_5m.get('1w_prev_low', df_5m['close'])

            bearish_conditions = [s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20]
            
            df_5m['matched_bearish'] = ""
            for i, cond in enumerate(bearish_conditions):
                mask = cond.fillna(False).astype(bool)
                df_5m.loc[mask, 'matched_bearish'] = df_5m.loc[mask, 'matched_bearish'] + str(i + 1) + ","
            
            df_5m['matched_bearish'] = df_5m['matched_bearish'].str.rstrip(',')
            df_5m['bearish_score'] = sum(cond.fillna(False).astype(int) for cond in bearish_conditions)

            # Apply Threshold Confluence
            df_5m['is_bullish'] = df_5m['bullish_score'] >= self.MIN_CONFLUENCE
            df_5m['is_bearish'] = df_5m['bearish_score'] >= self.MIN_CONFLUENCE

            df_5m.reset_index(inplace=True)
            df_5m['time'] = df_5m['time'].astype(str)
            df_5m.drop(columns=['date_only'], inplace=True, errors='ignore')
            df_5m = df_5m.replace([np.nan, np.inf, -np.inf], None)
            
            print(f"✅ Strategy Complete. Highest Confluence Score Found: Bullish({df_5m['bullish_score'].max()}), Bearish({df_5m['bearish_score'].max()})")
            return df_5m

        except Exception as e:
            print(f"\n❌ FATAL STRATEGY ERROR: {e}")
            traceback.print_exc() 
            return None