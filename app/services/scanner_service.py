from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional
import asyncio
import traceback

import pandas as pd


class MarketDataAdapter:
    """
    Converts raw history output into a clean DataFrame.
    Cleans time column, removes invalid rows, and normalizes OHLCV names.
    """

    @staticmethod
    def to_dataframe(raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data).copy()

        # Normalize common column names
        rename_map = {}
        for col in df.columns:
            lc = str(col).strip().lower()
            if lc in {"time", "timestamp", "date"}:
                rename_map[col] = "time"
            elif lc in {"open", "high", "low", "close", "volume"}:
                rename_map[col] = lc

        if rename_map:
            df.rename(columns=rename_map, inplace=True)

        # Ensure required columns exist
        for col in ["time", "open", "high", "low", "close", "volume"]:
            if col not in df.columns:
                df[col] = None

        # Safe time conversion - drop invalid times
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df = df.dropna(subset=["time"])

        # Sort by time
        df = df.sort_values("time").reset_index(drop=True)

        # Numeric conversion
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Drop invalid OHLC rows
        df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)

        return df


class MarketScannerService:
    """
    Scanner for current signal detection.
    """

    def __init__(
        self,
        history_service,
        strategy_service,
        max_workers: int = 8,
        scan_bull_threshold: int = 10,
        scan_bear_threshold: int = 10,
        min_candles: int = 100,
        chunk_size: int = 20,
    ):
        self.history_service = history_service
        self.strategy_service = strategy_service
        self.max_workers = max_workers
        self.scan_bull_threshold = scan_bull_threshold
        self.scan_bear_threshold = scan_bear_threshold
        self.min_candles = min_candles
        self.chunk_size = chunk_size

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            if value is None or value == "":
                return default
            if pd.isna(value):
                return default
            return int(float(value))
        except Exception:
            return default

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            if value is None or value == "":
                return default
            if pd.isna(value):
                return default
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _split_rules(value: Any) -> List[str]:
        if value is None or value == "":
            return []
        text = str(value).strip()
        if not text:
            return []
        return [x.strip() for x in text.split(",") if x.strip()]

    @staticmethod
    def _is_bad_symbol(symbol: str) -> bool:
        sym = (symbol or "").upper()
        return (
            "$" in sym
            or "-BE" in sym
            or "-SM" in sym
            or "-BZ" in sym
            or sym.endswith("-X")
        )

    @staticmethod
    def _chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
        if chunk_size <= 0:
            chunk_size = 20
        return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]

    def _decide_signal(
        self,
        bull_score: int,
        bear_score: int,
        direction: str,
    ) -> Optional[str]:
        bull_ok = bull_score >= self.scan_bull_threshold
        bear_ok = bear_score >= self.scan_bear_threshold

        if direction == "bullish":
            return "BUY" if bull_ok else None

        if direction == "bearish":
            return "SELL" if bear_ok else None

        # both
        if bull_ok and bear_ok:
            if bull_score > bear_score:
                return "BUY"
            if bear_score > bull_score:
                return "SELL"
            return None

        if bull_ok:
            return "BUY"
        if bear_ok:
            return "SELL"

        return None

    def scan_symbol(
        self,
        symbol: str,
        duration: str = "5 D",
        bar_size: str = "5 min",
        direction: str = "both",
    ) -> Optional[Dict[str, Any]]:
        try:
            symbol = str(symbol).strip().upper()

            if not symbol or self._is_bad_symbol(symbol):
                return None

            raw_data = self.history_service.get_history(
                symbol=symbol,
                duration=duration,
                bar_size=bar_size,
            )

            if not raw_data:
                return None

            df = MarketDataAdapter.to_dataframe(raw_data)
            if df.empty or len(df) < self.min_candles:
                return None

            processed_df = self.strategy_service.run_strategy(df)
            if processed_df is None or processed_df.empty:
                return None

            # CRITICAL FIX: Convert row to dict immediately to prevent pandas KeyError/NaT issues
            last_row = processed_df.iloc[-1].to_dict()

            bullish_score = self._safe_int(last_row.get("bullish_score", 0))
            bearish_score = self._safe_int(last_row.get("bearish_score", 0))

            signal_type = self._decide_signal(
                bull_score=bullish_score,
                bear_score=bearish_score,
                direction=direction,
            )

            if signal_type is None:
                return None

            if signal_type == "BUY":
                matched_rules_raw = last_row.get("matched_bullish", "")
                score = bullish_score
                max_score = 19
            else:
                matched_rules_raw = last_row.get("matched_bearish", "")
                score = bearish_score
                max_score = 18

            matched_rules = self._split_rules(matched_rules_raw)
            matched_count = len(matched_rules)

            return {
                "symbol": symbol,
                "signal": signal_type,
                "score": score,
                "signal_strength_pct": round((score / max_score) * 100, 2) if max_score else 0.0,
                "matched_count": matched_count,
                "matched_rules": ",".join(matched_rules),
                "time": str(last_row.get("time", "")),
                "open": self._safe_float(last_row.get("open", 0)),
                "high": self._safe_float(last_row.get("high", 0)),
                "low": self._safe_float(last_row.get("low", 0)),
                "close": self._safe_float(last_row.get("close", 0)),
                "volume": self._safe_float(last_row.get("volume", 0)),
                "bullish_score": bullish_score,
                "bearish_score": bearish_score,
                "is_bullish": bool(last_row.get("is_bullish", False)),
                "is_bearish": bool(last_row.get("is_bearish", False)),
            }

        except Exception as e:
            print(f"❌ Scanner failed for {symbol}: {e}")
            return None

    def _scan_batch(
        self,
        batch_symbols: List[str],
        duration: str,
        bar_size: str,
        direction: str,
    ) -> List[Dict[str, Any]]:
        """
        Scan one chunk of symbols concurrently.
        """
        batch_results: List[Dict[str, Any]] = []
        if not batch_symbols:
            return batch_results

        max_workers = min(self.max_workers, max(1, len(batch_symbols)))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(self.scan_symbol, sym, duration, bar_size, direction): sym
                for sym in batch_symbols
            }

            for future in as_completed(future_map):
                sym = future_map[future]
                try:
                    result = future.result()
                    if result:
                        batch_results.append(result)
                except Exception as e:
                    print(f"❌ Scanner worker crashed for {sym}: {e}")

        batch_results.sort(key=lambda x: (-int(x.get("score", 0) or 0), x.get("symbol", "")))
        return batch_results

    def scan_symbols_chunked(
        self,
        symbols: List[str],
        duration: str = "2 Y",
        bar_size: str = "5 min",
        direction: str = "both",
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Scan in chunks and return chunk-by-chunk results synchronously.
        """
        size = chunk_size or self.chunk_size

        clean_symbols = []
        seen = set()
        for s in symbols or []:
            sym = str(s).strip().upper()
            if sym and sym not in seen and not self._is_bad_symbol(sym):
                seen.add(sym)
                clean_symbols.append(sym)

        if not clean_symbols:
            return {
                "total_symbols": 0,
                "scanned_symbols": 0,
                "bullish_count": 0,
                "bearish_count": 0,
                "results": [],
                "chunks": [],
                "scan_bull_threshold": self.scan_bull_threshold,
                "scan_bear_threshold": self.scan_bear_threshold,
                "chunk_size": size,
            }

        chunks = self._chunk_list(clean_symbols, size)
        all_results: List[Dict[str, Any]] = []
        chunk_payloads: List[Dict[str, Any]] = []

        for idx, batch in enumerate(chunks, start=1):
            batch_results = self._scan_batch(batch, duration, bar_size, direction)
            all_results.extend(batch_results)

            all_results.sort(key=lambda x: (-int(x.get("score", 0) or 0), x.get("symbol", "")))

            chunk_payloads.append({
                "chunk_index": idx,
                "chunk_total": len(chunks),
                "chunk_symbols": len(batch),
                "chunk_results": batch_results,
                "chunk_bullish_count": sum(1 for r in batch_results if r.get("signal") == "BUY"),
                "chunk_bearish_count": sum(1 for r in batch_results if r.get("signal") == "SELL"),
                "completed_symbols": min(idx * size, len(clean_symbols)),
                "total_symbols": len(clean_symbols),
            })

        return {
            "total_symbols": len(clean_symbols),
            "scanned_symbols": len(clean_symbols),
            "bullish_count": sum(1 for r in all_results if r.get("signal") == "BUY"),
            "bearish_count": sum(1 for r in all_results if r.get("signal") == "SELL"),
            "results": all_results,
            "chunks": chunk_payloads,
            "scan_bull_threshold": self.scan_bull_threshold,
            "scan_bear_threshold": self.scan_bear_threshold,
            "chunk_size": size,
        }

    async def stream_scan_symbols(
        self,
        symbols: List[str],
        duration: str = "2 Y",
        bar_size: str = "5 min",
        direction: str = "both",
        chunk_size: Optional[int] = None,
    ):
        """
        Async generator for SSE streaming.
        """
        size = chunk_size or self.chunk_size

        clean_symbols = []
        seen = set()
        for s in symbols or []:
            sym = str(s).strip().upper()
            if sym and sym not in seen and not self._is_bad_symbol(sym):
                seen.add(sym)
                clean_symbols.append(sym)

        if not clean_symbols:
            yield {
                "event": "done",
                "total_symbols": 0,
                "scanned_symbols": 0,
                "bullish_count": 0,
                "bearish_count": 0,
                "results": [],
                "scan_bull_threshold": self.scan_bull_threshold,
                "scan_bear_threshold": self.scan_bear_threshold,
                "chunk_size": size,
            }
            return

        chunks = self._chunk_list(clean_symbols, size)
        all_results: List[Dict[str, Any]] = []

        yield {
            "event": "start",
            "total_symbols": len(clean_symbols),
            "chunk_total": len(chunks),
            "chunk_size": size,
        }

        # Fix: Run the ThreadPool batch inside the main async loop properly
        loop = asyncio.get_running_loop()
        
        for idx, batch in enumerate(chunks, start=1):
            batch_results = await loop.run_in_executor(
                None, 
                self._scan_batch, 
                batch, duration, bar_size, direction
            )

            all_results.extend(batch_results)
            all_results.sort(key=lambda x: (-int(x.get("score", 0) or 0), x.get("symbol", "")))

            yield {
                "event": "chunk",
                "chunk_index": idx,
                "chunk_total": len(chunks),
                "chunk_symbols": len(batch),
                "chunk_results": batch_results,
                "chunk_bullish_count": sum(1 for r in batch_results if r.get("signal") == "BUY"),
                "chunk_bearish_count": sum(1 for r in batch_results if r.get("signal") == "SELL"),
                "completed_symbols": min(idx * size, len(clean_symbols)),
                "total_symbols": len(clean_symbols),
                "results_so_far": all_results,
            }

        yield {
            "event": "done",
            "total_symbols": len(clean_symbols),
            "scanned_symbols": len(clean_symbols),
            "bullish_count": sum(1 for r in all_results if r.get("signal") == "BUY"),
            "bearish_count": sum(1 for r in all_results if r.get("signal") == "SELL"),
            "results": all_results,
            "scan_bull_threshold": self.scan_bull_threshold,
            "scan_bear_threshold": self.scan_bear_threshold,
            "chunk_size": size,
        }