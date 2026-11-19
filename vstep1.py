import asyncio
import json
import math
import time
from collections import deque
from datetime import datetime

import aiohttp
from rich import box
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from yarl import URL

# =====================================================================
# DEFAULT SETTINGS (Sesuai Konfigurasi Default)
# =====================================================================
DEFAULT_TIMEFRAME = "1m"
WARMUP_KLINES = 1500
DEFAULT_LINE_MODE = "Anchor"       # 1 = Anchor, 2 = Without Anchor
DEFAULT_VOL_TYPE = "Atr"           # 1 = Stdev, 2 = Atr
DEFAULT_LOOKBACK = 200             # min 2
DEFAULT_MULTIPLIER = 4.0           # contoh: 4.0

FAPI_TIME_URL = "https://fapi.binance.com/fapi/v1/time"
FAPI_REST_URL = "https://fapi.binance.com/fapi/v1/klines"

WSS_ENDPOINTS = [
    "wss://fstream.binance.com/ws",
    "wss://fstream.binance.com:9443/ws",
]

INTERVAL_MS_MAP = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}

STATE = {
    "mode": "INIT...",
    "total_updates": 0,
    "last_ws_msg_ts": 0.0,
    "server_offset_ms": 0,
    "used_weight_1m": 0,
}


def get_binance_now_ms() -> int:
    return int(time.time() * 1000) + STATE["server_offset_ms"]


class VolStepEngine:
    """
    Replikasi presisi logika Pine Script VolStep S/R menggunakan math manual
    (tanpa library TA) dengan Dual-Buffer agar transisi bar close instan & akurat.
    """

    def __init__(
        self,
        symbol: str,
        line_mode: str = DEFAULT_LINE_MODE,
        vol_type: str = DEFAULT_VOL_TYPE,
        length: int = DEFAULT_LOOKBACK,
        vol_mult: float = DEFAULT_MULTIPLIER,
    ):
        self.symbol = symbol.upper()
        self.line_mode = line_mode
        self.vol_type = vol_type
        self.length = length
        self.vol_mult = vol_mult

        # Histori bar yang sudah close (100% dari official kline close)
        self.closed_closes = deque(maxlen=length)
        self.tr_warmup = []
        self.confirmed_rma = None
        self.prev_closed_close = None
        self.closed_bars_count = 0
        self.last_closed_open_time = -1
        self.last_reconcile_ts = 0.0

        # Presisi desimal otomatis dari feed harga Binance
        self.price_decimals = 2

        # State SR Saat Ini (Confirmed saat candle close)
        self.confirmed_sr = None
        self.cur_level = None
        self.prev_level = None
        self.cur_pct = None
        self.cur_dir = None
        self.confirmed_vol_value = None

        # State Live Tick / Candle Berjalan (Dual-Buffer)
        self.live_open = None
        self.live_high = None
        self.live_low = None
        self.live_close = None
        self.tick_dir = 0  # 1 = naik, -1 = turun, 0 = tetap
        self.candle_open_time = 0
        self.candle_close_time = 0

        # State SR Next (Muncul-Hilang secara real-time di bar berjalan)
        self.live_vol_value = None
        self.next_sr_active = False
        self.next_sr_dir = None
        self.next_sr_price = None
        self.next_sr_pct = None

        # Sinyal Step Pine Script & Callback Hooks
        self.step_triggered = False
        self.step_direction = 0  # 1: UP (LONG), -1: DOWN (SHORT)
        self.on_candle_close = None
        self.on_live_tick = None

    def _detect_precision(self, raw_str: str):
        if not raw_str or "." not in raw_str:
            return
        trimmed = raw_str.rstrip("0")
        if "." in trimmed:
            dec = len(trimmed.split(".")[1])
            if dec > self.price_decimals:
                self.price_decimals = min(dec, 8)

    def fmt(self, val: float) -> str:
        if val is None:
            return "-"
        return f"{val:,.{self.price_decimals}f}"

    # -----------------------------------------------------------------
    # 1. MATH MANUAL: Pengganti ta.stdev() dan ta.atr()
    # -----------------------------------------------------------------
    @staticmethod
    def _calc_stdev_manual(values: list) -> float:
        n = len(values)
        if n == 0:
            return None
        mean = sum(values) / n
        variance = sum((x - mean) ** 2 for x in values) / n
        return math.sqrt(variance)

    @staticmethod
    def _calc_true_range(high: float, low: float, prev_close: float) -> float:
        if prev_close is None:
            return high - low
        return max(high - low, abs(high - prev_close), abs(low - prev_close))

    def _compute_volatility_closed(self, high: float, low: float, close: float) -> float:
        tr = self._calc_true_range(high, low, self.prev_closed_close)
        self.prev_closed_close = close
        self.closed_closes.append(close)
        self.closed_bars_count += 1

        if self.closed_bars_count < self.length:
            self.tr_warmup.append(tr)
            self.confirmed_rma = None
        elif self.closed_bars_count == self.length:
            self.tr_warmup.append(tr)
            self.confirmed_rma = sum(self.tr_warmup) / self.length
        else:
            self.confirmed_rma = (tr + (self.length - 1) * self.confirmed_rma) / self.length

        if self.closed_bars_count < self.length:
            return None

        if self.vol_type == "Stdev":
            raw_vol = self._calc_stdev_manual(list(self.closed_closes))
        else:
            raw_vol = self.confirmed_rma

        return raw_vol * self.vol_mult if raw_vol is not None else None

    def _compute_volatility_live(self, high: float, low: float, close: float) -> float:
        if self.closed_bars_count < self.length - 1:
            return None

        if self.vol_type == "Stdev":
            recent = list(self.closed_closes)[-(self.length - 1):] if self.length > 1 else []
            window = recent + [close]
            if len(window) < self.length:
                return None
            raw_vol = self._calc_stdev_manual(window)
        else:
            live_tr = self._calc_true_range(high, low, self.prev_closed_close)
            if self.closed_bars_count == self.length - 1:
                raw_vol = (sum(self.tr_warmup) + live_tr) / self.length
            else:
                raw_vol = (live_tr + (self.length - 1) * self.confirmed_rma) / self.length

        return raw_vol * self.vol_mult if raw_vol is not None else None

    # -----------------------------------------------------------------
    # 2. LOGIKA CANDLE CLOSE RESMI & DUAL-BUFFER LIVE TICK
    # -----------------------------------------------------------------
    def process_closed_candle(
        self,
        open_p: float,
        high_p: float,
        low_p: float,
        close_p: float,
        raw_close_str: str = None,
        open_time: int = None,
        close_time: int = None,
        interval_ms: int = None,
        is_warmup: bool = False,
    ):
        """
        Mengunci candle yang sudah resmi tutup (dari WSS x=True atau Fast REST Trigger).
        Tidak menimpa live_close jika candle baru sudah lebih dulu menerima tick.
        """
        if open_time is not None:
            if open_time <= self.last_closed_open_time:
                return
            self.last_closed_open_time = open_time

        if raw_close_str:
            self._detect_precision(raw_close_str)

        if self.confirmed_sr is None:
            self.confirmed_sr = close_p

        vol_value = self._compute_volatility_closed(high_p, low_p, close_p)
        self.confirmed_vol_value = vol_value

        is_step = False
        step_dir = 0
        if vol_value is not None:
            if self.line_mode == "Anchor":
                step_level = close_p
            else:
                direction = 1 if close_p > self.confirmed_sr else -1
                step_level = self.confirmed_sr + direction * vol_value

            if abs(close_p - self.confirmed_sr) > vol_value:
                new_level = step_level
                is_step = True
                step_dir = 1 if new_level > self.confirmed_sr else -1
            else:
                new_level = self.confirmed_sr
        else:
            new_level = self.confirmed_sr

        self.step_triggered = is_step
        self.step_direction = step_dir

        # barstate.isconfirmed: kunci newLevel ke confirmedSr (SR Saat Ini)
        self.confirmed_sr = new_level
        active_sr = self.confirmed_sr

        is_price_step = (self.cur_level is None) or (active_sr != self.cur_level)
        if is_price_step:
            if self.cur_level is not None:
                self.prev_level = self.cur_level
            self.cur_level = active_sr

            if self.prev_level is not None and self.prev_level != 0:
                self.cur_pct = ((active_sr - self.prev_level) / self.prev_level) * 100.0
                self.cur_dir = "LONG" if self.cur_pct >= 0 else "SHORT"
            else:
                self.cur_pct = None
                self.cur_dir = None

        # Dual-Buffer Check:
        # Jika candle baru belum menerima tick, inisialisasi candle baru dari close_p.
        # Jika candle baru SUDAH menerima tick duluan, biarkan harga live berjalan
        # dan langsung hitung ulang _evaluate_live_sr() terhadap confirmed_sr yang baru!
        if close_time is not None and interval_ms is not None:
            next_open = close_time + 1
            next_close = close_time + interval_ms
            if self.candle_open_time < next_open:
                self.candle_open_time = next_open
                self.candle_close_time = next_close
                self.live_open = close_p
                self.live_high = close_p
                self.live_low = close_p
                self.live_close = close_p
                self.next_sr_active = False
                self.next_sr_dir = None
                self.next_sr_price = None
                self.next_sr_pct = None
            else:
                self._evaluate_live_sr()
        else:
            self.next_sr_active = False
            self.next_sr_dir = None
            self.next_sr_price = None
            self.next_sr_pct = None

        if not is_warmup and callable(self.on_candle_close):
            try:
                self.on_candle_close(self, open_p, high_p, low_p, close_p, is_step, step_dir, open_time, close_time)
            except Exception:
                pass

    def _evaluate_live_sr(self):
        vol_value = self._compute_volatility_live(self.live_high, self.live_low, self.live_close)
        self.live_vol_value = vol_value

        if vol_value is None or self.confirmed_sr is None:
            self.next_sr_active = False
            return

        diff = self.live_close - self.confirmed_sr
        if abs(diff) > vol_value:
            self.next_sr_active = True
            self.next_sr_dir = "LONG" if diff > 0 else "SHORT"
            if self.line_mode == "Anchor":
                self.next_sr_price = self.live_close
            else:
                direction = 1 if diff > 0 else -1
                self.next_sr_price = self.confirmed_sr + direction * vol_value

            if self.confirmed_sr != 0:
                self.next_sr_pct = ((self.next_sr_price - self.confirmed_sr) / self.confirmed_sr) * 100.0
            else:
                self.next_sr_pct = 0.0
        else:
            self.next_sr_active = False
            self.next_sr_dir = None
            self.next_sr_price = None
            self.next_sr_pct = None

    def update_live_tick(
        self,
        price: float,
        high_p: float = None,
        low_p: float = None,
        open_p: float = None,
        open_time: int = None,
        close_time: int = None,
        raw_price_str: str = None,
        trade_time: int = None,
        interval_ms: int = 60_000,
    ):
        if raw_price_str:
            self._detect_precision(raw_price_str)

        # Jika tick aggTrade sudah masuk ke menit/candle baru sementara paket kline
        # belum roll-over, langsung buka buffer live baru tanpa membekukan layar
        if (
            trade_time is not None
            and self.candle_close_time > 0
            and trade_time > self.candle_close_time
        ):
            new_open = (trade_time // interval_ms) * interval_ms
            new_close = new_open + interval_ms - 1
            self.candle_open_time = new_open
            self.candle_close_time = new_close
            self.live_open = price
            self.live_high = price
            self.live_low = price
        elif open_time is not None:
            if open_time <= self.last_closed_open_time:
                return

            if open_time > self.candle_open_time:
                self.candle_open_time = open_time
                if close_time is not None:
                    self.candle_close_time = close_time
                self.live_open = open_p if open_p is not None else price
                self.live_high = high_p if high_p is not None else price
                self.live_low = low_p if low_p is not None else price
            else:
                if close_time is not None:
                    self.candle_close_time = close_time
                if open_p is not None:
                    self.live_open = open_p
                if high_p is not None:
                    self.live_high = high_p if self.live_high is None else max(self.live_high, high_p)
                if low_p is not None:
                    self.live_low = low_p if self.live_low is None else min(self.live_low, low_p)
        else:
            if self.live_open is None:
                self.live_open = price
            self.live_high = price if self.live_high is None else max(self.live_high, price)
            self.live_low = price if self.live_low is None else min(self.live_low, price)

        if self.live_close is not None and price != self.live_close:
            self.tick_dir = 1 if price > self.live_close else -1
        self.live_close = price

        self._evaluate_live_sr()

        if callable(self.on_live_tick):
            try:
                self.on_live_tick(self, price, self.live_high, self.live_low)
            except Exception:
                pass


# =====================================================================
# WARMUP DATA HISTORIS (AUTO FETCH 1500 KLINE, DELAY 0.2s SETIAP COIN)
# =====================================================================
async def sync_server_time(session: aiohttp.ClientSession):
    try:
        t0 = time.time() * 1000
        async with session.get(FAPI_TIME_URL, timeout=5) as resp:
            if resp.status == 200:
                t1 = time.time() * 1000
                data = await resp.json()
                latency = (t1 - t0) / 2.0
                STATE["server_offset_ms"] = int(data["serverTime"] + latency - t1)
    except Exception:
        STATE["server_offset_ms"] = 0


async def fetch_warmup_klines(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str,
    console: Console,
) -> list:
    console.print(
        f"[cyan]⏳ Fetching warmup [bold]{symbol}[/bold] ({interval}) [auto fetch 1500 klines]...[/cyan]"
    )

    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": 1500,
    }
    async with session.get(FAPI_REST_URL, params=params, timeout=10) as resp:
        if "X-MBX-USED-WEIGHT-1M" in resp.headers:
            STATE["used_weight_1m"] = int(resp.headers["X-MBX-USED-WEIGHT-1M"])
        if resp.status != 200:
            err_text = await resp.text()
            raise RuntimeError(f"Gagal fetch {symbol}: HTTP {resp.status} - {err_text}")
        data = await resp.json()

    if not data:
        data = []

    console.print(
        f"[green]✅ Selesai warmup [bold]{symbol}[/bold]: {len(data)} klines dimuat.[/green]"
    )
    return data


def initialize_engine_from_klines(engine: VolStepEngine, klines: list, interval: str):
    if not klines:
        return

    interval_ms = INTERVAL_MS_MAP.get(interval, 60_000)
    now_ms = get_binance_now_ms()

    for k in klines[:-1]:
        engine.process_closed_candle(
            open_p=float(k[1]),
            high_p=float(k[2]),
            low_p=float(k[3]),
            close_p=float(k[4]),
            raw_close_str=str(k[4]),
            open_time=int(k[0]),
            close_time=int(k[6]),
            interval_ms=interval_ms,
            is_warmup=True,
        )

    last_k = klines[-1]
    open_time = int(last_k[0])
    open_p = float(last_k[1])
    high_p = float(last_k[2])
    low_p = float(last_k[3])
    close_p = float(last_k[4])
    close_time = int(last_k[6])

    if close_time <= now_ms:
        engine.process_closed_candle(
            open_p=open_p,
            high_p=high_p,
            low_p=low_p,
            close_p=close_p,
            raw_close_str=str(last_k[4]),
            open_time=open_time,
            close_time=close_time,
            interval_ms=interval_ms,
            is_warmup=True,
        )
    else:
        engine.update_live_tick(
            price=close_p,
            high_p=high_p,
            low_p=low_p,
            open_p=open_p,
            open_time=open_time,
            close_time=close_time,
            raw_price_str=str(last_k[4]),
            interval_ms=interval_ms,
        )


# =====================================================================
# ENGINE 1: WEBSOCKET STREAMING (AGGTRADE + KLINE)
# =====================================================================
async def websocket_worker(engines: dict, interval: str):
    symbols_lower = [s.lower() for s in engines.keys()]
    streams = []
    for s in symbols_lower:
        streams.append(f"{s}@aggTrade")
        streams.append(f"{s}@kline_{interval}")

    interval_ms = INTERVAL_MS_MAP.get(interval, 60_000)
    endpoint_idx = 0

    async with aiohttp.ClientSession() as session:
        while True:
            ws_url = WSS_ENDPOINTS[endpoint_idx % len(WSS_ENDPOINTS)]
            try:
                async with session.ws_connect(
                    URL(ws_url, encoded=True),
                    heartbeat=15,
                    timeout=4.0,
                ) as ws:
                    sub_payload = {
                        "method": "SUBSCRIBE",
                        "params": streams,
                        "id": 1,
                    }
                    await ws.send_str(json.dumps(sub_payload))

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            payload = json.loads(msg.data)
                            if "result" in payload and payload.get("id") == 1:
                                continue

                            data = payload.get("data", payload)
                            event_type = data.get("e")
                            sym = data.get("s")

                            if not sym or sym not in engines:
                                continue

                            STATE["last_ws_msg_ts"] = time.time()
                            STATE["mode"] = "WSS REAL-TIME"
                            STATE["total_updates"] += 1

                            if "E" in data:
                                STATE["server_offset_ms"] = int(data["E"]) - int(time.time() * 1000)

                            eng = engines[sym]

                            if event_type == "aggTrade":
                                raw_p = str(data["p"])
                                trade_ts = int(data.get("T", data.get("E", 0)))
                                eng.update_live_tick(
                                    price=float(raw_p),
                                    raw_price_str=raw_p,
                                    trade_time=trade_ts,
                                    interval_ms=interval_ms,
                                )

                            elif event_type == "kline":
                                k = data["k"]
                                open_time = int(k["t"])
                                close_time = int(k["T"])
                                open_p = float(k["o"])
                                high_p = float(k["h"])
                                low_p = float(k["l"])
                                raw_c = str(k["c"])
                                close_p = float(raw_c)
                                is_closed = bool(k["x"])

                                if is_closed:
                                    eng.process_closed_candle(
                                        open_p=open_p,
                                        high_p=high_p,
                                        low_p=low_p,
                                        close_p=close_p,
                                        raw_close_str=raw_c,
                                        open_time=open_time,
                                        close_time=close_time,
                                        interval_ms=interval_ms,
                                    )
                                else:
                                    eng.update_live_tick(
                                        price=close_p,
                                        high_p=high_p,
                                        low_p=low_p,
                                        open_p=open_p,
                                        open_time=open_time,
                                        close_time=close_time,
                                        raw_price_str=raw_c,
                                        interval_ms=interval_ms,
                                    )

                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except Exception:
                endpoint_idx += 1
                await asyncio.sleep(1.0)


# =====================================================================
# ENGINE 2: FAST CLOSE TRIGGER (0.18s) & FALLBACK STREAM
# =====================================================================
async def fast_close_and_fallback_worker(engines: dict, interval: str):
    """
    Mengecek setiap 50ms:
    1. Tepat 180ms (0.18 detik) setelah pergantian menit, jika WSS kline (x=True)
       belum masuk, langsung tarik limit=2 dari fapi/v1/klines agar bar close
       terkunci instan & 100% sama dengan fetch kline.
    2. Menjadi fallback apabila WSS idle > 1.5 detik.
    """
    interval_ms = INTERVAL_MS_MAP.get(interval, 60_000)

    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(0.05)
            now_ms = get_binance_now_ms()
            expected_current_open = (now_ms // interval_ms) * interval_ms
            expected_prev_open = expected_current_open - interval_ms
            ms_into_new_bar = now_ms - expected_current_open

            ws_is_idle = (time.time() - STATE["last_ws_msg_ts"]) >= 1.5
            if ws_is_idle:
                STATE["mode"] = "FAPI DIRECT"

            for sym, eng in engines.items():
                now_ts = time.time()

                # Trigger cepat 180ms setelah candle berganti jika x=True belum diterima
                need_fast_close = (
                    eng.last_closed_open_time < expected_prev_open
                    and ms_into_new_bar >= 180
                    and (now_ts - eng.last_reconcile_ts) >= 0.25
                )

                need_idle_poll = ws_is_idle and (now_ts - eng.last_reconcile_ts) >= 0.40

                if not need_fast_close and not need_idle_poll:
                    continue

                eng.last_reconcile_ts = now_ts

                try:
                    params = {
                        "symbol": sym,
                        "interval": interval,
                        "limit": 2,
                    }
                    async with session.get(FAPI_REST_URL, params=params, timeout=2.5) as resp:
                        if "X-MBX-USED-WEIGHT-1M" in resp.headers:
                            STATE["used_weight_1m"] = int(resp.headers["X-MBX-USED-WEIGHT-1M"])

                        if resp.status != 200:
                            continue

                        klines = await resp.json()
                        if not klines:
                            continue

                        STATE["total_updates"] += 1

                        # Jika server Binance sudah membuka bar baru (klines[-1][0] >= expected_current_open),
                        # maka klines[0] dijamin 100% sudah final & close resmi di server.
                        if len(klines) >= 2 and int(klines[-1][0]) >= expected_current_open:
                            prev_k = klines[-2]
                            prev_open_time = int(prev_k[0])
                            prev_close_time = int(prev_k[6])
                            if prev_open_time > eng.last_closed_open_time:
                                eng.process_closed_candle(
                                    open_p=float(prev_k[1]),
                                    high_p=float(prev_k[2]),
                                    low_p=float(prev_k[3]),
                                    close_p=float(prev_k[4]),
                                    raw_close_str=str(prev_k[4]),
                                    open_time=prev_open_time,
                                    close_time=prev_close_time,
                                    interval_ms=interval_ms,
                                )

                        if ws_is_idle:
                            curr_k = klines[-1]
                            eng.update_live_tick(
                                price=float(curr_k[4]),
                                high_p=float(curr_k[2]),
                                low_p=float(curr_k[3]),
                                open_p=float(curr_k[1]),
                                open_time=int(curr_k[0]),
                                close_time=int(curr_k[6]),
                                raw_price_str=str(curr_k[4]),
                                interval_ms=interval_ms,
                            )
                except Exception:
                    continue


# =====================================================================
# RENDER TABEL RICH (15 FPS)
# =====================================================================
def build_screener_table(
    engines: dict,
    interval: str,
    line_mode: str,
    vol_type: str,
    vol_len: int,
    vol_mult: float,
) -> Panel:
    interval_ms = INTERVAL_MS_MAP.get(interval, 60_000)
    now_ms = get_binance_now_ms()

    server_dt = datetime.fromtimestamp(now_ms / 1000.0)
    now_str = server_dt.strftime("%H:%M:%S")

    expected_open = (now_ms // interval_ms) * interval_ms
    expected_close = expected_open + interval_ms
    rem_ms = max(0, expected_close - now_ms)
    rem_sec = int(rem_ms / 1000)
    mins, secs = divmod(rem_sec, 60)
    hrs, mins = divmod(mins, 60)
    countdown_str = f"{hrs:02d}:{mins:02d}:{secs:02d}" if hrs > 0 else f"{mins:02d}:{secs:02d}s"

    table = Table(
        box=box.ROUNDED,
        expand=True,
        header_style="bold cyan",
        border_style="bright_blue",
    )

    table.add_column("Coin", justify="left", style="bold white", no_wrap=True)
    table.add_column("Live Price (Tick)", justify="right", no_wrap=True)
    table.add_column("Sisa Waktu", justify="center", no_wrap=True)
    table.add_column("SR Saat Ini (Confirmed)", justify="left", no_wrap=True)
    table.add_column("SR Next (Live Tick Signal)", justify="left", no_wrap=True)

    for sym, eng in engines.items():
        # 1. Live Price per tick
        price_str = eng.fmt(eng.live_close)
        if eng.tick_dir > 0:
            price_cell = Text(f"▲ {price_str}", style="bold bright_green")
        elif eng.tick_dir < 0:
            price_cell = Text(f"▼ {price_str}", style="bold bright_red")
        else:
            price_cell = Text(f"• {price_str}", style="bold white")

        # 2. SR Saat Ini (Confirmed saat candle close)
        if eng.cur_level is not None:
            sr_text = f"SR {eng.fmt(eng.cur_level)}"
            if eng.cur_pct is not None:
                arrow = "▲" if eng.cur_pct >= 0 else "▼"
                col = "bright_green" if eng.cur_pct >= 0 else "bright_red"
                tag = "LONG" if eng.cur_pct >= 0 else "SHORT"
                sr_cell = Text.assemble(
                    (f"{sr_text} ", "bold white"),
                    (f"[{tag} {arrow} {abs(eng.cur_pct):.2f}%]", f"bold {col}"),
                )
            else:
                sr_cell = Text(sr_text, style="bold white")
        else:
            sr_cell = Text("-", style="dim")

        # 3. SR Next (Muncul-Hilang secara real-time selama periode bar belum close)
        if eng.next_sr_active:
            if eng.next_sr_dir == "LONG":
                next_cell = Text(
                    f"● SR LONG ▲ (Calon: {eng.fmt(eng.next_sr_price)} | +{abs(eng.next_sr_pct):.2f}%)",
                    style="bold black on bright_green",
                )
            else:
                next_cell = Text(
                    f"● SR SHORT ▼ (Calon: {eng.fmt(eng.next_sr_price)} | -{abs(eng.next_sr_pct):.2f}%)",
                    style="bold white on bright_red",
                )
        else:
            next_cell = Text("-", style="dim")

        table.add_row(
            sym,
            price_cell,
            countdown_str,
            sr_cell,
            next_cell,
        )

    subtitle = (
        f"[bold bright_green]●[/bold bright_green] {STATE['mode']} "
        f"(Ticks: {STATE['total_updates']:,} | W: {STATE['used_weight_1m']}/2400) | "
        f"{line_mode} | {vol_type}({vol_len})x{vol_mult:g} | "
        f"TF: {interval} (1500 klines) | {now_str}"
    )
    return Panel(
        table,
        title="[bold yellow]⚡ VOLATILITY STEP S/R REAL-TIME TICK SCREENER (BINANCE FUTURES) ⚡[/bold yellow]",
        subtitle=f"[cyan]{subtitle}[/cyan]",
        border_style="bright_blue",
    )


async def ui_render_loop(
    engines: dict,
    interval: str,
    line_mode: str,
    vol_type: str,
    vol_len: int,
    vol_mult: float,
    console: Console,
):
    with Live(
        build_screener_table(engines, interval, line_mode, vol_type, vol_len, vol_mult),
        console=console,
        refresh_per_second=15,
        screen=False,
    ) as live:
        while True:
            live.update(
                build_screener_table(
                    engines, interval, line_mode, vol_type, vol_len, vol_mult
                )
            )
            await asyncio.sleep(0.065)


# =====================================================================
# ENTRY POINT UTAMA
# =====================================================================
async def main():
    console = Console()
    console.rule("[bold cyan]Konfigurasi Screening VolStep S/R Binance Futures[/bold cyan]")

    # 1. Input Coin (pisahkan koma jika lebih dari 1)
    coins_input = console.input(
        "[bold green]1. Masukkan Coin (jika lebih dari 1 pisahkan dengan koma, contoh: BTCUSDT,ETHUSDT,SOLUSDT) [default: BTCUSDT] : [/bold green]"
    ).strip()
    symbols = []
    raw_coins = [c.strip() for c in coins_input.replace(" ", ",").split(",") if c.strip()] if coins_input else []
    for c in raw_coins:
        c = c.upper()
        if not (c.endswith("USDT") or c.endswith("USDC") or c.endswith("BUSD")):
            c += "USDT"
        symbols.append(c)
    symbols = list(dict.fromkeys(symbols))
    if not symbols:
        symbols = ["BTCUSDT"]

    # 2. Input Timeframe (Default: 1m)
    tf_input = console.input(
        "[bold green]2. Masukkan Timeframe (1m, 3m, 5m, 15m, 30m, 1h, 4h, 1d) [default: 1m] : [/bold green]"
    ).strip().lower()
    interval = tf_input if tf_input in INTERVAL_MS_MAP else DEFAULT_TIMEFRAME

    # 3. Pilih Line Mode (Default: 1 = Anchor)
    mode_input = console.input(
        "[bold green]3. Pilih Line Mode (1 = Anchor, 2 = Without Anchor) [default: 1] : [/bold green]"
    ).strip().lower()
    line_mode = "Without Anchor" if mode_input in ("2", "without anchor", "without") else DEFAULT_LINE_MODE

    # 4. Pilih Volatility Type (Default: 2 = Atr)
    vol_input = console.input(
        "[bold green]4. Pilih Volatility Type (1 = Stdev, 2 = Atr) [default: 2] : [/bold green]"
    ).strip().lower()
    vol_type = "Stdev" if vol_input in ("1", "stdev") else DEFAULT_VOL_TYPE

    # 5. Masukkan Lookback Period (Default: 200)
    len_input = console.input(
        "[bold green]5. Masukkan Lookback Period (min 2) [default: 200] : [/bold green]"
    ).strip()
    vol_len = max(2, int(len_input)) if len_input else DEFAULT_LOOKBACK

    # 6. Masukkan Multiplier (Default: 4.0)
    mult_input = console.input(
        "[bold green]6. Masukkan Multiplier (contoh: 4.0) [default: 4.0] : [/bold green]"
    ).strip()
    vol_mult = float(mult_input) if mult_input else DEFAULT_MULTIPLIER

    engines = {
        sym: VolStepEngine(
            symbol=sym,
            line_mode=line_mode,
            vol_type=vol_type,
            length=vol_len,
            vol_mult=vol_mult,
        )
        for sym in symbols
    }

    async with aiohttp.ClientSession() as session:
        await sync_server_time(session)
        for idx, sym in enumerate(symbols):
            klines = await fetch_warmup_klines(session, sym, interval, console)
            initialize_engine_from_klines(engines[sym], klines, interval)
            # Delay 0.2 detik setiap coin jika ada lebih dari 1 coin
            if len(symbols) > 1:
                await asyncio.sleep(0.2)

    console.print("\n[bold bright_green]🚀 Memulai Streaming Real-Time Tick... (Tekan Ctrl+C untuk keluar)[/bold bright_green]\n")

    await asyncio.gather(
        websocket_worker(engines, interval),
        fast_close_and_fallback_worker(engines, interval),
        ui_render_loop(engines, interval, line_mode, vol_type, vol_len, vol_mult, console),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram dihentikan.")