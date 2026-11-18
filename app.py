import asyncio
import json
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiohttp
from flask import Flask, Response, jsonify, render_template, request, send_file
from yarl import URL

from binance_api import BinanceFuturesClient
from strategy import FuturesStrategyEngine
from vstep1 import (
    DEFAULT_LINE_MODE,
    DEFAULT_LOOKBACK,
    DEFAULT_MULTIPLIER,
    DEFAULT_TIMEFRAME,
    DEFAULT_VOL_TYPE,
    INTERVAL_MS_MAP,
    STATE as VSTEP_STATE,
    VolStepEngine,
    get_binance_now_ms,
)

# Setup Flask
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("App")

# =========================================================================
# GLOBAL APPLICATION STATE & MANAGER
# =========================================================================
class AppManager:
    def __init__(self):
        self.lock = threading.RLock()

        # Konfigurasi Default
        self.config = {
            "api_key": "",
            "api_secret": "",
            "is_testnet": True,
            "exec_mode": "testnet_live",
            "symbol": "BTCUSDT",
            "timeframe": DEFAULT_TIMEFRAME,
            "line_mode": DEFAULT_LINE_MODE,
            "vol_type": "Stdev",  # Sesuai vtrail.txt
            "vol_len": DEFAULT_LOOKBACK,
            "vol_mult": DEFAULT_MULTIPLIER,
            "leverage": 15.0,
            "margin_pct": 25.0,
            "maker_fee_pct": 0.02,
            "taker_fee_pct": 0.05,
            "mmr_pct": 0.40,
            "pullback_pct": 0.25,
            "sl_fixed_pct": 1.00,
            "trail_act_pct": 0.80,
            "trail_cb_pct": 1.00,
            "timeout_bars": 48,
        }

        # Status Testnet Binance Resmi
        self.testnet_balance: Optional[float] = None
        self.testnet_connected: bool = False

        # Engine Objek
        self.vol_engine: Optional[VolStepEngine] = None
        self.strat_engine: Optional[FuturesStrategyEngine] = None
        self.binance_client = BinanceFuturesClient(is_testnet=True)

        # Histori Lilin untuk Chart UI (Maks 120 bar)
        self.candle_history: deque = deque(maxlen=120)

        # Status Background Task
        self.async_loop: Optional[asyncio.AbstractEventLoop] = None
        self.bg_thread: Optional[threading.Thread] = None
        self.running_tasks: List[asyncio.Task] = []
        self.is_restarting = False
        self.worker_active = False

    def init_engines(self):
        with self.lock:
            sym = self.config["symbol"]
            tf = self.config["timeframe"]

            # Buat VolStepEngine tanpa mengubah logika vstep1.py
            self.vol_engine = VolStepEngine(
                symbol=sym,
                line_mode=self.config["line_mode"],
                vol_type=self.config["vol_type"],
                length=int(self.config["vol_len"]),
                vol_mult=float(self.config["vol_mult"]),
            )

            # Update client testnet jika ada API keys
            self.binance_client = BinanceFuturesClient(
                api_key=self.config["api_key"],
                api_secret=self.config["api_secret"],
                is_testnet=self.config["is_testnet"],
            )

            # Buat StrategyEngine (Saldo awal diambil dari Testnet Binance jika terhubung)
            init_bal = self.testnet_balance or 0.0
            self.strat_engine = FuturesStrategyEngine(
                symbol=sym,
                initial_balance=init_bal,
                leverage=float(self.config["leverage"]),
                margin_pct=float(self.config["margin_pct"]),
                maker_fee_pct=float(self.config["maker_fee_pct"]),
                taker_fee_pct=float(self.config["taker_fee_pct"]),
                mmr_pct=float(self.config["mmr_pct"]),
                pullback_pct=float(self.config["pullback_pct"]),
                sl_fixed_pct=float(self.config["sl_fixed_pct"]),
                trail_act_pct=float(self.config["trail_act_pct"]),
                trail_cb_pct=float(self.config["trail_cb_pct"]),
                timeout_bars=int(self.config["timeout_bars"]),
            )

            # Sambungkan hook callback dari VolStepEngine ke StrategyEngine & Candle History
            self.vol_engine.on_candle_close = self._handle_candle_close
            self.vol_engine.on_live_tick = self._handle_live_tick

            # Coba sinkronisasi saldo testnet jika kunci tersedia
            if self.config["api_key"] and self.config["api_secret"]:
                self.sync_testnet_balance()

    def sync_testnet_balance(self):
        """Ambil saldo riil dari Binance Futures Testnet."""
        if not self.config.get("api_key") or not self.config.get("api_secret"):
            self.testnet_connected = False
            return False, "API Key dan Secret belum diisi."

        res = self.binance_client.verify_account()
        if res.get("success"):
            self.testnet_connected = True
            bal = float(res.get("total_balance_usdt", 0.0))
            self.testnet_balance = bal
            if self.strat_engine:
                self.strat_engine.update_testnet_balance(bal)
            return True, bal
        else:
            self.testnet_connected = False
            return False, res.get("error", "Gagal koneksi Testnet.")

    def _handle_candle_close(
        self,
        engine: VolStepEngine,
        open_p: float,
        high_p: float,
        low_p: float,
        close_p: float,
        is_step: bool,
        step_dir: int,
        open_time: int,
        close_time: int,
    ):
        """Callback saat candle resmi ditutup."""
        # Teruskan ke strategy engine
        if self.strat_engine:
            self.strat_engine.on_candle_close(
                open_p=open_p,
                high_p=high_p,
                low_p=low_p,
                close_p=close_p,
                is_step=is_step,
                step_dir=step_dir,
            )

        # Catat ke history chart
        dt_str = datetime.fromtimestamp((open_time or time.time() * 1000) / 1000.0).strftime("%H:%M")
        self.candle_history.append(
            {
                "time": dt_str,
                "open": open_p,
                "high": high_p,
                "low": low_p,
                "close": close_p,
                "sr": engine.confirmed_sr,
                "is_step": is_step,
            }
        )

    def _handle_live_tick(self, engine: VolStepEngine, price: float, high_p: float, low_p: float):
        """Callback saat live tick aggTrade / kline tick masuk."""
        if self.strat_engine:
            self.strat_engine.on_live_tick(price=price, high_p=high_p, low_p=low_p)


manager = AppManager()


# =========================================================================
# ASYNC WORKERS (WEBSOCKET + FAST CLOSE REST RECONCILIATION)
# =========================================================================
async def async_worker_routine():
    """Rutin utama asynchronous yang berjalan di background thread."""
    while True:
        try:
            with manager.lock:
                sym = manager.config["symbol"]
                interval = manager.config["timeframe"]
                engine = manager.vol_engine

            if not engine:
                await asyncio.sleep(0.5)
                continue

            manager.worker_active = True
            logger.info(f"Memulai warm-up dan streaming untuk {sym} ({interval})...")

            # 1. Warm-up 1500 bar
            async with aiohttp.ClientSession() as session:
                # Sync waktu Binance
                try:
                    async with session.get("https://fapi.binance.com/fapi/v1/time", timeout=5) as r:
                        if r.status == 200:
                            d = await r.json()
                            VSTEP_STATE["server_offset_ms"] = int(d["serverTime"]) - int(time.time() * 1000)
                except Exception as e:
                    logger.warning(f"Gagal sync server time: {e}")

                # Tarik klines
                params = {"symbol": sym.upper(), "interval": interval, "limit": 1000}
                async with session.get("https://fapi.binance.com/fapi/v1/klines", params=params, timeout=10) as resp:
                    if resp.status == 200:
                        klines = await resp.json()
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
                            )
                        if klines:
                            last_k = klines[-1]
                            close_t = int(last_k[6])
                            if close_t <= now_ms:
                                engine.process_closed_candle(
                                    open_p=float(last_k[1]),
                                    high_p=float(last_k[2]),
                                    low_p=float(last_k[3]),
                                    close_p=float(last_k[4]),
                                    raw_close_str=str(last_k[4]),
                                    open_time=int(last_k[0]),
                                    close_time=close_t,
                                    interval_ms=interval_ms,
                                )
                            else:
                                engine.update_live_tick(
                                    price=float(last_k[4]),
                                    high_p=float(last_k[2]),
                                    low_p=float(last_k[3]),
                                    open_p=float(last_k[1]),
                                    open_time=int(last_k[0]),
                                    close_time=close_t,
                                    raw_price_str=str(last_k[4]),
                                    interval_ms=interval_ms,
                                )
                        logger.info(f"Warm-up {sym} selesai: {len(klines)} klines dimuat.")

            # 2. Loop streaming WSS & Fast Close REST
            engines_map = {sym: engine}
            tasks = [
                asyncio.create_task(run_ws_stream(sym, interval, engine)),
                asyncio.create_task(run_fast_close_reconcile(sym, interval, engine)),
            ]
            manager.running_tasks = tasks

            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in pending:
                t.cancel()
            await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            logger.info("Worker routine dibatalkan untuk restart.")
            break
        except Exception as e:
            logger.error(f"Error pada worker routine: {e}")
            await asyncio.sleep(2.0)


async def run_ws_stream(symbol: str, interval: str, engine: VolStepEngine):
    """Worker WebSocket membaca @aggTrade dan @kline_{interval}."""
    sym_lower = symbol.lower()
    streams = [f"{sym_lower}@aggTrade", f"{sym_lower}@kline_{interval}"]
    interval_ms = INTERVAL_MS_MAP.get(interval, 60_000)

    wss_urls = [
        "wss://fstream.binance.com/ws",
        "wss://fstream.binance.com:9443/ws",
    ]
    idx = 0

    async with aiohttp.ClientSession() as session:
        while True:
            url = wss_urls[idx % len(wss_urls)]
            try:
                async with session.ws_connect(URL(url, encoded=True), heartbeat=15, timeout=5.0) as ws:
                    sub_payload = {"method": "SUBSCRIBE", "params": streams, "id": 1}
                    await ws.send_str(json.dumps(sub_payload))
                    VSTEP_STATE["mode"] = "WSS REAL-TIME"

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            payload = json.loads(msg.data)
                            if "result" in payload:
                                continue
                            data = payload.get("data", payload)
                            event_type = data.get("e")

                            VSTEP_STATE["last_ws_msg_ts"] = time.time()
                            VSTEP_STATE["total_updates"] += 1

                            if event_type == "aggTrade":
                                raw_p = str(data["p"])
                                trade_ts = int(data.get("T", data.get("E", 0)))
                                engine.update_live_tick(
                                    price=float(raw_p),
                                    raw_price_str=raw_p,
                                    trade_time=trade_ts,
                                    interval_ms=interval_ms,
                                )
                            elif event_type == "kline":
                                k = data["k"]
                                is_closed = bool(k["x"])
                                raw_c = str(k["c"])
                                if is_closed:
                                    engine.process_closed_candle(
                                        open_p=float(k["o"]),
                                        high_p=float(k["h"]),
                                        low_p=float(k["l"]),
                                        close_p=float(raw_c),
                                        raw_close_str=raw_c,
                                        open_time=int(k["t"]),
                                        close_time=int(k["T"]),
                                        interval_ms=interval_ms,
                                    )
                                else:
                                    engine.update_live_tick(
                                        price=float(raw_c),
                                        high_p=float(k["h"]),
                                        low_p=float(k["l"]),
                                        open_p=float(k["o"]),
                                        open_time=int(k["t"]),
                                        close_time=int(k["T"]),
                                        raw_price_str=raw_c,
                                        interval_ms=interval_ms,
                                    )
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"WSS disconnected ({e}), reconnecting...")
                idx += 1
                await asyncio.sleep(1.0)


async def run_fast_close_reconcile(symbol: str, interval: str, engine: VolStepEngine):
    """Mengecek setiap 50ms untuk Fast Close 180ms dan fallback polling."""
    interval_ms = INTERVAL_MS_MAP.get(interval, 60_000)

    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(0.05)
            now_ms = get_binance_now_ms()
            expected_current_open = (now_ms // interval_ms) * interval_ms
            expected_prev_open = expected_current_open - interval_ms
            ms_into_new_bar = now_ms - expected_current_open

            ws_is_idle = (time.time() - VSTEP_STATE["last_ws_msg_ts"]) >= 1.5
            now_ts = time.time()

            need_fast_close = (
                engine.last_closed_open_time < expected_prev_open
                and ms_into_new_bar >= 180
                and (now_ts - engine.last_reconcile_ts) >= 0.25
            )
            need_idle_poll = ws_is_idle and (now_ts - engine.last_reconcile_ts) >= 0.40

            if not need_fast_close and not need_idle_poll:
                continue

            engine.last_reconcile_ts = now_ts
            try:
                params = {"symbol": symbol.upper(), "interval": interval, "limit": 2}
                async with session.get("https://fapi.binance.com/fapi/v1/klines", params=params, timeout=2.5) as resp:
                    if resp.status != 200:
                        continue
                    klines = await resp.json()
                    if not klines:
                        continue

                    if len(klines) >= 2 and int(klines[-1][0]) >= expected_current_open:
                        prev_k = klines[-2]
                        prev_open_t = int(prev_k[0])
                        if prev_open_t > engine.last_closed_open_time:
                            engine.process_closed_candle(
                                open_p=float(prev_k[1]),
                                high_p=float(prev_k[2]),
                                low_p=float(prev_k[3]),
                                close_p=float(prev_k[4]),
                                raw_close_str=str(prev_k[4]),
                                open_time=prev_open_t,
                                close_time=int(prev_k[6]),
                                interval_ms=interval_ms,
                            )
            except asyncio.CancelledError:
                raise
            except Exception:
                continue


def start_background_loop():
    """Memulai thread background yang menjalankan event loop asyncio."""
    loop = asyncio.new_event_loop()
    manager.async_loop = loop

    def loop_thread_entry():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(async_worker_routine())

    t = threading.Thread(target=loop_thread_entry, daemon=True, name="BinanceStreamThread")
    manager.bg_thread = t
    t.start()


# =========================================================================
# FLASK HTTP & SSE ENDPOINTS
# =========================================================================
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/state")
def get_state():
    """Mengembalikan data real-time lengkap untuk dashboard UI."""
    with manager.lock:
        eng = manager.vol_engine
        strat = manager.strat_engine
        cfg = manager.config

        if not eng or not strat:
            return jsonify({"status": "initializing"})

        now_ms = get_binance_now_ms()
        interval_ms = INTERVAL_MS_MAP.get(cfg["timeframe"], 60_000)
        expected_open = (now_ms // interval_ms) * interval_ms
        expected_close = expected_open + interval_ms
        rem_ms = max(0, expected_close - now_ms)
        rem_sec = int(rem_ms / 1000)
        mins, secs = divmod(rem_sec, 60)
        countdown_str = f"{mins:02d}:{secs:02d}s"

        strat_state = strat.get_dashboard_state(eng.live_close)

        # Candlestick summary untuk mini chart
        candles = list(manager.candle_history)
        if eng.live_close is not None:
            candles.append(
                {
                    "time": "NOW",
                    "open": eng.live_open or eng.live_close,
                    "high": eng.live_high or eng.live_close,
                    "low": eng.live_low or eng.live_close,
                    "close": eng.live_close,
                    "sr": eng.confirmed_sr,
                    "is_step": False,
                }
            )

        return jsonify(
            {
                "status": "online",
                "config": cfg,
                "server_time": datetime.fromtimestamp(now_ms / 1000.0).strftime("%H:%M:%S"),
                "countdown": countdown_str,
                "mode": VSTEP_STATE.get("mode", "INIT"),
                "total_updates": VSTEP_STATE.get("total_updates", 0),
                "market": {
                    "symbol": cfg["symbol"],
                    "timeframe": cfg["timeframe"],
                    "price": eng.live_close,
                    "price_str": eng.fmt(eng.live_close),
                    "tick_dir": eng.tick_dir,
                    "open": eng.live_open,
                    "high": eng.live_high,
                    "low": eng.live_low,
                },
                "indicator": {
                    "confirmed_sr": eng.confirmed_sr,
                    "confirmed_sr_str": eng.fmt(eng.confirmed_sr),
                    "cur_level": eng.cur_level,
                    "cur_pct": eng.cur_pct,
                    "cur_dir": eng.cur_dir,
                    "next_sr_active": eng.next_sr_active,
                    "next_sr_dir": eng.next_sr_dir,
                    "next_sr_price": eng.next_sr_price,
                    "next_sr_price_str": eng.fmt(eng.next_sr_price),
                    "next_sr_pct": eng.next_sr_pct,
                    "line_mode": eng.line_mode,
                    "vol_type": eng.vol_type,
                    "vol_len": eng.length,
                    "vol_mult": eng.vol_mult,
                },
                "strategy": strat_state,
                "testnet": {
                    "connected": manager.testnet_connected,
                    "balance": manager.testnet_balance,
                },
                "candles": candles[-40:],  # kirim 40 candle terakhir untuk efisiensi SSE
            }
        )


@app.route("/api/config", methods=["POST"])
def update_config():
    """Memperbarui pengaturan indikator, leverage, margin, dan trailing stop."""
    data = request.get_json() or {}
    logger.info(f"Menerima pembaruan konfigurasi: {data}")

    with manager.lock:
        # Update config dictionary
        for key in manager.config.keys():
            if key in data:
                val = data[key]
                if key in ("leverage", "margin_pct", "maker_fee_pct", "taker_fee_pct",
                            "mmr_pct", "pullback_pct", "sl_fixed_pct", "trail_act_pct", "trail_cb_pct", "vol_mult"):
                    manager.config[key] = float(val)
                elif key in ("vol_len", "timeout_bars"):
                    manager.config[key] = int(val)
                elif key in ("is_testnet",):
                    manager.config[key] = bool(val)
                else:
                    manager.config[key] = str(val).strip()

        # Re-inisialisasi engine
        manager.init_engines()
        manager.candle_history.clear()

    # Cancel dan restart background async worker
    if manager.async_loop and manager.async_loop.is_running():
        for t in manager.running_tasks:
            manager.async_loop.call_soon_threadsafe(t.cancel)

    return jsonify({"success": True, "message": "Konfigurasi berhasil disimpan dan engine di-restart!"})


@app.route("/api/testnet/verify", methods=["POST"])
def verify_testnet():
    """Memverifikasi API Key dan Secret Key pada Binance Futures Testnet."""
    data = request.get_json() or {}
    api_key = data.get("api_key", "").strip()
    api_secret = data.get("api_secret", "").strip()

    if not api_key or not api_secret:
        return jsonify({"success": False, "error": "Harap masukkan API Key dan Secret Key."})

    client = BinanceFuturesClient(api_key=api_key, api_secret=api_secret, is_testnet=True)
    res = client.verify_account()

    if res.get("success"):
        with manager.lock:
            manager.config["api_key"] = api_key
            manager.config["api_secret"] = api_secret
            manager.binance_client = client
            manager.testnet_connected = True
            bal = float(res.get("total_balance_usdt", 0.0))
            manager.testnet_balance = bal
            if manager.strat_engine:
                manager.strat_engine.update_testnet_balance(bal)
    return jsonify(res)


@app.route("/api/reset", methods=["POST"])
def reset_strategy():
    """Reset saldo simulasi dan histori trade."""
    data = request.get_json() or {}
    new_bal = data.get("balance")
    with manager.lock:
        if manager.strat_engine:
            manager.strat_engine.reset(new_balance=new_bal)
    return jsonify({"success": True, "message": "Simulasi berhasil di-reset."})


@app.route("/api/bot/toggle", methods=["POST"])
def toggle_bot():
    """Start / Stop bot trading logic."""
    data = request.get_json() or {}
    with manager.lock:
        if not manager.strat_engine:
            return jsonify({"success": False, "error": "Strategy engine belum siap."})

        current = manager.strat_engine.is_bot_active
        target = not current if "active" not in data else bool(data["active"])

        if target:
            # Pastikan API Key Testnet valid dan terkoneksi
            ok, bal_or_err = manager.sync_testnet_balance()
            if not ok:
                return jsonify({
                    "success": False,
                    "error": f"{bal_or_err}. Masukkan API Key & Secret Binance Testnet pada menu Setup terlebih dahulu!"
                })

            manager.strat_engine.set_bot_active(True)
            return jsonify({
                "success": True,
                "is_bot_active": True,
                "balance": manager.testnet_balance,
                "message": f"Bot Aktif! Saldo Binance Testnet: ${manager.testnet_balance:,.2f} USDT"
            })
        else:
            manager.strat_engine.set_bot_active(False)
            return jsonify({
                "success": True,
                "is_bot_active": False,
                "message": "Bot Dihentikan (STOPPED)."
            })


@app.route("/api/stream")
def sse_stream():
    """Server-Sent Events (SSE) streaming real-time setiap 120ms."""
    def event_generator():
        while True:
            time.sleep(0.12)
            try:
                with manager.lock:
                    eng = manager.vol_engine
                    strat = manager.strat_engine
                    cfg = manager.config

                    if not eng or not strat:
                        continue

                    now_ms = get_binance_now_ms()
                    interval_ms = INTERVAL_MS_MAP.get(cfg["timeframe"], 60_000)
                    expected_open = (now_ms // interval_ms) * interval_ms
                    expected_close = expected_open + interval_ms
                    rem_ms = max(0, expected_close - now_ms)
                    mins, secs = divmod(int(rem_ms / 1000), 60)
                    countdown_str = f"{mins:02d}:{secs:02d}s"

                    strat_state = strat.get_dashboard_state(eng.live_close)

                    data = {
                        "server_time": datetime.fromtimestamp(now_ms / 1000.0).strftime("%H:%M:%S"),
                        "countdown": countdown_str,
                        "price": eng.live_close,
                        "price_str": eng.fmt(eng.live_close),
                        "tick_dir": eng.tick_dir,
                        "confirmed_sr": eng.confirmed_sr,
                        "confirmed_sr_str": eng.fmt(eng.confirmed_sr),
                        "cur_pct": eng.cur_pct,
                        "cur_dir": eng.cur_dir,
                        "next_sr_active": eng.next_sr_active,
                        "next_sr_dir": eng.next_sr_dir,
                        "next_sr_price": eng.next_sr_price,
                        "next_sr_price_str": eng.fmt(eng.next_sr_price),
                        "next_sr_pct": eng.next_sr_pct,
                        "strategy": strat_state,
                        "testnet": {
                            "connected": manager.testnet_connected,
                            "balance": manager.testnet_balance,
                        },
                    }
                yield f"data: {json.dumps(data)}\n\n"
            except Exception:
                continue

    return Response(event_generator(), mimetype="text/event-stream")


@app.route("/download/apk")
def download_apk():
    apk_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "vtrail-terminal.apk")
    if os.path.exists(apk_path):
        return send_file(apk_path, as_attachment=True, download_name="vtrail-terminal.apk", mimetype="application/vnd.android.package-archive")
    return "File APK tidak ditemukan di server", 404


# Inisialisasi awal saat modul di-load
manager.init_engines()
start_background_loop()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
