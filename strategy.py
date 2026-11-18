import json
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

TRADES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trades_history.json")


class FuturesStrategyEngine:
    """
    Engine Simulasi Binance Futures Cross & Pullback Limit Trailing Stop (M4 Macro)
    Mereplikasi 100% logika eksekusi dari vtrail.txt (Pine Script v6).
    """

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        initial_balance: float = 1000.0,
        leverage: float = 15.0,
        margin_pct: float = 25.0,       # 25.0%
        maker_fee_pct: float = 0.02,     # 0.02%
        taker_fee_pct: float = 0.05,     # 0.05%
        mmr_pct: float = 0.40,           # 0.40%
        pullback_pct: float = 0.25,      # 0.25%
        sl_fixed_pct: float = 1.00,      # 1.00%
        trail_act_pct: float = 0.80,     # 0.80%
        trail_cb_pct: float = 1.00,      # 1.00%
        timeout_bars: int = 48,
    ):
        self.symbol = symbol.upper()
        self.initial_balance = float(initial_balance)
        self.wallet_balance = float(initial_balance)
        self.leverage = float(leverage)
        self.margin_pct = float(margin_pct) / 100.0
        self.maker_fee_pct = float(maker_fee_pct) / 100.0
        self.taker_fee_pct = float(taker_fee_pct) / 100.0
        self.mmr_pct = float(mmr_pct) / 100.0
        self.pullback_pct = float(pullback_pct) / 100.0
        self.sl_fixed_pct = float(sl_fixed_pct) / 100.0
        self.trail_act_pct = float(trail_act_pct) / 100.0
        self.trail_cb_pct = float(trail_cb_pct) / 100.0
        self.timeout_bars = int(timeout_bars)

        # Status Posisi
        self.pos_side = 0           # 0: Flat, 1: LONG, -1: SHORT
        self.pos_qty = 0.0
        self.pos_entry_price = 0.0
        self.pos_margin_used = 0.0
        self.pos_sl_price = 0.0
        self.pos_highest = 0.0
        self.pos_lowest = 0.0
        self.pos_trail_act = False
        self.pos_open_time = 0.0
        self.is_liquidated = False

        # Status Pending Order (Pullback Limit)
        self.has_pending_order = False
        self.pending_side = 0       # 1: LONG, -1: SHORT
        self.pending_price = 0.0
        self.pending_sl = 0.0
        self.pending_bar_count = 0
        self.pending_placed_time = 0.0

        # Log & Histori Trade
        self.trade_id_counter = 0
        self.trade_history: List[Dict[str, Any]] = []
        self.event_logs: List[Dict[str, Any]] = []
        self.load_trades_from_file()

        # Kontrol Start / Stop Bot (Default: False / STOPPED sampai user menekan Start)
        self.is_bot_active = False

        self.log(
            "STATUS",
            f"Bot SIAGA (STOPPED) untuk {self.symbol}. Tekan tombol 'START BOT' untuk memulai pemindaian sinyal.",
        )

    def load_trades_from_file(self):
        """Memuat riwayat trade dari file JSON persisten agar tidak hilang saat restart."""
        try:
            if os.path.exists(TRADES_FILE):
                with open(TRADES_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        self.trade_history = data
                        if self.trade_history:
                            self.trade_id_counter = max((int(t.get("id", 0)) for t in self.trade_history), default=0)
        except Exception as e:
            print(f"[WARN] Gagal memuat {TRADES_FILE}: {e}")

    def save_trades_to_file(self):
        """Menyimpan riwayat trade ke file JSON persisten."""
        try:
            with open(TRADES_FILE, "w", encoding="utf-8") as f:
                json.dump(self.trade_history, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"[ERR] Gagal menyimpan {TRADES_FILE}: {e}")

    def clear_trade_history(self):
        """Menghapus seluruh riwayat trade dari memori dan file JSON persisten."""
        self.trade_history.clear()
        self.save_trades_to_file()
        self.log("STATUS", "Riwayat trade berhasil dihapus dari penyimpanan file JSON.")

    def log(self, category: str, text: str):
        now = datetime.now().strftime("%H:%M:%S")
        entry = {
            "time": now,
            "category": category,
            "text": text,
        }
        self.event_logs.insert(0, entry)
        if len(self.event_logs) > 100:
            self.event_logs.pop()

    def reset(self, new_balance: Optional[float] = None):
        """Reset saldo dan histori trade ke kondisi awal."""
        if new_balance is not None:
            self.initial_balance = float(new_balance)
        self.wallet_balance = self.initial_balance
        self.pos_side = 0
        self.pos_qty = 0.0
        self.pos_entry_price = 0.0
        self.pos_margin_used = 0.0
        self.pos_sl_price = 0.0
        self.pos_highest = 0.0
        self.pos_lowest = 0.0
        self.pos_trail_act = False
        self.is_liquidated = False
        self.has_pending_order = False
        self.trade_history.clear()
        self.log("RESET", f"Engine di-reset. Saldo dikembalikan ke ${self.wallet_balance:,.2f}")

    def set_bot_active(self, active: bool):
        self.is_bot_active = bool(active)
        status_txt = "BERJALAN (ACTIVE)" if self.is_bot_active else "DIHENTIKAN (STOPPED)"
        self.log("STATUS", f"Bot {status_txt}. " + ("Mencari sinyal baru..." if self.is_bot_active else "Tidak akan memasang order baru."))

    def update_testnet_balance(self, balance: float):
        """Update saldo langsung dari akun Binance Futures Testnet resmi."""
        self.wallet_balance = float(balance)
        if self.initial_balance <= 0.0:
            self.initial_balance = float(balance)
        self.log("INFO", f"Saldo Binance Testnet tersinkron: ${self.wallet_balance:,.2f} USDT")

    # =========================================================================
    # 1. CEK PENGISIAN ORDER (FILL)
    # =========================================================================
    def _try_fill_pending_order(self, current_price: float, low_p: float, high_p: float):
        if not self.has_pending_order or self.is_liquidated:
            return

        filled = False
        fill_price = 0.0

        if self.pending_side == 1 and low_p <= self.pending_price:
            filled = True
            fill_price = min(current_price, self.pending_price)
        elif self.pending_side == -1 and high_p >= self.pending_price:
            filled = True
            fill_price = max(current_price, self.pending_price)

        if filled:
            margin_used = self.wallet_balance * self.margin_pct
            if margin_used < 5.0:
                self.log("WARNING", f"Margin tidak cukup (${margin_used:.2f} < $5.00). Pending order dibatalkan.")
                self.has_pending_order = False
                return

            notional = margin_used * self.leverage
            qty = notional / fill_price
            maker_fee = notional * self.maker_fee_pct

            self.wallet_balance -= maker_fee
            self.pos_side = self.pending_side
            self.pos_qty = qty
            self.pos_entry_price = fill_price
            self.pos_margin_used = margin_used
            self.pos_sl_price = self.pending_sl
            self.pos_highest = fill_price
            self.pos_lowest = fill_price
            self.pos_trail_act = False
            self.pos_open_time = time.time()
            self.has_pending_order = False
            self.trade_id_counter += 1

            side_str = "LONG" if self.pos_side == 1 else "SHORT"
            act_target = fill_price * (1.0 + self.pos_side * self.trail_act_pct)
            self.log(
                "FILL",
                f"{side_str} LIMIT FILLED @ {fill_price:,.2f} | Notional: ${notional:,.2f} (Fee: -${maker_fee:.2f}) | "
                f"Initial SL: {self.pos_sl_price:,.2f} | Target Aktivasi Trail: {act_target:,.2f}",
            )

    # =========================================================================
    # 2. CEK LIKUIDASI (MMR BINANCE CROSS)
    # =========================================================================
    def _check_liquidation(self, worst_price: float):
        if self.pos_side == 0 or self.is_liquidated:
            return

        if self.pos_side == 1:
            unrealized_worst = self.pos_qty * (worst_price - self.pos_entry_price)
        else:
            unrealized_worst = self.pos_qty * (self.pos_entry_price - worst_price)

        maintenance_margin = (self.pos_qty * worst_price) * self.mmr_pct
        margin_balance = self.wallet_balance + unrealized_worst

        if margin_balance <= maintenance_margin:
            self.is_liquidated = True
            lost_balance = self.wallet_balance
            self.wallet_balance = 0.0
            side_str = "LONG" if self.pos_side == 1 else "SHORT"
            now_dt = datetime.now()
            now_ms = int(time.time() * 1000)

            trade_record = {
                "id": self.trade_id_counter,
                "order_id": int(now_ms / 1000) + self.trade_id_counter,
                "symbol": self.symbol,
                "side": side_str,
                "entry_price": round(self.pos_entry_price, 2),
                "exit_price": round(worst_price, 2),
                "qty": round(self.pos_qty, 4),
                "quote_qty": round(self.pos_qty * worst_price, 2),
                "commission": 0.0,
                "commission_asset": "USDT",
                "realized_pnl": round(-lost_balance, 2),
                "net_pnl": round(-lost_balance, 2),
                "roi_pct": -100.0,
                "peak_pct": 0.0,
                "reason": "LIQUIDATED (Margin Call)",
                "exit_time": now_dt.strftime("%H:%M:%S"),
                "exit_date": now_dt.strftime("%Y-%m-%d"),
                "time_ms": now_ms,
            }
            self.trade_history.insert(0, trade_record)
            self.save_trades_to_file()
            self.pos_side = 0
            self.log("LIQUIDATED", f"LIQUIDATED (Margin Call) pada harga {worst_price:,.2f}. Saldo habis ($0.00).")

    # =========================================================================
    # 3. MANAJEMEN TRAILING STOP & FIXED SL
    # =========================================================================
    def _manage_position_exit(self, high_p: float, low_p: float):
        if self.pos_side == 0 or self.is_liquidated:
            return

        if high_p > self.pos_highest:
            self.pos_highest = high_p
        if low_p < self.pos_lowest or self.pos_lowest == 0.0:
            self.pos_lowest = low_p

        close_trade = False
        exit_price = 0.0
        exit_reason = ""

        if self.pos_side == 1:  # LONG
            # Cek aktivasi trailing (+0.80%)
            if not self.pos_trail_act and self.pos_highest >= self.pos_entry_price * (1.0 + self.trail_act_pct):
                self.pos_trail_act = True
                self.log(
                    "TRAIL",
                    f"TRAIL ACTIVE: Long mencapai puncak {self.pos_highest:,.2f} (+{self.trail_act_pct*100:.2f}%). "
                    f"Trailing SL dinamis aktif mengunci profit.",
                )

            # Jika trailing aktif, geser SL mengikuti puncak dikurangi callback (1.00%)
            if self.pos_trail_act:
                dyn_sl = self.pos_highest * (1.0 - self.trail_cb_pct)
                if dyn_sl > self.pos_sl_price:
                    self.pos_sl_price = dyn_sl

            # Cek tersentuh SL / Trailing
            if low_p <= self.pos_sl_price:
                close_trade = True
                exit_price = self.pos_sl_price
                exit_reason = "Trail" if self.pos_trail_act else "SL"

        else:  # SHORT
            # Cek aktivasi trailing (-0.80%)
            if not self.pos_trail_act and self.pos_lowest <= self.pos_entry_price * (1.0 - self.trail_act_pct):
                self.pos_trail_act = True
                self.log(
                    "TRAIL",
                    f"TRAIL ACTIVE: Short mencapai dasar {self.pos_lowest:,.2f} (-{self.trail_act_pct*100:.2f}%). "
                    f"Trailing SL dinamis aktif mengunci profit.",
                )

            # Jika trailing aktif, geser SL mengikuti dasar ditambah callback (1.00%)
            if self.pos_trail_act:
                dyn_sl = self.pos_lowest * (1.0 + self.trail_cb_pct)
                if dyn_sl < self.pos_sl_price:
                    self.pos_sl_price = dyn_sl

            # Cek tersentuh SL / Trailing
            if high_p >= self.pos_sl_price:
                close_trade = True
                exit_price = self.pos_sl_price
                exit_reason = "Trail" if self.pos_trail_act else "SL"

        if close_trade:
            if self.pos_side == 1:
                pnl = self.pos_qty * (exit_price - self.pos_entry_price)
                peak_pct = ((self.pos_highest / self.pos_entry_price) - 1.0) * 100.0
            else:
                pnl = self.pos_qty * (self.pos_entry_price - exit_price)
                peak_pct = (1.0 - (self.pos_lowest / self.pos_entry_price)) * 100.0

            taker_fee = (self.pos_qty * exit_price) * self.taker_fee_pct
            net_pnl = pnl - taker_fee
            self.wallet_balance += net_pnl
            roi_pct = (net_pnl / self.pos_margin_used) * 100.0 if self.pos_margin_used > 0 else 0.0

            side_str = "LONG" if self.pos_side == 1 else "SHORT"
            now_dt = datetime.now()
            now_ms = int(time.time() * 1000)

            trade_record = {
                "id": self.trade_id_counter,
                "order_id": int(now_ms / 1000) + self.trade_id_counter,
                "symbol": self.symbol,
                "side": side_str,
                "entry_price": round(self.pos_entry_price, 2),
                "exit_price": round(exit_price, 2),
                "qty": round(self.pos_qty, 4),
                "quote_qty": round(self.pos_qty * exit_price, 2),
                "commission": round(taker_fee, 4),
                "commission_asset": "USDT",
                "realized_pnl": round(net_pnl, 2),
                "net_pnl": round(net_pnl, 2),
                "roi_pct": round(roi_pct, 2),
                "peak_pct": round(peak_pct, 2),
                "reason": exit_reason,
                "exit_time": now_dt.strftime("%H:%M:%S"),
                "exit_date": now_dt.strftime("%Y-%m-%d"),
                "time_ms": now_ms,
            }
            self.trade_history.insert(0, trade_record)
            self.save_trades_to_file()

            pnl_sign = "+" if net_pnl >= 0 else "-"
            pnl_str = f"{pnl_sign}${abs(net_pnl):,.2f}"
            self.log(
                "EXIT",
                f"EXIT {side_str} ({exit_reason}) @ {exit_price:,.2f} | Net PnL: {pnl_str} (ROI: {roi_pct:+.2f}%) | "
                f"Peak: +{peak_pct:.2f}% | Saldo: ${self.wallet_balance:,.2f}",
            )
            self.pos_side = 0

    # =========================================================================
    # 4. HOOK CANDLE CLOSE (PERSIS barstate.isconfirmed)
    # =========================================================================
    def on_candle_close(
        self,
        open_p: float,
        high_p: float,
        low_p: float,
        close_p: float,
        is_step: bool,
        step_dir: int,
    ):
        """Dipanggil setiap kali candle resmi tutup (barstate.isconfirmed)."""
        # A. Cek pending limit fill pada candle yang baru saja tutup
        if self.has_pending_order:
            self._try_fill_pending_order(open_p, low_p, high_p)
            if self.has_pending_order:
                self.pending_bar_count += 1
                if self.pending_bar_count > self.timeout_bars:
                    self.has_pending_order = False
                    self.log(
                        "TIMEOUT",
                        f"LIMIT ORDER EXPIRED: Tidak tersentuh selama {self.timeout_bars} bar. Dibatalkan.",
                    )

        # B. Cek Likuidasi & Exit Posisi
        if self.pos_side != 0 and not self.is_liquidated:
            worst_p = low_p if self.pos_side == 1 else high_p
            self._check_liquidation(worst_p)
            if self.pos_side != 0:
                self._manage_position_exit(high_p, low_p)

        # C. Pasang Sinyal Baru Jika Terjadi Step S/R Baru (Hanya jika Bot Aktif)
        if is_step and self.pos_side == 0 and not self.has_pending_order and not self.is_liquidated and self.is_bot_active:
            if step_dir == 1:  # Step Naik -> LONG Limit di bawah harga close
                self.has_pending_order = True
                self.pending_side = 1
                self.pending_price = close_p * (1.0 - self.pullback_pct)
                self.pending_sl = self.pending_price * (1.0 - self.sl_fixed_pct)
                self.pending_bar_count = 0
                self.pending_placed_time = time.time()
                self.log(
                    "SIGNAL",
                    f"STEP BUY CONFIRMED: Pasang Pending LONG Limit @ {self.pending_price:,.2f} "
                    f"(-{self.pullback_pct*100:.2f}% dari close {close_p:,.2f}) | SL Awal: {self.pending_sl:,.2f}",
                )
            elif step_dir == -1:  # Step Turun -> SHORT Limit di atas harga close
                self.has_pending_order = True
                self.pending_side = -1
                self.pending_price = close_p * (1.0 + self.pullback_pct)
                self.pending_sl = self.pending_price * (1.0 + self.sl_fixed_pct)
                self.pending_bar_count = 0
                self.pending_placed_time = time.time()
                self.log(
                    "SIGNAL",
                    f"STEP SELL CONFIRMED: Pasang Pending SHORT Limit @ {self.pending_price:,.2f} "
                    f"(+{self.pullback_pct*100:.2f}% dari close {close_p:,.2f}) | SL Awal: {self.pending_sl:,.2f}",
                )

    # =========================================================================
    # 5. HOOK LIVE TICK
    # =========================================================================
    def on_live_tick(self, price: float, high_p: float, low_p: float):
        """Dipanggil setiap tick masuk dari Binance WebSocket / Fast REST."""
        # A. Cek eksekusi pending order saat harga tick menyentuh limit
        if self.has_pending_order and not self.is_liquidated:
            self._try_fill_pending_order(price, low_p or price, high_p or price)

        # B. Update Trailing Stop & Cek SL Exit
        if self.pos_side != 0 and not self.is_liquidated:
            worst_p = low_p if self.pos_side == 1 else high_p
            self._check_liquidation(worst_p or price)
            if self.pos_side != 0:
                self._manage_position_exit(high_p or price, low_p or price)

    # =========================================================================
    # 6. EXPORT STATUS UNTUK DASHBOARD UI
    # =========================================================================
    def get_dashboard_state(self, current_price: float) -> Dict[str, Any]:
        """Menghitung metrik real-time untuk dirender pada Web Dashboard."""
        unrealized_pnl = 0.0
        roi_pct = 0.0
        dist_to_sl_pct = 0.0
        dist_to_act_pct = 0.0
        act_target_price = 0.0
        liq_price = 0.0

        if self.pos_side != 0 and current_price and self.pos_entry_price > 0:
            if self.pos_side == 1:
                unrealized_pnl = self.pos_qty * (current_price - self.pos_entry_price)
                dist_to_sl_pct = ((current_price - self.pos_sl_price) / current_price) * 100.0
                act_target_price = self.pos_entry_price * (1.0 + self.trail_act_pct)
                dist_to_act_pct = ((act_target_price - current_price) / current_price) * 100.0
                # Estimasi harga likuidasi
                liq_price = self.pos_entry_price - (self.wallet_balance / self.pos_qty)
            else:
                unrealized_pnl = self.pos_qty * (self.pos_entry_price - current_price)
                dist_to_sl_pct = ((self.pos_sl_price - current_price) / current_price) * 100.0
                act_target_price = self.pos_entry_price * (1.0 - self.trail_act_pct)
                dist_to_act_pct = ((current_price - act_target_price) / current_price) * 100.0
                liq_price = self.pos_entry_price + (self.wallet_balance / self.pos_qty)

            if self.pos_margin_used > 0:
                roi_pct = (unrealized_pnl / self.pos_margin_used) * 100.0

        # Statistik Trade
        total_trades = len(self.trade_history)
        win_trades = sum(1 for t in self.trade_history if t.get("net_pnl", 0) > 0)
        win_rate = (win_trades / total_trades * 100.0) if total_trades > 0 else 0.0
        total_realized_pnl = sum(t.get("net_pnl", 0) for t in self.trade_history)
        overall_roi = ((self.wallet_balance - self.initial_balance) / self.initial_balance * 100.0) if self.initial_balance > 0 else 0.0

        pending_dist_pct = 0.0
        if self.has_pending_order and current_price and self.pending_price > 0:
            pending_dist_pct = abs((current_price - self.pending_price) / current_price) * 100.0

        return {
            "symbol": self.symbol,
            "initial_balance": self.initial_balance,
            "wallet_balance": self.wallet_balance,
            "is_bot_active": self.is_bot_active,
            "total_realized_pnl": total_realized_pnl,
            "overall_roi": overall_roi,
            "total_trades": total_trades,
            "win_rate": win_rate,
            "is_liquidated": self.is_liquidated,
            "leverage": self.leverage,
            "margin_pct": self.margin_pct * 100.0,
            "position": {
                "active": self.pos_side != 0,
                "side": "LONG" if self.pos_side == 1 else ("SHORT" if self.pos_side == -1 else "FLAT"),
                "qty": self.pos_qty,
                "entry_price": self.pos_entry_price,
                "margin_used": self.pos_margin_used,
                "sl_price": self.pos_sl_price,
                "highest": self.pos_highest,
                "lowest": self.pos_lowest,
                "trail_activated": self.pos_trail_act,
                "unrealized_pnl": unrealized_pnl,
                "roi_pct": roi_pct,
                "act_target_price": act_target_price,
                "dist_to_sl_pct": dist_to_sl_pct,
                "dist_to_act_pct": dist_to_act_pct,
                "est_liq_price": max(0.0, liq_price),
            },
            "pending_order": {
                "active": self.has_pending_order,
                "side": "LONG" if self.pending_side == 1 else ("SHORT" if self.pending_side == -1 else "NONE"),
                "price": self.pending_price,
                "sl": self.pending_sl,
                "bar_count": self.pending_bar_count,
                "max_bars": self.timeout_bars,
                "dist_pct": pending_dist_pct,
            },
            "strategy_params": {
                "pullback_pct": self.pullback_pct * 100.0,
                "sl_fixed_pct": self.sl_fixed_pct * 100.0,
                "trail_act_pct": self.trail_act_pct * 100.0,
                "trail_cb_pct": self.trail_cb_pct * 100.0,
                "maker_fee_pct": self.maker_fee_pct * 100.0,
                "taker_fee_pct": self.taker_fee_pct * 100.0,
                "mmr_pct": self.mmr_pct * 100.0,
            },
            "recent_trades": self.trade_history[:50],
            "recent_logs": self.event_logs[:25],
        }
