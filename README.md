# Binance Futures Demo Strategy - VolStep PTL & Trailing Stop

Web App simulasi (*paper trading* / demo) Binance Futures Cross yang mengimplementasikan strategi **Volatility Step S/R Pullback Limit (PTL M4 Macro) & Dynamic Trailing Stop** yang 100% mereplikasi logika dari [vtrail.txt](file:///home/tomang/Desktop/gmgn/vtrail/vtrail.txt) (TradingView Pine Script v6) dengan engine data real-time berbasis WebSocket Binance Futures dari [vstep1.py](file:///home/tomang/Desktop/gmgn/vtrail/vstep1.py).

---

## 🚀 Cara Menjalankan Web App

1. Buka terminal di folder project:
   ```bash
   cd /home/tomang/Desktop/gmgn/vtrail
   ```

2. Jalankan aplikasi Flask:
   ```bash
   python3 app.py
   ```

3. Buka browser Anda dan akses:
   ```
   http://localhost:5000
   ```

---

## 🛠️ Fitur & Form Input Sesuai Pine Script

Klik tombol **⚙️ Pengaturan & API** di pojok kanan atas untuk mengubah parameter strategi secara dinamis tanpa perlu restart manual:

### 1. Binance Futures Testnet / Demo API
* **API Key & Secret Key**: Masukkan kredensial Binance Futures Testnet (`https://testnet.binancefuture.com`).
* **Test Koneksi**: Tombol *⚡ Test Koneksi & Cek Saldo Testnet* untuk memverifikasi API key dan memeriksa saldo akun testnet secara live.
* **Paper Trading Mode**: Jika API Key dikosongkan, bot otomatis berjalan dalam mode **Simulasi Lokal (Paper Trading)** dengan modal virtual tanpa risiko sama sekali.

### 2. Pasar & Timeframe
* **Koin**: BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT, DOGEUSDT, atau simbol USDT-M lainnya.
* **Timeframe**: `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d`.

### 3. Indikator Volatility Step S/R (Pine Script)
* **Line Mode**: `Anchor` (Lompat langsung ke close) atau `Without Anchor` (Bertahap).
* **Volatility Type**: `Stdev` (Standar Deviasi - default Pine Script) atau `Atr` (Average True Range).
* **Lookback Period ($L$)**: Default 200.
* **Multiplier ($M$)**: Default 4.0.

### 4. Strategi Cross Futures & Trailing Stop
* **Saldo Awal**: Default $1,000.00 USDT.
* **Leverage Cross**: Default 15x (mendukung 1x s/d 125x).
* **Margin Allocation %**: Default 25% dari saldo per posisi.
* **[PTL] Diskon Pullback Limit %**: Default 0.25% di bawah harga close untuk Long, di atas close untuk Short.
* **[PTL] Initial Stop Loss %**: Default 1.00% dari harga entry.
* **[Trailing] Aktivasi Profit %**: Default 0.80% profit untuk mengaktifkan trailing.
* **[Trailing] Callback Delta %**: Default 1.00% dari harga puncak (*peak*).
* **Pending Limit Timeout**: Default 48 candle (order otomatis hangus jika tidak tersentuh dalam 48 bar).
* **Biaya & Likuidasi**: Maker Fee (0.02%), Taker Fee (0.05%), Maintenance Margin Rate (0.40%).

---

## 📁 Struktur File Project

* [app.py](file:///home/tomang/Desktop/gmgn/vtrail/app.py): Backend Flask, WebSocket streaming runner, REST API endpoints, dan Server-Sent Events (SSE).
* [strategy.py](file:///home/tomang/Desktop/gmgn/vtrail/strategy.py): Engine simulasi strategi trading Binance Futures Cross (PTL + Trailing Stop + MMR).
* [binance_api.py](file:///home/tomang/Desktop/gmgn/vtrail/binance_api.py): Client HMAC-SHA256 otentikasi Binance Futures Testnet.
* [vstep1.py](file:///home/tomang/Desktop/gmgn/vtrail/vstep1.py): Core mathematical engine VolStep S/R (Dual-Buffer, Manual Stdev & ATR RMA, Fast 180ms close trigger).
* [vtrail.txt](file:///home/tomang/Desktop/gmgn/vtrail/vtrail.txt): Kode sumber asli Pine Script v6 TradingView.
* [templates/index.html](file:///home/tomang/Desktop/gmgn/vtrail/templates/index.html): UI dashboard real-time responsif bertema dark mode trading.
