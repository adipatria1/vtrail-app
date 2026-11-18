import hashlib
import hmac
import time
from typing import Any, Dict, Optional
import requests

TESTNET_REST_URL = "https://testnet.binancefuture.com"
MAINNET_REST_URL = "https://fapi.binance.com"


class BinanceFuturesClient:
    """
    Client otentikasi Binance Futures (Testnet & Mainnet)
    Menangani HMAC-SHA256 signature, pengecekan saldo, posisi, dan verifikasi API key/secret.
    """

    def __init__(self, api_key: str = "", api_secret: str = "", is_testnet: bool = True):
        self.api_key = api_key.strip()
        self.api_secret = api_secret.strip()
        self.is_testnet = is_testnet
        self.base_url = TESTNET_REST_URL if is_testnet else MAINNET_REST_URL

    def _sign(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = 10000
        query_str = "&".join(f"{k}={v}" for k, v in params.items())
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        params["signature"] = signature
        return params

    def ping(self) -> Dict[str, Any]:
        """Cek konektivitas ke server Binance Futures Testnet."""
        try:
            r = requests.get(f"{self.base_url}/fapi/v1/time", timeout=5)
            if r.status_code == 200:
                return {"success": True, "server_time": r.json().get("serverTime")}
            return {"success": False, "error": f"HTTP {r.status_code}: {r.text}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def verify_account(self) -> Dict[str, Any]:
        """
        Verifikasi API Key & Secret Key pada Binance Futures Testnet.
        Mengembalikan total saldo USDT dan saldo yang tersedia.
        """
        if not self.api_key or not self.api_secret:
            return {
                "success": False,
                "error": "API Key dan Secret Key tidak boleh kosong.",
            }

        try:
            params = self._sign({})
            headers = {"X-MBX-APIKEY": self.api_key}
            url = f"{self.base_url}/fapi/v2/balance"
            r = requests.get(url, params=params, headers=headers, timeout=6)

            if r.status_code == 200:
                data = r.json()
                usdt_asset = next((b for b in data if b.get("asset") == "USDT"), None)
                total_bal = float(usdt_asset["balance"]) if usdt_asset else 0.0
                avail_bal = float(usdt_asset["availableBalance"]) if usdt_asset else 0.0
                unrealized_pnl = float(usdt_asset.get("crossUnPnl", 0.0)) if usdt_asset else 0.0

                return {
                    "success": True,
                    "is_testnet": self.is_testnet,
                    "total_balance_usdt": total_bal,
                    "available_balance_usdt": avail_bal,
                    "unrealized_pnl": unrealized_pnl,
                    "assets": [
                        {
                            "asset": a["asset"],
                            "balance": float(a["balance"]),
                            "available": float(a["availableBalance"]),
                        }
                        for a in data
                        if float(a.get("balance", 0)) > 0
                    ],
                    "message": "Koneksi Binance Futures Testnet terverifikasi!",
                }
            else:
                err_json = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                err_msg = err_json.get("msg", r.text)
                return {
                    "success": False,
                    "error": f"Gagal otentikasi (Code {err_json.get('code', r.status_code)}): {err_msg}",
                }
        except Exception as e:
            return {"success": False, "error": f"Koneksi gagal: {str(e)}"}

    def get_positions(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Mengambil data posisi aktif dari Binance Testnet."""
        if not self.api_key or not self.api_secret:
            return {"success": False, "error": "API Key belum diisi."}

        try:
            params: Dict[str, Any] = {}
            if symbol:
                params["symbol"] = symbol.upper()
            params = self._sign(params)
            headers = {"X-MBX-APIKEY": self.api_key}
            r = requests.get(f"{self.base_url}/fapi/v2/positionRisk", params=params, headers=headers, timeout=6)
            if r.status_code == 200:
                positions = r.json()
                active = [
                    p for p in positions
                    if float(p.get("positionAmt", 0)) != 0
                ]
                return {"success": True, "positions": active}
            return {"success": False, "error": r.text}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_user_trades(self, symbol: str = "BTCUSDT", limit: int = 50) -> Dict[str, Any]:
        """
        Mengambil riwayat trade akun (filled trades) langsung dari endpoint resmi Binance:
        GET /fapi/v1/userTrades
        Setiap item berisi: id, orderId, symbol, side, price, qty, realizedPnl, commission, time, maker.
        """
        if not self.api_key or not self.api_secret:
            return {"success": False, "error": "API Key dan Secret Key belum diisi.", "trades": []}

        try:
            params = {
                "symbol": symbol.upper().strip(),
                "limit": min(limit, 100),
            }
            params = self._sign(params)
            headers = {"X-MBX-APIKEY": self.api_key}
            url = f"{self.base_url}/fapi/v1/userTrades"
            r = requests.get(url, params=params, headers=headers, timeout=6)

            if r.status_code == 200:
                data = r.json()
                formatted_trades = []
                for t in reversed(data):
                    time_dt = time.strftime("%H:%M:%S", time.localtime(t["time"] / 1000.0))
                    formatted_trades.append({
                        "id": t.get("id"),
                        "order_id": t.get("orderId"),
                        "symbol": t.get("symbol"),
                        "side": t.get("side"),
                        "price": float(t.get("price", 0)),
                        "qty": float(t.get("qty", 0)),
                        "quote_qty": float(t.get("quoteQty", 0)),
                        "realized_pnl": float(t.get("realizedPnl", 0)),
                        "commission": float(t.get("commission", 0)),
                        "commission_asset": t.get("commissionAsset", "USDT"),
                        "maker": t.get("maker", False),
                        "time": time_dt,
                        "time_ms": t.get("time"),
                    })
                return {"success": True, "trades": formatted_trades, "count": len(formatted_trades)}
            else:
                err_json = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                err_msg = err_json.get("msg", r.text)
                return {"success": False, "error": f"Binance Error ({r.status_code}): {err_msg}", "trades": []}
        except Exception as e:
            return {"success": False, "error": f"Koneksi gagal: {str(e)}", "trades": []}

    def get_all_orders(self, symbol: str = "BTCUSDT", limit: int = 50) -> Dict[str, Any]:
        """
        Mengambil seluruh riwayat order akun langsung dari endpoint resmi Binance:
        GET /fapi/v1/allOrders
        """
        if not self.api_key or not self.api_secret:
            return {"success": False, "error": "API Key dan Secret Key belum diisi.", "orders": []}

        try:
            params = {
                "symbol": symbol.upper().strip(),
                "limit": min(limit, 100),
            }
            params = self._sign(params)
            headers = {"X-MBX-APIKEY": self.api_key}
            url = f"{self.base_url}/fapi/v1/allOrders"
            r = requests.get(url, params=params, headers=headers, timeout=6)

            if r.status_code == 200:
                data = r.json()
                return {"success": True, "orders": list(reversed(data)), "count": len(data)}
            else:
                err_json = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                err_msg = err_json.get("msg", r.text)
                return {"success": False, "error": f"Binance Error ({r.status_code}): {err_msg}", "orders": []}
        except Exception as e:
            return {"success": False, "error": f"Koneksi gagal: {str(e)}", "orders": []}

