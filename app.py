#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# === 1. MONKEY PATCH FIRST ===
import eventlet

eventlet.monkey_patch()
# === 2. IMPORTS ===
import datetime
import time
import threading
import requests
import requests.packages.urllib3
from typing import Dict, List, Any, Optional
import sqlite3
import pandas as pd
import os
import random
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
from nsetools import Nse
import lxml
import re
import yfinance as yf
import numpy as np

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning)
# --------------------------------------------------------------------------- #
# CONFIG
# --------------------------------------------------------------------------- #
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"
SEND_TEXT_UPDATES = True
UPDATE_INTERVAL = 120
MAX_HISTORY_ROWS_DB = 10000
LIVE_DATA_INTERVAL = 15

# +++ FIX: Standardize symbols to match yfinance and NSE ETFs +++
AUTO_SYMBOLS = ["NIFTY", "FINNIFTY", "BANKNIFTY", "SENSEX", "INDIAVIX", "GOLD", "SILVER", "BTC-USD", "USD-INR"]

# --------------------------------------------------------------------------- #
# WEB DASHBOARD
# --------------------------------------------------------------------------- #
app = Flask(__name__, template_folder='.', static_folder='static')
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')
shared_data: Dict[str, Dict[str, Any]] = {}
todays_history: Dict[str, List[Dict[str, Any]]] = {sym: [] for sym in AUTO_SYMBOLS}
data_lock = threading.Lock()
last_alert: Dict[str, float] = {sym: 0 for sym in AUTO_SYMBOLS}
last_history_update: Dict[str, float] = {sym: 0 for sym in AUTO_SYMBOLS}
initial_underlying_values: Dict[str, Optional[float]] = {sym: None for sym in AUTO_SYMBOLS}
site_visits = 0


@app.route('/')
def index(): return render_template('dashboard.html')


@socketio.on('connect')
def handle_connect():
    global site_visits
    with data_lock:
        site_visits += 1
        socketio.emit('update_visits', {'count': site_visits})
    with data_lock:
        live_feed_summary = {sym: data.get('live_feed_summary', {}) for sym, data in shared_data.items()}
        emit('update', {'live': shared_data, 'live_feed_summary': live_feed_summary}, to=request.sid)
        emit('initial_todays_history', {'history': todays_history}, to=request.sid)


def broadcast_live_update():
    with data_lock:
        live_feed_summary = {sym: data.get('live_feed_summary', {}) for sym, data in shared_data.items()}
        socketio.emit('update', {'live': shared_data, 'live_feed_summary': live_feed_summary})


def broadcast_history_append(sym: str, new_history_item: Dict[str, Any]):
    with data_lock: socketio.emit('todays_history_append', {'symbol': sym, 'item': new_history_item})


@app.route('/history/<symbol>/<date_str>')
def get_historical_data(symbol: str, date_str: str):
    if symbol not in AUTO_SYMBOLS: return jsonify({"error": f"Invalid symbol: {symbol}"}), 400
    history_for_date: List[Dict[str, Any]] = []
    if analyzer.conn:
        cur = analyzer.conn.cursor()
        try:
            ist_day_start = datetime.datetime.strptime(date_str, "%Y-%m-%d")
            utc_day_start = ist_day_start - datetime.timedelta(hours=5, minutes=30)
            utc_day_end = utc_day_start + datetime.timedelta(days=1)
            cur.execute(
                "SELECT timestamp, sp, value, call_oi, put_oi, pcr, sentiment, add_exit, pcr_change, intraday_pcr FROM history WHERE symbol = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp DESC",
                (symbol, utc_day_start.strftime('%Y-%m-%d %H:%M:%S'), utc_day_end.strftime('%Y-%m-%d %H:%M:%S')))
            rows = cur.fetchall()
            for r in rows:
                history_for_date.append(
                    {'time': analyzer._convert_utc_to_ist_display(r[0]), 'sp': r[1], 'value': r[2], 'call_oi': r[3],
                     'put_oi': r[4], 'pcr': r[5], 'sentiment': r[6], 'add_exit': r[7],
                     'pcr_change': r[8] if r[8] is not None else 0.0,
                     'intraday_pcr': r[9] if r[9] is not None else 0.0})
        except Exception as e:
            return jsonify({"error": "Failed to query database."}), 500
    return jsonify({"history": history_for_date})


@app.route('/clear_todays_history', methods=['POST'])
def clear_history_endpoint():
    symbol_to_clear = request.json.get('symbol')
    analyzer.clear_todays_history_db(symbol_to_clear)
    return jsonify({"status": "success", "message": "Today's history cleared."})


# --------------------------------------------------------------------------- #
# ANALYZER CLASS
# --------------------------------------------------------------------------- #
class NseBseAnalyzer:
    def __init__(self):
        self.stop = threading.Event()
        self.session = requests.Session()
        self.nse_headers = {'user-agent': 'Mozilla/5.0'}
        self.url_oc = "https://www.nseindia.com/option-chain"
        self.url_nse = "https://www.nseindia.com/api/option-chain-indices?symbol="
        self.db_path = 'nse_bse_data.db'
        self.conn: Optional[sqlite3.Connection] = None

        # +++ FIX: Use consistent yfinance tickers and Indian commodity ETFs +++
        self.YFINANCE_SYMBOLS = ["SENSEX", "INDIAVIX", "GOLD", "SILVER", "BTC-USD", "USD-INR"]
        self.YFINANCE_TICKER_MAP = {
            "SENSEX": "^BSESN",
            "INDIAVIX": "^INDIAVIX",
            "GOLD": "GOLDBEES.NS",
            "SILVER": "SILVERBEES.NS",
            "BTC-USD": "BTC-USD",
            "USD-INR": "INR=X"
        }
        self.TICKER_ONLY_SYMBOLS = ["GOLD", "SILVER", "BTC-USD", "USD-INR"]

        self.previous_data = {}

        self._init_db()
        self.pcr_graph_data: Dict[str, List[Dict[str, Any]]] = {sym: [] for sym in AUTO_SYMBOLS}
        self.previous_pcr: Dict[str, float] = {sym: 0.0 for sym in AUTO_SYMBOLS}
        self._load_todays_history_from_db()
        self._load_initial_underlying_values()
        self._populate_initial_shared_chart_data()
        self.nse_tool = Nse()
        threading.Thread(target=self.run_loop, daemon=True).start()

    def _init_db(self):
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cur = self.conn.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, symbol TEXT, sp REAL, value REAL, call_oi REAL, put_oi REAL, pcr REAL, sentiment TEXT, add_exit TEXT, pcr_change REAL, intraday_pcr REAL)")
            self.conn.commit()
        except Exception as e:
            print(f"DB error: {e}")

    def _populate_initial_shared_chart_data(self):
        with data_lock:
            for sym in AUTO_SYMBOLS:
                if sym not in self.YFINANCE_SYMBOLS:
                    if sym not in shared_data: shared_data[sym] = {}
                    shared_data[sym]['pcr_chart_data'] = self.pcr_graph_data.get(sym, [])

    def _load_todays_history_from_db(self):
        if not self.conn: return
        try:
            cur = self.conn.cursor()
            ist_now = self._get_ist_time()
            utc_start_str = ist_now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(
                datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            for sym in AUTO_SYMBOLS:
                if sym in self.TICKER_ONLY_SYMBOLS: continue
                cur.execute(
                    "SELECT timestamp, sp, value, call_oi, put_oi, pcr, sentiment, add_exit, pcr_change, intraday_pcr FROM history WHERE symbol = ? AND timestamp >= ? ORDER BY timestamp DESC",
                    (sym, utc_start_str))
                rows = cur.fetchall()
                with data_lock:
                    todays_history[sym] = []
                    for r in rows:
                        todays_history[sym].append(
                            {'time': self._convert_utc_to_ist_display(r[0]), 'sp': r[1], 'value': r[2], 'call_oi': r[3],
                             'put_oi': r[4], 'pcr': r[5], 'sentiment': r[6], 'add_exit': r[7],
                             'pcr_change': r[8] if r[8] is not None else 0.0,
                             'intraday_pcr': r[9] if r[9] is not None else 0.0})
                    if sym not in self.YFINANCE_SYMBOLS:
                        if todays_history[sym]: self.previous_pcr[sym] = todays_history[sym][0]['pcr']
                        self.pcr_graph_data[sym] = [{"TIME": item['time'], "PCR": item['pcr']} for item in
                                                    reversed(todays_history[sym])]
        except Exception as e:
            print(f"History load error: {e}")

    def _load_initial_underlying_values(self):
        pass

    def _get_ist_time(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=5, minutes=30)

    def _convert_utc_to_ist_display(self, utc_timestamp_str: str) -> str:
        try:
            utc_dt = datetime.datetime.strptime(utc_timestamp_str, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=datetime.timezone.utc)
            return (utc_dt + datetime.timedelta(hours=5, minutes=30)).strftime("%H:%M")
        except (ValueError, TypeError):
            return "00:00"

    def clear_todays_history_db(self, sym: Optional[str] = None):
        pass

    def run_loop(self):
        try:
            self.session.get(self.url_oc, headers=self.nse_headers, timeout=10, verify=False)
        except requests.exceptions.RequestException as e:
            print(f"Initial NSE session setup failed: {e}")
        while not self.stop.is_set():
            for sym in AUTO_SYMBOLS:
                try:
                    self.fetch_and_process_symbol(sym)
                except Exception as e:
                    print(f"{sym} error during fetch and process: {e}")
            time.sleep(LIVE_DATA_INTERVAL)

    def fetch_and_process_symbol(self, sym: str):
        if sym in self.YFINANCE_SYMBOLS:
            self._process_yfinance_data(sym)
        else:
            self._process_nse_data(sym)

    def _process_yfinance_data(self, sym: str):
        try:
            ticker_str = self.YFINANCE_TICKER_MAP.get(sym)
            if not ticker_str: return
            ticker = yf.Ticker(ticker_str)
            hist = ticker.history(period="2d")
            if hist.empty or len(hist) < 2: return

            current_price, previous_close = hist['Close'].iloc[-1], hist['Close'].iloc[-2]
            change = current_price - previous_close
            pct_change = (change / previous_close) * 100 if previous_close != 0 else 0

            with data_lock:
                if sym not in shared_data: shared_data[sym] = {}
                shared_data[sym]['live_feed_summary'] = {'current_value': round(current_price, 4),
                                                         'change': round(change, 4),
                                                         'percentage_change': round(pct_change, 2)}

                if sym not in self.TICKER_ONLY_SYMBOLS:
                    sentiment = "Mild Bearish" if change < 0 else "Mild Bullish" if change > 0 else "Neutral"
                    summary = {'time': self._get_ist_time().strftime("%H:%M"), 'sp': round(change, 2),
                               'value': round(current_price, 2), 'pcr': round(pct_change, 2), 'sentiment': sentiment,
                               'call_oi': 0, 'put_oi': 0, 'add_exit': "Live Price", 'pcr_change': 0, 'intraday_pcr': 0,
                               'expiry': 'N/A'}
                    shared_data[sym]['summary'] = summary
                    shared_data[sym]['strikes'], shared_data[sym]['max_pain_chart_data'], shared_data[sym][
                        'ce_oi_chart_data'], shared_data[sym]['pe_oi_chart_data'], shared_data[sym][
                        'pcr_chart_data'] = [], [], [], [], []

            print(f"{sym} YFINANCE DATA UPDATED | Value: {current_price:.2f}")
            broadcast_live_update()

            if sym not in self.TICKER_ONLY_SYMBOLS:
                now = time.time()
                if now - last_history_update.get(sym, 0) >= UPDATE_INTERVAL:
                    with data_lock: todays_history[sym].insert(0, summary)
                    broadcast_history_append(sym, summary)
                    last_history_update[sym] = now
                    self._save_db(sym, summary)
                if now - last_alert.get(sym, 0) >= UPDATE_INTERVAL:
                    self.send_alert(sym, summary)
                    last_alert[sym] = now
        except Exception as e:
            print(f"{sym} yfinance processing error: {e}")

    def _get_oi_buildup(self, price_change, oi_change):
        # First, check for significant price movement to do full analysis
        if abs(price_change) > 0.01:  # Use a small threshold to avoid floating point issues
            if price_change > 0 and oi_change > 0: return "Long Buildup"
            if price_change > 0 and oi_change < 0: return "Short Covering"
            if price_change < 0 and oi_change > 0: return "Short Buildup"
            if price_change < 0 and oi_change < 0: return "Long Unwinding"

        # If price change is not significant (like on the first run), check only OI change
        if oi_change > 100:
            return "Fresh OI Added"
        if oi_change < -100:
            return "OI Exited"

        return ""

    def _process_nse_data(self, sym: str):
        try:
            url = self.url_nse + sym
            resp = self.session.get(url, headers=self.nse_headers, timeout=10, verify=False)
            if resp.status_code == 401:
                self.session.get(self.url_oc, headers=self.nse_headers, timeout=5, verify=False)
                resp = self.session.get(url, headers=self.nse_headers, timeout=10, verify=False)

            data = resp.json()
            expiry = data['records']['expiryDates'][0]

            underlying = data['records']['underlyingValue']

            # +++ THE DEFINITIVE FIX FOR TypeError +++
            prev_price = initial_underlying_values.get(sym)
            if prev_price is None:
                price_change = 0
                initial_underlying_values[sym] = float(underlying)
            else:
                price_change = underlying - prev_price
            # +++ END OF FIX +++

            ce_values = [d['CE'] for d in data['records']['data'] if 'CE' in d and d['expiryDate'] == expiry]
            pe_values = [d['PE'] for d in data['records']['data'] if 'PE' in d and d['expiryDate'] == expiry]
            if not ce_values or not pe_values: return

            df_ce = pd.DataFrame(ce_values)
            df_pe = pd.DataFrame(pe_values)
            df = pd.merge(df_ce[['strikePrice', 'openInterest', 'changeinOpenInterest']],
                          df_pe[['strikePrice', 'openInterest', 'changeinOpenInterest']], on='strikePrice', how='outer',
                          suffixes=('_call', '_put')).fillna(0)

            sp = self.get_atm_strike(df, underlying)
            if not sp: return

            idx_list = df[df['strikePrice'] == sp].index.tolist()
            if not idx_list: return
            idx = idx_list[0]

            strikes_data, ce_add_strikes, ce_exit_strikes, pe_add_strikes, pe_exit_strikes = [], [], [], [], []

            for i in range(-10, 11):
                if not (0 <= idx + i < len(df)): continue
                row = df.iloc[idx + i]
                strike = int(row['strikePrice'])

                call_oi = int(row['openInterest_call'])
                put_oi = int(row['openInterest_put'])
                call_coi = int(row['changeinOpenInterest_call'])
                put_coi = int(row['changeinOpenInterest_put'])

                call_buildup = self._get_oi_buildup(price_change, call_coi)
                put_buildup = self._get_oi_buildup(price_change, put_coi)

                call_action = "ADD" if call_coi > 0 else "EXIT" if call_coi < 0 else ""
                put_action = "ADD" if put_coi > 0 else "EXIT" if put_coi < 0 else ""
                if call_action == "ADD":
                    ce_add_strikes.append(str(strike))
                elif call_action == "EXIT":
                    ce_exit_strikes.append(str(strike))
                if put_action == "ADD":
                    pe_add_strikes.append(str(strike))
                elif put_action == "EXIT":
                    pe_exit_strikes.append(str(strike))

                strikes_data.append(
                    {'strike': strike, 'call_oi': call_oi, 'call_coi': call_coi, 'call_action': call_action,
                     'put_oi': put_oi, 'put_coi': put_coi, 'put_action': put_action, 'is_atm': i == 0,
                     'call_buildup': call_buildup, 'put_buildup': put_buildup})

            total_call_oi = int(df['openInterest_call'].sum())
            total_put_oi = int(df['openInterest_put'].sum())
            total_call_coi = int(df['changeinOpenInterest_call'].sum())
            total_put_coi = int(df['changeinOpenInterest_put'].sum())

            pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi else 0.0
            pcr_change = round(pcr - self.previous_pcr.get(sym, pcr), 2) if self.previous_pcr.get(sym,
                                                                                                  0.0) != 0.0 else 0.0
            intraday_pcr = round(total_put_coi / total_call_coi, 2) if total_call_coi != 0 else 0.0

            atm_index_in_strikes_data = next((j for j, s in enumerate(strikes_data) if s['is_atm']), 10)
            atm_call_coi = strikes_data[atm_index_in_strikes_data]['call_coi']
            atm_put_coi = strikes_data[atm_index_in_strikes_data]['put_coi']
            diff = round((atm_call_coi - atm_put_coi) / 1000, 1)
            base_sentiment = self.get_sentiment(diff, pcr)
            enhanced_sentiment = self._get_enhanced_sentiment(sym, base_sentiment)
            add_exit_str = " | ".join(filter(None,
                                             [f"CE Add: {', '.join(sorted(ce_add_strikes))}" if ce_add_strikes else "",
                                              f"PE Add: {', '.join(sorted(pe_add_strikes))}" if pe_add_strikes else "",
                                              f"CE Exit: {', '.join(sorted(ce_exit_strikes))}" if ce_exit_strikes else "",
                                              f"PE Exit: {', '.join(sorted(pe_exit_strikes))}" if pe_exit_strikes else ""])) or "No Change"

            summary = {'time': self._get_ist_time().strftime("%H:%M"), 'sp': int(sp), 'value': int(round(underlying)),
                       'call_oi': round(atm_call_coi / 1000, 1), 'put_oi': round(atm_put_coi / 1000, 1), 'pcr': pcr,
                       'sentiment': enhanced_sentiment, 'expiry': expiry, 'add_exit': add_exit_str,
                       'pcr_change': pcr_change, 'intraday_pcr': intraday_pcr}

            pulse_summary = {'total_call_oi': total_call_oi, 'total_put_oi': total_put_oi,
                             'total_call_coi': total_call_coi, 'total_put_coi': total_put_coi}

            max_pain_df = self._calculate_max_pain(df_ce, df_pe)
            ce_dt_for_charts = df_ce.sort_values(['openInterest'], ascending=False)
            pe_dt_for_charts = df_pe.sort_values(['openInterest'], ascending=False)
            final_ce_data = ce_dt_for_charts[['strikePrice', 'openInterest']].iloc[:10].to_dict(orient='records')
            final_pe_data = pe_dt_for_charts[['strikePrice', 'openInterest']].iloc[:10].to_dict(orient='records')

            with data_lock:
                if sym not in shared_data: shared_data[sym] = {}
                shared_data[sym].update({'summary': summary, 'strikes': strikes_data, 'pulse_summary': pulse_summary})

                shared_data[sym]['max_pain_chart_data'] = max_pain_df.to_dict(orient='records')
                shared_data[sym]['ce_oi_chart_data'] = final_ce_data
                shared_data[sym]['pe_oi_chart_data'] = final_pe_data
                shared_data[sym]['pcr_chart_data'] = self.pcr_graph_data.get(sym, [])

                shared_data[sym]['live_feed_summary'] = {'current_value': int(round(underlying)),
                                                         'change': round(price_change, 2), 'percentage_change': round((
                                                                                                                                  price_change / initial_underlying_values.get(
                                                                                                                              sym,
                                                                                                                              underlying)) * 100 if initial_underlying_values.get(
                        sym) else 0, 2)}

            print(f"{sym} LIVE DATA UPDATED | SP: {sp} | PCR: {pcr}")
            broadcast_live_update()

            now = time.time()
            if now - last_history_update.get(sym, 0) >= UPDATE_INTERVAL:
                self.previous_pcr[sym] = pcr
                with data_lock:
                    todays_history[sym].insert(0, summary)
                    if sym not in self.YFINANCE_SYMBOLS:
                        self.pcr_graph_data[sym].append({"TIME": summary['time'], "PCR": summary['pcr']})
                        if len(self.pcr_graph_data[sym]) > 180: self.pcr_graph_data[sym].pop(0)
                        shared_data[sym]['pcr_chart_data'] = self.pcr_graph_data[sym]
                broadcast_history_append(sym, summary)
                last_history_update[sym] = now
                self._save_db(sym, summary)

            if now - last_alert.get(sym, 0) >= UPDATE_INTERVAL:
                self.send_alert(sym, summary)
                last_alert[sym] = now
        except Exception as e:
            import traceback
            print(f"{sym} processing error: {e}\n{traceback.format_exc()}")

    def get_atm_strike(self, df: pd.DataFrame, underlying: float) -> Optional[int]:
        try:
            strikes = df['strikePrice'].astype(int).unique()
            return min(strikes, key=lambda x: abs(x - underlying)) if len(strikes) > 0 else None
        except Exception as e:
            return None

    def get_sentiment(self, diff: float, pcr: float) -> str:
        if diff > 10 or pcr < 0.7: return "Strong Bearish"
        if diff < -10 or pcr > 1.3: return "Strong Bullish"
        if diff > 2 or (0.7 <= pcr < 0.9): return "Mild Bearish"
        if diff < -2 or (1.1 < pcr <= 1.3): return "Mild Bullish"
        return "Neutral"

    def _get_enhanced_sentiment(self, sym: str, base_sentiment: str) -> str:
        return base_sentiment

    def send_alert(self, sym: str, row: Dict[str, Any]):
        pass

    def _save_db(self, sym, row):
        if not self.conn: return
        try:
            ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            self.conn.execute(
                "INSERT INTO history (timestamp, symbol, sp, value, call_oi, put_oi, pcr, sentiment, add_exit, pcr_change, intraday_pcr) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (ts, sym, row.get('sp', 0), row.get('value', 0), row.get('call_oi', 0), row.get('put_oi', 0),
                 row.get('pcr', 0), row.get('sentiment', ''), row.get('add_exit', ''), row.get('pcr_change', 0.0),
                 row.get('intraday_pcr', 0.0)))
            self.conn.execute(
                "DELETE FROM history WHERE id NOT IN (SELECT id FROM history WHERE symbol = ? ORDER BY timestamp DESC LIMIT ?) AND symbol = ?;",
                (sym, MAX_HISTORY_ROWS_DB, sym))
            self.conn.commit()
        except Exception as e:
            print(f"DB save error for {sym}: {e}")

    def _calculate_max_pain(self, ce_dt: pd.DataFrame, pe_dt: pd.DataFrame) -> pd.DataFrame:
        MxPn_CE = ce_dt[['strikePrice', 'openInterest']]
        MxPn_PE = pe_dt[['strikePrice', 'openInterest']]
        MxPn_Df = pd.merge(MxPn_CE, MxPn_PE, on=['strikePrice'], how='outer', suffixes=('_call', '_Put')).fillna(0)
        StrikePriceList = MxPn_Df['strikePrice'].values.tolist()
        OiCallList = MxPn_Df['openInterest_call'].values.tolist()
        OiPutList = MxPn_Df['openInterest_Put'].values.tolist()
        TCVSP = [int(self._total_option_pain_for_strike(StrikePriceList, OiCallList, OiPutList, sp_val)) for sp_val in
                 StrikePriceList]
        max_pain_df = pd.DataFrame({'StrikePrice': StrikePriceList, 'TotalMaxPain': TCVSP})
        if not max_pain_df.empty:
            min_pain_strike = max_pain_df.loc[max_pain_df['TotalMaxPain'].idxmin()]['StrikePrice']
            strikes_sorted = sorted(StrikePriceList)
            try:
                center_idx = strikes_sorted.index(min_pain_strike)
            except ValueError:
                center_idx = (pd.Series(strikes_sorted) - min_pain_strike).abs().idxmin()
            start_idx = max(0, center_idx - 8)
            end_idx = min(len(strikes_sorted), center_idx + 8)
            strikes_in_window = strikes_sorted[start_idx:end_idx]
            return max_pain_df[max_pain_df['StrikePrice'].isin(strikes_in_window)]
        return pd.DataFrame()

    def _total_option_pain_for_strike(self, strike_price_list: List[float], oi_call_list: List[int],
                                      oi_put_list: List[int], mxpn_strike: float) -> float:
        total_cash_value = sum((max(0, mxpn_strike - strike) * call_oi) + (max(0, strike - mxpn_strike) * put_oi) for
                               strike, call_oi, put_oi in zip(strike_price_list, oi_call_list, oi_put_list))
        return total_cash_value


if __name__ == '__main__':
    analyzer = NseBseAnalyzer()
    print("WEB DASHBOARD LIVE → http://127.0.0.1:5000")
    socketio.run(app, host='0.0.0.0', port=5000)
