#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# === 1. MONKEY PATCH FIRST ===
import eventlet

# It's generally best practice to monkey patch at the very beginning
# before other modules that might be affected are imported.
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
# For web scraping
from bs4 import BeautifulSoup
# For NseTools integration
from nsetools import Nse
# Import lxml explicitly (ensure you have it installed: pip install lxml)
import lxml
import re  # For regex in VIX scraping

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning)
# --------------------------------------------------------------------------- #
# CONFIG
# --------------------------------------------------------------------------- #
TELEGRAM_BOT_TOKEN = "8081075640:AAEDdrUSdg8BRX4CcKJ4W7jG1EOnSFTQPr4"
TELEGRAM_CHAT_ID = "275529219"
SEND_TEXT_UPDATES = True
UPDATE_INTERVAL = 120  # 2 minutes (for history, Telegram, and PCR graph data points)
MAX_HISTORY_ROWS_DB = 10000  # Max rows to keep in DB for a symbol
LIVE_DATA_INTERVAL = 15  # 15 seconds (for summary cards and option chain tables)
# DHANHQ API KEYS (FREE FROM dhan.co)
DHAN_CLIENT_ID = "YOUR_CLIENT_ID"  # <<< IMPORTANT: Replace with your actual DHAN_CLIENT_ID
DHAN_ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"  # <<< IMPORTANT: Replace with your actual DHAN_ACCESS_TOKEN
AUTO_SYMBOLS = ["NIFTY", "FINNIFTY", "BANKNIFTY", "SENSEX", "INDIAVIX"]


# --------------------------------------------------------------------------- #
# TELEGRAM
# --------------------------------------------------------------------------- #
def send_telegram_text(message: str):
    if not SEND_TEXT_UPDATES or not TELEGRAM_BOT_TOKEN: return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'HTML',
        'disable_web_page_preview': True
    }
    for _ in range(3):
        try:
            r = requests.post(url, data=payload, timeout=15)
            return r.status_code == 200
        except requests.exceptions.RequestException as e:
            print(f"Telegram send error: {e}")
            time.sleep(3)
    return False


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
def index():
    return render_template('dashboard.html')


@socketio.on('connect')
def handle_connect():
    global site_visits
    with data_lock:
        site_visits += 1
        socketio.emit('update_visits', {'count': site_visits})

    with data_lock:
        safe_live = {}
        live_feed_summary = {}
        for sym, data in shared_data.items():
            safe_live[sym] = {}
            if 'summary' in data:
                safe_live[sym]['summary'] = data['summary']
            if 'strikes' in data:
                safe_live[sym]['strikes'] = data['strikes']
            if 'live_feed_summary' in data:
                live_feed_summary[sym] = data['live_feed_summary']
            if 'pcr_chart_data' in data:
                safe_live[sym]['pcr_chart_data'] = data['pcr_chart_data']
            if 'max_pain_chart_data' in data:
                safe_live[sym]['max_pain_chart_data'] = data['max_pain_chart_data']
            if 'ce_oi_chart_data' in data:
                safe_live[sym]['ce_oi_chart_data'] = data['ce_oi_chart_data']
            if 'pe_oi_chart_data' in data:
                safe_live[sym]['pe_oi_chart_data'] = data['pe_oi_chart_data']

        emit('update', {'live': safe_live, 'live_feed_summary': live_feed_summary}, to=request.sid)
        emit('initial_todays_history', {'history': todays_history}, to=request.sid)


def broadcast_live_update():
    with data_lock:
        safe_live = {}
        live_feed_summary = {}
        for sym, data in shared_data.items():
            safe_live[sym] = {}
            if 'summary' in data:
                safe_live[sym]['summary'] = data['summary']
            if 'strikes' in data:
                safe_live[sym]['strikes'] = data['strikes']
            if 'live_feed_summary' in data:
                live_feed_summary[sym] = data['live_feed_summary']
            if 'pcr_chart_data' in data:
                safe_live[sym]['pcr_chart_data'] = data['pcr_chart_data']
            if 'max_pain_chart_data' in data:
                safe_live[sym]['max_pain_chart_data'] = data['max_pain_chart_data']
            if 'ce_oi_chart_data' in data:
                safe_live[sym]['ce_oi_chart_data'] = data['ce_oi_chart_data']
            if 'pe_oi_chart_data' in data:
                safe_live[sym]['pe_oi_chart_data'] = data['pe_oi_chart_data']
        socketio.emit('update', {'live': safe_live, 'live_feed_summary': live_feed_summary})


def broadcast_history_append(sym: str, new_history_item: Dict[str, Any]):
    with data_lock:
        socketio.emit('todays_history_append', {'symbol': sym, 'item': new_history_item})


@app.route('/history/<symbol>/<date_str>')
def get_historical_data(symbol: str, date_str: str):
    try:
        datetime.datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
    if symbol not in AUTO_SYMBOLS:
        return jsonify({"error": f"Invalid symbol: {symbol}"}), 400

    history_for_date: List[Dict[str, Any]] = []
    with data_lock:
        if analyzer.conn:
            cur = analyzer.conn.cursor()
            try:
                ist_day_start = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                utc_day_start = ist_day_start - datetime.timedelta(hours=5, minutes=30)
                utc_day_end = utc_day_start + datetime.timedelta(days=1)

                cur.execute("""
                    SELECT timestamp, sp, value, call_oi, put_oi, pcr, sentiment, add_exit, pcr_change, intraday_pcr
                    FROM history
                    WHERE symbol = ? AND timestamp >= ? AND timestamp < ?
                    ORDER BY timestamp DESC
                """, (symbol, utc_day_start.strftime('%Y-%m-%d %H:%M:%S'), utc_day_end.strftime('%Y-%m-%d %H:%M:%S')))
                rows = cur.fetchall()
                for r in rows:
                    history_for_date.append({
                        'time': analyzer._convert_utc_to_ist_display(r[0]), 'sp': r[1], 'value': r[2],
                        'call_oi': r[3], 'put_oi': r[4], 'pcr': r[5], 'sentiment': r[6],
                        'add_exit': r[7], 'pcr_change': r[8], 'intraday_pcr': r[9]
                    })
            except Exception as e:
                print(f"Error during historical data fetch: {e}")
                return jsonify({"error": "Failed to query database."}), 500
        else:
            return jsonify({"error": "Database not connected."}), 500
    return jsonify({"history": history_for_date})


@app.route('/clear_todays_history', methods=['POST'])
def clear_history_endpoint():
    symbol_to_clear = request.json.get('symbol')
    if symbol_to_clear not in AUTO_SYMBOLS and symbol_to_clear is not None:
        return jsonify({"status": "error", "message": f"Invalid symbol: {symbol_to_clear}"}), 400

    analyzer.clear_todays_history_db(symbol_to_clear)
    return jsonify({"status": "success", "message": "Today's history cleared."})


# --------------------------------------------------------------------------- #
# NSE + BSE ANALYZER
# --------------------------------------------------------------------------- #
class NseBseAnalyzer:
    def __init__(self):
        self.stop = threading.Event()
        self.session = requests.Session()
        self.nse_headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'accept-language': 'en,gu;q=0.9,hi;q=0.8', 'accept-encoding': 'gzip, deflate, br',
        }
        self.dhan_headers = {
            'access-token': DHAN_ACCESS_TOKEN, 'client-id': DHAN_CLIENT_ID,
            'Content-Type': 'application/json'
        }
        self.url_oc = "https://www.nseindia.com/option-chain"
        self.url_nse = "https://www.nseindia.com/api/option-chain-indices?symbol="
        self.url_dhan = "https://api.dhan.co/v2/optionchain"
        self.db_path = 'nse_bse_data.db'
        self.conn: Optional[sqlite3.Connection] = None
        self._init_db()
        self.pcr_graph_data: Dict[str, List[Dict[str, Any]]] = {sym: [] for sym in AUTO_SYMBOLS if sym != "INDIAVIX"}
        self.last_pcr_graph_update_time: Dict[str, float] = {sym: 0 for sym in AUTO_SYMBOLS if sym != "INDIAVIX"}

        self.previous_pcr: Dict[str, float] = {sym: 0.0 for sym in AUTO_SYMBOLS}

        self._load_todays_history_from_db()
        self._load_initial_underlying_values()
        self._populate_initial_shared_chart_data()
        self.nse_tool = Nse()
        self.nse_tool_symbol_map = {
            "NIFTY": "NIFTY 50", "BANKNIFTY": "NIFTY BANK", "FINNIFTY": "NIFNIFTY"
        }
        self.last_pcr_data_reset_date: Dict[str, datetime.date] = {sym: datetime.date.min for sym in AUTO_SYMBOLS if sym != "INDIAVIX"}
        threading.Thread(target=self.run_loop, daemon=True).start()

    def _init_db(self):
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cur = self.conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT, symbol TEXT, sp REAL, value REAL,
                    call_oi REAL, put_oi REAL, pcr REAL, sentiment TEXT,
                    add_exit TEXT, pcr_change REAL, intraday_pcr REAL
                )
            """)
            cur.execute("PRAGMA table_info(history)")
            cols = [column[1] for column in cur.fetchall()]
            if 'add_exit' not in cols:
                cur.execute("ALTER TABLE history ADD COLUMN add_exit TEXT")
            if 'pcr_change' not in cols:
                cur.execute("ALTER TABLE history ADD COLUMN pcr_change REAL DEFAULT 0")
            if 'intraday_pcr' not in cols:
                cur.execute("ALTER TABLE history ADD COLUMN intraday_pcr REAL DEFAULT 0")
            self.conn.commit()
        except Exception as e:
            print(f"DB error: {e}")

    def _populate_initial_shared_chart_data(self):
        with data_lock:
            for sym in AUTO_SYMBOLS:
                if sym != "INDIAVIX":
                    if sym not in shared_data:
                        shared_data[sym] = {
                            'pcr_chart_data': [], 'max_pain_chart_data': [],
                            'ce_oi_chart_data': [], 'pe_oi_chart_data': []
                        }
                    shared_data[sym]['pcr_chart_data'] = self.pcr_graph_data.get(sym, [])

    def _load_todays_history_from_db(self):
        if not self.conn: return
        try:
            cur = self.conn.cursor()
            ist_now = self._get_ist_time()
            ist_day_start = ist_now.replace(hour=0, minute=0, second=0, microsecond=0)
            utc_equivalent_start = ist_day_start.astimezone(datetime.timezone.utc)
            utc_start_str = utc_equivalent_start.strftime('%Y-%m-%d %H:%M:%S')

            for sym in AUTO_SYMBOLS:
                cur.execute("""
                    SELECT timestamp, sp, value, call_oi, put_oi, pcr, sentiment, add_exit, pcr_change, intraday_pcr
                    FROM history
                    WHERE symbol = ? AND timestamp >= ?
                    ORDER BY timestamp DESC
                """, (sym, utc_start_str))
                rows = cur.fetchall()
                todays_history[sym] = []

                for r in rows:
                    history_item = {
                        'time': self._convert_utc_to_ist_display(r[0]), 'sp': r[1], 'value': r[2],
                        'call_oi': r[3], 'put_oi': r[4], 'pcr': r[5], 'sentiment': r[6],
                        'add_exit': r[7], 'pcr_change': r[8] if r[8] is not None else 0.0,
                        'intraday_pcr': r[9] if r[9] is not None else 0.0
                    }
                    todays_history[sym].append(history_item)

                if sym != "INDIAVIX":
                    if todays_history[sym]:
                        self.previous_pcr[sym] = todays_history[sym][0]['pcr']

                    self.pcr_graph_data[sym] = []
                    for history_item in reversed(todays_history[sym]):
                        self.pcr_graph_data[sym].append({"TIME": history_item['time'], "PCR": history_item['pcr']})
        except Exception as e:
            print(f"History load error: {e}")

    def _load_initial_underlying_values(self):
        if not self.conn: return
        with data_lock:
            cur = self.conn.cursor()
            ist_now = self._get_ist_time()
            ist_day_start = ist_now.replace(hour=0, minute=0, second=0, microsecond=0)
            utc_equivalent_start = ist_day_start.astimezone(datetime.timezone.utc)
            utc_start_str = utc_equivalent_start.strftime('%Y-%m-%d %H:%M:%S')

            for sym in AUTO_SYMBOLS:
                cur.execute("""
                    SELECT value FROM history WHERE symbol = ? AND timestamp >= ?
                    ORDER BY timestamp ASC LIMIT 1
                """, (sym, utc_start_str))
                result = cur.fetchone()
                initial_underlying_values[sym] = float(result[0]) if result else None

    def _get_ist_time(self) -> datetime.datetime:
        utc_now = datetime.datetime.now(datetime.timezone.utc)
        ist_offset = datetime.timedelta(hours=5, minutes=30)
        return utc_now + ist_offset

    def _convert_utc_to_ist_display(self, utc_timestamp_str: str) -> str:
        try:
            utc_dt = datetime.datetime.strptime(utc_timestamp_str, "%Y-%m-%d %H:%M:%S")
            utc_dt = utc_dt.replace(tzinfo=datetime.timezone.utc)
            ist_dt = utc_dt + datetime.timedelta(hours=5, minutes=30)
            return ist_dt.strftime("%H:%M")
        except (ValueError, TypeError):
            return "00:00"

    def clear_todays_history_db(self, sym: Optional[str] = None):
        if not self.conn: return
        try:
            cur = self.conn.cursor()
            ist_now = self._get_ist_time()
            ist_day_start = ist_now.replace(hour=0, minute=0, second=0, microsecond=0)
            utc_equivalent_start = ist_day_start.astimezone(datetime.timezone.utc)
            utc_start_str = utc_equivalent_start.strftime('%Y-%m-%d %H:%M:%S')

            symbols_to_clear = [sym] if sym else AUTO_SYMBOLS

            if sym:
                cur.execute("DELETE FROM history WHERE symbol = ? AND timestamp >= ?", (sym, utc_start_str))
                print(f"Cleared today's history for {sym} from DB.")
            else:
                cur.execute("DELETE FROM history WHERE timestamp >= ?", (utc_start_str,))
                print(f"Cleared today's history for ALL symbols from DB.")
            self.conn.commit()

            with data_lock:
                for s in symbols_to_clear:
                    todays_history[s] = []
                    if s != "INDIAVIX":
                        self.pcr_graph_data[s] = []
                        self.last_pcr_graph_update_time[s] = 0
                        self.previous_pcr[s] = 0.0
                        if s in shared_data:
                            shared_data[s]['pcr_chart_data'] = []
                            shared_data[s]['max_pain_chart_data'] = []
                            shared_data[s]['ce_oi_chart_data'] = []
                            shared_data[s]['pe_oi_chart_data'] = []

            socketio.emit('initial_todays_history', {'history': todays_history})
            broadcast_live_update()
        except Exception as e:
            print(f"DB cleanup error: {e}")

    def run_loop(self):
        try:
            self.session.get(self.url_oc, headers=self.nse_headers, timeout=10, verify=False)
        except requests.exceptions.RequestException as e:
            print(f"Initial NSE session setup failed: {e}")

        while not self.stop.is_set():
            current_date_ist = self._get_ist_time().date()
            for sym in AUTO_SYMBOLS:
                if sym != "INDIAVIX" and self.last_pcr_data_reset_date.get(sym, datetime.date.min) < current_date_ist:
                    print(f"Resetting PCR graph data for {sym} for new day: {current_date_ist}")
                    with data_lock:
                        self.pcr_graph_data[sym] = []
                        if sym in shared_data:
                            shared_data[sym]['pcr_chart_data'] = []
                    self.last_pcr_graph_update_time[sym] = 0
                    self.last_pcr_data_reset_date[sym] = current_date_ist
                    self.previous_pcr[sym] = 0.0

                if initial_underlying_values.get(sym) is not None:
                    last_alert_dt_utc = datetime.datetime.fromtimestamp(last_alert.get(sym, 0), tz=datetime.timezone.utc)
                    last_alert_dt_ist = last_alert_dt_utc + datetime.timedelta(hours=5, minutes=30)
                    if current_date_ist != last_alert_dt_ist.date():
                        initial_underlying_values[sym] = None

                try:
                    self.fetch_and_process_symbol(sym)
                except Exception as e:
                    print(f"{sym} error during fetch and process: {e}")
            time.sleep(LIVE_DATA_INTERVAL)

    def fetch_and_process_symbol(self, sym: str):
        if sym == "SENSEX": self._process_sensex_data(sym)
        elif sym == "INDIAVIX": self._process_indiavix_data(sym)
        else: self._process_nse_data(sym)

    def _process_nse_data(self, sym: str):
        try:
            url = self.url_nse + sym
            resp = self.session.get(url, headers=self.nse_headers, timeout=10, verify=False)
            if resp.status_code == 401:
                self.session.get(self.url_oc, headers=self.nse_headers, timeout=5, verify=False)
                resp = self.session.get(url, headers=self.nse_headers, timeout=10, verify=False)

            data = resp.json()
            expiries = data.get('records', {}).get('expiryDates', [])
            if not expiries: return

            expiry = expiries[0]
            ce_values = [x['CE'] for x in data.get('records', {}).get('data', []) if 'CE' in x and x.get('expiryDate', '') == expiry]
            pe_values = [x['PE'] for x in data.get('records', {}).get('data', []) if 'PE' in x and x.get('expiryDate', '') == expiry]
            if not ce_values or not pe_values: return

            underlying = data.get('records', {}).get('underlyingValue', 0)
            df_ce = pd.DataFrame(ce_values)[['strikePrice', 'openInterest', 'changeinOpenInterest']]
            df_pe = pd.DataFrame(pe_values)[['strikePrice', 'openInterest', 'changeinOpenInterest']]
            df = pd.merge(df_ce, df_pe, on='strikePrice', how='outer', suffixes=('_call', '_put')).fillna(0)

            sp = self.get_atm_strike(df, underlying)
            if not sp: return

            idx_list = df[df['strikePrice'] == sp].index.tolist()
            if not idx_list: return
            idx = idx_list[0]

            strikes_data = []
            ce_add_strikes, ce_exit_strikes, pe_add_strikes, pe_exit_strikes = [], [], [], []

            for i in range(-10, 11):
                if not (0 <= idx + i < len(df)): continue
                row = df.iloc[idx + i]
                strike, call_oi, put_oi, call_coi, put_coi = int(row['strikePrice']), int(row['openInterest_call']), int(row['openInterest_put']), int(row['changeinOpenInterest_call']), int(row['changeinOpenInterest_put'])
                call_action = "ADD" if call_coi > 0 else "EXIT" if call_coi < 0 else ""
                put_action = "ADD" if put_coi > 0 else "EXIT" if put_coi < 0 else ""
                if call_action == "ADD": ce_add_strikes.append(str(strike))
                elif call_action == "EXIT": ce_exit_strikes.append(str(strike))
                if put_action == "ADD": pe_add_strikes.append(str(strike))
                elif put_action == "EXIT": pe_exit_strikes.append(str(strike))
                strikes_data.append({
                    'strike': strike, 'call_oi': call_oi, 'call_coi': call_coi, 'call_action': call_action,
                    'put_oi': put_oi, 'put_coi': put_coi, 'put_action': put_action, 'is_atm': i == 0
                })

            total_call_oi = sum(df['openInterest_call'])
            total_put_oi = sum(df['openInterest_put'])
            pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi else 0.0

            if self.previous_pcr.get(sym, 0.0) == 0.0:
                pcr_change = 0.0
            else:
                pcr_change = round(pcr - self.previous_pcr.get(sym, pcr), 2)

            total_change_in_call_oi = sum(df['changeinOpenInterest_call'])
            total_change_in_put_oi = sum(df['changeinOpenInterest_put'])
            intraday_pcr = round(total_change_in_put_oi / total_change_in_call_oi, 2) if total_change_in_call_oi != 0 else 0.0

            atm_index_in_strikes_data = next((j for j, s in enumerate(strikes_data) if s['is_atm']), 10)
            atm_call_coi = strikes_data[atm_index_in_strikes_data]['call_coi']
            atm_put_coi = strikes_data[atm_index_in_strikes_data]['put_coi']
            diff = round((atm_call_coi - atm_put_coi) / 1000, 1)

            base_sentiment = self.get_sentiment(diff, pcr)
            enhanced_sentiment = self._get_enhanced_sentiment(sym, base_sentiment)

            add_exit_parts = []
            if ce_add_strikes: add_exit_parts.append(f"CE Add: {', '.join(sorted(ce_add_strikes))}")
            if pe_add_strikes: add_exit_parts.append(f"PE Add: {', '.join(sorted(pe_add_strikes))}")
            if ce_exit_strikes: add_exit_parts.append(f"CE Exit: {', '.join(sorted(ce_exit_strikes))}")
            if pe_exit_strikes: add_exit_parts.append(f"PE Exit: {', '.join(sorted(pe_exit_strikes))}")
            add_exit_str = " | ".join(add_exit_parts) or "No Change"

            summary = {
                'time': self._get_ist_time().strftime("%H:%M"), 'sp': int(sp), 'value': int(round(underlying)),
                'call_oi': round(atm_call_coi / 1000, 1), 'put_oi': round(atm_put_coi / 1000, 1),
                'pcr': pcr, 'sentiment': enhanced_sentiment, 'expiry': expiry, 'add_exit': add_exit_str,
                'pcr_change': pcr_change, 'intraday_pcr': intraday_pcr
            }
            result = {'summary': summary, 'strikes': strikes_data}

            try:
                nse_sym_for_ltp = self.nse_tool_symbol_map.get(sym, sym)
                ltp = self.nse_tool.get_index_quote(nse_sym_for_ltp).get('lastPrice', underlying)
            except Exception as e:
                ltp = underlying
                print(f"nsetools LTP error for {sym}: {e}")

            max_pain_df = self._calculate_max_pain(sym, pd.DataFrame(ce_values), pd.DataFrame(pe_values), ltp)
            ce_dt_for_charts = pd.DataFrame(ce_values).sort_values(['openInterest'], ascending=False)
            pe_dt_for_charts = pd.DataFrame(pe_values).sort_values(['openInterest'], ascending=False)
            final_ce_data = ce_dt_for_charts[['strikePrice', 'openInterest']].iloc[:10].to_dict(orient='records')
            final_pe_data = pe_dt_for_charts[['strikePrice', 'openInterest']].iloc[:10].to_dict(orient='records')

            with data_lock:
                if sym not in shared_data: shared_data[sym] = {}
                shared_data[sym].update(result)
                if initial_underlying_values.get(sym) is None:
                    initial_underlying_values[sym] = float(underlying)
                change = underlying - initial_underlying_values[sym]
                pct_change = (change / initial_underlying_values[sym]) * 100 if initial_underlying_values[sym] else 0
                shared_data[sym]['live_feed_summary'] = {
                    'current_value': int(round(underlying)), 'change': round(change, 2),
                    'percentage_change': round(pct_change, 2)
                }
                shared_data[sym]['max_pain_chart_data'] = max_pain_df.to_dict(orient='records')
                shared_data[sym]['ce_oi_chart_data'] = final_ce_data
                shared_data[sym]['pe_oi_chart_data'] = final_pe_data
                shared_data[sym]['pcr_chart_data'] = self.pcr_graph_data.get(sym, [])

            print(f"{sym} LIVE DATA UPDATED | SP: {sp} | PCR: {pcr}")
            broadcast_live_update()

            now = time.time()
            if now - last_history_update.get(sym, 0) >= UPDATE_INTERVAL:
                self.previous_pcr[sym] = pcr
                with data_lock:
                    todays_history[sym].insert(0, summary)
                    if sym != "INDIAVIX":
                        self.pcr_graph_data[sym].append({"TIME": summary['time'], "PCR": summary['pcr']})
                        if len(self.pcr_graph_data[sym]) > 180: self.pcr_graph_data[sym].pop(0)
                        self.last_pcr_graph_update_time[sym] = now
                        shared_data[sym]['pcr_chart_data'] = self.pcr_graph_data[sym]
                broadcast_history_append(sym, summary)
                last_history_update[sym] = now
                self._save_db(sym, summary)

            if now - last_alert.get(sym, 0) >= UPDATE_INTERVAL:
                self.send_alert(sym, summary)
                last_alert[sym] = now
        except requests.exceptions.RequestException as e:
            print(f"{sym} network error: {e}")
        except Exception as e:
            import traceback
            print(f"{sym} processing error: {e}\n{traceback.format_exc()}")

    def _process_sensex_data(self, sym: str):
        if DHAN_CLIENT_ID == "YOUR_CLIENT_ID" or DHAN_ACCESS_TOKEN == "YOUR_ACCESS_TOKEN":
            print("SENSEX: DhanHQ keys not set. Using mock data.")
            self._mock_sensex(sym)
            return

        try:
            payload = {"exchange": "BSE", "segment": "BSE_FO", "instrument": "SENSEX"}
            resp = requests.post(self.url_dhan, json=payload, headers=self.dhan_headers, timeout=10)

            if resp.status_code != 200:
                print(f"SENSEX DHAN API Error: Status {resp.status_code}, Response: {resp.text}")
                self._mock_sensex(sym)
                return

            data = resp.json()
            underlying = data.get('underlyingValue', 0)
            if underlying == 0: return

            expiry = data.get('expiryDate')
            option_chain = data.get('optionChainDetails', [])
            if not option_chain: return

            records = [{'strikePrice': item['strikePrice'],
                        'openInterest_call': item.get('callOI', 0),
                        'changeinOpenInterest_call': item.get('callChangeInOI', 0),
                        'openInterest_put': item.get('putOI', 0),
                        'changeinOpenInterest_put': item.get('putChangeInOI', 0)} for item in option_chain]
            df = pd.DataFrame(records)

            sp = self.get_atm_strike(df, underlying)
            if not sp: return

            idx_list = df[df['strikePrice'] == sp].index.tolist()
            if not idx_list: return
            idx = idx_list[0]

            strikes_data, ce_add, ce_exit, pe_add, pe_exit = [], [], [], [], []
            for i in range(-10, 11):
                if not (0 <= idx + i < len(df)): continue
                row = df.iloc[idx + i]
                strike, call_coi, put_coi = int(row['strikePrice']), int(row['changeinOpenInterest_call']), int(row['changeinOpenInterest_put'])
                call_action = "ADD" if call_coi > 0 else "EXIT" if call_coi < 0 else ""
                put_action = "ADD" if put_coi > 0 else "EXIT" if put_coi < 0 else ""
                if call_action == "ADD": ce_add.append(str(strike))
                elif call_action == "EXIT": ce_exit.append(str(strike))
                if put_action == "ADD": pe_add.append(str(strike))
                elif put_action == "EXIT": pe_exit.append(str(strike))
                strikes_data.append({'strike': strike, 'call_oi': int(row['openInterest_call']), 'call_coi': call_coi, 'call_action': call_action,
                                     'put_oi': int(row['openInterest_put']), 'put_coi': put_coi, 'put_action': put_action, 'is_atm': i == 0})

            total_call_oi = sum(df['openInterest_call'])
            total_put_oi = sum(df['openInterest_put'])
            pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi else 0.0

            if self.previous_pcr.get(sym, 0.0) == 0.0: pcr_change = 0.0
            else: pcr_change = round(pcr - self.previous_pcr.get(sym, pcr), 2)

            total_change_in_call_oi = sum(df['changeinOpenInterest_call'])
            total_change_in_put_oi = sum(df['changeinOpenInterest_put'])
            intraday_pcr = round(total_change_in_put_oi / total_change_in_call_oi, 2) if total_change_in_call_oi != 0 else 0.0

            atm_call_coi = strikes_data[10]['call_coi']
            atm_put_coi = strikes_data[10]['put_coi']
            diff = round((atm_call_coi - atm_put_coi) / 1000, 1)

            base_sentiment = self.get_sentiment(diff, pcr)
            enhanced_sentiment = self._get_enhanced_sentiment(sym, base_sentiment)

            add_exit_str = " | ".join(filter(None, [f"CE Add: {', '.join(sorted(ce_add))}" if ce_add else "", f"PE Add: {', '.join(sorted(pe_add))}" if pe_add else "",
                                                    f"CE Exit: {', '.join(sorted(ce_exit))}" if ce_exit else "", f"PE Exit: {', '.join(sorted(pe_exit))}" if pe_exit else ""])) or "No Change"

            summary = {'time': self._get_ist_time().strftime("%H:%M"), 'sp': int(sp), 'value': int(round(underlying)),
                       'call_oi': round(atm_call_coi / 1000, 1), 'put_oi': round(atm_put_coi / 1000, 1), 'pcr': pcr,
                       'sentiment': enhanced_sentiment, 'expiry': expiry, 'add_exit': add_exit_str,
                       'pcr_change': pcr_change, 'intraday_pcr': intraday_pcr}

            with data_lock:
                if sym not in shared_data: shared_data[sym] = {}
                shared_data[sym].update({'summary': summary, 'strikes': strikes_data})
                if initial_underlying_values.get(sym) is None: initial_underlying_values[sym] = float(underlying)
                change = underlying - initial_underlying_values[sym]
                pct_change = (change / initial_underlying_values[sym]) * 100 if initial_underlying_values[sym] else 0
                shared_data[sym]['live_feed_summary'] = {'current_value': int(round(underlying)), 'change': round(change, 2), 'percentage_change': round(pct_change, 2)}
                shared_data[sym]['pcr_chart_data'] = self.pcr_graph_data.get(sym, [])
                shared_data[sym]['max_pain_chart_data'], shared_data[sym]['ce_oi_chart_data'], shared_data[sym]['pe_oi_chart_data'] = [], [], []

            print(f"{sym} DHAN API DATA UPDATED | SP: {sp} | PCR: {pcr}")
            broadcast_live_update()

            now = time.time()
            if now - last_history_update.get(sym, 0) >= UPDATE_INTERVAL:
                self.previous_pcr[sym] = pcr
                with data_lock:
                    todays_history[sym].insert(0, summary)
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
            print(f"SENSEX processing error: {e}\n{traceback.format_exc()}")
            self._mock_sensex(sym)

    # +++ NEW: ROBUST METHOD FOR INDIA VIX via nsetools +++
    def _process_indiavix_data(self, sym: str):
        try:
            vix_data = self.nse_tool.get_index_quote("INDIA VIX")

            if not vix_data or 'lastPrice' not in vix_data:
                print("INDIAVIX nsetools error: Invalid data received.")
                self._mock_indiavix(sym)
                return

            current_vix = vix_data['lastPrice']
            change = vix_data['change']
            pct_change = vix_data['pChange']

            sentiment = "Mild Bearish" if change > 0 else "Mild Bullish" if change < 0 else "Neutral"
            summary = {
                'time': self._get_ist_time().strftime("%H:%M"),
                'sp': change,
                'value': current_vix,
                'call_oi': 0.0, 'put_oi': 0.0,
                'pcr': pct_change,
                'sentiment': sentiment,
                'expiry': "N/A", 'add_exit': "Live Data",
                'pcr_change': 0.0, 'intraday_pcr': 0.0
            }

            with data_lock:
                if sym not in shared_data: shared_data[sym] = {}
                shared_data[sym]['summary'] = summary
                shared_data[sym]['live_feed_summary'] = {'current_value': current_vix, 'change': change, 'percentage_change': pct_change}

            print(f"INDIAVIX LIVE DATA UPDATED | Value: {current_vix}")
            broadcast_live_update()

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
            print(f"INDIAVIX processing error: {e}")
            self._mock_indiavix(sym)

    def _mock_indiavix(self, sym: str):
        current_vix = random.uniform(10.0, 30.0)
        change = random.uniform(-1.5, 1.5)
        pct_change = (change / (current_vix - change)) * 100 if (current_vix - change) != 0 else 0
        sentiment = "Mild Bearish" if change > 0 else "Mild Bullish" if change < 0 else "Neutral"
        summary = {
            'time': self._get_ist_time().strftime("%H:%M"), 'sp': round(change, 2),
            'value': round(current_vix, 2), 'call_oi': 0.0, 'put_oi': 0.0,
            'pcr': round(pct_change, 2), 'sentiment': sentiment, 'expiry': "N/A", 'add_exit': "Mock Data",
            'pcr_change': 0.0, 'intraday_pcr': 0.0
        }
        with data_lock:
            if sym not in shared_data: shared_data[sym] = {}
            shared_data[sym]['summary'] = summary
            shared_data[sym]['live_feed_summary'] = {'current_value': round(current_vix, 2), 'change': round(change, 2), 'percentage_change': round(pct_change, 2)}
        broadcast_live_update()

    def _mock_sensex(self, sym: str):
        underlying = 75000 + random.randint(-500, 500)
        sp = round(underlying / 100) * 100
        pcr = round(random.uniform(0.6, 1.4), 2)
        intraday_pcr = round(random.uniform(0.5, 1.5), 2)

        if self.previous_pcr.get(sym, 0.0) == 0.0: pcr_change = 0.0
        else: pcr_change = round(pcr - self.previous_pcr.get(sym, pcr), 2)

        base_sentiment = self.get_sentiment(0, pcr)
        enhanced_sentiment = self._get_enhanced_sentiment(sym, base_sentiment)

        summary = {'time': self._get_ist_time().strftime("%H:%M"), 'sp': int(sp), 'value': int(round(underlying)),
                   'call_oi': round(random.uniform(1, 10), 1), 'put_oi': round(random.uniform(1, 10), 1), 'pcr': pcr,
                   'sentiment': enhanced_sentiment, 'expiry': "Mock Expiry", 'add_exit': "Mock Change",
                   'pcr_change': pcr_change, 'intraday_pcr': intraday_pcr}
        strikes_data = [{'strike': sp + i * 100, 'call_oi': random.randint(100, 5000), 'call_coi': random.randint(-500, 500), 'call_action': 'ADD',
                         'put_oi': random.randint(100, 5000), 'put_coi': random.randint(-500, 500), 'put_action': 'ADD', 'is_atm': i == 0} for i in range(-10, 11)]

        with data_lock:
            if sym not in shared_data: shared_data[sym] = {}
            shared_data[sym].update({'summary': summary, 'strikes': strikes_data})
            if initial_underlying_values.get(sym) is None: initial_underlying_values[sym] = float(underlying)
            change = underlying - initial_underlying_values[sym]
            pct_change = (change / initial_underlying_values[sym]) * 100 if initial_underlying_values[sym] else 0
            shared_data[sym]['live_feed_summary'] = {'current_value': int(round(underlying)), 'change': round(change, 2), 'percentage_change': round(pct_change, 2)}
            shared_data[sym]['pcr_chart_data'], shared_data[sym]['max_pain_chart_data'], shared_data[sym]['ce_oi_chart_data'], shared_data[sym]['pe_oi_chart_data'] = [], [], [], []
        broadcast_live_update()

    def get_atm_strike(self, df: pd.DataFrame, underlying: float) -> Optional[int]:
        try:
            strikes = df['strikePrice'].astype(int).unique()
            return min(strikes, key=lambda x: abs(x - underlying)) if len(strikes) > 0 else None
        except Exception as e:
            print(f"Error getting ATM strike: {e}")
            return None

    def get_sentiment(self, diff: float, pcr: float) -> str:
        if diff > 10 or pcr < 0.7: return "Strong Bearish"
        if diff < -10 or pcr > 1.3: return "Strong Bullish"
        if diff > 2 or (pcr > 0.7 and pcr < 0.9): return "Mild Bearish"
        if diff < -2 or (pcr > 1.1 and pcr < 1.3): return "Mild Bullish"
        return "Neutral"

    def _get_enhanced_sentiment(self, sym: str, base_sentiment: str) -> str:
        with data_lock:
            history = todays_history.get(sym, [])[:7]
            if len(history) < 3:
                return base_sentiment

            pressure_score = 0
            for item in history:
                oi_changes = item.get('add_exit', '')
                if 'CE Add' in oi_changes: pressure_score -= 1
                if 'PE Exit' in oi_changes: pressure_score -= 1
                if 'PE Add' in oi_changes: pressure_score += 1
                if 'CE Exit' in oi_changes: pressure_score += 1

        if "Bullish" in base_sentiment:
            if pressure_score <= -4: return f"{base_sentiment} (Fading)"
            if pressure_score >= 4: return f"{base_sentiment} (Rising)"
        elif "Bearish" in base_sentiment:
            if pressure_score >= 4: return f"{base_sentiment} (Weakening)"
            if pressure_score <= -4: return f"{base_sentiment} (Intensifying)"
        elif base_sentiment == "Neutral":
            if pressure_score >= 4: return "Neutral (Bullish Pressure)"
            if pressure_score <= -4: return "Neutral (Bearish Pressure)"

        return base_sentiment

    def send_alert(self, sym: str, row: Dict[str, Any]):
        if sym == "INDIAVIX":
            live_feed = shared_data.get(sym, {}).get('live_feed_summary', {})
            msg = f"<b>{sym} LIVE</b>\n<b>Value:</b> <code>{live_feed.get('current_value', 0):.2f}</code>\n<b>Change:</b> <code>{live_feed.get('change', 0):+.2f}</code> | <b>Pct:</b> <code>{live_feed.get('percentage_change', 0):+.2f}%</code>\n<b>Sentiment:</b> <b>{row['sentiment']}</b>"
        else:
            msg = f"<b>{sym} LIVE</b> | <i>{row['expiry']}</i>\n<b>SP:</b> <code>{int(row['sp'])}</code> | <b>Value:</b> <code>{int(row['value'])}</code>\n<b>Call COI:</b> <code>{row['call_oi']:+}</code>K | <b>Put COI:</b> <code>{row['put_oi']:+}</code>K\n<b>PCR:</b> <b>{row['pcr']:.2f} (Δ {row.get('pcr_change', 0.0):+.2f})</b>\n<b>Intraday PCR:</b> <b>{row.get('intraday_pcr', 0.0):.2f}</b>\n<b>OI Change:</b> <code>{row['add_exit']}</code>\n<b>Sentiment:</b> <b>{row['sentiment']}</b>"
        send_telegram_text(f"{msg.strip()}\n@aditya_nse_bot")

    def _save_db(self, sym, row):
        if not self.conn: return
        try:
            current_timestamp_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            self.conn.execute(
                "INSERT INTO history (timestamp, symbol, sp, value, call_oi, put_oi, pcr, sentiment, add_exit, pcr_change, intraday_pcr) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (current_timestamp_utc, sym, row['sp'], row['value'], row['call_oi'], row['put_oi'], row['pcr'], row['sentiment'], row['add_exit'], row.get('pcr_change', 0.0), row.get('intraday_pcr', 0.0))
            )
            self.conn.execute("""
                DELETE FROM history WHERE id NOT IN (
                    SELECT id FROM history WHERE symbol = ? ORDER BY timestamp DESC LIMIT ?
                ) AND symbol = ?;
            """, (sym, MAX_HISTORY_ROWS_DB, sym))
            self.conn.commit()
        except Exception as e:
            print(f"DB save error for {sym}: {e}")

    def _calculate_max_pain(self, sym: str, ce_dt: pd.DataFrame, pe_dt: pd.DataFrame, ltp: float,
                            dhan_data: bool = False) -> pd.DataFrame:
        if dhan_data:
            MxPn_CE = ce_dt[['strikePrice', 'callOI']].rename(columns={'callOI': 'openInterest'})
            MxPn_PE = pe_dt[['strikePrice', 'putOI']].rename(columns={'putOI': 'openInterest'})
        else:
            MxPn_CE = ce_dt[['strikePrice', 'openInterest']]
            MxPn_PE = pe_dt[['strikePrice', 'openInterest']]

        MxPn_Df = pd.merge(MxPn_CE, MxPn_PE, on=['strikePrice'], how='outer', suffixes=('_call', '_Put')).fillna(0)
        if 'openInterest_call' not in MxPn_Df.columns and 'openInterest_Put' not in MxPn_Df.columns:
            MxPn_Df.columns = ['strikePrice', 'openInterest_call', 'openInterest_Put']

        StrikePriceList = MxPn_Df['strikePrice'].values.tolist()
        OiCallList = MxPn_Df['openInterest_call'].values.tolist()
        OiPutList = MxPn_Df['openInterest_Put'].values.tolist()

        TCVSP = []
        for p in range(len(StrikePriceList)):
            mxpn_strike = StrikePriceList[p]
            tc = self._total_option_pain_for_strike(StrikePriceList, OiCallList, OiPutList, mxpn_strike)
            TCVSP.insert(p, int(tc))

        max_pain_df = pd.DataFrame(list(zip(StrikePriceList, TCVSP)), columns=["StrikePrice", "TotalMaxPain"])
        max_pain_df['StrikePrice'] = pd.to_numeric(max_pain_df['StrikePrice'])
        max_pain_df['TotalMaxPain'] = pd.to_numeric(max_pain_df['TotalMaxPain'])

        if not max_pain_df.empty:
            min_idx = max_pain_df['TotalMaxPain'].idxmin()
            min_pain_strike = max_pain_df.loc[min_idx, 'StrikePrice']

            strikes_sorted = sorted(StrikePriceList)
            try:
                closest_strike_index_in_sorted_list = strikes_sorted.index(min_pain_strike)
            except ValueError:
                closest_strike_index_in_sorted_list = (pd.Series(strikes_sorted) - min_pain_strike).abs().argsort()[:1].iloc[0]

            start_index_for_window = max(0, closest_strike_index_in_sorted_list - 8)
            end_index_for_window = min(len(strikes_sorted), closest_strike_index_in_sorted_list + 8)
            strikes_in_window = strikes_sorted[start_index_for_window:end_index_for_window]

            max_pain_chart_df = max_pain_df[max_pain_df['StrikePrice'].isin(strikes_in_window)].reset_index(drop=True)
            return max_pain_chart_df

        return pd.DataFrame(columns=["StrikePrice", "TotalMaxPain"])

    def _total_option_pain_for_strike(self, strike_price_list: List[float], oi_call_list: List[int],
                                      oi_put_list: List[int], mxpn_strike: float) -> float:
        total_cash_value = 0
        for k in range(len(strike_price_list)):
            strike = strike_price_list[k]
            call_oi = oi_call_list[k]
            put_oi = oi_put_list[k]

            intrinsic_call = max(0, mxpn_strike - strike)
            intrinsic_put = max(0, strike - mxpn_strike)

            total_cash_value += (intrinsic_call * call_oi)
            total_cash_value += (intrinsic_put * put_oi)
        return total_cash_value

# --------------------------------------------------------------------------- #
# MAIN
# --------------------------------------------------------------------------- #
if __name__ == '__main__':
    analyzer = NseBseAnalyzer()
    print("WEB DASHBOARD LIVE → http://127.0.0.1:5000")
    print("SENSEX: Add DhanHQ keys for real data")
    socketio.run(app, host='0.0.0.0', port=5000)
