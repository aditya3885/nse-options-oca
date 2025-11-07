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
last_history_update: Dict[str, float] = {sym: 0 for sym in AUTO_SYMBOLS}  # Track last history update time per symbol
# New: Store initial underlying values for calculating live feed change
initial_underlying_values: Dict[str, Optional[float]] = {sym: None for sym in AUTO_SYMBOLS}


@app.route('/')
def index():
    return render_template('dashboard.html')


@socketio.on('connect')
def handle_connect(sid=None):
    with data_lock:
        # Send initial live data
        safe_live = {}
        live_feed_summary = {}  # Prepare live feed summary for initial connect
        for sym, data in shared_data.items():
            # Check if 'summary' and 'strikes' exist before adding to safe_live
            safe_live[sym] = {}  # Initialize for each symbol to avoid KeyError later
            if 'summary' in data:
                safe_live[sym]['summary'] = data['summary']
            if 'strikes' in data:
                safe_live[sym]['strikes'] = data['strikes']

            # Also add live feed data to initial payload
            if 'live_feed_summary' in data:  # Check if live_feed_summary exists in shared_data
                live_feed_summary[sym] = data['live_feed_summary']

            # Include chart data if available
            if 'pcr_chart_data' in data:
                safe_live[sym]['pcr_chart_data'] = data['pcr_chart_data']
            if 'max_pain_chart_data' in data:
                safe_live[sym]['max_pain_chart_data'] = data['max_pain_chart_data']
            if 'ce_oi_chart_data' in data:
                safe_live[sym]['ce_oi_chart_data'] = data['ce_oi_chart_data']
            if 'pe_oi_chart_data' in data:
                safe_live[sym]['pe_oi_chart_data'] = data['pe_oi_chart_data']

        emit('update', {'live': safe_live, 'live_feed_summary': live_feed_summary}, to=sid)
        # Send initial "today's" history for all symbols
        emit('initial_todays_history', {'history': todays_history}, to=sid)


def broadcast_live_update():
    with data_lock:
        safe_live = {}
        live_feed_summary = {}  # Collect live feed summary for broadcast
        for sym, data in shared_data.items():
            safe_live[sym] = {}  # Initialize for each symbol
            if 'summary' in data:
                safe_live[sym]['summary'] = data['summary']
            if 'strikes' in data:
                safe_live[sym]['strikes'] = data['strikes']

            if 'live_feed_summary' in data:  # Check if live_feed_summary exists in shared_data
                live_feed_summary[sym] = data['live_feed_summary']

            # Include chart data if available
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
    """Sends a single new history item to append to today's history on the frontend."""
    with data_lock:
        socketio.emit('todays_history_append', {'symbol': sym, 'item': new_history_item})


@app.route('/history/<symbol>/<date_str>')
def get_historical_data(symbol: str, date_str: str):
    """API endpoint to fetch historical data for a specific symbol and date."""
    try:
        # Validate date_str format (YYYY-MM-DD)
        datetime.datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
    if symbol not in AUTO_SYMBOLS:
        return jsonify({"error": f"Invalid symbol: {symbol}"}), 400
    history_for_date: List[Dict[str, Any]] = []
    with data_lock:  # Use data_lock as it's shared across Flask/SocketIO threads
        # Access the analyzer's connection if it exists
        if analyzer.conn:
            cur = analyzer.conn.cursor()
            # Filter by symbol and date part of timestamp
            # ORDER BY timestamp DESC to get latest first from DB
            cur.execute("""
                SELECT timestamp, sp, value, call_oi, put_oi, pcr, sentiment, add_exit
                FROM history
                WHERE symbol = ? AND SUBSTR(timestamp, 1, 10) = ?
                ORDER BY timestamp DESC
            """, (symbol, date_str))
            rows = cur.fetchall()
            for r in rows:
                history_for_date.append({
                    'time': r[0].split()[1][:5],  # Just HH:MM
                    'sp': r[1],
                    'value': r[2],
                    'call_oi': r[3],
                    'put_oi': r[4],
                    'pcr': r[5],
                    'sentiment': r[6],
                    'add_exit': r[7]
                })
        else:
            return jsonify({"error": "Database not connected."}), 500
    return jsonify({"history": history_for_date})


# --------------------------------------------------------------------------- #
# NSE + BSE ANALYZER
# --------------------------------------------------------------------------- #
class NseBseAnalyzer:
    def __init__(self):
        self.stop = threading.Event()
        self.session = requests.Session()
        self.nse_headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'accept-language': 'en,gu;q=0.9,hi;q=0.8',
            'accept-encoding': 'gzip, deflate, br',
        }
        self.dhan_headers = {
            'access-token': DHAN_ACCESS_TOKEN,
            'client-id': DHAN_CLIENT_ID,
            'Content-Type': 'application/json'
        }
        self.url_oc = "https://www.nseindia.com/option-chain"
        self.url_nse = "https://www.nseindia.com/api/option-chain-indices?symbol="
        # Updated VIX URL for scraping
        self.url_indiavix_groww = "https://groww.in/indices/india-vix"
        self.url_dhan = "https://api.dhan.co/v2/optionchain"
        self.db_path = 'nse_bse_data.db'
        self.conn: Optional[sqlite3.Connection] = None
        self._init_db()
        # Changed pcr_data to pcr_graph_data for clarity
        self.pcr_graph_data: Dict[str, List[Dict[str, Any]]] = {sym: [] for sym in AUTO_SYMBOLS if sym != "INDIAVIX"}
        # New: Track last update time for PCR graph data points specifically
        self.last_pcr_graph_update_time: Dict[str, float] = {sym: 0 for sym in AUTO_SYMBOLS if sym != "INDIAVIX"}
        self._load_todays_history_from_db()  # Load today's history and reconstruct pcr_graph_data
        # Load initial underlying values from DB or set to None
        self._load_initial_underlying_values()

        # Initialize nsetools
        self.nse_tool = Nse()
        # Mapping for nsetools symbols
        self.nse_tool_symbol_map = {
            "NIFTY": "NIFTY 50",
            "BANKNIFTY": "NIFTY BANK",
            "FINNIFTY": "NIFNIFTY" # Common alternative for FINNIFTY in nsetools, verify if needed
        }

        # PCR reset date tracker (only for non-VIX symbols)
        self.last_pcr_data_reset_date: Dict[str, datetime.date] = {sym: datetime.date.min for sym in AUTO_SYMBOLS if
                                                                   sym != "INDIAVIX"}

        threading.Thread(target=self.run_loop, daemon=True).start()

    def _init_db(self):
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cur = self.conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT, symbol TEXT, sp REAL, value REAL, -- SP and Value can be REAL for VIX
                    call_oi REAL, put_oi REAL, pcr REAL, sentiment TEXT,
                    add_exit TEXT
                )
            """)
            cur.execute("PRAGMA table_info(history)")
            cols = [column[1] for column in cur.fetchall()]
            if 'add_exit' not in cols:
                cur.execute("ALTER TABLE history ADD COLUMN add_exit TEXT")
            # SQLite is flexible, so REAL type should handle both ints and floats.
            self.conn.commit()
        except Exception as e:
            print(f"DB error: {e}")

    def _load_todays_history_from_db(self):
        """Loads today's history into todays_history for initial frontend display
           and reconstructs pcr_graph_data from it based on UPDATE_INTERVAL."""
        if not self.conn: return
        try:
            cur = self.conn.cursor()
            today_str = datetime.date.today().strftime("%Y-%m-%d")
            for sym in AUTO_SYMBOLS:
                cur.execute("""
                    SELECT timestamp, sp, value, call_oi, put_oi, pcr, sentiment, add_exit
                    FROM history
                    WHERE symbol = ? AND SUBSTR(timestamp, 1, 10) = ?
                    ORDER BY timestamp ASC
                """, (sym, today_str))
                rows = cur.fetchall()
                todays_history[sym] = [] # Clear existing if any
                if sym != "INDIAVIX":
                    self.pcr_graph_data[sym] = [] # Clear PCR data for reconstruction

                last_pcr_add_time = 0.0 # To simulate the 2-minute interval for graph data
                for r in rows:
                    history_item = {
                        'time': r[0].split()[1][:5], # HH:MM
                        'sp': r[1],
                        'value': r[2],
                        'call_oi': r[3],
                        'put_oi': r[4],
                        'pcr': r[5],
                        'sentiment': r[6],
                        'add_exit': r[7]
                    }
                    todays_history[sym].append(history_item)

                    if sym != "INDIAVIX": # Reconstruct PCR data for charting
                        # Reconstruct the timestamp for the 2-minute interval check
                        item_datetime = datetime.datetime.strptime(r[0], "%Y-%m-%d %H:%M:%S")
                        item_timestamp_float = item_datetime.timestamp()

                        if item_timestamp_float - last_pcr_add_time >= UPDATE_INTERVAL:
                            self.pcr_graph_data[sym].append({"TIME": history_item['time'], "PCR": history_item['pcr']})
                            last_pcr_add_time = item_timestamp_float
                            # Update last_pcr_graph_update_time if this is the most recent entry
                            self.last_pcr_graph_update_time[sym] = item_timestamp_float

        except Exception as e:
            print(f"History load error: {e}")

    def _load_initial_underlying_values(self):
        """Loads the first underlying value for each symbol from today's history in DB."""
        if not self.conn: return
        with data_lock:
            cur = self.conn.cursor()
            today_str = datetime.date.today().strftime("%Y-%m-%d")
            for sym in AUTO_SYMBOLS:
                cur.execute("""
                    SELECT value
                    FROM history
                    WHERE symbol = ? AND SUBSTR(timestamp, 1, 10) = ?
                    ORDER BY timestamp ASC
                    LIMIT 1
                """, (sym, today_str))
                result = cur.fetchone()
                if result:
                    initial_underlying_values[sym] = float(result[0])
                else:
                    initial_underlying_values[sym] = None  # No historical data for today yet

    def run_loop(self):
        # Initial call to get cookies and session data
        try:
            self.session.get(self.url_oc, headers=self.nse_headers, timeout=10, verify=False)
        except requests.exceptions.RequestException as e:
            print(f"Initial NSE session setup failed: {e}")

        while not self.stop.is_set():
            current_date = datetime.date.today()
            for sym in AUTO_SYMBOLS:
                # Reset PCR data (and last_pcr_data_reset_date) at the start of a new day
                if sym != "INDIAVIX" and self.last_pcr_data_reset_date[sym] < current_date:
                    print(f"Resetting PCR graph data for {sym} for new day: {current_date}")
                    self.pcr_graph_data[sym] = []
                    self.last_pcr_graph_update_time[sym] = 0 # Reset PCR graph update time for new day
                    self.last_pcr_data_reset_date[sym] = current_date
                # Also reset initial_underlying_values for a new day
                # This check assumes last_alert[sym] is updated at least once a day.
                # A more robust check might involve comparing current_date with a stored "last_reset_date" for initial_underlying_values.
                if initial_underlying_values[sym] is not None and \
                   datetime.datetime.now().date() != datetime.datetime.fromtimestamp(last_alert[sym]).date():
                    initial_underlying_values[sym] = None


                try:
                    self.fetch_and_process_symbol(sym)
                except Exception as e:
                    print(f"{sym} error during fetch and process: {e}")
            time.sleep(LIVE_DATA_INTERVAL)  # Update live data every 15 seconds

    def fetch_and_process_symbol(self, sym: str):
        if sym == "SENSEX":
            self._process_sensex_data(sym)
        elif sym == "INDIAVIX":
            self._process_indiavix_data(sym)  # New VIX processing
        else:
            self._process_nse_data(sym)

    def _process_nse_data(self, sym: str):
        url = self.url_nse + sym
        try:
            resp = self.session.get(url, headers=self.nse_headers, timeout=10, verify=False)
            if resp.status_code == 401:
                # Re-fetch cookies if session expired (401 Unauthorized)
                self.session.get(self.url_oc, headers=self.nse_headers, timeout=5, verify=False)
                # Retry the request without explicitly passing `cookies=self.cookies`
                # as `requests.Session` handles cookies automatically.
                resp = self.session.get(url, headers=self.nse_headers, timeout=10, verify=False)
            data = resp.json()
            expiries = data.get('records', {}).get('expiryDates', [])
            if not expiries:
                print(f"{sym}: No expiry dates found.")
                return
            expiry = expiries[0]
            ce_values = [x['CE'] for x in data.get('records', {}).get('data', []) if
                         'CE' in x and x.get('expiryDate', '') == expiry]
            pe_values = [x['PE'] for x in data.get('records', {}).get('data', []) if
                         'PE' in x and x.get('expiryDate', '') == expiry]
            if not ce_values or not pe_values:  # Renamed from ce/pe to ce_values/pe_values
                print(f"{sym}: No CE or PE data found for expiry {expiry}.")
                return

            underlying = ce_values[0].get('underlyingValue', 0)
            df_ce = pd.DataFrame(ce_values)[['strikePrice', 'openInterest', 'changeinOpenInterest']]
            df_pe = pd.DataFrame(pe_values)[['strikePrice', 'openInterest', 'changeinOpenInterest']]
            df = pd.merge(df_ce, df_pe, on='strikePrice', how='outer', suffixes=('_call', '_put')).fillna(0)
            sp = self.get_atm_strike(df, underlying)
            if not sp:
                print(f"{sym}: Could not determine ATM strike.")
                return
            idx_list = df[df['strikePrice'] == sp].index.tolist()
            if not idx_list:
                print(f"{sym}: ATM strike {sp} not found in DataFrame.")
                return
            idx = idx_list[0]

            strikes_data = []
            ce_add_strikes = []
            ce_exit_strikes = []
            pe_add_strikes = []
            pe_exit_strikes = []
            for i_strike in range(-5, 6):  # Renamed loop variable to avoid conflict
                if idx + i_strike < 0 or idx + i_strike >= len(df): continue
                row = df.iloc[idx + i_strike]
                strike = int(row['strikePrice'])
                call_oi = int(row['openInterest_call'])
                put_oi = int(row['openInterest_put'])
                call_coi = int(row['changeinOpenInterest_call'])
                put_coi = int(row['changeinOpenInterest_put'])
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
                strikes_data.append({
                    'strike': strike,
                    'call_oi': call_oi,
                    'call_coi': call_coi,
                    'call_action': call_action,
                    'put_oi': put_oi,
                    'put_coi': put_coi,
                    'put_action': put_action,
                    'is_atm': i_strike == 0
                })
            total_call_oi = sum(df['openInterest_call'])
            total_put_oi = sum(df['openInterest_put'])
            pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi else 0.0

            # Ensure atm_index_in_strikes_data is correctly determined
            atm_index_in_strikes_data = -1
            for j_strike, s_data in enumerate(strikes_data):  # Renamed loop variable
                if s_data['strike'] == sp:
                    atm_index_in_strikes_data = j_strike
                    break

            if atm_index_in_strikes_data == -1:
                print(f"{sym}: ATM strike {sp} not found in filtered strikes_data, using default 5th element.")
                atm_index_in_strikes_data = 5  # Fallback, assuming 5th element is ATM if -5 to +5 range is used

            atm_call_coi = strikes_data[atm_index_in_strikes_data]['call_coi']
            atm_put_coi = strikes_data[atm_index_in_strikes_data]['put_coi']
            diff = round((atm_call_coi - atm_put_coi) / 1000, 1)
            sentiment = self.get_sentiment(diff, pcr)
            concise_add_exit_parts = []
            if ce_add_strikes:
                concise_add_exit_parts.append(f"CE Add: {', '.join(sorted(ce_add_strikes))}")
            if pe_add_strikes:
                concise_add_exit_parts.append(f"PE Add: {', '.join(sorted(pe_add_strikes))}")
            if ce_exit_strikes:
                concise_add_exit_parts.append(f"CE Exit: {', '.join(sorted(ce_exit_strikes))}")
            if pe_exit_strikes:
                concise_add_exit_parts.append(f"PE Exit: {', '.join(sorted(pe_exit_strikes))}")
            concise_add_exit_string = " | ".join(concise_add_exit_parts) if concise_add_exit_parts else "No Change"
            summary = {
                'time': datetime.datetime.now().strftime("%H:%M"),
                'sp': int(sp),
                'value': int(round(underlying)),
                'call_oi': round(atm_call_coi / 1000, 1),
                'put_oi': round(atm_put_coi / 1000, 1),
                'pcr': pcr,
                'sentiment': sentiment,
                'expiry': expiry,
                'add_exit': concise_add_exit_string
            }
            result = {'summary': summary, 'strikes': strikes_data}

            # --- Start: Integration of PCR and Max Pain logic ---
            # Get LTP for Max Pain calculation using nsetools
            try:
                # Use the mapping, fall back to original symbol if not found in mapping
                nse_sym_for_ltp = self.nse_tool_symbol_map.get(sym, sym)
                ltp_data = self.nse_tool.get_index_quote(nse_sym_for_ltp)
                ltp = ltp_data.get('lastPrice', underlying)  # Fallback to underlying if nsetools fails
            except Exception as e:
                print(f"Error fetching LTP for {sym} using nsetools: {e}. Using underlying value.")
                ltp = underlying

            # Prepare dataframes for Max Pain
            ce_dt_MaxPain = pd.DataFrame(ce_values)
            pe_dt_MaxPain = pd.DataFrame(pe_values)
            max_pain_df = self._calculate_max_pain(sym, ce_dt_MaxPain, pe_dt_MaxPain, ltp)

            # Prepare data for OI charts (top 10)
            ce_dt_for_charts = pd.DataFrame(ce_values).sort_values(['openInterest'], ascending=False)
            pe_dt_for_charts = pd.DataFrame(pe_values).sort_values(['openInterest'], ascending=False)
            final_ce_data = ce_dt_for_charts[['strikePrice', 'openInterest']].iloc[:10].to_dict(orient='records')
            final_pe_data = pe_dt_for_charts[['strikePrice', 'openInterest']].iloc[:10].to_dict(orient='records')

            # --- End: Integration of PCR and Max Pain logic ---

            with data_lock:
                shared_data[sym] = result
                # Update live feed summary
                if initial_underlying_values[sym] is None:
                    initial_underlying_values[sym] = float(underlying)  # Set baseline
                change = underlying - initial_underlying_values[sym]
                percentage_change = (change / initial_underlying_values[sym]) * 100 if initial_underlying_values[
                    sym] else 0
                shared_data[sym]['live_feed_summary'] = {
                    'current_value': int(round(underlying)),
                    'change': round(change, 2),
                    'percentage_change': round(percentage_change, 2)
                }
                # Add chart data (Max Pain, CE OI, PE OI) to shared_data
                shared_data[sym]['max_pain_chart_data'] = max_pain_df.to_dict(orient='records')
                shared_data[sym]['ce_oi_chart_data'] = final_ce_data
                shared_data[sym]['pe_oi_chart_data'] = final_pe_data
                # Add PCR chart data to shared_data (even if not updated this cycle, it's the current state)
                shared_data[sym]['pcr_chart_data'] = self.pcr_graph_data[sym]


            print(f"{sym} LIVE DATA UPDATED | SP: {sp} | PCR: {pcr} | {summary['add_exit']}")
            broadcast_live_update() # This sends the updated shared_data, including pcr_chart_data

            now = time.time()
            # History update logic (every UPDATE_INTERVAL)
            if now - last_history_update[sym] >= UPDATE_INTERVAL:
                with data_lock:
                    todays_history[sym].append(summary)
                    # Now update pcr_graph_data for charting ONLY when history is updated (2-min interval)
                    if sym != "INDIAVIX":
                        self.pcr_graph_data[sym].append({"TIME": summary['time'], "PCR": summary['pcr']})
                        # Keep pcr_graph_data manageable (e.g., last X points for a reasonable graph window)
                        if len(self.pcr_graph_data[sym]) > (60 / (UPDATE_INTERVAL / 60)) * 6: # e.g., 6 hours of 2-min data
                            self.pcr_graph_data[sym].pop(0)
                        self.last_pcr_graph_update_time[sym] = now # Update the last time PCR graph data was added
                    # Update pcr_chart_data in shared_data to reflect the newly added point
                    shared_data[sym]['pcr_chart_data'] = self.pcr_graph_data[sym]

                broadcast_history_append(sym, summary)
                last_history_update[sym] = now
                self._save_db(sym, summary)  # Save to DB with history update frequency
            # Telegram alert logic (every UPDATE_INTERVAL)
            if now - last_alert[sym] >= UPDATE_INTERVAL:
                self.send_alert(sym, summary)
                last_alert[sym] = now
        except requests.exceptions.RequestException as req_err:
            print(f"{sym} network error: {req_err}")
        except Exception as e:
            print(f"{sym} error: {e}")

    def _process_sensex_data(self, sym: str):
        if not DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN == "YOUR_ACCESS_TOKEN":
            print("SENSEX: Using mock data (DhanHQ keys not configured or default)")
            self._mock_sensex(sym)  # Pass sym to mock function
            return
        try:
            payload = {"symbol": "SENSEX", "instrument": "INDEX", "expiry": "ALL"}
            resp = requests.post(self.url_dhan, headers=self.dhan_headers, json=payload, timeout=15)
            resp.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            data = resp.json()
            if data.get('status') != 'success': raise Exception(
                f"DhanHQ API failed: {data.get('message', 'Unknown error')}")
            chain = data.get('data', [])
            if not chain: raise Exception("No data received from DhanHQ API")
            underlying = chain[0].get('underlyingValue', 0)
            df = pd.DataFrame(chain)
            df = df[['strikePrice', 'callOI', 'callChangeInOI', 'putOI', 'putChangeInOI']].fillna(0)
            sp = self.get_atm_strike(df, underlying)
            if not sp:
                print(f"{sym}: Could not determine ATM strike from DhanHQ data.")
                return
            idx_list = df[df['strikePrice'] == sp].index.tolist()
            if not idx_list:
                print(f"{sym}: ATM strike {sp} not found in DhanHQ DataFrame.")
                return
            idx = idx_list[0]

            strikes_data = []
            ce_add_strikes = []
            ce_exit_strikes = []
            pe_add_strikes = []
            pe_exit_strikes = []
            for i_strike in range(-5, 6):  # Renamed loop variable
                if idx + i_strike < 0 or idx + i_strike >= len(df): continue
                row = df.iloc[idx + i_strike]
                strike = int(row['strikePrice'])
                call_oi = int(row['callOI'])
                put_oi = int(row['putOI'])
                call_coi = int(row['callChangeInOI'])
                put_coi = int(row['putChangeInOI'])
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
                strikes_data.append({
                    'strike': strike,
                    'call_oi': call_oi,
                    'call_coi': call_coi,
                    'call_action': call_action,
                    'put_oi': put_oi,
                    'put_coi': put_coi,
                    'put_action': put_action,
                    'is_atm': i_strike == 0
                })
            total_call_oi = sum(df['callOI'])
            total_put_oi = sum(df['putOI'])
            pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi else 0.0

            # Ensure atm_index_in_strikes_data is correctly determined
            atm_index_in_strikes_data = -1
            for j_strike, s_data in enumerate(strikes_data):  # Renamed loop variable
                if s_data['strike'] == sp:
                    atm_index_in_strikes_data = j_strike
                    break

            if atm_index_in_strikes_data == -1:
                print(f"{sym}: ATM strike {sp} not found in filtered strikes_data, using default 5th element.")
                atm_index_in_strikes_data = 5  # Fallback, assuming 5th element is ATM if -5 to +5 range is used

            atm_call_coi = strikes_data[atm_index_in_strikes_data]['call_coi']
            atm_put_coi = strikes_data[atm_index_in_strikes_data]['put_coi']

            diff = round((atm_call_coi - atm_put_coi) / 1000, 1)
            sentiment = self.get_sentiment(diff, pcr)
            concise_add_exit_parts = []
            if ce_add_strikes:
                concise_add_exit_parts.append(f"CE Add: {', '.join(sorted(ce_add_strikes))}")
            if pe_add_strikes:
                concise_add_exit_parts.append(f"PE Add: {', '.join(sorted(pe_add_strikes))}")
            if ce_exit_strikes:
                concise_add_exit_parts.append(f"CE Exit: {', '.join(sorted(ce_exit_strikes))}")
            if pe_exit_strikes:
                concise_add_exit_parts.append(f"PE Exit: {', '.join(sorted(pe_exit_strikes))}")
            concise_add_exit_string = " | ".join(concise_add_exit_parts) if concise_add_exit_parts else "No Change"
            summary = {
                'time': datetime.datetime.now().strftime("%H:%M"),
                'sp': int(sp),
                'value': int(round(underlying)),
                'call_oi': round(atm_call_coi / 1000, 1),
                'put_oi': round(atm_put_coi / 1000, 1),
                'pcr': pcr,
                'sentiment': sentiment,
                'expiry': chain[0].get('expiryDate', ''),  # Assuming first item has expiryDate
                'add_exit': concise_add_exit_string
            }
            result = {'summary': summary, 'strikes': strikes_data}

            # --- Start: Integration of PCR and Max Pain logic for SENSEX ---
            ltp = underlying  # Use underlying from DhanHQ as LTP

            # Prepare dataframes for Max Pain
            ce_dt_MaxPain = pd.DataFrame(chain)  # Use original chain data
            pe_dt_MaxPain = pd.DataFrame(chain)  # Use original chain data
            max_pain_df = self._calculate_max_pain(sym, ce_dt_MaxPain, pe_dt_MaxPain, ltp, dhan_data=True)

            # Prepare data for OI charts (top 10)
            ce_dt_for_charts = pd.DataFrame(chain).sort_values(['callOI'], ascending=False)
            pe_dt_for_charts = pd.DataFrame(chain).sort_values(['putOI'], ascending=False)
            final_ce_data = ce_dt_for_charts[['strikePrice', 'callOI']].iloc[:10].rename(
                columns={'callOI': 'openInterest'}).to_dict(orient='records')
            final_pe_data = pe_dt_for_charts[['strikePrice', 'putOI']].iloc[:10].rename(
                columns={'putOI': 'openInterest'}).to_dict(orient='records')

            # --- End: Integration of PCR and Max Pain logic ---

            with data_lock:
                shared_data[sym] = result
                # Update live feed summary
                if initial_underlying_values[sym] is None:
                    initial_underlying_values[sym] = float(underlying)  # Set baseline
                change = underlying - initial_underlying_values[sym]
                percentage_change = (change / initial_underlying_values[sym]) * 100 if initial_underlying_values[
                    sym] else 0
                shared_data[sym]['live_feed_summary'] = {
                    'current_value': int(round(underlying)),
                    'change': round(change, 2),
                    'percentage_change': round(percentage_change, 2)
                }
                # Add chart data (Max Pain, CE OI, PE OI) to shared_data
                shared_data[sym]['max_pain_chart_data'] = max_pain_df.to_dict(orient='records')
                shared_data[sym]['ce_oi_chart_data'] = final_ce_data
                shared_data[sym]['pe_oi_chart_data'] = final_pe_data
                # Add PCR chart data to shared_data (even if not updated this cycle, it's the current state)
                shared_data[sym]['pcr_chart_data'] = self.pcr_graph_data[sym]

            print(f"SENSEX REAL DATA UPDATED | SP: {sp} | PCR: {pcr} | {summary['add_exit']}")
            broadcast_live_update()
            now = time.time()
            if now - last_history_update[sym] >= UPDATE_INTERVAL:
                with data_lock:
                    todays_history[sym].append(summary)
                    # Now update pcr_graph_data for charting ONLY when history is updated (2-min interval)
                    if sym != "INDIAVIX":
                        self.pcr_graph_data[sym].append({"TIME": summary['time'], "PCR": summary['pcr']})
                        if len(self.pcr_graph_data[sym]) > (60 / (UPDATE_INTERVAL / 60)) * 6: # e.g., 6 hours of 2-min data
                            self.pcr_graph_data[sym].pop(0)
                        self.last_pcr_graph_update_time[sym] = now
                    # Update pcr_chart_data in shared_data to reflect the newly added point
                    shared_data[sym]['pcr_chart_data'] = self.pcr_graph_data[sym]

                broadcast_history_append(sym, summary)
                last_history_update[sym] = now
                self._save_db(sym, summary)
            if now - last_alert[sym] >= UPDATE_INTERVAL:
                self.send_alert(sym, summary)
                last_alert[sym] = now
        except requests.exceptions.RequestException as req_err:
            print(f"SENSEX DhanHQ network error: {req_err}. Falling back to mock.")
            self._mock_sensex(sym)
        except Exception as e:
            print(f"SENSEX DhanHQ error: {e}. Falling back to mock.")
            self._mock_sensex(sym)

    def _process_indiavix_data(self, sym: str):
        # --- WEB SCRAPING INDIAVIX DATA FROM GROWW.IN ---
        current_vix: Optional[float] = None
        scraped_change = 0.0
        scraped_percentage_change = 0.0
        sentiment = "Neutral"
        try:
            groww_headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            resp = requests.get(self.url_indiavix_groww, headers=groww_headers, timeout=10)
            resp.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            # Use 'lxml' parser explicitly
            soup = BeautifulSoup(resp.text, 'lxml')
            # --- Updated CSS Selectors for Groww.in ---
            # Main VIX value
            vix_value_element = soup.select_one('div.gsc84Text.gsi216Text.gsi216Flex:nth-child(1) div.gsc84Text')
            if vix_value_element:
                current_vix = float(vix_value_element.text.replace(',', '').strip())
            else:
                print("INDIAVIX scraping: Could not find VIX value element. Falling back to mock data.")
                self._mock_indiavix(sym)
                return
            # Change and Percentage Change
            # Look for the container holding these two values, then extract them
            change_container = soup.select_one(
                'div.gsi216Flex.gsi216Flex.gsi216Flex.gsi216Flex')  # This targets the flex container holding both

            if change_container:
                # Find all divs with class 'gsc84Text' within this container
                change_elements = change_container.find_all('div', class_='gsc84Text')

                if len(change_elements) >= 2:  # Expect at least 2: one for change, one for percentage
                    scraped_change_text = change_elements[0].text.replace(',', '').strip()
                    scraped_percentage_change_text = change_elements[1].text.replace('(', '').replace(')', '').replace(
                        '%', '').strip()

                    scraped_change = float(scraped_change_text)
                    scraped_percentage_change = float(scraped_percentage_change_text)

                    # Determine sentiment based on change
                    if scraped_change > 0:
                        sentiment = "Mild Bearish"  # VIX up means more fear
                    elif scraped_change < 0:
                        sentiment = "Mild Bullish"  # VIX down means less fear
                    else:
                        sentiment = "Neutral"
                else:
                    print(
                        "INDIAVIX scraping: Could not find both change/pct change elements within container. Using default change/pct change for VIX.")
            else:
                print("INDIAVIX scraping: Could not find change container. Using default change/pct change for VIX.")
            # --- End Updated CSS Selectors ---

            if current_vix is None:  # If scraping failed completely, ensure mock is called
                self._mock_indiavix(sym)
                return

            # Set initial underlying value if not set
            with data_lock:
                if initial_underlying_values[sym] is None:
                    # If we successfully scraped current VIX and change, calculate baseline.
                    # Otherwise, just set current_vix as the baseline.
                    if current_vix is not None:
                        # Attempt to calculate initial value if change is known
                        if scraped_change is not None:
                            initial_underlying_values[sym] = current_vix - scraped_change
                        else:
                            initial_underlying_values[sym] = current_vix  # Fallback if change not scraped

            # VIX doesn't have OI, PCR, SP, etc. so we adapt the summary structure
            # SP field will store 'change', PCR field will store 'percentage_change'
            summary = {
                'time': datetime.datetime.now().strftime("%H:%M"),
                'sp': round(scraped_change, 2),  # Store change in 'sp' field for history table display
                'value': round(current_vix, 2),  # VIX value in 'value' field
                'call_oi': 0.0,  # Not applicable for VIX
                'put_oi': 0.0,  # Not applicable for VIX
                'pcr': round(scraped_percentage_change, 2),  # Store pct change in 'pcr' field
                'sentiment': sentiment,
                'expiry': "N/A",  # Not applicable for VIX
                'add_exit': "N/A"  # No specific OI Changes for VIX
            }
            result = {'summary': summary, 'strikes': []}  # VIX has no option chain strikes
            with data_lock:
                shared_data[sym] = result
                shared_data[sym]['live_feed_summary'] = {
                    'current_value': round(current_vix, 2),
                    'change': round(scraped_change, 2),
                    'percentage_change': round(scraped_percentage_change, 2)
                }
            print(
                f"{sym} SCRAPED DATA UPDATED | Value: {current_vix:.2f} | Change: {scraped_change:+.2f} | Pct: {scraped_percentage_change:+.2f}%")
            broadcast_live_update()
            now = time.time()
            if now - last_history_update[sym] >= UPDATE_INTERVAL:
                with data_lock:
                    todays_history[sym].append(summary)
                broadcast_history_append(sym, summary)
                last_history_update[sym] = now
                self._save_db(sym, summary)
            if now - last_alert[sym] >= UPDATE_INTERVAL:
                self.send_alert(sym, summary)
                last_alert[sym] = now
        except requests.exceptions.RequestException as req_err:
            print(f"INDIAVIX scraping network error: {req_err}. Falling back to mock data.")
            self._mock_indiavix(sym)
            return
        except (ValueError, AttributeError, IndexError) as parse_err:  # Added IndexError for list access
            print(
                f"INDIAVIX scraping parsing error: {parse_err}. HTML structure might have changed. Falling back to mock data.")
            self._mock_indiavix(sym)
            return
        except Exception as e:
            print(f"INDIAVIX unexpected scraping error: {e}. Falling back to mock data.")
            self._mock_indiavix(sym)
            return

    def _mock_indiavix(self, sym: str):
        """Fallback mock data for INDIAVIX if scraping fails."""
        current_vix = random.uniform(10.0, 30.0)
        with data_lock:
            if initial_underlying_values[sym] is None:
                # Set a plausible initial value for mock if not already set
                initial_underlying_values[sym] = current_vix - random.uniform(-2.0, 2.0)
                if initial_underlying_values[sym] < 0:
                    initial_underlying_values[sym] = 10.0  # Ensure it's not negative

            # Calculate change and percentage change based on the initial value
            change = current_vix - (
                initial_underlying_values[sym] if initial_underlying_values[sym] is not None else current_vix)
            percentage_change = (change / initial_underlying_values[sym]) * 100 if initial_underlying_values[sym] else 0

        sentiment = "Neutral"
        if change > 0:
            sentiment = "Mild Bearish"
        elif change < 0:
            sentiment = "Mild Bullish"

        summary = {
            'time': datetime.datetime.now().strftime("%H:%M"),
            'sp': round(change, 2),  # Store change in 'sp' field for consistency with history table
            'value': round(current_vix, 2),  # VIX value in 'value' field
            'call_oi': 0.0,
            'put_oi': 0.0,
            'pcr': round(percentage_change, 2),  # Store pct change in 'pcr' field
            'sentiment': sentiment,
            'expiry': "N/A",
            'add_exit': "Mock Data"
        }
        result = {'summary': summary, 'strikes': []}
        with data_lock:
            shared_data[sym] = result
            shared_data[sym]['live_feed_summary'] = {
                'current_value': round(current_vix, 2),
                'change': round(change, 2),
                'percentage_change': round(percentage_change, 2)
            }
        print(
            f"{sym} MOCK DATA UPDATED | Value: {current_vix:.2f} | Change: {change:+.2f} | Pct: {percentage_change:+.2f}% (Fallback)")
        broadcast_live_update()
        now = time.time()
        if now - last_history_update[sym] >= UPDATE_INTERVAL:
            with data_lock:
                todays_history[sym].append(summary)
            broadcast_history_append(sym, summary)
            last_history_update[sym] = now
            self._save_db(sym, summary)
        if now - last_alert[sym] >= UPDATE_INTERVAL:
            self.send_alert(sym, summary)
            last_alert[sym] = now

    def _mock_sensex(self, sym: str):  # Added sym parameter
        underlying = 75000 + random.randint(-500, 500)
        sp = round(underlying / 100) * 100
        strikes_data = []
        ce_add_strikes = []
        ce_exit_strikes = []
        pe_add_strikes = []
        pe_exit_strikes = []
        for i_strike in range(-5, 6):  # Renamed loop variable
            s = sp + i_strike * 100
            call_coi = random.randint(100, 5000)
            put_coi = random.randint(100, 5000)
            call_action = "ADD" if call_coi > 0 else "EXIT" if call_coi < 0 else ""
            put_action = "ADD" if put_coi > 0 else "EXIT" if put_coi < 0 else ""
            if call_action == "ADD":
                ce_add_strikes.append(str(s))
            elif call_action == "EXIT":
                ce_exit_strikes.append(str(s))
            if put_action == "ADD":
                pe_add_strikes.append(str(s))
            elif put_action == "EXIT":
                pe_exit_strikes.append(str(s))
            strikes_data.append({
                'strike': s,
                'call_oi': random.randint(100, 5000),
                'call_coi': call_coi,
                'call_action': call_action,
                'put_oi': random.randint(100, 5000),
                'put_coi': put_coi,
                'put_action': put_action,
                'is_atm': i_strike == 0
            })

        # Corrected: Ensure atm_call_coi and atm_put_coi are derived from strikes_data
        # assuming the 5th element (index 5) is the ATM strike in a -5 to +5 range.
        if len(strikes_data) > 5:
            atm_call_coi = strikes_data[5]['call_coi']
            atm_put_coi = strikes_data[5]['put_coi']
        else:
            # Fallback if strikes_data is not as expected (e.g., less than 11 elements)
            atm_call_coi = 0
            atm_put_coi = 0

        diff = round((atm_call_coi - atm_put_coi) / 1000, 1)
        pcr = round(random.uniform(0.6, 1.4), 2)
        sentiment = self.get_sentiment(diff, pcr)
        concise_add_exit_parts = []
        if ce_add_strikes:
            concise_add_exit_parts.append(f"CE Add: {', '.join(sorted(ce_add_strikes))}")
        if pe_add_strikes:
            concise_add_exit_parts.append(f"PE Add: {', '.join(sorted(pe_add_strikes))}")
        if ce_exit_strikes:
            concise_add_exit_parts.append(f"CE Exit: {', '.join(sorted(ce_exit_strikes))}")
        if pe_exit_strikes:
            concise_add_exit_parts.append(f"PE Exit: {', '.join(sorted(pe_exit_strikes))}")
        concise_add_exit_string = " | ".join(concise_add_exit_parts) if concise_add_exit_parts else "No Change"
        summary = {
            'time': datetime.datetime.now().strftime("%H:%M"),
            'sp': int(sp),
            'value': int(round(underlying)),
            'call_oi': round(atm_call_coi / 1000, 1),
            'put_oi': round(atm_put_coi / 1000, 1),
            'pcr': pcr,
            'sentiment': sentiment,
            'expiry': "28-Mar-2025",  # Mock expiry
            'add_exit': concise_add_exit_string
        }
        result = {'summary': summary, 'strikes': strikes_data}

        # --- Start: Integration of PCR and Max Pain logic for SENSEX Mock ---
        ltp = underlying

        # Generate mock data for Max Pain
        mock_max_pain_data = []
        # Ensure a reasonable range of strikes for mock data
        strike_min = int(sp - 500)
        strike_max = int(sp + 500)
        # Ensure strikes are multiples of 100 (typical for SENSEX)
        strike_min = (strike_min // 100) * 100
        strike_max = (strike_max // 100) * 100

        for s_val in range(strike_min, strike_max + 1, 100):
            # Explicitly make TotalMaxPain a float to avoid FutureWarning
            mock_max_pain_data.append({'StrikePrice': s_val, 'TotalMaxPain': float(random.randint(10000000000, 100000000000))})
        mock_max_pain_df = pd.DataFrame(mock_max_pain_data)
        # Ensure the column is float type
        mock_max_pain_df['TotalMaxPain'] = mock_max_pain_df['TotalMaxPain'].astype(float)


        # Simulate min pain around the current sp
        if not mock_max_pain_df.empty:
            min_pain_strike = sp + random.choice([-200, 0, 200])
            # Find the actual strikePrice closest to min_pain_strike in the generated mock data
            if not mock_max_pain_df.empty:
                closest_mock_strike = mock_max_pain_df.iloc[
                    (mock_max_pain_df['StrikePrice'] - min_pain_strike).abs().argsort()[:1]]
                if not closest_mock_strike.empty:
                    # This assignment now works without FutureWarning because TotalMaxPain is float
                    mock_max_pain_df.loc[closest_mock_strike.index[0], 'TotalMaxPain'] /= random.uniform(2, 5)

        # Prepare mock OI charts (top 10)
        mock_ce_oi_data = [{'strikePrice': s['strike'], 'openInterest': s['call_oi']} for s in strikes_data[:10]]
        mock_pe_oi_data = [{'strikePrice': s['strike'], 'openInterest': s['put_oi']} for s in strikes_data[:10]]

        # --- End: Integration of PCR and Max Pain logic for SENSEX Mock ---

        with data_lock:
            shared_data[sym] = result  # Use sym parameter
            # Update live feed summary for mock data
            if initial_underlying_values[sym] is None:
                initial_underlying_values[sym] = float(underlying)  # Set baseline
            change = underlying - initial_underlying_values[sym]
            percentage_change = (change / initial_underlying_values[sym]) * 100 if initial_underlying_values[
                sym] else 0
            shared_data[sym]['live_feed_summary'] = {
                'current_value': int(round(underlying)),
                'change': round(change, 2),
                'percentage_change': round(percentage_change, 2)
            }
            # Add chart data (Max Pain, CE OI, PE OI) to shared_data for mock
            shared_data[sym]['max_pain_chart_data'] = mock_max_pain_df.to_dict(orient='records')
            shared_data[sym]['ce_oi_chart_data'] = mock_ce_oi_data
            shared_data[sym]['pe_oi_chart_data'] = mock_pe_oi_data
            # Add PCR chart data to shared_data (even if not updated this cycle, it's the current state)
            shared_data[sym]['pcr_chart_data'] = self.pcr_graph_data[sym]

        print(f"{sym} MOCK DATA UPDATED | SP: {sp} | PCR: {pcr} | {summary['add_exit']}")  # Use sym parameter
        broadcast_live_update()
        now = time.time()
        if now - last_history_update[sym] >= UPDATE_INTERVAL:  # Use sym parameter
            with data_lock:
                todays_history[sym].append(summary)  # Use sym parameter
                # Now update pcr_graph_data for charting ONLY when history is updated (2-min interval)
                if sym != "INDIAVIX":
                    self.pcr_graph_data[sym].append({"TIME": summary['time'], "PCR": summary['pcr']})
                    if len(self.pcr_graph_data[sym]) > (60 / (UPDATE_INTERVAL / 60)) * 6: # e.g., 6 hours of 2-min data
                        self.pcr_graph_data[sym].pop(0)
                    self.last_pcr_graph_update_time[sym] = now
                # Update pcr_chart_data in shared_data to reflect the newly added point
                shared_data[sym]['pcr_chart_data'] = self.pcr_graph_data[sym]

            broadcast_history_append(sym, summary)  # Use sym parameter
            last_history_update[sym] = now  # Use sym parameter
            self._save_db(sym, summary)  # Use sym parameter
        if now - last_alert[sym] >= UPDATE_INTERVAL:  # Use sym parameter
            self.send_alert(sym, summary)  # Use sym parameter
            last_alert[sym] = now  # Use sym parameter

    def get_atm_strike(self, df: pd.DataFrame, underlying: float) -> Optional[int]:
        try:
            strikes = df['strikePrice'].astype(int).unique()
            if len(strikes) == 0:
                return None
            return min(strikes, key=lambda x: abs(x - underlying))
        except Exception as e:
            print(f"Error getting ATM strike: {e}")
            return None

    def get_sentiment(self, diff: float, pcr: float) -> str:
        if diff > 10 or pcr < 0.7: return "Strong Bearish"
        if diff < -10 or pcr > 1.3: return "Strong Bullish"
        if diff > 2 or (0.9 < pcr <= 1.3): return "Mild Bearish"  # Corrected parenthesis for clarity
        if diff < -2 or (0.7 <= pcr < 0.9): return "Mild Bullish"  # Corrected parenthesis for clarity
        return "Neutral"

    def send_alert(self, sym: str, row: Dict[str, Any]):
        # Adjusting message for VIX which doesn't have SP, Call COI, Put COI, PCR in the same way
        if sym == "INDIAVIX":
            # Pull directly from live_feed_summary for accuracy in alert
            live_feed = shared_data.get(sym, {}).get('live_feed_summary', {})
            msg = f"""
<b>{sym} LIVE</b>
<b>Value:</b> <code>{live_feed.get('current_value', row['value']):.2f}</code>
<b>Change:</b> <code>{live_feed.get('change', 0.0):+.2f}</code> | <b>Pct:</b> <code>{live_feed.get('percentage_change', 0.0):+.2f}%</code>
<b>Sentiment:</b> <b>{row['sentiment']}</b>
<b>Notes:</b> <code>{row['add_exit']}</code>
@aditya_nse_bot
            """.strip()
        else:
            msg = f"""
<b>{sym} LIVE</b> | <i>{row['expiry']}</i>
<b>SP:</b> <code>{int(row['sp'])}</code> | <b>Value:</b> <code>{int(row['value'])}</code>
<b>Call COI:</b> <code>{row['call_oi']:+}</code>K | <b>Put COI:</b> <code>{row['put_oi']:+}</code>K
<b>PCR:</b> <b>{row['pcr']:.2f}</b>
<b>OI Change:</b> <code>{row['add_exit']}</code>
<b>Sentiment:</b> <b>{row['sentiment']}</b>
@aditya_nse_bot
            """.strip()
        send_telegram_text(msg)

    def _save_db(self, sym, row):
        if not self.conn: return
        try:
            current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.conn.execute(
                "INSERT INTO history (timestamp, symbol, sp, value, call_oi, put_oi, pcr, sentiment, add_exit) VALUES (?,?,?,?,?,?,?,?,?)",
                (current_timestamp, sym, row['sp'], row['value'],
                 row['call_oi'], row['put_oi'], row['pcr'], row['sentiment'], row['add_exit'])
            )
            # Optional: Prune old entries for this symbol if MAX_HISTORY_ROWS_DB is exceeded
            # This keeps the DB from growing infinitely large for a single symbol
            self.conn.execute("""
                DELETE FROM history
                WHERE id NOT IN (
                    SELECT id FROM history WHERE symbol = ? ORDER BY timestamp DESC LIMIT ?
                ) AND symbol = ?;
            """, (sym, MAX_HISTORY_ROWS_DB, sym))
            self.conn.commit()
        except Exception as e:
            print(f"DB save error for {sym}: {e}")

    # --- Methods for Max Pain Calculation ---
    def _calculate_max_pain(self, sym: str, ce_dt: pd.DataFrame, pe_dt: pd.DataFrame, ltp: float,
                            dhan_data: bool = False) -> pd.DataFrame:
        """Calculates Max Pain for a given symbol."""
        if dhan_data:  # Adjust column names for DhanHQ data
            MxPn_CE = ce_dt[['strikePrice', 'callOI']].rename(columns={'callOI': 'openInterest'})
            MxPn_PE = pe_dt[['strikePrice', 'putOI']].rename(columns={'putOI': 'openInterest'})
        else:  # Standard NSE data
            MxPn_CE = ce_dt[['strikePrice', 'openInterest']]
            MxPn_PE = pe_dt[['strikePrice', 'openInterest']]

        MxPn_Df = pd.merge(MxPn_CE, MxPn_PE, on=['strikePrice'], how='outer', suffixes=('_call', '_Put')).fillna(0)
        # Ensure column names are correct after merge, if not already
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

        # Filter to a reasonable range around the minimum pain strike
        if not max_pain_df.empty:
            min_idx = max_pain_df['TotalMaxPain'].idxmin()
            min_pain_strike = max_pain_df.loc[min_idx, 'StrikePrice']

            # Find closest index to min_pain_strike in the sorted list of strikes
            strikes_sorted = sorted(StrikePriceList)
            try:
                closest_strike_index_in_sorted_list = strikes_sorted.index(min_pain_strike)
            except ValueError:
                # If min_pain_strike isn't exactly in strikes_sorted (due to float comparison or missing strike),
                # find the nearest one.
                closest_strike_index_in_sorted_list = \
                    (pd.Series(strikes_sorted) - min_pain_strike).abs().argsort()[:1].iloc[0]

            # Define a window around the min pain strike
            # Get the actual index in the *original* StrikePriceList for slicing the dataframe
            # This is safer than relying on implicit index after sorting.
            start_index_for_window = max(0, closest_strike_index_in_sorted_list - 8)
            end_index_for_window = min(len(strikes_sorted), closest_strike_index_in_sorted_list + 8)

            strikes_in_window = strikes_sorted[start_index_for_window:end_index_for_window]

            # Filter the DataFrame to only include strikes within this window
            max_pain_chart_df = max_pain_df[max_pain_df['StrikePrice'].isin(strikes_in_window)].reset_index(drop=True)
            return max_pain_chart_df
        return pd.DataFrame(columns=["StrikePrice", "TotalMaxPain"])

    def _total_option_pain_for_strike(self, strike_price_list: List[float], oi_call_list: List[int],
                                      oi_put_list: List[int], mxpn_strike: float) -> float:
        """Calculates total option pain for a given strike price."""
        total_cash_value = 0

        for k in range(len(strike_price_list)):
            strike = strike_price_list[k]
            call_oi = oi_call_list[k]
            put_oi = oi_put_list[k]

            # Intrinsic value for Call (max(0, assumed_spot - strike))
            # If the current spot (mxpn_strike) is below the strike, the call is OTM, intrinsic value is 0.
            # Otherwise, intrinsic value is mxpn_strike - strike.
            intrinsic_call = max(0, mxpn_strike - strike)

            # Intrinsic value for Put (max(0, strike - assumed_spot))
            # If the current spot (mxpn_strike) is above the strike, the put is OTM, intrinsic value is 0.
            # Otherwise, intrinsic value is strike - mxpn_strike.
            intrinsic_put = max(0, strike - mxpn_strike)

            total_cash_value += (intrinsic_call * call_oi)
            total_cash_value += (intrinsic_put * put_oi)

        return total_cash_value


# --------------------------------------------------------------------------- #
# CREATE HTML & CSS
# --------------------------------------------------------------------------- #
def create_html():
    html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NSE OCA PRO WEB</title>
    <link rel="stylesheet" href="/static/style.css">
    <script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script> <!-- Chart.js -->
</head>
<body>
    <div class="container">
        <h1>NSE OCA PRO LIVE DASHBOARD</h1>
        <div class="live-feed-container"> <!-- NEW LIVE FEED CONTAINER -->
            <div class="live-feed-item">
                <span class="feed-symbol">NIFTY:</span>
                <span id="feed-nifty-value" class="feed-value">-</span>
                <span id="feed-nifty-change" class="feed-change">-</span>
                <span id="feed-nifty-pct" class="feed-pct">-</span>
            </div>
            <div class="live-feed-item">
                <span class="feed-symbol">FINNIFTY:</span>
                <span id="feed-finnifty-value" class="feed-value">-</span>
                <span id="feed-finnifty-change" class="feed-change">-</span>
                <span id="feed-finnifty-pct" class="feed-pct">-</span>
            </div>
            <div class="live-feed-item">
                <span class="feed-symbol">BANKNIFTY:</span>
                <span id="feed-banknifty-value" class="feed-value">-</span>
                <span id="feed-banknifty-change" class="feed-change">-</span>
                <span id="feed-banknifty-pct" class="feed-pct">-</span>
            </div>
            <div class="live-feed-item">
                <span class="feed-symbol">SENSEX:</span>
                <span id="feed-sensex-value" class="feed-value">-</span>
                <span id="feed-sensex-change" class="feed-change">-</span>
                <span id="feed-sensex-pct" class="feed-pct">-</span>
            </div>
            <div class="live-feed-item"> <!-- NEW INDIAVIX LIVE FEED ITEM -->
                <span class="feed-symbol">INDIAVIX:</span>
                <span id="feed-indiavix-value" class="feed-value">-</span>
                <span id="feed-indiavix-change" class="feed-change">-</span>
                <span id="feed-indiavix-pct" class="feed-pct">-</span>
            </div>
        </div> <!-- END NEW LIVE FEED CONTAINER -->
        <div class="grid">
            <div class="card" id="NIFTY_summary">
                <h2>NIFTY</h2>
                <p>SP: <span id="nifty-sp">-</span></p>
                <p>Value: <span id="nifty-value">-</span></p>
                <p>Call COI: <span id="nifty-call">-</span>K</p>
                <p>Put COI: <span id="nifty-put">-</span>K</p>
                <p>PCR: <span id="nifty-pcr">-</span></p>
                <p>Sentiment: <span id="nifty-sentiment">-</span></p>
                <p>OI Changes: <span id="nifty-add-exit">-</span></p>
            </div>
            <div class="card" id="FINNIFTY_summary">
                <h2>FINNIFTY</h2>
                <p>SP: <span id="finnifty-sp">-</span></p>
                <p>Value: <span id="finnifty-value">-</span></p>
                <p>Call COI: <span id="finnifty-call">-</span>K</p>
                <p>Put COI: <span id="finnifty-put">-</span>K</p>
                <p>PCR: <span id="finnifty-pcr">-</span></p>
                <p>Sentiment: <span id="finnifty-sentiment">-</span></p>
                <p>OI Changes: <span id="finnifty-add-exit">-</span></p>
            </div>
            <div class="card" id="BANKNIFTY_summary">
                <h2>BANKNIFTY</h2>
                <p>SP: <span id="banknifty-sp">-</span></p>
                <p>Value: <span id="banknifty-value">-</span></p>
                <p>Call COI: <span id="banknifty-call">-</span>K</p>
                <p>Put COI: <span id="banknifty-put">-</span>K</p>
                <p>PCR: <span id="banknifty-pcr">-</span></p>
                <p>Sentiment: <span id="banknifty-sentiment">-</span></p>
                <p>OI Changes: <span id="banknifty-add-exit">-</span></p>
            </div>
            <div class="card" id="SENSEX_summary">
                <h2>SENSEX</h2>
                <p>SP: <span id="sensex-sp">-</span></p>
                <p>Value: <span id="sensex-value">-</span></p>
                <p>Call COI: <span id="sensex-call">-</span>K</p>
                <p>Put COI: <span id="sensex-put">-</span>K</p>
                <p>PCR: <span id="sensex-pcr">-</span></p>
                <p>Sentiment: <span id="sensex-sentiment">-</span></p>
                <p>OI Changes: <span id="sensex-add-exit">-</span></p>
            </div>
            <div class="card" id="INDIAVIX_summary"> <!-- NEW INDIAVIX SUMMARY CARD -->
                <h2>INDIAVIX</h2>
                <p>Value: <span id="indiavix-value">-</span></p>
                <p>Change: <span id="indiavix-change">-</span></p>
                <p>Pct Change: <span id="indiavix-pct">-</span>%</p>
                <p>Sentiment: <span id="indiavix-sentiment">-</span></p>
                <p>Notes: <span id="indiavix-add-exit">N/A</span></p> <!-- VIX has no OI changes -->
            </div>
        </div>
        <div class="tabs">
            <button class="tablink active" onclick="openTab(event, 'NIFTY')" id="NIFTYOpen" data-symbol="NIFTY">NIFTY</button>
            <button class="tablink" onclick="openTab(event, 'FINNIFTY')" data-symbol="FINNIFTY">FINNIFTY</button>
            <button class="tablink" onclick="openTab(event, 'BANKNIFTY')" data-symbol="BANKNIFTY">BANKNIFTY</button>
            <button class="tablink" onclick="openTab(event, 'SENSEX')" data-symbol="SENSEX">SENSEX</button>
            <button class="tablink" onclick="openTab(event, 'INDIAVIX')" data-symbol="INDIAVIX">INDIAVIX</button> <!-- NEW INDIAVIX TAB BUTTON -->
        </div>
        <div id="NIFTY_tab" class="tabcontent" style="display: block;">
            <h3 style="margin-top: 20px;">NIFTY Today's Updates (appends every 2 min)</h3>
            <div class="table-container">
                <table id="nifty-todays-history-table">
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>SP</th>
                            <th>Value</th>
                            <th>Call COI</th>
                            <th>Put COI</th>
                            <th>PCR</th>
                            <th>Sentiment</th>
                            <th style="width: 30%;">OI Changes</th>
                        </tr>
                    </thead>
                    <tbody>
                        <!-- Today's history rows will be dynamically added here -->
                    </tbody>
                </table>
            </div>
            <h3>NIFTY Charts</h3>
            <div class="chart-grid">
                <div class="chart-container"><canvas id="nifty-pcr-chart"></canvas></div>
                <div class="chart-container"><canvas id="nifty-max-pain-chart"></canvas></div>
                <div class="chart-container"><canvas id="nifty-ce-oi-chart"></canvas></div>
                <div class="chart-container"><canvas id="nifty-pe-oi-chart"></canvas></div>
            </div>
            <h3>NIFTY Option Chain (ATM ±5)</h3> <!-- Moved below Today's Updates -->
            <div class="table-container">
                <table id="nifty-table">
                    <thead><tr>
                        <th>Strike</th>
                        <th>Call OI</th><th>Call COI</th><th class="action-header">Call Action</th>
                        <th>Put OI</th><th>Put COI</th><th class="action-header">Put Action</th>
                    </tr></thead>
                    <tbody></tbody>
                </table>
            </div>
            <h3 style="margin-top: 20px;">NIFTY Historical Data</h3>
            <div class="history-controls">
                <label for="nifty-history-date">Date:</label>
                <input type="date" id="nifty-history-date" class="history-date-picker">
                <button onclick="loadHistoricalData('NIFTY')">Load History</button>
            </div>
            <div class="table-container">
                <table id="nifty-historical-data-table">
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>SP</th>
                            <th>Value</th>
                            <th>Call COI</th>
                            <th>Put COI</th>
                            <th>PCR</th>
                            <th>Sentiment</th>
                            <th style="width: 30%;">OI Changes</th>
                        </tr>
                    </thead>
                    <tbody>
                        <!-- Historical data rows will be loaded here -->
                    </tbody>
                </table>
            </div>
        </div>
        <div id="FINNIFTY_tab" class="tabcontent">
            <h3 style="margin-top: 20px;">FINNIFTY Today's Updates (appends every 2 min)</h3>
            <div class="table-container">
                <table id="finnifty-todays-history-table">
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>SP</th>
                            <th>Value</th>
                            <th>Call COI</th>
                            <th>Put COI</th>
                            <th>PCR</th>
                            <th>Sentiment</th>
                            <th style="width: 30%;">OI Changes</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
            <h3>FINNIFTY Charts</h3>
            <div class="chart-grid">
                <div class="chart-container"><canvas id="finnifty-pcr-chart"></canvas></div>
                <div class="chart-container"><canvas id="finnifty-max-pain-chart"></canvas></div>
                <div class="chart-container"><canvas id="finnifty-ce-oi-chart"></canvas></div>
                <div class="chart-container"><canvas id="finnifty-pe-oi-chart"></canvas></div>
            </div>
            <h3>FINNIFTY Option Chain (ATM ±5)</h3> <!-- Moved below Today's Updates -->
            <div class="table-container">
                <table id="finnifty-table">
                    <thead><tr>
                        <th>Strike</th>
                        <th>Call OI</th><th>Call COI</th><th class="action-header">Call Action</th>
                        <th>Put OI</th><th>Put COI</th><th class="action-header">Put Action</th>
                    </tr></thead>
                    <tbody></tbody>
                </table>
            </div>
            <h3 style="margin-top: 20px;">FINNIFTY Historical Data</h3>
            <div class="history-controls">
                <label for="finnifty-history-date">Date:</label>
                <input type="date" id="finnifty-history-date" class="history-date-picker">
                <button onclick="loadHistoricalData('FINNIFTY')">Load History</button>
            </div>
            <div class="table-container">
                <table id="finnifty-historical-data-table">
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>SP</th>
                            <th>Value</th>
                            <th>Call COI</th>
                            <th>Put COI</th>
                            <th>PCR</th>
                            <th>Sentiment</th>
                            <th style="width: 30%;">OI Changes</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>
        <div id="BANKNIFTY_tab" class="tabcontent">
            <h3 style="margin-top: 20px;">BANKNIFTY Today's Updates (appends every 2 min)</h3>
            <div class="table-container">
                <table id="banknifty-todays-history-table">
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>SP</th>
                            <th>Value</th>
                            <th>Call COI</th>
                            <th>Put COI</th>
                            <th>PCR</th>
                            <th>Sentiment</th>
                            <th style="width: 30%;">OI Changes</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
            <h3>BANKNIFTY Charts</h3>
            <div class="chart-grid">
                <div class="chart-container"><canvas id="banknifty-pcr-chart"></canvas></div>
                <div class="chart-container"><canvas id="banknifty-max-pain-chart"></canvas></div>
                <div class="chart-container"><canvas id="banknifty-ce-oi-chart"></canvas></div>
                <div class="chart-container"><canvas id="banknifty-pe-oi-chart"></canvas></div>
            </div>
            <h3>BANKNIFTY Option Chain (ATM ±5)</h3> <!-- Moved below Today's Updates -->
            <div class="table-container">
                <table id="banknifty-table">
                    <thead><tr>
                        <th>Strike</th>
                        <th>Call OI</th><th>Call COI</th><th class="action-header">Call Action</th>
                        <th>Put OI</th><th>Put COI</th><th class="action-header">Put Action</th>
                    </tr></thead>
                    <tbody></tbody>
                </table>
            </div>
            <h3 style="margin-top: 20px;">BANKNIFTY Historical Data</h3>
            <div class="history-controls">
                <label for="banknifty-history-date">Date:</label>
                <input type="date" id="banknifty-history-date" class="history-date-picker">
                <button onclick="loadHistoricalData('BANKNIFTY')">Load History</button>
            </div>
            <div class="table-container">
                <table id="banknifty-historical-data-table">
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>SP</th>
                            <th>Value</th>
                            <th>Call COI</th>
                            <th>Put COI</th>
                            <th>PCR</th>
                            <th>Sentiment</th>
                            <th style="width: 30%;">OI Changes</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>
        <div id="SENSEX_tab" class="tabcontent">
            <h3 style="margin-top: 20px;">SENSEX Today's Updates (appends every 2 min)</h3>
            <div class="table-container">
                <table id="sensex-todays-history-table">
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>SP</th>
                            <th>Value</th>
                            <th>Call COI</th>
                            <th>Put COI</th>
                            <th>PCR</th>
                            <th>Sentiment</th>
                            <th style="width: 30%;">OI Changes</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
            <h3>SENSEX Charts</h3>
            <div class="chart-grid">
                <div class="chart-container"><canvas id="sensex-pcr-chart"></canvas></div>
                <div class="chart-container"><canvas id="sensex-max-pain-chart"></canvas></div>
                <div class="chart-container"><canvas id="sensex-ce-oi-chart"></canvas></div>
                <div class="chart-container"><canvas id="sensex-pe-oi-chart"></canvas></div>
            </div>
            <h3>SENSEX Option Chain (ATM ±5)</h3> <!-- Moved below Today's Updates -->
            <div class="table-container">
                <table id="sensex-table">
                    <thead><tr>
                        <th>Strike</th>
                        <th>Call OI</th><th>Call COI</th><th class="action-header">Call Action</th>
                        <th>Put OI</th><th>Put COI</th><th class="action-header">Put Action</th>
                    </tr></thead>
                    <tbody></tbody>
                </table>
            </div>
            <h3 style="margin-top: 20px;">SENSEX Historical Data</h3>
            <div class="history-controls">
                <label for="sensex-history-date">Date:</label>
                <input type="date" id="sensex-history-date" class="history-date-picker">
                <button onclick="loadHistoricalData('SENSEX')">Load History</button>
            </div>
            <div class="table-container">
                <table id="sensex-historical-data-table">
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>SP</th>
                            <th>Value</th>
                            <th>Call COI</th>
                            <th>Put COI</th>
                            <th>PCR</th>
                            <th>Sentiment</th>
                            <th style="width: 30%;">OI Changes</th>
                        </tr>
                    </thead>
                    <tbody>
                        <!-- Historical data rows will be loaded here -->
                    </tbody>
                </table>
            </div>
        </div>
        <div id="INDIAVIX_tab" class="tabcontent"> <!-- NEW INDIAVIX TAB CONTENT -->
            <h3 style="margin-top: 20px;">INDIAVIX Today's Updates (appends every 2 min)</h3>
            <div class="table-container">
                <table id="indiavix-todays-history-table">
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>Value</th>
                            <th>Change</th>
                            <th>Pct Change</th>
                            <th>Sentiment</th>
                            <th style="width: 30%;">Notes</th> <!-- VIX doesn't have OI changes, use Notes -->
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
            <h3 style="margin-top: 20px;">INDIAVIX Historical Data</h3>
            <div class="history-controls">
                <label for="indiavix-history-date">Date:</label>
                <input type="date" id="indiavix-history-date" class="history-date-picker">
                <button onclick="loadHistoricalData('INDIAVIX')">Load History</button>
            </div>
            <div class="table-container">
                <table id="indiavix-historical-data-table">
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>Value</th>
                            <th>Change</th>
                            <th>Pct Change</th>
                            <th>Sentiment</th>
                            <th style="width: 30%;">Notes</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
        </div> <!-- END NEW INDIAVIX TAB CONTENT -->
        <footer>@aditya_nse_bot | Updated: <span id="time">-</span></footer>
    </div>
    <script>
        const socket = io();
        // Store chart instances globally to easily update them
        const charts = {};

        socket.on('connect', () => {
            console.log('Socket.IO: Connected to server!');
        });
        socket.on('disconnect', () => {
            console.warn('Socket.IO: Disconnected from server!');
        });
        socket.on('connect_error', (error) => {
            console.error('Socket.IO: Connection error:', error);
        });
        socket.on('error', (error) => {
            console.error('Socket.IO: Generic error:', error);
        });

        // Function to render or update a Chart.js chart
        function renderChart(chartId, chartType, labels, data, label, backgroundColor, borderColor, yAxisLabel, options = {}) {
            const ctx = document.getElementById(chartId);
            if (!ctx) {
                console.error(`Canvas element for chartId '${chartId}' not found.`);
                return;
            }

            // Destroy existing chart instance to prevent memory leaks and ensure proper re-render
            if (charts[chartId]) {
                charts[chartId].destroy();
            }

            // Create new chart
            charts[chartId] = new Chart(ctx, {
                type: chartType,
                data: {
                    labels: labels,
                    datasets: [{
                        label: label,
                        data: data,
                        backgroundColor: backgroundColor,
                        borderColor: borderColor,
                        borderWidth: 1,
                        fill: chartType === 'line' ? false : true,
                        tension: chartType === 'line' ? 0.4 : 0,
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            display: true,
                            labels: {
                                color: '#bbb'
                            }
                        }
                    },
                    scales: {
                        x: {
                            ticks: { color: '#bbb' },
                            grid: { color: 'rgba(255,255,255,0.1)' }
                        },
                        y: {
                            beginAtZero: true,
                            ticks: { color: '#bbb' },
                            grid: { color: 'rgba(255,255,255,0.1)' },
                            title: {
                                display: true,
                                text: yAxisLabel,
                                color: '#fff'
                            }
                        }
                    },
                    ...options // Merge additional options
                }
            });
        }

        function updateCharts(symbol, chartData) {
            const lower = symbol.toLowerCase();
            // Destroy existing charts for this symbol before re-rendering
            // This is crucial when switching tabs to ensure charts are properly drawn
            ['pcr-chart', 'max-pain-chart', 'ce-oi-chart', 'pe-oi-chart'].forEach(chartSuffix => {
                const chartId = `${lower}-${chartSuffix}`;
                if (charts[chartId]) {
                    charts[chartId].destroy();
                    delete charts[chartId]; // Remove from cache
                }
            });


            if (chartData.pcr_chart_data && chartData.pcr_chart_data.length > 0) {
                const labels = chartData.pcr_chart_data.map(item => item.TIME);
                const values = chartData.pcr_chart_data.map(item => item.PCR);
                renderChart(`${lower}-pcr-chart`, 'line', labels, values, 'PCR', 'rgba(75, 192, 192, 0.6)', 'rgba(75, 192, 192, 1)', 'PCR Value', {
                    scales: { y: { min: 0, max: 2.0 } } // PCR typically between 0 and 2
                });
            } else {
                // Clear PCR chart if no data
                 const chartId = `${lower}-pcr-chart`;
                 if (charts[chartId]) {
                     charts[chartId].destroy();
                     delete charts[chartId];
                 }
            }

            if (chartData.max_pain_chart_data && chartData.max_pain_chart_data.length > 0) {
                const labels = chartData.max_pain_chart_data.map(item => item.StrikePrice);
                const values = chartData.max_pain_chart_data.map(item => item.TotalMaxPain);
                renderChart(`${lower}-max-pain-chart`, 'bar', labels, values, 'Max Pain', 'rgba(255, 159, 64, 0.6)', 'rgba(255, 159, 64, 1)', 'Total Max Pain');
            } else {
                // Clear Max Pain chart if no data
                const chartId = `${lower}-max-pain-chart`;
                if (charts[chartId]) {
                    charts[chartId].destroy();
                    delete charts[chartId];
                }
            }

            if (chartData.ce_oi_chart_data && chartData.ce_oi_chart_data.length > 0) {
                const labels = chartData.ce_oi_chart_data.map(item => item.strikePrice);
                const values = chartData.ce_oi_chart_data.map(item => item.openInterest);
                renderChart(`${lower}-ce-oi-chart`, 'bar', labels, values, 'CE OI', 'rgba(255, 99, 132, 0.6)', 'rgba(255, 99, 132, 1)', 'Open Interest (CE)');
            } else {
                // Clear CE OI chart if no data
                const chartId = `${lower}-ce-oi-chart`;
                if (charts[chartId]) {
                    charts[chartId].destroy();
                    delete charts[chartId];
                }
            }

            if (chartData.pe_oi_chart_data && chartData.pe_oi_chart_data.length > 0) {
                const labels = chartData.pe_oi_chart_data.map(item => item.strikePrice);
                const values = chartData.pe_oi_chart_data.map(item => item.openInterest);
                renderChart(`${lower}-pe-oi-chart`, 'bar', labels, values, 'PE OI', 'rgba(54, 162, 235, 0.6)', 'rgba(54, 162, 235, 1)', 'Open Interest (PE)');
            } else {
                // Clear PE OI chart if no data
                const chartId = `${lower}-pe-oi-chart`;
                if (charts[chartId]) {
                    charts[chartId].destroy();
                    delete charts[chartId];
                }
            }
        }


        // Function to append a single history item to the correct "Today's Updates" table
        function appendTodaysHistoryItem(symbol, item, prepend = true, isVix = false) { // Added isVix flag
            const lower = symbol.toLowerCase();
            const historyTbodySelector = `#${lower}-todays-history-table tbody`;
            const historyTbody = document.querySelector(historyTbodySelector);
            if (!historyTbody) {
                console.error(`appendTodaysHistoryItem ERROR: History tbody NOT FOUND for selector: '${historyTbodySelector}'.`);
                return;
            }
            const tr = document.createElement('tr');
            if (isVix) {
                 tr.innerHTML = `
                    <td>${item.time}</td>
                    <td>${item.value}</td>
                    <td style="color: ${item.sp >= 0 ? '#4CAF50' : '#F44336'}">${item.sp >= 0 ? '+' : ''}${item.sp}</td> <!-- item.sp is Change for VIX -->
                    <td style="color: ${item.pcr >= 0 ? '#4CAF50' : '#F44336'}">${item.pcr >= 0 ? '+' : ''}${item.pcr}%</td> <!-- item.pcr is Pct Change for VIX -->
                    <td style="color: ${item.sentiment.includes('Bullish') ? '#4CAF50' : item.sentiment.includes('Bearish') ? '#F44336' : '#FFC107'}">
                        ${item.sentiment}
                    </td>
                    <td>${item.add_exit || 'N/A'}</td> <!-- Use add_exit for general notes/N/A -->
                `;
            } else {
                tr.innerHTML = `
                    <td>${item.time}</td>
                    <td>${item.sp}</td>
                    <td>${item.value}</td>
                    <td>${item.call_oi}${item.call_oi !== 0 ? 'K' : ''}</td>
                    <td>${item.put_oi}${item.put_oi !== 0 ? 'K' : ''}</td>
                    <td>${item.pcr}</td>
                    <td style="color: ${item.sentiment.includes('Bullish') ? '#4CAF50' : item.sentiment.includes('Bearish') ? '#F44336' : '#FFC107'}">
                        ${item.sentiment}
                    </td>
                    <td>${item.add_exit}</td>
                `;
            }
            if (prepend) { // Add to top
                historyTbody.prepend(tr);
            } else { // Add to bottom
                historyTbody.appendChild(tr);
            }
            const historyTableContainer = historyTbody.closest('.table-container');
            if (historyTableContainer) {
                if (prepend) {
                    historyTableContainer.scrollTop = 0;
                } else {
                    historyTableContainer.scrollTop = historyTableContainer.scrollHeight;
                }
            }
            // console.log(`Today's history appended for ${symbol}:`, item); // Too verbose for frequent updates
        }
        // Function to load and display historical data for a specific date
        async function loadHistoricalData(symbol) {
            const lower = symbol.toLowerCase();
            const dateInput = document.getElementById(`${lower}-history-date`);
            const selectedDate = dateInput.value; // Format: YYYY-MM-DD
            if (!selectedDate) {
                alert("Please select a date to load historical data.");
                return;
            }
            const historicalTbodySelector = `#${lower}-historical-data-table tbody`;
            const historicalTbody = document.querySelector(historicalTbodySelector);
            if (!historicalTbody) {
                console.error(`loadHistoricalData ERROR: Historical tbody NOT FOUND for selector: '${historicalTbodySelector}'.`);
                return;
            }
            historicalTbody.innerHTML = '<tr><td colspan="8">Loading...</td></tr>'; // Default colspan for 8 columns
            try {
                const response = await fetch(`/history/${symbol}/${selectedDate}`);
                const data = await response.json();
                historicalTbody.innerHTML = '';
                if (data.error) {
                    // Adjust colspan for VIX if an error occurs
                    const colspan = (symbol === 'INDIAVIX') ? 6 : 8;
                    historicalTbody.innerHTML = `<tr><td colspan="${colspan}" style="color: red;">Error: ${data.error}</td></tr>`;
                    console.error("Error loading historical data:", data.error);
                    return;
                }
                if (data.history.length === 0) {
                    // Adjust colspan for VIX if no data
                    const colspan = (symbol === 'INDIAVIX') ? 6 : 8;
                    historicalTbody.innerHTML = `<tr><td colspan="${colspan}">No data found for this date.</td></tr>`;
                    return;
                }
                const isVix = (symbol === 'INDIAVIX');
                // Backend sends data ORDER BY timestamp DESC, so iterate forwards and append to DOM
                data.history.forEach(item => {
                    const tr = document.createElement('tr');
                    if (isVix) {
                        tr.innerHTML = `
                            <td>${item.time}</td>
                            <td>${item.value}</td>
                            <td style="color: ${item.sp >= 0 ? '#4CAF50' : '#F44336'}">${item.sp >= 0 ? '+' : ''}${item.sp}</td>
                            <td style="color: ${item.pcr >= 0 ? '#4CAF50' : '#F44336'}">${item.pcr >= 0 ? '+' : ''}${item.pcr}%</td>
                            <td style="color: ${item.sentiment.includes('Bullish') ? '#4CAF50' : item.sentiment.includes('Bearish') ? '#F44336' : '#FFC107'}">
                                ${item.sentiment}
                            </td>
                            <td>${item.add_exit || 'N/A'}</td>
                        `;
                    } else {
                        tr.innerHTML = `
                            <td>${item.time}</td>
                            <td>${item.sp}</td>
                            <td>${item.value}</td>
                            <td>${item.call_oi}${item.call_oi !== 0 ? 'K' : ''}</td>
                            <td>${item.put_oi}${item.put_oi !== 0 ? 'K' : ''}</td>
                            <td>${item.pcr}</td>
                            <td style="color: ${item.sentiment.includes('Bullish') ? '#4CAF50' : item.sentiment.includes('Bearish') ? '#F44336' : '#FFC107'}">
                                ${item.sentiment}
                            </td>
                            <td>${item.add_exit}</td>
                        `;
                    }
                    historicalTbody.appendChild(tr); // Append to show latest first
                });
                console.log(`Historical data loaded for ${symbol} on ${selectedDate}.`);
            } catch (error) {
                // Adjust colspan for VIX if a fetch error
                const colspan = (symbol === 'INDIAVIX') ? 6 : 8;
                historicalTbody.innerHTML = `<tr><td colspan="${colspan}" style="color: red;">Failed to load history.</td></tr>`;
                console.error("Fetch error loading historical data:", error);
            }
        }

        // Modified openTab function
        function openTab(evt, symbol) { // Now expects symbol directly
            console.log("--- openTab called ---");
            console.log("openTab: Target symbol:", symbol);

            document.querySelectorAll(".tabcontent").forEach(el => {
                el.style.display = "none";
                el.style.border = "";
                el.style.boxShadow = "";
            });
            document.querySelectorAll(".tablink").forEach(el => el.classList.remove("active"));

            const targetTabElement = document.getElementById(`${symbol}_tab`); // Construct ID using symbol
            if (targetTabElement) {
                console.log(`openTab: Found target tab element '${symbol}_tab'. Setting display to 'block'.`);
                targetTabElement.style.display = "block";

                // Update charts for the newly active tab if data is in cache and it's not INDIAVIX
                if (symbol !== 'INDIAVIX' && shared_data_cache[symbol] && shared_data_cache[symbol].chart_data) {
                    console.log(`openTab: Rendering charts for ${symbol} from cache.`);
                    updateCharts(symbol, shared_data_cache[symbol].chart_data);
                } else if (symbol !== 'INDIAVIX') { // Not VIX, but no chart data yet
                    console.log(`openTab: No chart data in cache for ${symbol} yet. Clearing chart canvases.`);
                    // Clear chart canvases if no data is found for the symbol, for non-VIX symbols
                    const lower = symbol.toLowerCase();
                    ['pcr-chart', 'max-pain-chart', 'ce-oi-chart', 'pe-oi-chart'].forEach(chartSuffix => {
                        const chartId = `${lower}-${chartSuffix}`;
                        if (charts[chartId]) {
                            charts[chartId].destroy();
                            delete charts[chartId];
                        }
                    });
                }
            } else {
                console.error(`openTab ERROR: Target tab element '${symbol}_tab' not found.`);
            }
            evt.currentTarget.classList.add("active");
            console.log("--- openTab finished ---");
        }

        // Cache for shared data to ensure charts can be rendered when tabs are switched
        const shared_data_cache = {};

        // Handler for initial "today's" history load (on connect)
        socket.on('initial_todays_history', (data) => {
            console.log("INITIAL TODAY'S HISTORY RECEIVED:", data.history);
            if (!data.history) return;
            Object.keys(data.history).forEach(sym => {
                const lower = sym.toLowerCase();
                const historyTbodySelector = `#${lower}-todays-history-table tbody`;
                const historyTbody = document.querySelector(historyTbodySelector);
                if (historyTbody) {
                    historyTbody.innerHTML = ''; // Clear before populating
                    const isVix = (sym === 'INDIAVIX');
                    // Backend sends todays_history in ASC order (oldest first).
                    // Iterate forwards, but prepend to DOM to show latest first.
                    data.history[sym].slice().reverse().forEach(item => { // Reverse to prepend correctly
                        appendTodaysHistoryItem(sym, item, true, isVix); // Prepend each item
                    });
                } else {
                    console.error(`INITIAL HISTORY ERROR: Today's History tbody NOT FOUND for selector: '${historyTbodySelector}'.`);
                }
            });
        });
        // Handler for live updates (summary cards, option chain tables, AND live feed) - every 15 seconds
        socket.on('update', (data) => {
            // console.log("--- LIVE UPDATE RECEIVED (15s) ---"); // Too verbose
            // console.log("LIVE UPDATE: Raw data:", data); // Uncomment for verbose logging
            if (!data.live || Object.keys(data.live).length === 0) {
                console.warn("LIVE UPDATE: Received empty or invalid data.live object, skipping update.");
                return;
            }
            // UPDATE LIVE INDEX FEED
            if (data.live_feed_summary) {
                Object.keys(data.live_feed_summary).forEach(sym => {
                    const feed = data.live_feed_summary[sym];
                    const lower = sym.toLowerCase();
                    const valueElement = document.getElementById(`feed-${lower}-value`);
                    const changeElement = document.getElementById(`feed-${lower}-change`);
                    const pctElement = document.getElementById(`feed-${lower}-pct`);
                    if (valueElement) valueElement.textContent = feed.current_value;
                    if (changeElement) {
                        changeElement.textContent = `${feed.change >= 0 ? '+' : ''}${feed.change}`;
                        changeElement.style.color = feed.change >= 0 ? '#4CAF50' : '#F44336'; // Green for positive, Red for negative
                    }
                    if (pctElement) {
                        pctElement.textContent = `(${feed.percentage_change >= 0 ? '+' : ''}${feed.percentage_change}%)`;
                        pctElement.style.color = feed.percentage_change >= 0 ? '#4CAF50' : '#F44336'; // Green for positive, Red for negative
                    }
                });
            }
            // UPDATE SUMMARY CARDS
            Object.keys(data.live).forEach(sym => {
                const s = data.live[sym].summary;
                const lower = sym.toLowerCase();
                if (!s) {
                    console.warn(`LIVE UPDATE: Summary data is missing for symbol: ${sym}, skipping card update.`);
                    return;
                }
                // Special handling for INDIAVIX summary card
                if (sym === 'INDIAVIX') {
                    const vixValueElement = document.getElementById(`${lower}-value`);
                    const vixChangeElement = document.getElementById(`${lower}-change`);
                    const vixPctElement = document.getElementById(`${lower}-pct`);
                    const vixSentimentElement = document.getElementById(`${lower}-sentiment`);
                    const vixNotesElement = document.getElementById(`${lower}-add-exit`); // Renamed from add-exit to notes for VIX
                    if (vixValueElement) vixValueElement.textContent = s.value; // Use s.value for VIX value
                    // For VIX, use the live_feed_summary for change/pct change in the card
                    if (data.live_feed_summary && data.live_feed_summary[sym]) {
                         const feed = data.live_feed_summary[sym];
                         if (vixChangeElement) {
                            vixChangeElement.textContent = `${feed.change >= 0 ? '+' : ''}${feed.change}`;
                            vixChangeElement.style.color = feed.change >= 0 ? '#4CAF50' : '#F44336';
                         }
                         if (vixPctElement) {
                            vixPctElement.textContent = `${feed.percentage_change >= 0 ? '+' : ''}${feed.percentage_change}`;
                            vixPctElement.style.color = feed.percentage_change >= 0 ? '#4CAF50' : '#F44336';
                         }
                    }
                    if (vixSentimentElement) {
                        vixSentimentElement.textContent = s.sentiment;
                        vixSentimentElement.style.color = s.sentiment.includes('Bullish') ? '#4CAF50' :
                                                          s.sentiment.includes('Bearish') ? '#F44336' : '#FFC107';
                    }
                    if (vixNotesElement) vixNotesElement.textContent = s.add_exit; // Should be N/A for VIX
                } else {
                    // Existing logic for other symbols
                    const summaryUpdates = {
                        'sp': 'sp',
                        'value': 'value',
                        'call': 'call_oi',
                        'put': 'put_oi',
                        'pcr': 'pcr',
                        'sentiment': 'sentiment',
                        'add-exit': 'add_exit'
                    };
                    for (const idSuffix in summaryUpdates) {
                        const dataKey = summaryUpdates[idSuffix];
                        const elementId = `${lower}-${idSuffix}`;
                        const element = document.getElementById(elementId);
                        if (element) {
                            if (idSuffix === 'sentiment') {
                                element.textContent = s.sentiment;
                                element.style.color = s.sentiment.includes('Bullish') ? '#4CAF50' :
                                                      s.sentiment.includes('Bearish') ? '#F44336' : '#FFC107';
                            } else {
                                element.textContent = s[dataKey];
                            }
                        } else {
                            console.error(`LIVE UPDATE ERROR: Summary element NOT FOUND: '${elementId}'.`);
                        }
                    }
                }
            });
            // UPDATE OPTION CHAIN TABLES (only for non-VIX symbols)
            Object.keys(data.live).forEach(sym => {
                if (sym === 'INDIAVIX') return; // Skip VIX, it has no option chain table
                const s = data.live[sym].summary;
                const lower = sym.toLowerCase();
                const tbodySelector = `#${lower}-table tbody`;
                const tbody = document.querySelector(tbodySelector);
                if (!tbody) {
                    console.error(`LIVE UPDATE ERROR: Table tbody NOT FOUND for selector: '${tbodySelector}'.`);
                    return;
                }
                if (!data.live[sym].strikes) {
                    console.warn(`LIVE UPDATE: Table: Strikes data missing for ${sym}, skipping table update.`);
                    return;
                }
                tbody.innerHTML = '';
                data.live[sym].strikes.forEach(row => {
                    const isAtm = row.is_atm;
                    const tr = document.createElement('tr');
                    if (isAtm) tr.style.backgroundColor = '#2a2a2a';
                    tr.innerHTML = `
                        <td style="font-weight: ${isAtm ? 'bold' : 'normal'}; color: ${isAtm ? '#4CAF50' : '#fff'}">
                            ${row.strike}
                        </td>
                        <td>${row.call_oi.toLocaleString()}</td>
                        <td style="color: ${row.call_coi > 0 ? '#4CAF50' : row.call_coi < 0 ? '#F44336' : '#fff'}; font-weight: bold">
                            ${row.call_coi >= 0 ? '+' : ''}${row.call_coi}
                        </td>
                        <td class="action-cell" style="color: ${row.call_action === 'ADD' ? '#4CAF50' : '#F44336'}; font-weight: bold">
                            ${row.call_action}
                        </td>
                        <td>${row.put_oi.toLocaleString()}</td>
                        <td style="color: ${row.put_coi > 0 ? '#4CAF50' : row.put_coi < 0 ? '#F44336' : '#fff'}; font-weight: bold">
                            ${row.put_coi >= 0 ? '+' : ''}${row.put_coi}
                        </td>
                        <td class="action-cell" style="color: ${row.put_action === 'ADD' ? '#4CAF50' : '#F44336'}; font-weight: bold">
                            ${row.put_action}
                        </td>
                    `;
                    tbody.appendChild(tr);
                });
            });

            // Update Charts if the tab is currently active
            Object.keys(data.live).forEach(sym => {
                if (sym === 'INDIAVIX') return; // VIX has no charts

                // Store chart data in cache
                // Ensure chart_data is always defined, even if empty
                const chartDataForSym = {
                    pcr_chart_data: data.live[sym].pcr_chart_data || [],
                    max_pain_chart_data: data.live[sym].max_pain_chart_data || [],
                    ce_oi_chart_data: data.live[sym].ce_oi_chart_data || [],
                    pe_oi_chart_data: data.live[sym].pe_oi_chart_data || []
                };
                shared_data_cache[sym] = shared_data_cache[sym] || {};
                shared_data_cache[sym].chart_data = chartDataForSym;

                const currentActiveTabButton = document.querySelector('.tablink.active');
                if (currentActiveTabButton && currentActiveTabButton.dataset.symbol === sym) {
                    console.log(`LIVE UPDATE: Rendering charts for active tab ${sym}.`);
                    updateCharts(sym, chartDataForSym);
                }
            });

            const timeElement = document.getElementById('time');
            if (timeElement) {
                timeElement.textContent = new Date().toLocaleTimeString();
            } else {
                console.error("LIVE UPDATE ERROR: Footer time element NOT FOUND: 'time'.");
            }
            // console.log("--- LIVE UPDATE FINISHED ---"); // Too verbose
        });
        // NEW: Handler for "Today's Updates" history append - every 2 minutes
        socket.on('todays_history_append', (data) => {
            // console.log("--- TODAY'S HISTORY APPEND RECEIVED (2min) ---"); // Too verbose
            // console.log(`TODAY'S HISTORY APPEND: New item for ${data.symbol}:`, data.item); // Too verbose
            if (data.symbol && data.item) {
                const isVix = (data.symbol === 'INDIAVIX');
                appendTodaysHistoryItem(data.symbol, data.item, true, isVix); // Prepend new item, pass isVix
            } else {
                console.warn("TODAY'S HISTORY APPEND: Received invalid data.");
            }
            // console.log("--- TODAY'S HISTORY APPEND FINISHED ---"); // Too verbose
        });
        // Set today's date as default for date pickers
        document.addEventListener('DOMContentLoaded', () => {
            const today = new Date();
            const yyyy = today.getFullYear();
            const mm = String(today.getMonth() + 1).padStart(2, '0'); // Months are 0-indexed
            const dd = String(today.getDate()).padStart(2, '0');
            const todayStr = `${yyyy}-${mm}-${dd}`;
            document.querySelectorAll('.history-date-picker').forEach(input => {
                input.value = todayStr;
            });
            // Trigger the click on the default open tab button to render its charts
            const defaultOpenButton = document.getElementById("NIFTYOpen"); // Assuming NIFTY is default
            if (defaultOpenButton) {
                defaultOpenButton.click();
            } else {
                console.error("Default open tab button not found.");
            }
        });
    </script>
</body>
</html>"""
    with open('dashboard.html', 'w') as f:
        f.write(html)


def create_css():
    os.makedirs('static', exist_ok=True)
    css = """* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: 'Segoe UI', sans-serif; background: #121212; color: #fff; }
.container { max-width: 1400px; margin: 20px auto; padding: 20px; }
h1 { text-align: center; margin-bottom: 20px; color: #4CAF50; }
/* NEW LIVE FEED STYLES */
.live-feed-container {
    display: flex;
    justify-content: center; /* Center items */
    flex-wrap: wrap;
    background: #1e1e1e;
    padding: 15px;
    border-radius: 12px;
    margin-bottom: 20px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
}
.live-feed-item {
    display: flex;
    align-items: baseline;
    font-size: 1.1em;
    font-weight: bold;
    margin: 5px 15px;
}
.feed-symbol {
    color: #2196F3; /* Blue */
    margin-right: 5px;
}
.feed-value {
    color: #fff;
    margin-right: 8px;
}
.feed-change {
    margin-right: 5px;
}
.feed-pct {
    font-size: 0.9em;
}
/* END NEW LIVE FEED STYLES */
.grid {
    display: grid;
    /* Adjusted for 5 columns */
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); /* Min width adjusted for 5 cards */
    gap: 20px;
}
.card { background: #1e1e1e; border-radius: 12px; padding: 20px; box-shadow: 0 4px 12px rgba(0,0,0,0.3); }
.card h2 { margin-bottom: 15px; color: #2196F3; }
.card p { margin: 8px 0; font-size: 14px; }
.tabs { margin: 30px 0; text-align: center; }
.tablink { background: #1e1e1e; color: #fff; padding: 12px 24px; margin: 0 8px; border: none; border-radius: 8px; cursor: pointer; font-size: 15px; transition: 0.3s; }
.tablink:hover { background: #333; }
.tablink.active { background: #4CAF50; }
.tabcontent { display: none; padding: 25px; background: #1e1e1e; border-radius: 12px; margin-bottom: 20px; }
.tabcontent h3 { margin-bottom: 15px; color: #2196F3; }
.table-container { max-height: 500px; overflow-y: auto; border: 1px solid #333; border-radius: 8px; margin-bottom: 20px; } /* Added margin-bottom */
.history-controls {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 15px;
    background: #1e1e1e;
    padding: 10px;
    border-radius: 8px;
    box-shadow: 0 2px 5px rgba(0,0,0,0.2);
}
.history-controls label {
    font-weight: bold;
    color: #fff;
}
.history-controls input[type="date"] {
    padding: 8px;
    border: 1px solid #333;
    border-radius: 4px;
    background: #2d2d2d;
    color: #fff;
    font-family: 'Segoe UI', sans-serif;
    outline: none;
}
.history-controls button {
    padding: 8px 15px;
    background-color: #007bff; /* Blue load button */
    color: white;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    font-size: 14px;
    transition: background-color 0.2s;
}
.history-controls button:hover {
    background-color: #0056b3;
}
table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
    table-layout: fixed; /* Ensures columns respect widths */
}
th, td {
    padding: 10px;
    text-align: center;
    border-bottom: 1px solid #333;
    /* Adjusted width for fewer columns */
    width: auto;
    vertical-align: top;
}
/* Default Option Chain Table Column Widths */
#nifty-table th:nth-child(1), #finnifty-table th:nth-child(1), #banknifty-table th:nth-child(1), #sensex-table th:nth-child(1) { width: 12%; } /* Strike */
#nifty-table th:nth-child(2), #finnifty-table th:nth-child(2), #banknifty-table th:nth-child(2), #sensex-table th:nth-child(2) { width: 14%; } /* Call OI */
#nifty-table th:nth-child(3), #finnifty-table th:nth-child(3), #banknifty-table th:nth-child(3), #sensex-table th:nth-child(3) { width: 14%; } /* Call COI */
#nifty-table th:nth-child(4), #finnifty-table th:nth-child(4), #banknifty-table th:nth-child(4), #sensex-table th:nth-child(4) { width: 12%; } /* Call Action */
#nifty-table th:nth-child(5), #finnifty-table th:nth-child(5), #banknifty-table th:nth-child(5), #sensex-table th:nth-child(5) { width: 14%; } /* Put OI */
#nifty-table th:nth-child(6), #finnifty-table th:nth-child(6), #banknifty-table th:nth-child(6), #sensex-table th:nth-child(6) { width: 14%; } /* Put COI */
#nifty-table th:nth-child(7), #finnifty-table th:nth-child(7), #banknifty-table th:nth-child(7), #sensex-table th:nth-child(7) { width: 12%; } /* Put Action */


/* General styles for history table headers (Non-VIX) - 8 columns */
#nifty-todays-history-table th:nth-child(1), #nifty-historical-data-table th:nth-child(1),
#finnifty-todays-history-table th:nth-child(1), #finnifty-historical-data-table th:nth-child(1),
#banknifty-todays-history-table th:nth-child(1), #banknifty-historical-data-table th:nth-child(1),
#sensex-todays-history-table th:nth-child(1), #sensex-historical-data-table th:nth-child(1) { width: 8%; } /* Time */

#nifty-todays-history-table th:nth-child(2), #nifty-historical-data-table th:nth-child(2),
#finnifty-todays-history-table th:nth-child(2), #finnifty-historical-data-table th:nth-child(2),
#banknifty-todays-history-table th:nth-child(2), #banknifty-historical-data-table th:nth-child(2),
#sensex-todays-history-table th:nth-child(2), #sensex-historical-data-table th:nth-child(2) { width: 8%; } /* SP */

#nifty-todays-history-table th:nth-child(3), #nifty-historical-data-table th:nth-child(3),
#finnifty-todays-history-table th:nth-child(3), #finnifty-historical-data-table th:nth-child(3),
#banknifty-todays-history-table th:nth-child(3), #banknifty-historical-data-table th:nth-child(3),
#sensex-todays-history-table th:nth-child(3), #sensex-historical-data-table th:nth-child(3) { width: 8%; } /* Value */

#nifty-todays-history-table th:nth-child(4), #nifty-historical-data-table th:nth-child(4),
#finnifty-todays-history-table th:nth-child(4), #finnifty-historical-data-table th:nth-child(4),
#banknifty-todays-history-table th:nth-child(4), #banknifty-historical-data-table th:nth-child(4),
#sensex-todays-history-table th:nth-child(4), #sensex-historical-data-table th:nth-child(4) { width: 10%; } /* Call COI */

#nifty-todays-history-table th:nth-child(5), #nifty-historical-data-table th:nth-child(5),
#finnifty-todays-history-table th:nth-child(5), #finnifty-historical-data-table th:nth-child(5),
#banknifty-todays-history-table th:nth-child(5), #banknifty-historical-data-table th:nth-child(5),
#sensex-todays-history-table th:nth-child(5), #sensex-historical-data-table th:nth-child(5) { width: 10%; } /* Put COI */

#nifty-todays-history-table th:nth-child(6), #nifty-historical-data-table th:nth-child(6),
#finnifty-todays-history-table th:nth-child(6), #finnifty-historical-data-table th:nth-child(6),
#banknifty-todays-history-table th:nth-child(6), #banknifty-historical-data-table th:nth-child(6),
#sensex-todays-history-table th:nth-child(6), #sensex-historical-data-table th:nth-child(6) { width: 7%; } /* PCR */

#nifty-todays-history-table th:nth-child(7), #nifty-historical-data-table th:nth-child(7),
#finnifty-todays-history-table th:nth-child(7), #finnifty-historical-data-table th:nth-child(7),
#banknifty-todays-history-table th:nth-child(7), #banknifty-historical-data-table th:nth-child(7),
#sensex-todays-history-table th:nth-child(7), #sensex-historical-data-table th:nth-child(7) { width: 15%; } /* Sentiment */

#nifty-todays-history-table th:nth-child(8), #nifty-historical-data-table th:nth-child(8),
#finnifty-todays-history-table th:nth-child(8), #finnifty-historical-data-table th:nth-child(8),
#banknifty-todays-history-table th:nth-child(8), #banknifty-historical-data-table th:nth-child(8),
#sensex-todays-history-table th:nth-child(8), #sensex-historical-data-table th:nth-child(8) { width: 34%; } /* OI Changes */


/* Specific overrides for VIX history tables as they have fewer columns (6 columns) */
#indiavix-todays-history-table th:nth-child(1), #indiavix-historical-data-table th:nth-child(1) { width: 10%; } /* Time */
#indiavix-todays-history-table th:nth-child(2), #indiavix-historical-data-table th:nth-child(2) { width: 12%; } /* Value */
#indiavix-todays-history-table th:nth-child(3), #indiavix-historical-data-table th:nth-child(3) { width: 12%; } /* Change */
#indiavix-todays-history-table th:nth-child(4), #indiavix-historical-data-table th:nth-child(4) { width: 12%; } /* Pct Change */
#indiavix-todays-history-table th:nth-child(5), #indiavix-historical-data-table th:nth-child(5) { width: 15%; } /* Sentiment */
#indiavix-todays-history-table th:nth-child(6), #indiavix-historical-data-table th:nth-child(6) { width: 39%; } /* Notes */
/* Styles for action cells to prevent wrapping and ensure visibility */
.action-cell {
    white-space: nowrap;
    overflow: hidden; /* Hide overflow if text is still too long */
    text-overflow: ellipsis; /* Show ellipsis if text is cut off */
}
/* The action-header class can still be used for specific styling if needed,
   but individual nth-child selectors are more precise for fixed layout */
.action-header {
    /* width: 8%; */ /* Already handled by nth-child, keep if you want to override */
}

/* NEW Chart Grid Layout */
.chart-grid {
    display: grid;
    /* Adjusted for larger charts */
    grid-template-columns: repeat(auto-fit, minmax(450px, 1fr)); /* Allow wider charts */
    gap: 20px;
    margin-bottom: 20px;
}

.chart-container {
    background: #1e1e1e;
    padding: 15px;
    border-radius: 12px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    height: 450px; /* Increased height for charts */
}


footer { text-align: center; margin-top: 30px; font-size: 12px; color: #888; }"""
    with open('static/style.css', 'w') as f:
        f.write(css)


# --------------------------------------------------------------------------- #
# MAIN
# --------------------------------------------------------------------------- #
if __name__ == '__main__':
    create_html()
    create_css()
    analyzer = NseBseAnalyzer()
    print("WEB DASHBOARD LIVE → http://127.0.0.1:5000")
    print("SENSEX: Add DhanHQ keys for real data")
    socketio.run(app, host='0.0.0.0', port=5000)