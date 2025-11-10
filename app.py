#!/usr/bin/env python3
# -- coding: utf-8 --
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
import pytz
from zipfile import ZipFile
from io import BytesIO

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning)

# --------------------------------------------------------------------------- #
# CONFIG
# --------------------------------------------------------------------------- #
TELEGRAM_BOT_TOKEN = "8081075640:AAEDdrUSdg8BRX4CcKJ4W7jG1EOnSFTQPr4"
TELEGRAM_CHAT_ID = "275529219"
SEND_TEXT_UPDATES = True
UPDATE_INTERVAL = 120
MAX_HISTORY_ROWS_DB = 10000
LIVE_DATA_INTERVAL = 15
EQUITY_FETCH_INTERVAL = 300  # 5 minutes

AUTO_SYMBOLS = ["NIFTY", "FINNIFTY", "BANKNIFTY", "SENSEX", "INDIAVIX", "GOLD", "SILVER", "BTC-USD", "USD-INR"]

# --------------------------------------------------------------------------- #
# WEB DASHBOARD & GLOBAL VARS
# --------------------------------------------------------------------------- #
app = Flask(__name__, template_folder='.', static_folder='static')
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')  # Changed cors_allowed_origins to "*"
shared_data: Dict[str, Dict[str, Any]] = {}
todays_history: Dict[str, List[Dict[str, Any]]] = {sym: [] for sym in AUTO_SYMBOLS}
data_lock = threading.Lock()
last_alert: Dict[str, float] = {sym: 0 for sym in AUTO_SYMBOLS}
last_history_update: Dict[str, float] = {sym: 0 for sym in AUTO_SYMBOLS}
initial_underlying_values: Dict[str, Optional[float]] = {sym: None for sym in AUTO_SYMBOLS}
site_visits = 0

fno_stocks_list = []
equity_data_cache: Dict[str, Dict[str, Any]] = {}
previous_improving_list = set()
previous_worsening_list = set()
latest_bhavcopy_data = {}


@app.route('/')
def index(): return render_template('dashboard.html')


@app.route('/api/stocks')
def get_stocks():
    return jsonify(fno_stocks_list)


@app.route('/api/bhavcopy')
def get_bhavcopy_data():
    return jsonify(latest_bhavcopy_data)


# +++ NEW: API endpoint to get historical Bhavcopy data for a specific date +++
@app.route('/api/bhavcopy/<date_str>')
def get_historical_bhavcopy(date_str: str):
    if not analyzer.conn:
        return jsonify({"error": "Database connection not available."}), 500
    try:
        # Validate date format
        datetime.datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date format. Please use YYYY-MM-DD."}), 400

    try:
        cur = analyzer.conn.cursor()
        cur.execute(
            "SELECT symbol, close, volume, pct_change, delivery_pct, open, high, low FROM bhavcopy_data WHERE date = ?",
            (date_str,)
        )
        rows = cur.fetchall()

        if not rows:
            return jsonify({"error": f"No Bhavcopy data found for {date_str}."}), 404

        equities = []
        for row in rows:
            equities.append({
                "Symbol": row[0],
                "Close": row[1],
                "Volume": row[2],
                "% Change": row[3],
                "Delivery %": row[4],
                "Open": row[5],
                "High": row[6],
                "Low": row[7]
            })

        return jsonify({"equities": equities, "date": date_str})

    except Exception as e:
        print(f"Error fetching historical bhavcopy for {date_str}: {e}")
        return jsonify({"error": "An internal error occurred while fetching data."}), 500


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


@socketio.on('fetch_equity_data')
def handle_fetch_equity(data):
    stock_symbol = data.get('symbol')
    if stock_symbol and stock_symbol in fno_stocks_list:
        print(f"Received request to fetch data for equity: {stock_symbol}")
        socketio.start_background_task(target=analyzer.process_and_emit_equity_data, symbol=stock_symbol,
                                       sid=request.sid)


@socketio.on('run_bhavcopy_manually')
def handle_manual_bhavcopy_run():
    print("--- Manual Bhavcopy scan triggered by user ---")

    def manual_scan_wrapper():
        now_ist = analyzer._get_ist_time()
        # Try to find Bhavcopy for the last 5 days, starting with today.
        # This loop will attempt to download for today, then yesterday, etc.
        scan_successful = False
        for i in range(5):
            target_day = now_ist - datetime.timedelta(days=i)
            if analyzer.run_bhavcopy_for_date(target_day):
                print(f"Manual Scan Success: Processed Bhavcopy for date: {target_day.strftime('%Y-%m-%d')}")
                analyzer.send_telegram_message(
                    f"✅ Manual Scan: Bhavcopy for {target_day.strftime('%d-%b-%Y')} processed and loaded.")
                scan_successful = True
                break  # Exit loop on first success

        if not scan_successful:
            analyzer.send_telegram_message(f"❌ Manual Scan: Failed to process Bhavcopy for the last 5 days.")

    socketio.start_background_task(manual_scan_wrapper)


def broadcast_live_update():
    with data_lock:
        live_feed_summary = {sym: data.get('live_feed_summary', {}) for sym, data in shared_data.items()}
        socketio.emit('update', {'live': shared_data, 'live_feed_summary': live_feed_summary})


def broadcast_history_append(sym: str, new_history_item: Dict[str, Any]):
    with data_lock: socketio.emit('todays_history_append', {'symbol': sym, 'item': new_history_item})


@app.route('/history/<symbol>/<date_str>')
def get_historical_data(symbol: str, date_str: str):
    if symbol not in AUTO_SYMBOLS and symbol not in fno_stocks_list: return jsonify(
        {"error": f"Invalid symbol: {symbol}"}), 400
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
        self.nse_headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            "Referer": "https://www.nseindia.com/", "Accept": "/"
        }
        self.url_oc = "https://www.nseindia.com/option-chain"
        self.url_indices = "https://www.nseindia.com/api/option-chain-indices?symbol="
        self.url_equities = "https://www.nseindia.com/api/option-chain-equities?symbol="
        self.url_symbols = "https://www.nseindia.com/api/underlying-information"
        self.db_path = 'nse_bse_data.db'
        self.conn: Optional[sqlite3.Connection] = None
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        self.YFINANCE_SYMBOLS = ["SENSEX", "INDIAVIX", "GOLD", "SILVER", "BTC-USD", "USD-INR"]
        self.YFINANCE_TICKER_MAP = {"SENSEX": "^BSESN", "INDIAVIX": "^INDIAVIX", "GOLD": "GOLDBEES.NS",
                                    "SILVER": "SILVERBEES.NS", "BTC-USD": "BTC-USD", "USD-INR": "INR=X"}
        self.TICKER_ONLY_SYMBOLS = ["GOLD", "SILVER", "BTC-USD", "USD-INR"]
        self.previous_data = {}
        self._init_db()
        self.pcr_graph_data: Dict[str, List[Dict[str, Any]]] = {}
        self.previous_pcr: Dict[str, float] = {}

        self.bhavcopy_running = threading.Event()

        self.BHAVCOPY_INDICES = {"NIFTY": "NIFTY 50", "BANKNIFTY": "NIFTY BANK", "FINNIFTY": "NIFTY FIN SERVICE",
                                 "SENSEX": "S&P BSE SENSEX"}
        self.bhavcopy_last_run_date = None

        self.get_stock_symbols()
        self._load_todays_history_from_db()
        self._load_initial_underlying_values()
        self._populate_initial_shared_chart_data()
        self._load_latest_bhavcopy_from_db()

        threading.Thread(target=self.run_loop, daemon=True).start()
        threading.Thread(target=self.equity_fetcher_thread, daemon=True).start()
        threading.Thread(target=self.bhavcopy_scanner_thread, daemon=True).start()

        self.send_telegram_message("NSE OCA PRO Bot is Online\n\nMonitoring will begin during market hours.")

    def get_stock_symbols(self):
        global fno_stocks_list
        try:
            request = self.session.get(self.url_oc, headers=self.nse_headers, timeout=10)
            cookies = dict(request.cookies)
            response = self.session.get(self.url_symbols, headers=self.nse_headers, timeout=10, cookies=cookies)
            response.raise_for_status()
            json_data = response.json()
            fno_stocks_list = sorted([item['symbol'] for item in json_data['data']['UnderlyingList']])
            print(f"Successfully fetched {len(fno_stocks_list)} F&O stock symbols.")
        except Exception as e:
            print(f"Fatal error: Could not fetch stock symbols. {e}")
            fno_stocks_list = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN"]

    def _init_db(self):
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, symbol TEXT, sp REAL, value REAL, call_oi REAL, put_oi REAL, pcr REAL, sentiment TEXT, add_exit TEXT, pcr_change REAL, intraday_pcr REAL)")
            self.conn.execute(
                """CREATE TABLE IF NOT EXISTS bhavcopy_data (
                    date TEXT,
                    symbol TEXT,
                    close REAL,
                    volume INTEGER,
                    pct_change REAL,
                    delivery_pct TEXT,
                    "open" REAL,
                    high REAL,
                    low REAL,
                    PRIMARY KEY (date, symbol)
                )"""
            )
            self.conn.commit()
        except Exception as e:
            print(f"DB error: {e}")

    def _load_latest_bhavcopy_from_db(self):
        global latest_bhavcopy_data
        if not self.conn: return
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT date FROM bhavcopy_data ORDER BY date DESC LIMIT 1")
            latest_date_row = cur.fetchone()
            if not latest_date_row:
                print("No previous Bhavcopy data found in the database.")
                return

            latest_date = latest_date_row[0]
            cur.execute(
                "SELECT symbol, close, volume, pct_change, delivery_pct, open, high, low FROM bhavcopy_data WHERE date = ?",
                (latest_date,)
            )
            rows = cur.fetchall()
            equities = []
            for row in rows:
                equities.append({
                    "Symbol": row[0],
                    "Close": row[1],
                    "Volume": row[2],
                    "% Change": row[3],
                    "Delivery %": row[4],
                    "Open": row[5],
                    "High": row[6],
                    "Low": row[7]
                })

            if equities:
                latest_bhavcopy_data = {"equities": equities, "date": latest_date}
                print(f"Successfully loaded Bhavcopy data for {latest_date} from database.")

        except Exception as e:
            print(f"Error loading Bhavcopy from DB: {e}")

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
            utc_start_str = ist_now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(pytz.utc).strftime(
                '%Y-%m-%d %H:%M:%S')
            all_symbols_to_load = AUTO_SYMBOLS + fno_stocks_list
            for sym in all_symbols_to_load:
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
                        if todays_history.get(sym) and todays_history[sym]: self.previous_pcr[sym] = \
                            todays_history[sym][0]['pcr']
                        self.pcr_graph_data[sym] = [{"TIME": item['time'], "PCR": item['pcr']} for item in
                                                    reversed(todays_history.get(sym, []))]
        except Exception as e:
            print(f"History load error: {e}")

    def _load_initial_underlying_values(self):
        pass

    def _get_ist_time(self) -> datetime.datetime:
        return datetime.datetime.now(self.ist_timezone)

    def _convert_utc_to_ist_display(self, utc_timestamp_str: str) -> str:
        try:
            utc_dt = datetime.datetime.strptime(utc_timestamp_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.utc)
            return (utc_dt.astimezone(self.ist_timezone)).strftime("%H:%M")
        except (ValueError, TypeError):
            return "00:00"

    def clear_todays_history_db(self, sym: Optional[str] = None):
        if not self.conn: return
        try:
            cur = self.conn.cursor()
            ist_now = self._get_ist_time()
            utc_start_str = ist_now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(pytz.utc).strftime(
                '%Y-%m-%d %H:%M:%S')
            if sym:
                cur.execute("DELETE FROM history WHERE symbol = ? AND timestamp >= ?", (sym, utc_start_str))
                if sym in todays_history: todays_history[sym] = []
            else:
                cur.execute("DELETE FROM history WHERE timestamp >= ?", (utc_start_str,))
                for key in todays_history: todays_history[key] = []
            self.conn.commit()
            print(f"Cleared today's history for: {'All' if not sym else sym}")
        except Exception as e:
            print(f"Error clearing history: {e}")

    def run_loop(self):
        try:
            self.session.get(self.url_oc, headers=self.nse_headers, timeout=10, verify=False)
        except requests.exceptions.RequestException as e:
            print(f"Initial NSE session setup failed: {e}")
        while not self.stop.is_set():
            symbols_to_process = [s for s in AUTO_SYMBOLS if s not in fno_stocks_list]
            for sym in symbols_to_process:
                try:
                    self.fetch_and_process_symbol(sym)
                except Exception as e:
                    print(f"{sym} error during fetch and process: {e}")
            time.sleep(LIVE_DATA_INTERVAL)

    def equity_fetcher_thread(self):
        print("Equity fetcher thread started.")
        while not self.stop.is_set():
            if self.bhavcopy_running.is_set():
                print("Bhavcopy scan in progress, pausing equity fetcher for 10 seconds...")
                time.sleep(10)
                continue

            now_ist = self._get_ist_time()
            market_open = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
            market_close = now_ist.replace(hour=15, minute=45, second=0, microsecond=0)

            if market_open <= now_ist <= market_close:
                print(f"Market is open. Starting equity fetch cycle at {now_ist.strftime('%H:%M:%S')}")
                for i, symbol in enumerate(fno_stocks_list):
                    try:
                        print(f"Fetching equity {i + 1}/{len(fno_stocks_list)}: {symbol}")
                        equity_data = self._process_equity_data(symbol)
                        if equity_data:
                            previous_data = equity_data_cache.get(symbol, {}).get('current')
                            equity_data_cache[symbol] = {'current': equity_data, 'previous': previous_data}
                            # No self.save_db for equity data in general, only for specific summaries if needed
                            # self.save_db(symbol, equity_data['summary'])
                        time.sleep(1)
                    except Exception as e:
                        print(f"Error fetching {symbol} in equity loop: {e}")
                self.rank_and_emit_movers()
                print(f"Equity fetch cycle finished. Sleeping for {EQUITY_FETCH_INTERVAL} seconds.")
                time.sleep(EQUITY_FETCH_INTERVAL)
            else:
                print(f"Market is closed. Equity fetcher sleeping. Current time: {now_ist.strftime('%H:%M:%S')}")
                time.sleep(60)

    # +++ MODIFIED: Smart scheduler for Bhavcopy scan +++
    def bhavcopy_scanner_thread(self):
        print("Bhavcopy scanner thread started.")
        while not self.stop.is_set():
            now_ist = self._get_ist_time()
            # Run if it's 9 PM or later AND it hasn't run for today's date yet.
            if now_ist.hour >= 21 and (self.bhavcopy_last_run_date != now_ist.date().isoformat()):
                print(f"--- Triggering daily Bhavcopy scan for {now_ist.date().isoformat()} ---")

                # Try to find Bhavcopy for the last 5 days, starting with today.
                scan_successful = False
                for i in range(5):
                    target_day = now_ist - datetime.timedelta(days=i)
                    if self.run_bhavcopy_for_date(target_day):
                        print(f"Successfully processed Bhavcopy for date: {target_day.strftime('%Y-%m-%d')}")
                        scan_successful = True
                        break  # Exit loop on first success

                # Mark today's scan as attempted, even if unsuccessful, to prevent re-running.
                self.bhavcopy_last_run_date = now_ist.date().isoformat()

                if scan_successful:
                    self.send_telegram_message(f"✅ Daily Bhavcopy scan complete.")
                else:
                    print("Could not find any Bhavcopy in the last 5 days during the scheduled scan.")

            # Check every 15 minutes
            time.sleep(900)

    def run_bhavcopy_for_date(self, date_obj):
        global latest_bhavcopy_data
        target_date_str = date_obj.strftime('%d%m%Y')
        print(f"Attempting to download Bhavcopy for {target_date_str}...")

        self.bhavcopy_running.set()
        try:
            bhav_session = requests.Session()
            bhav_session.headers.update(self.nse_headers)

            # Corrected URL format for sec_bhavdata_full
            url_eq = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{target_date_str}.csv"

            success = False
            try:
                print(f"Trying Equity Bhavcopy: {url_eq}")
                r = bhav_session.get(url_eq, timeout=40)
                if r.status_code == 200:
                    path_eq = f"Bhavcopy_Downloads/NSE/sec_bhavdata_full_{target_date_str}.csv"
                    os.makedirs(os.path.dirname(path_eq), exist_ok=True)
                    with open(path_eq, "wb") as f:
                        f.write(r.content)
                    print("Equity Bhavcopy downloaded")
                    success = True
                else:
                    print(f"Equity Bhavcopy not found (status {r.status_code})")
            except Exception as e:
                print(f"Equity download failed: {e}")

            if success:
                print(f"Bhavcopy downloaded successfully for {target_date_str}.")
                extracted_data = self.extract_bhavcopy_data_from_new_file(target_date_str)
                if extracted_data and extracted_data.get("equities"):
                    print(f"Successfully extracted {len(extracted_data['equities'])} records from Bhavcopy.")
                    latest_bhavcopy_data = extracted_data
                    print("Bhavcopy data extracted, stored in memory, and saved to DB.")
                    socketio.emit('bhavcopy_update', latest_bhavcopy_data)
                else:
                    print("Bhavcopy downloaded, but no relevant equity data was extracted.")
                return True
            else:
                print(f"No Bhavcopy found for {target_date_str}.")
                return False
        finally:
            self.bhavcopy_running.clear()

    def extract_bhavcopy_data_from_new_file(self, target_date_str):
        date_obj = datetime.datetime.strptime(target_date_str, '%d%m%Y')
        # Corrected file_path to match the new URL format
        file_path = f"Bhavcopy_Downloads/NSE/sec_bhavdata_full_{target_date_str}.csv"
        db_date_str = date_obj.strftime('%Y-%m-%d')

        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return None

        try:
            df = pd.read_csv(file_path, skipinitialspace=True)
            df.columns = [col.strip().upper() for col in df.columns]
            print(f"Bhavcopy columns found and standardized: {df.columns.tolist()}")

            df['SYMBOL'] = df['SYMBOL'].str.strip()
            df['SERIES'] = df['SERIES'].str.strip()

            eq_df = df[df['SERIES'] == 'EQ'].copy()
            print(f"Total rows in Bhavcopy: {len(df)}, Rows in 'EQ' series: {len(eq_df)}")

            fno_df = eq_df[eq_df['SYMBOL'].isin(fno_stocks_list)].copy()
            print(f"Found {len(fno_df)} F&O stocks in the Bhavcopy file.")

            if fno_df.empty:
                print("No F&O stocks were found in the Bhavcopy EQ series data. No data to extract.")
                return None

            equity_data = []
            for index, r in fno_df.iterrows():
                sym = r['SYMBOL']
                prev_close = r['PREV_CLOSE']
                close_price = r['CLOSE_PRICE']
                volume = r['TTL_TRD_QNTY']
                open_price = r.get('OPEN_PRICE', None)
                high_price = r.get('HIGH_PRICE', None)
                low_price = r.get('LOW_PRICE', None)

                chg = ((close_price - prev_close) / prev_close * 100) if prev_close and prev_close > 0 else 0
                delivery_str = str(r.get('DELIV_PER', 'N/A')).strip()
                delivery = delivery_str if delivery_str != '-' and delivery_str != 'N/A' else 'N/A'

                equity_data.append({
                    "Symbol": sym,
                    "Close": round(close_price, 2),
                    "Volume": int(volume),
                    "% Change": round(chg, 2),
                    "Delivery %": delivery,
                    "Open": round(open_price, 2) if open_price is not None else None,
                    "High": round(high_price, 2) if high_price is not None else None,
                    "Low": round(low_price, 2) if low_price is not None else None
                })

            if self.conn and equity_data:
                cur = self.conn.cursor()
                for stock in equity_data:
                    cur.execute(
                        "INSERT OR REPLACE INTO bhavcopy_data (date, symbol, close, volume, pct_change, delivery_pct, open, high, low) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            db_date_str,
                            stock['Symbol'],
                            stock['Close'],
                            stock['Volume'],
                            stock['% Change'],
                            str(stock['Delivery %']),
                            stock['Open'],
                            stock['High'],
                            stock['Low']
                        )
                    )
                self.conn.commit()
                print(f"Saved {len(equity_data)} bhavcopy records for {db_date_str} to the database.")

            return {"equities": equity_data, "date": db_date_str}
        except KeyError as e:
            print(f"CRITICAL ERROR: A required column is missing in the Bhavcopy CSV: {e}. Data extraction failed.")
            return None
        except Exception as e:
            print(f"Error reading new Bhavcopy file or saving to DB: {e}")
            return None

    def send_telegram_message(self, message):
        if not SEND_TEXT_UPDATES or TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
            return

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message,
            'parse_mode': 'Markdown'
        }
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                print("Successfully sent Telegram notification.")
            else:
                print(
                    f"Failed to send Telegram notification. Status: {response.status_code}, Response: {response.text}")
        except Exception as e:
            print(f"Exception while sending Telegram message: {e}")

    def calculate_strength_score(self, data: Dict) -> float:
        summary = data.get('summary', {})
        sentiment_map = {"Strong Bullish": 2, "Mild Bullish": 1, "Neutral": 0, "Mild Bearish": -1, "Strong Bearish": -2}
        sentiment_score = sentiment_map.get(summary.get('sentiment', 'Neutral'), 0)
        pcr = summary.get('pcr', 1.0)
        intraday_pcr = summary.get('intraday_pcr', 1.0)
        return round((sentiment_score * 1.5) + (pcr * 1.0) + (intraday_pcr * 1.2), 2)

    def calculate_reversal_score(self, current_data: Dict, previous_data: Optional[Dict]) -> float:
        if not previous_data: return 0.0

        current_summary = current_data.get('summary', {})
        previous_summary = previous_data.get('summary', {})

        current_pcr = current_summary.get('pcr', 1.0)
        current_intraday_pcr = current_summary.get('intraday_pcr', 1.0)
        previous_intraday_pcr = previous_summary.get('intraday_pcr', 1.0)

        sentiment_map = {"Strong Bullish": 2, "Mild Bullish": 1, "Neutral": 0, "Mild Bearish": -1, "Strong Bearish": -2}
        sentiment_score = sentiment_map.get(current_summary.get('sentiment', 'Neutral'), 0)

        intraday_pcr_change = current_intraday_pcr - previous_intraday_pcr

        bullish_reversal_score = (1 / (current_pcr + 0.1)) * (current_intraday_pcr * 1.5) + (
                intraday_pcr_change * 10) + sentiment_score

        return round(bullish_reversal_score, 2)

    def rank_and_emit_movers(self):
        global previous_improving_list, previous_worsening_list
        if not equity_data_cache: return

        strength_scores = []
        reversal_scores = []

        for symbol, data_points in equity_data_cache.items():
            current_data = data_points.get('current')
            previous_data = data_points.get('previous')
            if not current_data: continue

            strength_score = self.calculate_strength_score(current_data)
            strength_scores.append(
                {'symbol': symbol, 'score': strength_score, 'sentiment': current_data['summary']['sentiment'],
                 'pcr': current_data['summary']['pcr']})

            reversal_score = self.calculate_reversal_score(current_data, previous_data)
            reversal_scores.append(
                {'symbol': symbol, 'score': reversal_score, 'sentiment': current_data['summary']['sentiment'],
                 'pcr': current_data['summary']['pcr']})

        strength_scores.sort(key=lambda x: x['score'], reverse=True)
        reversal_scores.sort(key=lambda x: x['score'], reverse=True)

        top_strongest = strength_scores[:10]
        top_weakest = strength_scores[-10:][::-1]
        top_improving = reversal_scores[:10]
        top_worsening = sorted(reversal_scores, key=lambda x: x['score'])[:10]

        socketio.emit('top_movers_update', {
            'strongest': top_strongest,
            'weakest': top_weakest,
            'improving': top_improving,
            'worsening': top_worsening
        })
        print("Emitted Top Movers update with 4 categories.")

        current_improving_symbols = {stock['symbol'] for stock in top_improving}
        if current_improving_symbols != previous_improving_list:
            print("Change detected in Potential Buys list. Sending Telegram notification.")
            message = "Potential Buys Update (Getting Better)\n\n"
            for i, stock in enumerate(top_improving):
                message += f"{i + 1}. {stock['symbol']}* (Score: {stock['score']})\n"
            self.send_telegram_message(message)
            previous_improving_list = current_improving_symbols

        current_worsening_symbols = {stock['symbol'] for stock in top_worsening}
        if current_worsening_symbols != previous_worsening_list:
            print("Change detected in Potential Sells list. Sending Telegram notification.")
            message = "Potential Sells Update (Getting Worsening)\n\n"
            for i, stock in enumerate(top_worsening):
                message += f"{i + 1}. {stock['symbol']} (Score: {stock['score']})\n"
            self.send_telegram_message(message)
            previous_worsening_list = current_worsening_symbols

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

            if hist.empty or len(hist) < 2:
                if sym == "GOLD":
                    print(
                        f"Warning: Could not fetch data for {sym} ($GOLDBEES.NS). It might be temporarily unavailable.")
                return

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
                    # For yfinance symbols that are not just tickers, create a basic summary for history
                    summary = {'time': self._get_ist_time().strftime("%H:%M"), 'sp': round(change, 2),
                               'value': round(current_price, 2), 'pcr': round(pct_change, 2), 'sentiment': sentiment,
                               'call_oi': 0, 'put_oi': 0, 'add_exit': "Live Price", 'pcr_change': 0, 'intraday_pcr': 0,
                               'expiry': 'N/A'}
                    shared_data[sym]['summary'] = summary
                    # Clear chart data for YFinance symbols as they don't have OC data
                    shared_data[sym]['strikes'], shared_data[sym]['max_pain_chart_data'], shared_data[sym][
                        'ce_oi_chart_data'], shared_data[sym]['pe_oi_chart_data'], shared_data[sym][
                        'pcr_chart_data'] = [], [], [], [], []

            print(f"{sym} YFINANCE DATA UPDATED | Value: {current_price:.2f}")
            broadcast_live_update()

            if sym not in self.TICKER_ONLY_SYMBOLS:
                now = time.time()
                if now - last_history_update.get(sym, 0) >= UPDATE_INTERVAL:
                    with data_lock:
                        if sym not in todays_history: todays_history[sym] = []
                        todays_history[sym].insert(0, summary)
                    broadcast_history_append(sym, summary)
                    last_history_update[sym] = now
                    self._save_db(sym, summary)
                if now - last_alert.get(sym, 0) >= UPDATE_INTERVAL:
                    self.send_alert(sym, summary)
                    last_alert[sym] = now
        except Exception as e:
            print(f"{sym} yfinance processing error: {e}")

    def _get_oi_buildup(self, price_change, oi_change):
        if abs(price_change) > 0.01:
            if price_change > 0 and oi_change > 0: return "Long Buildup"
            if price_change > 0 and oi_change < 0: return "Short Covering"
            if price_change < 0 and oi_change > 0: return "Short Buildup"
            if price_change < 0 and oi_change < 0: return "Long Unwinding"
        if oi_change > 100: return "Fresh OI Added"
        if oi_change < -100: return "OI Exited"
        return ""

    def _process_nse_data(self, sym: str):
        try:
            url = self.url_indices + sym
            resp = self.session.get(url, headers=self.nse_headers, timeout=10, verify=False)
            if resp.status_code == 401:
                self.session.get(self.url_oc, headers=self.nse_headers, timeout=5, verify=False)
                resp = self.session.get(url, headers=self.nse_headers, timeout=10, verify=False)

            data = resp.json()
            expiry_dates = data['records']['expiryDates']
            if not expiry_dates:  # If no expiry dates, it might be a holiday or data not available
                print(f"No expiry dates found for {sym}. Skipping processing.")
                return

            expiry = expiry_dates[0]
            underlying = data['records']['underlyingValue']

            prev_price = initial_underlying_values.get(sym)
            if prev_price is None:
                price_change = 0
                initial_underlying_values[sym] = float(underlying)
            else:
                price_change = underlying - prev_price

            ce_values = [d['CE'] for d in data['records']['data'] if 'CE' in d and d['expiryDate'] == expiry]
            pe_values = [d['PE'] for d in data['records']['data'] if 'PE' in d and d['expiryDate'] == expiry]
            if not ce_values or not pe_values:
                print(f"No CE or PE values found for {sym} for expiry {expiry}. Skipping processing.")
                return

            df_ce = pd.DataFrame(ce_values)
            df_pe = pd.DataFrame(pe_values)
            df = pd.merge(df_ce[['strikePrice', 'openInterest', 'changeinOpenInterest']],
                          df_pe[['strikePrice', 'openInterest', 'changeinOpenInterest']], on='strikePrice', how='outer',
                          suffixes=('_call', '_put')).fillna(0)

            sp = self.get_atm_strike(df, underlying)
            if not sp:
                print(f"Could not determine ATM strike for {sym}. Skipping processing.")
                return

            idx_list = df[df['strikePrice'] == sp].index.tolist()
            if not idx_list:
                print(f"ATM strike {sp} not found in dataframe for {sym}. Skipping processing.")
                return
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
                    if sym not in todays_history: todays_history[sym] = []
                    todays_history[sym].insert(0, summary)
                    if sym not in self.YFINANCE_SYMBOLS:
                        if sym not in self.pcr_graph_data: self.pcr_graph_data[sym] = []
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

    def process_and_emit_equity_data(self, symbol: str, sid: str):
        print(f"Processing equity data for {symbol} for client {sid}")
        try:
            if symbol in equity_data_cache and equity_data_cache[symbol].get('current'):
                print(f"Using cached data for {symbol}")
                equity_data = equity_data_cache[symbol]['current']
            else:
                print(f"No cache, fetching live data for {symbol}")
                equity_data = self._process_equity_data(symbol)

            if equity_data:
                socketio.emit('equity_data_update', {'symbol': symbol, 'data': equity_data}, to=sid)
            else:
                socketio.emit('equity_data_update', {'symbol': symbol, 'error': 'No data found.'}, to=sid)
        except Exception as e:
            import traceback
            print(f"Error processing equity {symbol}: {e}\n{traceback.format_exc()}")
            socketio.emit('equity_data_update', {'symbol': symbol, 'error': str(e)}, to=sid)

    def _process_equity_data(self, sym: str) -> Optional[Dict]:
        try:
            url = self.url_equities + sym
            resp = self.session.get(url, headers=self.nse_headers, timeout=10, verify=False)
            if resp.status_code == 401:
                self.session.get(self.url_oc, headers=self.nse_headers, timeout=5, verify=False)
                resp = self.session.get(url, headers=self.nse_headers, timeout=10, verify=False)

            resp.raise_for_status()
            data = resp.json()

            if not data.get('records') or not data['records'].get('data'):
                print(f"No option chain data returned for {sym}. Skipping processing.")
                return None

            expiry_dates = data['records']['expiryDates']
            if not expiry_dates:
                print(f"No expiry dates found for {sym}. Skipping processing.")
                return None
            expiry = expiry_dates[0]

            underlying = data['records']['underlyingValue']

            prev_price = initial_underlying_values.get(sym, underlying)
            price_change = underlying - prev_price
            if initial_underlying_values.get(sym) is None:
                initial_underlying_values[sym] = float(underlying)

            ce_values = [d['CE'] for d in data['records']['data'] if 'CE' in d and d['expiryDate'] == expiry]
            pe_values = [d['PE'] for d in data['records']['data'] if 'PE' in d and d['expiryDate'] == expiry]
            if not ce_values or not pe_values:
                print(f"No CE or PE values found for {sym} for expiry {expiry}. Skipping processing.")
                return None

            df_ce = pd.DataFrame(ce_values)
            df_pe = pd.DataFrame(pe_values)
            df = pd.merge(df_ce[['strikePrice', 'openInterest', 'changeinOpenInterest']],
                          df_pe[['strikePrice', 'openInterest', 'changeinOpenInterest']], on='strikePrice', how='outer',
                          suffixes=('_call', '_put')).fillna(0)

            sp = self.get_atm_strike(df, underlying)
            if not sp:
                print(f"Could not determine ATM strike for {sym}. Skipping processing.")
                return None

            idx_list = df[df['strikePrice'] == sp].index.tolist()
            if not idx_list:
                print(f"ATM strike {sp} not found in dataframe for {sym}. Skipping processing.")
                return None
            idx = idx_list[0]

            strikes_data = []
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
                strikes_data.append(
                    {'strike': strike, 'call_oi': call_oi, 'call_coi': call_coi, 'call_action': call_action,
                     'put_oi': put_oi, 'put_coi': put_coi, 'put_action': put_action, 'is_atm': i == 0,
                     'call_buildup': call_buildup, 'put_buildup': put_buildup})

            total_call_oi = int(df['openInterest_call'].sum())
            total_put_oi = int(df['openInterest_put'].sum())
            total_call_coi = int(df['changeinOpenInterest_call'].sum())
            total_put_coi = int(df['changeinOpenInterest_put'].sum())

            pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi else 0.0
            intraday_pcr = round(total_put_coi / total_call_coi, 2) if total_call_coi != 0 else 0.0

            atm_index_in_strikes_data = next((j for j, s in enumerate(strikes_data) if s['is_atm']), 10)
            atm_call_coi = strikes_data[atm_index_in_strikes_data]['call_coi']
            atm_put_coi = strikes_data[atm_index_in_strikes_data]['put_coi']
            diff = round((atm_call_coi - atm_put_coi) / 1000, 1)
            sentiment = self.get_sentiment(diff, pcr)

            summary = {'sp': int(sp), 'value': float(underlying), 'pcr': pcr, 'sentiment': sentiment,
                       'intraday_pcr': intraday_pcr}
            pulse_summary = {'total_call_oi': total_call_oi, 'total_put_oi': total_put_oi,
                             'total_call_coi': total_call_coi, 'total_put_coi': total_put_coi}

            return {'summary': summary, 'strikes': strikes_data, 'pulse_summary': pulse_summary}
        except Exception as e:
            print(f"Error processing equity data for {sym}: {e}")
            return None

    def get_atm_strike(self, df: pd.DataFrame, underlying: float) -> Optional[int]:
        try:
            strikes = df['strikePrice'].astype(int).unique()
            # Find the strike price closest to the underlying value
            if len(strikes) > 0:
                closest_strike = min(strikes, key=lambda x: abs(x - underlying))
                # Ensure the closest strike is within a reasonable range (e.g., +/- 10% of underlying)
                # to avoid issues with very illiquid or mispriced option chains
                if abs((closest_strike - underlying) / underlying) < 0.20:  # 20% threshold
                    return int(closest_strike)
                else:
                    print(
                        f"Warning: Closest strike {closest_strike} is too far from underlying {underlying}. Check data for consistency.")
                    return None
            else:
                return None
        except Exception as e:
            print(f"Error getting ATM strike: {e}")
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
        try:
            with data_lock:
                live_feed = shared_data.get(sym, {}).get('live_feed_summary', {})

            change = live_feed.get('change', 0)
            pct_change = live_feed.get('percentage_change', 0)

            change_str = f"+{change:.2f}" if change >= 0 else f"{change:.2f}"
            pct_str = f"+{pct_change:.2f}%" if pct_change >= 0 else f"{pct_change:.2f}%"

            message = f"* {sym.upper()} Update*\n\n"
            message += f"• Value: {row.get('value', 'N/A')}\n"
            message += f"• Change: {change_str} ({pct_str})\n"
            message += f"• PCR: {row.get('pcr', 'N/A')}\n"
            message += f"• Sentiment: {row.get('sentiment', 'N/A')}\n"

            if 'add_exit' in row and row['add_exit'] != "Live Price" and row['add_exit'] != "No Change":
                message += f"\nOI Changes:\n{row['add_exit']}"

            self.send_telegram_message(message)
        except Exception as e:
            print(f"Error formatting alert for {sym}: {e}")

    def _save_db(self, sym, row):
        if not self.conn: return
        try:
            ts = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
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
