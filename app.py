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
# from nsetools import Nse # Not used in current logic
# import lxml # Not used in current logic
# import re # Not used in current logic
import yfinance as yf
import numpy as np
import pytz
from zipfile import ZipFile  # Used for F&O Bhavcopy
from io import BytesIO

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning)

# --------------------------------------------------------------------------- #
# CONFIG
# --------------------------------------------------------------------------- #
TELEGRAM_BOT_TOKEN = "8081075640:AAEDdrUSdg8BRX4CcKJ4W7jG1EOnSFTQPr4"  # Replace with your bot token
TELEGRAM_CHAT_ID = "275529219"  # Replace with your chat ID
SEND_TEXT_UPDATES = True
UPDATE_INTERVAL = 120
MAX_HISTORY_ROWS_DB = 10000
LIVE_DATA_INTERVAL = 15
EQUITY_FETCH_INTERVAL = 300  # 5 minutes

AUTO_SYMBOLS = ["NIFTY", "FINNIFTY", "BANKNIFTY", "SENSEX", "INDIAVIX", "GOLD", "SILVER", "BTC-USD", "USD-INR"]
# Define NSE indices you want to track historical Bhavcopy for
NSE_INDEX_BHAVCOPY_SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "INDIAVIX"]

# --------------------------------------------------------------------------- #
# WEB DASHBOARD & GLOBAL VARS
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

fno_stocks_list = []
equity_data_cache: Dict[str, Dict[str, Any]] = {}
previous_improving_list = set()
previous_worsening_list = set()
# latest_bhavcopy_data will now combine both equity and index data for display
latest_bhavcopy_data: Dict[str, Any] = {"equities": [], "indices": [], "date": None}


@app.route('/')
def index(): return render_template('dashboard.html')


@app.route('/api/stocks')
def get_stocks():
    return jsonify(fno_stocks_list)


@app.route('/api/bhavcopy')
def get_bhavcopy_data():
    return jsonify(latest_bhavcopy_data)


# API endpoint to get historical Bhavcopy data for a specific date
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
        data = analyzer._get_bhavcopy_for_date(date_str)
        if not data["equities"] and not data["indices"] and not data["block_deals"] and not data["fno_data"]:
            return jsonify(
                {"error": f"No Bhavcopy data (equity, index, block deals, or F&O) found for {date_str}."}), 404

        data["date"] = date_str  # Add date to the response
        return jsonify(data)

    except Exception as e:
        print(f"Error fetching historical bhavcopy for {date_str}: {e}")
        return jsonify({"error": "An internal error occurred while fetching data."}), 500


# NEW: API endpoint for strategy analysis
@app.route('/api/bhavcopy/strategies/<date_str>')
def get_bhavcopy_strategies(date_str: str) -> Dict[str, Any]:  # Adjusted return type hint
    if not analyzer.conn:
        return {"error": "Database connection not available."}  # Return dict for consistency
    try:
        datetime.datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return {"error": "Invalid date format. Please use YYYY-MM-DD."}

    results = {}
    try:
        # FII-DII Delivery Divergence Trap (FDDT) - Placeholder
        results['FDDT'] = analyzer.analyze_fii_dii_delivery_divergence(date_str)

        # Zero-Delivery Future Roll Trap (ZDFT)
        results['ZDFT'] = analyzer.analyze_zero_delivery_future_roll(date_str)

        # Block-Deal Ghost Pump (BDGP)
        results['BDGP'] = analyzer.analyze_block_deal_ghost_pump(date_str)

        # VWAP Anchor Reversion (VAR-7)
        results['VAR7'] = analyzer.analyze_vwap_anchor_reversion(date_str)

        # Hidden Bonus Arbitrage (HBA-21) - Placeholder
        results['HBA21'] = analyzer.analyze_hidden_bonus_arbitrage(date_str)

        # OI Momentum Trap (Long Buildup vs. Short Covering Detector)
        results['OIMT'] = analyzer.analyze_oi_momentum_trap(date_str)

        # Volume Surge Scanner for Swing Breakouts
        results['VSSB'] = analyzer.analyze_volume_surge_scanner(date_str)

        # Options OI Anomaly for Directional Bets - Placeholder
        results['OOAD'] = analyzer.analyze_options_oi_anomaly(date_str)

        # Corporate Action Arbitrage via Delivery Traps - Placeholder
        results['CAADT'] = analyzer.analyze_corporate_action_arbitrage(date_str)

    except Exception as e:
        print(f"Error running strategies for {date_str}: {e}")
        return {"error": f"An internal error occurred while running strategies: {e}"}

    return results


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
        scan_successful = False
        # Try to scan for up to the last 5 days
        for i in range(5):
            target_day = now_ist - datetime.timedelta(days=i)
            # Skip weekends
            if target_day.weekday() >= 5:  # Saturday or Sunday
                continue

            if analyzer.run_bhavcopy_for_date(target_day, trigger_manual=True):
                print(f"Manual Scan Success: Processed Bhavcopy for date: {target_day.strftime('%Y-%m-%d')}")
                analyzer.send_telegram_message(
                    f"✅ Manual Scan: Bhavcopy for {target_day.strftime('%d-%b-%Y')} processed and loaded.")
                scan_successful = True
                break  # Exit loop on first success

        if not scan_successful:
            analyzer.send_telegram_message(f"❌ Manual Scan: Failed to process Bhavcopy for the last few trading days.")

    socketio.start_background_task(manual_scan_wrapper)


@socketio.on('run_bhavcopy_for_date_and_analyze')
def handle_run_bhavcopy_for_date_and_analyze(data):
    date_str = data.get('date')
    if not date_str:
        emit('bhavcopy_analysis_status', {'success': False, 'message': 'Date is required for analysis.'})
        return

    try:
        date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
        socketio.start_background_task(target=analyzer._run_bhavcopy_and_analyze_wrapper, date_obj=date_obj,
                                       sid=request.sid)
    except ValueError:
        emit('bhavcopy_analysis_status', {'success': False, 'message': 'Invalid date format. Use YYYY-MM-DD.'})
    except Exception as e:
        print(f"Error triggering bhavcopy and analysis: {e}")
        emit('bhavcopy_analysis_status', {'success': False, 'message': f'Error triggering analysis: {e}'})


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
        # More comprehensive headers to mimic a browser
        self.nse_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'application/json, text/plain, */*',
            'Connection': 'keep-alive',
            'Referer': 'https://www.nseindia.com/option-chain'  # Crucial for some endpoints
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

        self.BHAVCOPY_INDICES_MAP = {"NIFTY": "NIFTY 50", "BANKNIFTY": "NIFTY BANK", "FINNIFTY": "NIFTY FIN SERVICE",
                                     "INDIAVIX": "INDIA VIX"}  # SENSEX is BSE, not in NSE index bhavcopy
        self.bhavcopy_last_run_date = None

        # Initial session setup to get cookies
        self._set_nse_session_cookies()
        self.get_stock_symbols()
        self._load_todays_history_from_db()
        self._load_initial_underlying_values()
        self._populate_initial_shared_chart_data()
        self._load_latest_bhavcopy_from_db()

        threading.Thread(target=self.run_loop, daemon=True).start()
        threading.Thread(target=self.equity_fetcher_thread, daemon=True).start()
        threading.Thread(target=self.bhavcopy_scanner_thread, daemon=True).start()

        self.send_telegram_message("NSE OCA PRO Bot is Online\n\nMonitoring will begin during market hours.")

    def _set_nse_session_cookies(self):
        """Refreshes the session cookies required by NSE."""
        print("Attempting to refresh NSE session cookies...")
        try:
            response = self.session.get(self.url_oc, headers=self.nse_headers, timeout=10, verify=False)
            response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
            print("NSE session cookies refreshed successfully.")
            return True
        except requests.exceptions.RequestException as e:
            print(f"Failed to refresh NSE session cookies: {e}")
            return False

    def get_stock_symbols(self):
        global fno_stocks_list
        try:
            # Ensure cookies are fresh before fetching symbols
            if not self._set_nse_session_cookies():
                print("Could not get fresh cookies, falling back to hardcoded symbols.")
                fno_stocks_list = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN"]
                return

            response = self.session.get(self.url_symbols, headers=self.nse_headers, timeout=10, verify=False)
            response.raise_for_status()
            json_data = response.json()
            fno_stocks_list = sorted([item['symbol'] for item in json_data['data']['UnderlyingList']])
            print(f"Successfully fetched {len(fno_stocks_list)} F&O stock symbols.")
        except requests.exceptions.RequestException as e:
            print(f"Fatal error: Could not fetch stock symbols. {e}. Falling back to hardcoded symbols.")
            fno_stocks_list = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN"]
        except Exception as e:
            print(f"An unexpected error occurred while fetching stock symbols: {e}. Falling back to hardcoded symbols.")
            fno_stocks_list = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN"]

    def _init_db(self):
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, symbol TEXT, sp REAL, value REAL, call_oi REAL, put_oi REAL, pcr REAL, sentiment TEXT, add_exit TEXT, pcr_change REAL, intraday_pcr REAL)")

            # Table for Equity Bhavcopy Data (CM Segment)
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
                    prev_close REAL,
                    trading_type TEXT, -- 'EQ' for normal equity, 'B' for block deals etc.
                    PRIMARY KEY (date, symbol, trading_type)
                )"""
            )
            # Table for Index Bhavcopy Data
            self.conn.execute(
                """CREATE TABLE IF NOT EXISTS index_bhavcopy_data (
                    date TEXT,
                    symbol TEXT,
                    close REAL,
                    pct_change REAL,
                    "open" REAL,
                    high REAL,
                    low REAL,
                    PRIMARY KEY (date, symbol)
                )"""
            )
            # NEW: Table for F&O Bhavcopy Data (Futures and Options, simplified for strategies)
            # FIX: Removed IFNULL from PRIMARY KEY definition
            self.conn.execute(
                """CREATE TABLE IF NOT EXISTS fno_bhavcopy_data (
                    date TEXT,
                    symbol TEXT,
                    instrument TEXT, -- EQ, FUTIDX, FUTSTK, OPTIDX, OPTSTK
                    expiry_date TEXT,
                    strike_price REAL, -- NULL for futures
                    option_type TEXT, -- NULL for futures, CE/PE for options
                    open_interest INTEGER,
                    change_in_oi INTEGER,
                    volume INTEGER,
                    close REAL,
                    delivery_pct REAL, -- For futures
                    PRIMARY KEY (date, symbol, instrument, expiry_date, strike_price, option_type)
                )"""
            )
            # NEW: Table for Block Deal Data
            self.conn.execute(
                """CREATE TABLE IF NOT EXISTS block_deal_data (
                    date TEXT,
                    symbol TEXT,
                    trade_type TEXT, -- Should be 'B'
                    quantity INTEGER,
                    price REAL,
                    PRIMARY KEY (date, symbol, quantity, price) -- Composite key for uniqueness of a specific block deal
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
            # Get latest date from any of the bhavcopy tables
            latest_dates = []
            cur.execute("SELECT date FROM bhavcopy_data ORDER BY date DESC LIMIT 1")
            eq_date = cur.fetchone()
            if eq_date: latest_dates.append(eq_date[0])

            cur.execute("SELECT date FROM index_bhavcopy_data ORDER BY date DESC LIMIT 1")
            idx_date = cur.fetchone()
            if idx_date: latest_dates.append(idx_date[0])

            cur.execute("SELECT date FROM fno_bhavcopy_data ORDER BY date DESC LIMIT 1")
            fno_date = cur.fetchone()
            if fno_date: latest_dates.append(fno_date[0])

            latest_date = max(latest_dates) if latest_dates else None

            if not latest_date:
                print("No previous Bhavcopy data found in the database.")
                return

            # Fetch equity data for the latest date (only 'EQ' trading type)
            cur.execute(
                "SELECT symbol, close, volume, pct_change, delivery_pct, open, high, low FROM bhavcopy_data WHERE date = ? AND trading_type = 'EQ'",
                (latest_date,)
            )
            equity_rows = cur.fetchall()
            equities = []
            for row in equity_rows:
                equities.append({
                    "Symbol": row[0],
                    "Close": row[1],
                    "Volume": row[2],
                    "Pct Change": row[3],
                    "Delivery %": row[4],
                    "Open": row[5],
                    "High": row[6],
                    "Low": row[7]
                })

            # Fetch index data for the latest date
            cur.execute(
                "SELECT symbol, close, pct_change, open, high, low FROM index_bhavcopy_data WHERE date = ?",
                (latest_date,)
            )
            index_rows = cur.fetchall()
            indices = []
            for row in index_rows:
                indices.append({
                    "Symbol": row[0],
                    "Close": row[1],
                    "Pct Change": row[2],
                    "Open": row[3],
                    "High": row[4],
                    "Low": row[5]
                })

            if equities or indices:
                latest_bhavcopy_data["equities"] = equities
                latest_bhavcopy_data["indices"] = indices
                latest_bhavcopy_data["date"] = latest_date
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
            # Ensure session cookies are fresh at the start of the loop
            self._set_nse_session_cookies()
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
                # Refresh cookies before starting a new cycle of equity fetches
                self._set_nse_session_cookies()
                for i, symbol in enumerate(fno_stocks_list):
                    try:
                        print(f"Fetching equity {i + 1}/{len(fno_stocks_list)}: {symbol}")
                        equity_data = self._process_equity_data(symbol)
                        if equity_data:
                            previous_data = equity_data_cache.get(symbol, {}).get('current')
                            equity_data_cache[symbol] = {'current': equity_data, 'previous': previous_data}
                        time.sleep(1)  # Small delay between requests to avoid hitting rate limits too hard
                    except Exception as e:
                        print(f"Error fetching {symbol} in equity loop: {e}")
                self.rank_and_emit_movers()
                print(f"Equity fetch cycle finished. Sleeping for {EQUITY_FETCH_INTERVAL} seconds.")
                time.sleep(EQUITY_FETCH_INTERVAL)
            else:
                print(f"Market is closed. Equity fetcher sleeping. Current time: {now_ist.strftime('%H:%M:%S')}")
                time.sleep(60)

    def bhavcopy_scanner_thread(self):
        print("Bhavcopy scanner thread started.")
        while not self.stop.is_set():
            now_ist = self._get_ist_time()
            # Run daily scan after market hours (e.g., 9 PM IST) if not already run for today
            if now_ist.hour >= 21 and (self.bhavcopy_last_run_date != now_ist.date().isoformat()):
                print(f"--- Triggering daily Bhavcopy scan for {now_ist.date().isoformat()} ---")

                scan_successful = False
                # Try to scan for up to the last 5 days
                for i in range(5):
                    target_day = now_ist - datetime.timedelta(days=i)
                    # Skip weekends
                    if target_day.weekday() >= 5:  # Saturday or Sunday
                        continue

                    if self.run_bhavcopy_for_date(target_day):
                        print(f"Successfully processed Bhavcopy for date: {target_day.strftime('%Y-%m-%d')}")
                        scan_successful = True
                        break  # Exit loop on first success

                self.bhavcopy_last_run_date = now_ist.date().isoformat()

                if scan_successful:
                    self.send_telegram_message(f"✅ Daily Bhavcopy scan complete.")
                else:
                    self.send_telegram_message(
                        f"❌ Daily Bhavcopy scan: Failed to process Bhavcopy for the last few trading days.")

            time.sleep(900)  # Check every 15 minutes

    def download_bhavcopy_file_by_name(self, date_obj, file_pattern_or_name, target_folder, is_zip=False):
        """
        Attempts to download a bhavcopy file given a date and a file pattern/name.
        It tries a few common NSE base URLs.
        """
        date_dmy = date_obj.strftime('%d%m%Y')
        date_ymd = date_obj.strftime('%Y-%m-%d')
        month_upper = date_obj.strftime('%b').upper()
        year = date_obj.year

        # Base URLs to try - Prioritized nsearchives.nseindia.com as it seems more reliable from your tests
        possible_base_urls = [
            # Prioritized nsearchives
            "https://nsearchives.nseindia.com/products/content/",
            f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/{year}/{month_upper}/",
            "https://nsearchives.nseindia.com/content/indices/",
            "https://nsearchives.nseindia.com/content/fo/",  # New base for BhavCopy_NSE_FO
            "https://nsearchives.nseindia.com/content/equities/",  # New base for block.csv, bulk.csv etc.
            "https://nsearchives.nseindia.com/archives/fo/",  # New base for fo.zip, sec_ban etc.
            "https://nsearchives.nseindia.com/archives/nsccl/sett/",  # New base for FOSett_prce
            "https://nsearchives.nseindia.com/archives/cd/bhav/",  # New base for CD bhavcopy
            "https://nsearchives.nseindia.com/content/com/",  # New base for COM bhavcopy
            "https://nsearchives.nseindia.com/archives/ird/bhav/",  # New base for IRD bhavcopy

            # Secondary (www1.nseindia.com) - often has TLS issues
            f"https://www1.nseindia.com/content/historical/DERIVATIVES/{year}/{month_upper}/",
            "https://www1.nseindia.com/products/content/",

            # Tertiary (archives.nseindia.com) - less common for these specific files
            "https://archives.nseindia.com/content/fo/bhav/",
            "https://archives.nseindia.com/content/equities/bulk_block/",
            "https://archives.nseindia.com/content/historical/equities/",
            "https://archives.nseindia.com/content/nsccl/fao_bhav/",
        ]

        # Generate specific filenames based on pattern and date formats
        file_candidates = []
        # Standard replacements
        if "DDMMYYYY" in file_pattern_or_name:
            file_candidates.append(file_pattern_or_name.replace("DDMMYYYY", date_dmy))
        if "YYYY-MM-DD" in file_pattern_or_name:
            file_candidates.append(file_pattern_or_name.replace("YYYY-MM-DD", date_ymd))
        if "DDMMYY" in file_pattern_or_name:
            file_candidates.append(file_pattern_or_name.replace("DDMMYY", date_obj.strftime('%d%m%y')))
        if "YYYYMMDD" in file_pattern_or_name:  # For nsccl.YYYYMMDD.s.zip, BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip
            file_candidates.append(file_pattern_or_name.replace("YYYYMMDD", date_obj.strftime('%Y%m%d')))

        # Add the original pattern/name itself, in case it's a fixed name like "block.csv" or "fo.zip"
        if file_pattern_or_name not in file_candidates:
            file_candidates.append(file_pattern_or_name)

        # Ensure correct suffix if not already present
        final_file_candidates = []
        for fn in file_candidates:
            # Handle cases where the pattern already includes .zip or .csv
            if is_zip and not fn.endswith(".zip"):
                final_file_candidates.append(f"{fn}.zip")
            elif not is_zip and not (fn.endswith(".csv") or fn.endswith(".DAT") or fn.endswith(".xls")):  # Added .xls
                final_file_candidates.append(f"{fn}.csv")  # Default to .csv if not specified
            else:
                final_file_candidates.append(fn)

        for base_url in possible_base_urls:
            # Use .format() only if the base_url contains placeholders
            formatted_base_url = base_url
            if '{year}' in base_url or '{month_upper}' in base_url:
                formatted_base_url = base_url.format(year=year, month_upper=month_upper)

            for filename in final_file_candidates:
                full_url = formatted_base_url + filename
                print(f"Trying to download: {full_url}")
                try:
                    response = self.session.get(full_url, headers=self.nse_headers, timeout=40)
                    if response.status_code == 200:
                        path = os.path.join(target_folder, filename)
                        os.makedirs(target_folder, exist_ok=True)
                        with open(path, "wb") as f:
                            f.write(response.content)
                        print(f"Successfully downloaded {filename} from {full_url}")
                        return path
                    elif response.status_code == 404:
                        continue  # File not found, try next candidate
                    else:
                        print(f"Download failed for {full_url} with status {response.status_code}")
                except requests.exceptions.RequestException as e:
                    print(f"Error downloading {full_url}: {e}")
        return None

    def _run_bhavcopy_and_analyze_wrapper(self, date_obj, sid):
        date_str = date_obj.strftime('%Y-%m-%d')
        socketio.emit('bhavcopy_analysis_status',
                      {'success': True, 'message': f'Attempting to download and process Bhavcopy for {date_str}...'},
                      to=sid)

        # Step 1: Download and Process Bhavcopy Files
        download_success = self.run_bhavcopy_for_date(date_obj, trigger_manual=True)

        if download_success:
            socketio.emit('bhavcopy_analysis_status',
                          {'success': True, 'message': f'Bhavcopy for {date_str} processed. Analyzing strategies...'},
                          to=sid)
            # Step 2: Trigger Strategy Analysis (via API call, which will then emit results)
            try:
                # Direct call to strategy analysis logic
                results = {}
                results['FDDT'] = self.analyze_fii_dii_delivery_divergence(date_str)
                results['ZDFT'] = self.analyze_zero_delivery_future_roll(date_str)
                results['BDGP'] = self.analyze_block_deal_ghost_pump(date_str)
                results['VAR7'] = self.analyze_vwap_anchor_reversion(date_str)
                results['HBA21'] = self.analyze_hidden_bonus_arbitrage(date_str)
                results['OIMT'] = self.analyze_oi_momentum_trap(date_str)
                results['VSSB'] = self.analyze_volume_surge_scanner(date_str)
                results['OOAD'] = self.analyze_options_oi_anomaly(date_str)
                results['CAADT'] = self.analyze_corporate_action_arbitrage(date_str)

                socketio.emit('bhavcopy_strategy_results', {'date': date_str, 'results': results}, to=sid)
                socketio.emit('bhavcopy_analysis_status',
                              {'success': True, 'message': f'Strategies for {date_str} analyzed successfully.'}, to=sid)
            except Exception as e:
                print(f"Error analyzing strategies for {date_str}: {e}")
                socketio.emit('bhavcopy_analysis_status',
                              {'success': False, 'message': f'Error analyzing strategies: {e}'}, to=sid)
        else:
            socketio.emit('bhavcopy_analysis_status',
                          {'success': False, 'message': f'Failed to download or process Bhavcopy for {date_str}.'},
                          to=sid)

    def run_bhavcopy_for_date(self, date_obj, trigger_manual=False):
        global latest_bhavcopy_data
        target_date_str_dmy = date_obj.strftime('%d%m%Y')
        target_date_str_ymd = date_obj.strftime('%Y-%m-%d')
        print(f"Attempting to download Bhavcopy for {target_date_str_dmy}...")

        self.bhavcopy_running.set()
        try:
            self._set_nse_session_cookies()

            cm_path = None
            fno_path = None
            index_path = None
            block_path = None
            bulk_path = None

            # --- CM Bhavcopy ---
            print(f"Trying to find CM Bhavcopy for {target_date_str_dmy}...")
            cm_path = self.download_bhavcopy_file_by_name(date_obj, f"sec_bhavdata_full_{target_date_str_dmy}.csv",
                                                          "Bhavcopy_Downloads/NSE")
            if not cm_path:  # Try alternative name if standard fails
                cm_path = self.download_bhavcopy_file_by_name(date_obj,
                                                              f"BhavCopy_NSE_CM_0_0_0_{date_obj.strftime('%Y%m%d')}_F_0000.csv.zip",
                                                              "Bhavcopy_Downloads/NSE", is_zip=True)
                if cm_path: print(f"Found alternative CM Bhavcopy: {os.path.basename(cm_path)}")

            # --- F&O Bhavcopy ---
            print(f"Trying to find F&O Bhavcopy for {target_date_str_dmy}...")
            # Try standard name first (foDDMMYYYYbhav.csv.zip)
            fno_path = self.download_bhavcopy_file_by_name(date_obj, f"fo{target_date_str_dmy}bhav.csv.zip",
                                                           "Bhavcopy_Downloads/NSE", is_zip=True)
            if not fno_path:  # Try foDDMMYYYY.zip (from your new list)
                fno_path = self.download_bhavcopy_file_by_name(date_obj, f"fo{target_date_str_dmy}.zip",
                                                               "Bhavcopy_Downloads/NSE", is_zip=True)
            if not fno_path:  # Try PRDDMMYY.zip (your previous example)
                fno_path = self.download_bhavcopy_file_by_name(date_obj, f"PR{date_obj.strftime('%d%m%y')}.zip",
                                                               "Bhavcopy_Downloads/NSE", is_zip=True)
            if not fno_path:  # Try BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip (from your new list, exact match)
                fno_path = self.download_bhavcopy_file_by_name(date_obj,
                                                               f"BhavCopy_NSE_FO_0_0_0_{date_obj.strftime('%Y%m%d')}_F_0000.csv.zip",
                                                               "Bhavcopy_Downloads/NSE", is_zip=True)
            if not fno_path:  # Try nsccl.YYYYMMDD.s.zip (from your new list, general NSCCL FO)
                fno_path = self.download_bhavcopy_file_by_name(date_obj, f"nsccl.{date_obj.strftime('%Y%m%d')}.s.zip",
                                                               "Bhavcopy_Downloads/NSE", is_zip=True)
            if fno_path: print(f"Found F&O Bhavcopy: {os.path.basename(fno_path)}")

            # --- Index Bhavcopy ---
            print(f"Trying to find Index Bhavcopy for {target_date_str_dmy}...")
            index_path = self.download_bhavcopy_file_by_name(date_obj, f"ind_close_all_{target_date_str_dmy}.csv",
                                                             "Bhavcopy_Downloads/NSE")
            if not index_path:
                print(
                    f"Standard Index Bhavcopy not found. Relying on process_and_save_bhavcopy_files to potentially extract from other files.")
                # For now, if ind_close_all.csv not found, we rely on the process_and_save_bhavcopy_files
                # to potentially extract index info from other files if it's present or just skip.
                pass

                # --- Block Deals (separate file) ---
            print(f"Trying to find Block Deals file for {target_date_str_dmy}...")
            block_path = self.download_bhavcopy_file_by_name(date_obj, "block.csv", "Bhavcopy_Downloads/NSE")
            if block_path: print(f"Found Block Deals file: {os.path.basename(block_path)}")

            # --- Bulk Deals (separate file) ---
            print(f"Trying to find Bulk Deals file for {target_date_str_dmy}...")
            bulk_path = self.download_bhavcopy_file_by_name(date_obj, "bulk.csv", "Bhavcopy_Downloads/NSE")
            if bulk_path: print(f"Found Bulk Deals file: {os.path.basename(bulk_path)}")

            if cm_path or fno_path or index_path or block_path or bulk_path:  # Changed to OR any path is found
                downloaded_files_str = f"{'CM ' if cm_path else ''}{'F&O ' if fno_path else ''}{'Index ' if index_path else ''}{'Block ' if block_path else ''}{'Bulk ' if bulk_path else ''}"
                print(f"Bhavcopy files for {target_date_str_dmy} downloaded ({downloaded_files_str.strip()}).")

                # Pass file paths to processing function, so it knows which files to read
                extracted_data = self.process_and_save_bhavcopy_files(
                    target_date_str_dmy, cm_path, fno_path, index_path, block_path, bulk_path  # Pass bulk_path
                )

                if extracted_data and (
                        extracted_data.get("equities") or extracted_data.get("indices") or extracted_data.get(
                    "fno_data") or extracted_data.get("block_deals")):
                    print(
                        f"Successfully extracted {len(extracted_data.get('equities', []))} equity, {len(extracted_data.get('fno_data', []))} F&O, {len(extracted_data.get('indices', []))} index, and {len(extracted_data.get('block_deals', []))} block deal records from Bhavcopy.")
                    latest_bhavcopy_data = extracted_data  # Update global cache
                    print("Bhavcopy data extracted, stored in memory, and saved to DB.")
                    socketio.emit('bhavcopy_update', latest_bhavcopy_data)
                else:
                    print(f"Bhavcopy downloaded for {target_date_str_dmy}, but no relevant data was extracted.")
                return True
            else:
                print(f"No Bhavcopy files found for {target_date_str_dmy}.")
                return False
        finally:
            self.bhavcopy_running.clear()

    def process_and_save_bhavcopy_files(self, target_date_str_dmy, cm_file_path=None, fno_file_path=None,
                                        index_file_path=None, block_file_path=None, bulk_file_path=None):
        date_obj = datetime.datetime.strptime(target_date_str_dmy, '%d%m%Y')
        db_date_str = date_obj.strftime('%Y-%m-%d')

        all_equities_for_date = []
        all_indices_for_date = []
        all_fno_data_for_date = []
        all_block_deals_for_date = []  # This will now be populated from block.csv or bulk.csv

        # --- Process CM Bhavcopy ---
        # Read from cm_file_path if available
        if cm_file_path and os.path.exists(cm_file_path):
            try:
                # If it's a ZIP, extract first
                if cm_file_path.endswith('.zip'):
                    with ZipFile(cm_file_path, 'r') as zip_ref:
                        csv_name = zip_ref.namelist()[0]
                        with zip_ref.open(csv_name) as csv_file:
                            df = pd.read_csv(csv_file, skipinitialspace=True)
                else:
                    df = pd.read_csv(cm_file_path, skipinitialspace=True)

                df.columns = [col.strip().upper() for col in df.columns]

                print(f"--- CM Bhavcopy for {db_date_str} loaded ---")
                df['SYMBOL'] = df['SYMBOL'].str.strip()
                df['SERIES'] = df['SERIES'].str.strip()

                if 'TRADING_TYPE' not in df.columns:
                    print("WARNING: 'TRADING_TYPE' column not found in CM Bhavcopy. Defaulting to 'EQ'.")
                    df['TRADING_TYPE'] = 'EQ'
                df['TRADING_TYPE'] = df['TRADING_TYPE'].fillna('EQ').str.strip()
                df['DELIV_PER'] = df['DELIV_PER'].replace('-', 'N/A')

                eq_df = df[df['SERIES'] == 'EQ'].copy()

                print(f"CM EQ (all) count: {len(eq_df)}")
                for index, r in eq_df.iterrows():
                    sym = r['SYMBOL']
                    prev_close = r.get('PREV_CLOSE', None)
                    close_price = r.get('CLOSE_PRICE', None)
                    volume = r.get('TTL_TRD_QNTY', None)
                    open_price = r.get('OPEN_PRICE', None)
                    high_price = r.get('HIGH_PRICE', None)
                    low_price = r.get('LOW_PRICE', None)
                    trading_type = r.get('TRADING_TYPE', 'EQ')

                    chg = ((
                                   close_price - prev_close) / prev_close * 100) if prev_close and prev_close > 0 and close_price is not None else 0
                    delivery_str = str(r.get('DELIV_PER', 'N/A')).strip()
                    delivery = delivery_str if delivery_str != 'N/A' else 'N/A'

                    all_equities_for_date.append({
                        "Symbol": sym,
                        "Close": round(close_price, 2) if close_price is not None else None,
                        "Volume": int(volume) if volume is not None else None,
                        "Pct Change": round(chg, 2),
                        "Delivery %": delivery,
                        "Open": round(open_price, 2) if open_price is not None else None,
                        "High": round(high_price, 2) if high_price is not None else None,
                        "Low": round(low_price, 2) if low_price is not None else None,
                        "Prev Close": round(prev_close, 2) if prev_close is not None else None,
                        "Trading Type": trading_type
                    })
            except KeyError as e:
                print(
                    f"CRITICAL ERROR: A required column is missing in the CM Bhavcopy CSV: {e}. Data extraction failed.")
            except Exception as e:
                print(f"Error reading CM Bhavcopy file: {e}")

        # --- Process F&O Bhavcopy ---
        if fno_file_path and os.path.exists(fno_file_path):
            try:
                # Assuming F&O files are always ZIPs
                with ZipFile(fno_file_path, 'r') as zip_ref:
                    csv_name = zip_ref.namelist()[0]
                    with zip_ref.open(csv_name) as csv_file:
                        df_fo = pd.read_csv(csv_file, skipinitialspace=True)
                        df_fo.columns = [col.strip().upper() for col in df_fo.columns]

                        print(f"--- F&O Bhavcopy for {db_date_str} loaded ---")
                        # Adjusted column names based on common NSE F&O bhavcopy format
                        df_fo['OPEN_INTEREST'] = pd.to_numeric(df_fo.get('OPEN_INT', df_fo.get('OPEN_INTEREST')),
                                                               errors='coerce').fillna(0).astype(int)
                        df_fo['CHG_IN_OI'] = pd.to_numeric(df_fo.get('CHG_IN_OI', df_fo.get('CHANGE_IN_OI')),
                                                           errors='coerce').fillna(0).astype(int)
                        df_fo['CONTRACTS'] = pd.to_numeric(df_fo.get('CONTRACTS', df_fo.get('TRADED_QTY')),
                                                           errors='coerce').fillna(0).astype(int)
                        df_fo['CLOSE'] = pd.to_numeric(df_fo.get('CLOSE', df_fo.get('LAST_PRICE')),
                                                       errors='coerce').fillna(0).round(2)
                        df_fo['STRIKE_PR'] = pd.to_numeric(df_fo.get('STRIKE_PR', df_fo.get('STRIKE_PRICE')),
                                                           errors='coerce')
                        df_fo['OPTION_TYP'] = df_fo.get('OPTION_TYP', df_fo.get('OPTION_TYPE')).fillna('N/A')

                        df_fo['DELIV_PER'] = pd.to_numeric(df_fo.get('DELIV_PER', pd.Series(np.nan, index=df_fo.index)),
                                                           errors='coerce').fillna(np.nan)

                        relevant_fo_df = df_fo[
                            (df_fo['INSTRUMENT'].isin(['FUTSTK', 'FUTIDX', 'OPTSTK', 'OPTIDX']))
                        ].copy()
                        print(f"F&O relevant entries count: {len(relevant_fo_df)}")
                        for index, r in relevant_fo_df.iterrows():
                            all_fno_data_for_date.append({
                                "Symbol": r['SYMBOL'],
                                "Instrument": r['INSTRUMENT'],
                                "Expiry Date": r.get('EXPIRY_DT', r.get('EXPIRY_DATE', None)),
                                "Strike Price": r['STRIKE_PR'] if not pd.isna(r['STRIKE_PR']) else None,
                                "Option Type": r['OPTION_TYP'] if r['OPTION_TYP'] != 'N/A' else None,
                                "Open Interest": r['OPEN_INTEREST'],
                                "Change in OI": r['CHG_IN_OI'],
                                "Volume": r['CONTRACTS'],
                                "Close": r['CLOSE'],
                                "Delivery %": r['DELIV_PER'] if not pd.isna(r['DELIV_PER']) else None
                            })
            except KeyError as e:
                print(
                    f"CRITICAL ERROR: A required column is missing in the F&O Bhavcopy CSV: {e}. Data extraction failed.")
            except Exception as e:
                print(f"Error reading F&O Bhavcopy file: {e}")

        # --- Process Index Bhavcopy ---
        if index_file_path and os.path.exists(index_file_path):
            try:
                df_idx = pd.read_csv(index_file_path, skipinitialspace=True)
                df_idx.columns = [col.strip().upper() for col in df_idx.columns]

                print(f"--- Index Bhavcopy for {db_date_str} loaded ---")
                for index, r_idx in df_idx.iterrows():
                    index_name_raw = r_idx.get('INDEX NAME', r_idx.get('INDEX', '')).strip()
                    index_name_processed = index_name_raw.replace(' ', '_').upper()

                    mapped_symbol = None
                    for sym_key, map_value in self.BHAVCOPY_INDICES_MAP.items():
                        if map_value.replace(' ', '_').upper() == index_name_processed:
                            mapped_symbol = sym_key
                            break

                    if mapped_symbol and mapped_symbol in NSE_INDEX_BHAVCOPY_SYMBOLS:
                        close_price = r_idx.get('CLOSING INDEX VALUE', None)
                        open_price = r_idx.get('OPEN INDEX VALUE', None)
                        high_price = r_idx.get('HIGH INDEX VALUE', None)
                        low_price = r_idx.get('LOW INDEX VALUE', None)
                        pct_change = r_idx.get('CHANGE(%)', None)

                        try:
                            close_price = float(str(close_price).replace(',', '')) if close_price is not None else None
                            open_price = float(str(open_price).replace(',', '')) if open_price is not None else None
                            high_price = float(str(high_price).replace(',', '')) if high_price is not None else None
                            low_price = float(str(low_price).replace(',', '')) if low_price is not None else None
                            pct_change = float(str(pct_change).replace(',', '')) if pct_change is not None else None
                        except ValueError:
                            print(f"WARNING: Could not convert numeric index value for {index_name_raw}. Skipping.")
                            continue

                        if close_price is not None:
                            all_indices_for_date.append({
                                "Symbol": mapped_symbol,
                                "Close": round(float(close_price), 2),
                                "Pct Change": round(float(pct_change), 2) if pct_change is not None else 0.0,
                                "Open": round(float(open_price), 2) if open_price is not None else None,
                                "High": round(float(high_price), 2) if high_price is not None else None,
                                "Low": round(float(low_price), 2) if low_price is not None else None
                            })
            except KeyError as e:
                print(
                    f"CRITICAL ERROR: A required column is missing in the Index Bhavcopy CSV: {e}. Data extraction failed.")
            except Exception as e:
                print(f"Error reading Index Bhavcopy file: {e}")

        # --- Process Block Deal (block.csv) ---
        if block_file_path and os.path.exists(block_file_path):
            try:
                df_block = pd.read_csv(block_file_path, skipinitialspace=True)
                df_block.columns = [col.strip().upper() for col in df_block.columns]

                print(f"--- Block Deals (block.csv) for {db_date_str} loaded ---")
                for index, r in df_block.iterrows():
                    # Assuming columns like 'SYMBOL', 'NO_OF_SHARES', 'TRADE_PRICE' (common for block/bulk)
                    all_block_deals_for_date.append({
                        "Symbol": r['SYMBOL'],
                        "Trade Type": r.get('TRADE_TYPE', 'B'),  # Default to 'B' if not specified
                        "Quantity": int(r['NO_OF_SHARES']),  # Corrected column name
                        "Price": round(float(r['TRADE_PRICE']), 2)  # Corrected column name
                    })
                print(f"Block Deals count from block.csv: {len(all_block_deals_for_date)}")

            except KeyError as e:
                print(
                    f"CRITICAL ERROR: A required column is missing in the Block Deal CSV (block.csv): {e}. Data extraction failed.")
            except Exception as e:
                print(f"Error reading Block Deal file (block.csv): {e}")

        # --- Process Bulk Deal (bulk.csv) ---
        if bulk_file_path and os.path.exists(bulk_file_path):
            try:
                df_bulk = pd.read_csv(bulk_file_path, skipinitialspace=True)
                df_bulk.columns = [col.strip().upper() for col in df_bulk.columns]

                print(f"--- Bulk Deals (bulk.csv) for {db_date_str} loaded ---")
                for index, r in df_bulk.iterrows():
                    # Assuming columns like 'SYMBOL', 'NO_OF_SHARES', 'TRADE_PRICE'
                    all_block_deals_for_date.append({  # Append to the same list for BDGP strategy
                        "Symbol": r['SYMBOL'],
                        "Trade Type": r.get('TRADE_TYPE', 'K'),  # Use a different type for Bulk deals, e.g., 'K'
                        "Quantity": int(r['NO_OF_SHARES']),  # Corrected column name
                        "Price": round(float(r['TRADE_PRICE']), 2)  # Corrected column name
                    })
                print(
                    f"Bulk Deals count from bulk.csv: {len(df_bulk)}. Total block/bulk deals: {len(all_block_deals_for_date)}")

            except KeyError as e:
                print(
                    f"CRITICAL ERROR: A required column is missing in the Bulk Deal CSV (bulk.csv): {e}. Data extraction failed.")
            except Exception as e:
                print(f"Error reading Bulk Deal file (bulk.csv): {e}")

        if self.conn:
            cur = self.conn.cursor()
            # Save Equity Data
            if all_equities_for_date:
                # Clear previous EQ data for this date to avoid duplicates on re-processing
                cur.execute("DELETE FROM bhavcopy_data WHERE date = ? AND trading_type = 'EQ'", (db_date_str,))
                for stock in all_equities_for_date:
                    cur.execute(
                        "INSERT OR REPLACE INTO bhavcopy_data (date, symbol, close, volume, pct_change, delivery_pct, open, high, low, prev_close, trading_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            db_date_str,
                            stock['Symbol'],
                            stock['Close'],
                            stock['Volume'],
                            stock['Pct Change'],
                            str(stock['Delivery %']),
                            stock['Open'],
                            stock['High'],
                            stock['Low'],
                            stock['Prev Close'],
                            stock['Trading Type']
                        )
                    )
                print(f"Saved {len(all_equities_for_date)} equity bhavcopy records for {db_date_str} to the database.")

            # Save Index Data
            if all_indices_for_date:
                cur.execute("DELETE FROM index_bhavcopy_data WHERE date = ?", (db_date_str,))
                for index_data in all_indices_for_date:
                    cur.execute(
                        "INSERT OR REPLACE INTO index_bhavcopy_data (date, symbol, close, pct_change, open, high, low) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            db_date_str,
                            index_data['Symbol'],
                            index_data['Close'],
                            index_data['Pct Change'],
                            index_data['Open'],
                            index_data['High'],
                            index_data['Low']
                        )
                    )
                print(f"Saved {len(all_indices_for_date)} index bhavcopy records for {db_date_str} to the database.")

            # Save F&O Data
            if all_fno_data_for_date:
                cur.execute("DELETE FROM fno_bhavcopy_data WHERE date = ?", (db_date_str,))
                for fno_item in all_fno_data_for_date:
                    cur.execute(
                        "INSERT OR REPLACE INTO fno_bhavcopy_data (date, symbol, instrument, expiry_date, strike_price, option_type, open_interest, change_in_oi, volume, close, delivery_pct) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            db_date_str,
                            fno_item['Symbol'],
                            fno_item['Instrument'],
                            fno_item['Expiry Date'],
                            fno_item['Strike Price'],
                            fno_item['Option Type'],
                            fno_item['Open Interest'],
                            fno_item['Change in OI'],
                            fno_item['Volume'],
                            fno_item['Close'],
                            fno_item['Delivery %']
                        )
                    )
                print(f"Saved {len(all_fno_data_for_date)} F&O bhavcopy records for {db_date_str} to the database.")

            # Save Block Deal Data (from either block.csv or bulk.csv)
            if all_block_deals_for_date:
                cur.execute("DELETE FROM block_deal_data WHERE date = ?", (db_date_str,))
                for block_deal in all_block_deals_for_date:
                    cur.execute(
                        "INSERT OR REPLACE INTO block_deal_data (date, symbol, trade_type, quantity, price) VALUES (?, ?, ?, ?, ?)",
                        (
                            db_date_str,
                            block_deal['Symbol'],
                            block_deal['Trade Type'],
                            block_deal['Quantity'],
                            block_deal['Price']
                        )
                    )
                print(
                    f"Saved {len(all_block_deals_for_date)} block/bulk deal records for {db_date_str} to the database.")

            self.conn.commit()

        return {"equities": all_equities_for_date, "indices": all_indices_for_date, "fno_data": all_fno_data_for_date,
                "block_deals": all_block_deals_for_date, "date": db_date_str}

    def _get_bhavcopy_for_date(self, date_str: str) -> Dict[str, Any]:
        """Helper to fetch all relevant bhavcopy data for a given date."""
        print(f"DEBUG: _get_bhavcopy_for_date called for date: {date_str}")
        cur = self.conn.cursor()

        # Fetch ALL equity data (only 'EQ' trading type) for the date
        cur.execute(
            "SELECT symbol, close, volume, pct_change, delivery_pct, open, high, low, prev_close, trading_type FROM bhavcopy_data WHERE date = ? AND trading_type = 'EQ'",
            (date_str,)
        )
        equities = [{
            "Symbol": row[0], "Close": row[1], "Volume": row[2], "Pct Change": row[3],
            "Delivery %": float(str(row[4]).replace('%', '')) if isinstance(row[4], str) and str(
                row[4]).strip() != 'N/A' else None,  # Ensure float conversion
            "Open": row[5], "High": row[6], "Low": row[7], "Prev Close": row[8], "Trading Type": row[9]
        } for row in cur.fetchall()]
        print(f"DEBUG: _get_bhavcopy_for_date - Found {len(equities)} EQ records for {date_str}")

        # Fetch index data
        cur.execute(
            "SELECT symbol, close, pct_change, open, high, low FROM index_bhavcopy_data WHERE date = ?",
            (date_str,)
        )
        indices = [{
            "Symbol": row[0], "Close": row[1], "Pct Change": row[2],
            "Open": row[3], "High": row[4], "Low": row[5]
        } for row in cur.fetchall()]
        print(f"DEBUG: _get_bhavcopy_for_date - Found {len(indices)} Index records for {date_str}")

        # Fetch F&O data (futures and options)
        cur.execute(
            "SELECT symbol, instrument, expiry_date, strike_price, option_type, open_interest, change_in_oi, volume, close, delivery_pct FROM fno_bhavcopy_data WHERE date = ?",
            (date_str,)
        )
        fno_data = [{
            "Symbol": row[0], "Instrument": row[1], "Expiry Date": row[2], "Strike Price": row[3],
            "Option Type": row[4], "Open Interest": row[5], "Change in OI": row[6], "Volume": row[7],
            "Close": row[8], "Delivery %": row[9]
        } for row in cur.fetchall()]
        print(f"DEBUG: _get_bhavcopy_for_date - Found {len(fno_data)} F&O records for {date_str}")

        # Fetch block deal data
        cur.execute(
            "SELECT symbol, trade_type, quantity, price FROM block_deal_data WHERE date = ?",
            (date_str,)
        )
        block_deals = [{
            "Symbol": row[0], "Trade Type": row[1], "Quantity": row[2], "Price": row[3]
        } for row in cur.fetchall()]
        print(f"DEBUG: _get_bhavcopy_for_date - Found {len(block_deals)} Block Deal records for {date_str}")

        return {"equities": equities, "indices": indices, "fno_data": fno_data, "block_deals": block_deals}

    def _get_historical_bhavcopy_for_stock(self, symbol: str, start_date_str: str, end_date_str: str, limit: int) -> \
            List[Dict[str, Any]]:
        """
        Fetches historical equity bhavcopy data for a specific stock over a period.
        `limit` is the maximum number of records to return.
        """
        cur = self.conn.cursor()
        cur.execute(
            "SELECT date, close, volume, delivery_pct, open, high, low, prev_close FROM bhavcopy_data WHERE symbol = ? AND date BETWEEN ? AND ? AND trading_type = 'EQ' ORDER BY date DESC LIMIT ?",
            (symbol, start_date_str, end_date_str, limit)
        )
        history = []
        for row in cur.fetchall():
            history.append({
                "date": row[0],
                "Close": row[1],
                "Volume": row[2],
                "Delivery %": float(str(row[3]).replace('%', '')) if isinstance(row[3], str) and str(
                    row[3]).strip() != 'N/A' else None,
                "Open": row[4],
                "High": row[5],
                "Low": row[6],
                "Prev Close": row[7]
            })
        return history

    # --- STRATEGY IMPLEMENTATIONS ---

    def analyze_fii_dii_delivery_divergence(self, date_str: str) -> List[Dict[str, Any]]:
        """
        Strategy 1: FII-DII Delivery Divergence Trap (FDDT)
        Concept: FIIs & DIIs report net positions daily, but Bhavcopy reveals exact delivery footprints stock-by-stock.
        Signal: FII Net Buy > ₹200 Cr, DII Net Sell > ₹150 Cr, Delivery % < 18%.
        """
        # --- PLACEHOLDER ---
        # This strategy requires integrating FII/DII activity CSV data.
        # Without that, it cannot be fully implemented.
        # For now, it will return a placeholder message.
        return [{"Symbol": "N/A", "Signal": "Requires FII/DII data integration."}]

    def analyze_zero_delivery_future_roll(self, date_str: str) -> List[Dict[str, Any]]:
        """
        Strategy 2: Zero-Delivery Future Roll Trap (ZDFT)
        Concept: Near-month future closes with < 0.8% delivery but OI explodes in next month.
        Signal: Short on expiry day close.
        """
        signals = []
        try:
            current_day_data = self._get_bhavcopy_for_date(date_str)
            current_fno_df = pd.DataFrame(current_day_data['fno_data'])

            if current_fno_df.empty:
                print(f"DEBUG: ZDFT for {date_str} - No F&O data available.")
                return [{"Symbol": "N/A", "Near Month Delivery %": "N/A", "Next Month OI Increase %": "N/A",
                         "Signal": "No F&O data for " + date_str}]

            # Filter for Futures (FUTSTK only, as per problem description)
            futures_df = current_fno_df[current_fno_df['Instrument'] == 'FUTSTK'].copy()

            if futures_df.empty:
                print(f"DEBUG: ZDFT for {date_str} - No FUTSTK data available.")
                return [{"Symbol": "N/A", "Near Month Delivery %": "N/A", "Next Month OI Increase %": "N/A",
                         "Signal": "No Futures (FUTSTK) data for " + date_str}]

            # Get previous 2 trading days for 2-day OI change
            current_date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            prev_trading_day_str_1 = self._get_previous_trading_day(current_date_obj, 1)  # 1 day ago
            prev_trading_day_str_2 = self._get_previous_trading_day(current_date_obj, 2)  # 2 days ago

            if not prev_trading_day_str_1 or not prev_trading_day_str_2:
                print(f"DEBUG: ZDFT for {date_str} - Not enough previous trading days for OI comparison.")
                return [{"Symbol": "N/A", "Near Month Delivery %": "N/A", "Next Month OI Increase %": "N/A",
                         "Signal": "Not enough previous trading days for OI comparison."}]

            prev_day_data_2 = self._get_bhavcopy_for_date(prev_trading_day_str_2)  # Data from 2 days ago
            prev_fno_df_2 = pd.DataFrame(prev_day_data_2['fno_data'])

            if prev_fno_df_2.empty:
                print(f"DEBUG: ZDFT for {date_str} - No F&O data from 2 days ago for OI comparison.")
                return [{"Symbol": "N/A", "Near Month Delivery %": "N/A", "Next Month OI Increase %": "N/A",
                         "Signal": "No F&O data from 2 days ago for OI comparison."}]

            prev_futures_df_2 = prev_fno_df_2[prev_fno_df_2['Instrument'] == 'FUTSTK']

            for symbol in futures_df['Symbol'].unique():
                stock_futures = futures_df[futures_df['Symbol'] == symbol].copy()

                # Sort by expiry date to find near and next month
                stock_futures['Expiry Date'] = pd.to_datetime(stock_futures['Expiry Date'])
                stock_futures = stock_futures.sort_values(by='Expiry Date')

                # Need at least two expiries to check next month roll
                if len(stock_futures) >= 2:
                    near_month_future = stock_futures.iloc[0]
                    next_month_future = stock_futures.iloc[1]

                    near_month_delivery_pct = near_month_future['Delivery %']

                    next_month_oi_current = next_month_future['Open Interest']

                    prev_next_month_future_2_days_ago = prev_futures_df_2[
                        (prev_futures_df_2['Symbol'] == symbol) &
                        (prev_futures_df_2['Instrument'] == 'FUTSTK') &
                        (pd.to_datetime(prev_futures_df_2['Expiry Date']) == next_month_future['Expiry Date'])
                        # Compare datetime objects
                        ]

                    if not prev_next_month_future_2_days_ago.empty:
                        next_month_oi_2_days_ago = prev_next_month_future_2_days_ago.iloc[0]['Open Interest']
                        oi_increase_pct = ((
                                                   next_month_oi_current - next_month_oi_2_days_ago) / next_month_oi_2_days_ago * 100) if next_month_oi_2_days_ago > 0 else float(
                            'inf')

                        # Condition: near-month delivery < 0.8% AND next-month OI increased > 40% in 2 days
                        if (near_month_delivery_pct is not None and near_month_delivery_pct < 0.8) and \
                                (oi_increase_pct > 40):
                            signals.append({
                                "Symbol": symbol,
                                "Near Month Delivery %": f"{near_month_delivery_pct:.2f}%",
                                "Next Month OI Increase %": f"{oi_increase_pct:.2f}%",
                                "Signal": "Zero-Delivery Future Roll Trap: Short on expiry day close."
                            })

        except Exception as e:
            print(f"Error in Zero-Delivery Future Roll Trap for {date_str}: {e}")
            signals.append({"Symbol": "N/A", "Near Month Delivery %": "N/A", "Next Month OI Increase %": "N/A",
                            "Signal": f"Error during analysis: {e}"})

        return signals if signals else [
            {"Symbol": "N/A", "Near Month Delivery %": "N/A", "Next Month OI Increase %": "N/A",
             "Signal": "No Zero-Delivery Future Roll Trap signals found for " + date_str}]

    def analyze_block_deal_ghost_pump(self, date_str: str) -> List[Dict[str, Any]]:
        """
        Strategy 3: Block-Deal Ghost Pump (BDGP)
        Concept: Stock opens +4% with huge volume, but block deal was done at -8% discount.
        Signal: Short at open, target previous close.
        """
        signals = []
        try:
            current_day_data = self._get_bhavcopy_for_date(date_str)
            equities_df = pd.DataFrame(current_day_data['equities'])
            block_deals_df = pd.DataFrame(current_day_data['block_deals'])

            print(
                f"DEBUG: BDGP for {date_str} - equities_df count: {len(equities_df)}, block_deals_df count: {len(block_deals_df)}")

            if equities_df.empty and block_deals_df.empty:  # Only return error if BOTH are empty
                return [{"Symbol": "N/A", "Block Deal Price": "N/A", "Previous Close": "N/A", "Open Price": "N/A",
                         "Block Discount %": "N/A", "Open Gap Up %": "N/A",
                         "Signal": "No Equity or Block Deal data for " + date_str}]
            elif equities_df.empty:
                return [{"Symbol": "N/A", "Block Deal Price": "N/A", "Previous Close": "N/A", "Open Price": "N/A",
                         "Block Discount %": "N/A", "Open Gap Up %": "N/A", "Signal": "No Equity data for " + date_str}]
            elif block_deals_df.empty:
                return [{"Symbol": "N/A", "Block Deal Price": "N/A", "Previous Close": "N/A", "Open Price": "N/A",
                         "Block Discount %": "N/A", "Open Gap Up %": "N/A",
                         "Signal": "No Block Deal data for " + date_str}]

            for index, bd in block_deals_df.iterrows():
                symbol = bd['Symbol']
                bd_price = bd['Price']

                equity_data = equities_df[equities_df['Symbol'] == symbol]
                if not equity_data.empty:
                    eq = equity_data.iloc[0]
                    prev_close = eq['Prev Close']
                    current_open = eq['Open']

                    if prev_close is not None and current_open is not None and bd_price is not None:
                        # Block price < previous close * 0.92 (-8% discount)
                        is_discount_block_deal = (
                                bd_price < prev_close * 0.92) if prev_close and prev_close > 0 else False
                        # Today open > previous close * 1.04 (+4% open gap)
                        is_open_gap_up = (current_open > prev_close * 1.04) if prev_close and prev_close > 0 else False

                        if is_discount_block_deal and is_open_gap_up:
                            signals.append({
                                "Symbol": symbol,
                                "Block Deal Price": f"{bd_price:.2f}",
                                "Previous Close": f"{prev_close:.2f}",
                                "Open Price": f"{current_open:.2f}",
                                "Block Discount %": f"{((bd_price - prev_close) / prev_close * 100):.2f}%",
                                "Open Gap Up %": f"{((current_open - prev_close) / prev_close * 100):.2f}%",
                                "Signal": "Block-Deal Ghost Pump: Short at open."
                            })
        except Exception as e:
            print(f"Error in Block-Deal Ghost Pump for {date_str}: {e}")
            signals.append({"Symbol": "N/A", "Block Deal Price": "N/A", "Previous Close": "N/A", "Open Price": "N/A",
                            "Block Discount %": "N/A", "Open Gap Up %": "N/A", "Signal": f"Error during analysis: {e}"})

        return signals if signals else [
            {"Symbol": "N/A", "Block Deal Price": "N/A", "Previous Close": "N/A", "Open Price": "N/A",
             "Block Discount %": "N/A", "Open Gap Up %": "N/A",
             "Signal": "No Block-Deal Ghost Pump signals found for " + date_str}]

    def analyze_vwap_anchor_reversion(self, date_str: str) -> List[Dict[str, Any]]:
        """
        Strategy 4: VWAP Anchor Reversion (VAR-7)
        Concept: Stocks that close > +7% from VWAP with delivery < 12% revert hard next day.
        Signal: Short next day open.
        """
        signals = []
        try:
            current_day_data = self._get_bhavcopy_for_date(date_str)
            equities_df = pd.DataFrame(current_day_data['equities'])
            print(f"DEBUG: VAR-7 for {date_str} - equities_df count: {len(equities_df)}")

            if equities_df.empty:
                return [{"Symbol": "N/A", "Close Price": "N/A", "VWAP": "N/A", "Close vs VWAP %": "N/A",
                         "Delivery %": "N/A", "Signal": "No Equity data for " + date_str}]

            for index, eq in equities_df.iterrows():
                symbol = eq['Symbol']
                volume = eq['Volume']
                close_price = eq['Close']
                open_price = eq['Open']
                high_price = eq['High']
                low_price = eq['Low']
                delivery_pct = eq['Delivery %']

                if all(x is not None for x in [volume, close_price, open_price, high_price,
                                               low_price]) and delivery_pct is not None and volume > 0:
                    # Calculate VWAP using (High + Low + Close) / 3 if no Turnover directly in this table
                    vwap = (high_price + low_price + close_price) / 3.0

                    if vwap > 0:
                        close_vs_vwap_pct = ((close_price - vwap) / vwap * 100)

                        if close_vs_vwap_pct > 7 and delivery_pct is not None and delivery_pct < 12:  # Close > +7% from VWAP and delivery < 12%
                            signals.append({
                                "Symbol": symbol,
                                "Close Price": f"{close_price:.2f}",
                                "VWAP": f"{vwap:.2f}",
                                "Close vs VWAP %": f"{close_vs_vwap_pct:.2f}%",
                                "Delivery %": f"{delivery_pct:.2f}%",
                                "Signal": "VWAP Anchor Reversion: Short next day open."
                            })
        except Exception as e:
            print(f"Error in VWAP Anchor Reversion for {date_str}: {e}")
            signals.append(
                {"Symbol": "N/A", "Close Price": "N/A", "VWAP": "N/A", "Close vs VWAP %": "N/A", "Delivery %": "N/A",
                 "Signal": f"Error during analysis: {e}"})

        return signals if signals else [
            {"Symbol": "N/A", "Close Price": "N/A", "VWAP": "N/A", "Close vs VWAP %": "N/A", "Delivery %": "N/A",
             "Signal": "No VWAP Anchor Reversion signals found for " + date_str}]

    def analyze_hidden_bonus_arbitrage(self, date_str: str) -> List[Dict[str, Any]]:
        """
        Strategy 5: Hidden Bonus Arbitrage (HBA-21)
        Requires NSE corporate action list and multi-day delivery data.
        This is a placeholder.
        """
        return [{"Symbol": "N/A", "Signal": "Requires Corporate Actions data and multi-day delivery trend analysis."}]

    def analyze_oi_momentum_trap(self, date_str: str) -> List[Dict[str, Any]]:
        """
        Strategy 6: OI Momentum Trap (Long Buildup vs. Short Covering Detector)
        Concept: Classify based on Price Change and OI Change. Focus on long buildup.
        """
        signals = []
        try:
            current_day_data = self._get_bhavcopy_for_date(date_str)
            current_equities_df = pd.DataFrame(current_day_data['equities'])
            current_fno_df = pd.DataFrame(current_day_data['fno_data'])

            print(
                f"DEBUG: OIMT for {date_str} - current_equities_df count: {len(current_equities_df)}, current_fno_df count: {len(current_fno_df)}")

            if current_equities_df.empty or current_fno_df.empty:
                return [{"Symbol": "N/A", "Price Change %": "N/A", "OI Change": "N/A", "Signal Type": "N/A",
                         "Institutional Conviction (Delivery > 60%)": "N/A",
                         "Signal": "No Equity or F&O data for " + date_str}]

            # Get the date of the previous trading day
            current_date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            prev_trading_day_str = self._get_previous_trading_day(current_date_obj, 1)
            if not prev_trading_day_str:
                print(f"DEBUG: OIMT for {date_str} - Not enough previous trading days for OI Momentum Trap.")
                return [{"Symbol": "N/A", "Price Change %": "N/A", "OI Change": "N/A", "Signal Type": "N/A",
                         "Institutional Conviction (Delivery > 60%)": "N/A",
                         "Signal": "Not enough previous trading days for OI Momentum Trap."}]

            prev_day_data = self._get_bhavcopy_for_date(prev_trading_day_str)
            prev_equities_df = pd.DataFrame(prev_day_data['equities'])
            prev_fno_df = pd.DataFrame(prev_day_data['fno_data'])
            print(
                f"DEBUG: OIMT for {date_str} - prev_equities_df count: {len(prev_equities_df)}, prev_fno_df count: {len(prev_fno_df)}")

            if prev_equities_df.empty or prev_fno_df.empty:
                print(f"DEBUG: OIMT for {date_str} - No previous day Equity or F&O data for OI Momentum Trap.")
                return [{"Symbol": "N/A", "Price Change %": "N/A", "OI Change": "N/A", "Signal Type": "N/A",
                         "Institutional Conviction (Delivery > 60%)": "N/A",
                         "Signal": "No previous day Equity or F&O data for OI Momentum Trap."}]

            # Focus on FUTSTK for OI analysis (assuming near month for simplicity)
            current_futures_df = current_fno_df[current_fno_df['Instrument'] == 'FUTSTK'].copy()
            prev_futures_df = prev_fno_df[prev_fno_df['Instrument'] == 'FUTSTK'].copy()

            for symbol in current_futures_df['Symbol'].unique():
                stock_futures = current_futures_df[current_futures_df['Symbol'] == symbol]
                prev_stock_futures = prev_futures_df[prev_futures_df['Symbol'] == symbol]

                current_eq = current_equities_df[current_equities_df['Symbol'] == symbol]
                prev_eq = prev_equities_df[prev_equities_df['Symbol'] == symbol]

                if not stock_futures.empty and not prev_stock_futures.empty and not current_eq.empty and not prev_eq.empty:
                    # Get near-month futures (simplification: just take the first one if multiple expiries)
                    current_fut_near = stock_futures.sort_values(by='Expiry Date').iloc[0]
                    prev_fut_near = prev_stock_futures.sort_values(by='Expiry Date').iloc[
                        0]  # Use prev_stock_futures here

                    current_eq_row = current_eq.iloc[0]
                    prev_eq_row = prev_eq.iloc[0]

                    price_change_pct = (
                            (current_eq_row['Close'] - prev_eq_row['Close']) / prev_eq_row['Close'] * 100) if \
                        prev_eq_row['Close'] > 0 else 0
                    oi_change = current_fut_near['Open Interest'] - prev_fut_near['Open Interest']

                    signal_type = "N/A"
                    if price_change_pct > 0 and oi_change > 0:
                        signal_type = "Long Buildup"
                    elif price_change_pct > 0 and oi_change < 0:
                        signal_type = "Short Covering"
                    elif price_change_pct < 0 and oi_change > 0:
                        signal_type = "Short Buildup"
                    elif price_change_pct < 0 and oi_change < 0:
                        signal_type = "Long Unwinding"

                    # Cross-check with delivery % from EQ Bhavcopy
                    delivery_pct = current_eq_row['Delivery %']
                    institutional_conviction = "No"
                    if delivery_pct is not None and delivery_pct != 'N/A' and float(
                            str(delivery_pct).replace('%', '')) > 60:
                        institutional_conviction = "Yes"

                    signals.append({
                        "Symbol": symbol,
                        "Price Change %": f"{price_change_pct:.2f}%",
                        "OI Change": f"{oi_change:,}",
                        "Signal Type": signal_type,
                        "Institutional Conviction (Delivery > 60%)": institutional_conviction
                    })

        except Exception as e:
            print(f"Error in OI Momentum Trap for {date_str}: {e}")
            signals.append({"Symbol": "N/A", "Price Change %": "N/A", "OI Change": "N/A", "Signal Type": "N/A",
                            "Institutional Conviction (Delivery > 60%)": "N/A",
                            "Signal": f"Error during analysis: {e}"})

        return signals if signals else [
            {"Symbol": "N/A", "Price Change %": "N/A", "OI Change": "N/A", "Signal Type": "N/A",
             "Institutional Conviction (Delivery > 60%)": "N/A",
             "Signal": "No OI Momentum Trap signals found for " + date_str}]

    def analyze_volume_surge_scanner(self, date_str: str) -> List[Dict[str, Any]]:
        """
        Strategy 7: Volume Surge Scanner for Swing Breakouts
        Concept: Today's volume > 2x avg. 10-day volume AND delivery % > 50%.
        """
        signals = []
        try:
            current_day_data = self._get_bhavcopy_for_date(date_str)
            current_equities_df = pd.DataFrame(current_day_data['equities'])

            print(f"DEBUG: VSSB for {date_str} - current_equities_df count: {len(current_equities_df)}")

            if current_equities_df.empty:
                return [{"Symbol": "N/A", "Today's Volume": "N/A", "10-Day Avg Volume": "N/A", "Volume Multiple": "N/A",
                         "Delivery %": "N/A", "Price Change %": "N/A", "Signal": "No Equity data for " + date_str}]

            current_date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()

            for index, eq in current_equities_df.iterrows():
                symbol = eq['Symbol']
                today_volume = eq['Volume']
                today_delivery_pct = eq['Delivery %']
                today_pct_change = eq['Pct Change']

                if today_volume is None or today_volume == 0 or today_delivery_pct is None or str(
                        today_delivery_pct).strip() == 'N/A':
                    continue

                # Convert delivery_pct to float
                today_delivery_pct_float = float(str(today_delivery_pct).replace('%', ''))

                # Fetch up to 10 previous trading days for average volume
                # Look back 30 calendar days to ensure we get 10 *trading* days
                historical_data = self._get_historical_bhavcopy_for_stock(
                    symbol,
                    (current_date_obj - datetime.timedelta(days=30)).strftime('%Y-%m-%d'),
                    # Look back more days to ensure 10 trading days
                    (current_date_obj - datetime.timedelta(days=1)).strftime('%Y-%m-%d'),  # Up to yesterday
                    10  # Limit to 10 records
                )
                historical_df = pd.DataFrame(historical_data)

                if len(historical_df) >= 10:  # Need at least 10 previous trading days for average
                    # Calculate average volume from previous days
                    avg_volume = historical_df['Volume'].mean()

                    if avg_volume > 0 and today_volume > (2 * avg_volume) and today_delivery_pct_float > 50:
                        signals.append({
                            "Symbol": symbol,
                            "Today's Volume": f"{today_volume:,}",
                            "10-Day Avg Volume": f"{avg_volume:,.0f}",
                            "Volume Multiple": f"{(today_volume / avg_volume):.2f}x",
                            "Delivery %": f"{today_delivery_pct:.2f}%",
                            "Price Change %": f"{today_pct_change:.2f}%",
                            "Signal": "Volume Surge: Potential Swing Breakout"
                        })
        except Exception as e:
            print(f"Error in Volume Surge Scanner for {date_str}: {e}")
            signals.append(
                {"Symbol": "N/A", "Today's Volume": "N/A", "10-Day Avg Volume": "N/A", "Volume Multiple": "N/A",
                 "Delivery %": "N/A", "Price Change %": "N/A", "Signal": f"Error during analysis: {e}"})

        return signals if signals else [
            {"Symbol": "N/A", "Today's Volume": "N/A", "10-Day Avg Volume": "N/A", "Volume Multiple": "N/A",
             "Delivery %": "N/A", "Price Change %": "N/A",
             "Signal": "No Volume Surge Scanner signals found for " + date_str}]

    def analyze_options_oi_anomaly(self, date_str: str) -> List[Dict[str, Any]]:
        """
        Strategy 8: Options OI Anomaly for Directional Bets
        Requires Options Bhavcopy (OPTSTK/OPTIDX), which is NOT fully parsed/stored.
        This is a placeholder.
        """
        return [{"Symbol": "N/A",
                 "Signal": "Requires detailed Options Bhavcopy parsing and storage for strike-wise OI analysis."}]

    def analyze_corporate_action_arbitrage(self, date_str: str) -> List[Dict[str, Any]]:
        """
        Strategy 9: Corporate Action Arbitrage via Delivery Traps
        Requires NSE corporate action list and multi-day delivery data.
        This is a placeholder.
        """
        return [{"Symbol": "N/A", "Signal": "Requires Corporate Actions data and multi-day delivery trend analysis."}]

    def _get_previous_trading_day(self, current_date: datetime.date, num_days_back: int) -> Optional[str]:
        """Helper to find the Nth previous trading day(s) by checking DB dates."""
        if not self.conn:
            return None

        cur = self.conn.cursor()
        # Find the Nth previous date that has bhavcopy data
        # This query gets distinct dates from bhavcopy_data, orders them descending, and limits to num_days_back.
        # Then it takes the last one (which is the oldest of the 'num_days_back' previous dates).
        query = """
            SELECT DISTINCT date FROM bhavcopy_data
            WHERE date < ? AND trading_type = 'EQ'
            ORDER BY date DESC
            LIMIT ?
        """
        cur.execute(query, (current_date.strftime('%Y-%m-%d'), num_days_back))
        rows = cur.fetchall()

        if len(rows) < num_days_back:
            # Not enough historical data
            print(
                f"DEBUG: _get_previous_trading_day for {current_date} (back {num_days_back}) - Only found {len(rows)} previous trading days.")
            return None

        # Return the oldest date among the fetched 'num_days_back' previous dates
        return rows[-1][0]

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
        sentiment_map = {"Strong Bullish": 2, "Mild Bullish": 1, "Neutral": 0, "Mild Bearish": -1, "Strong Bearish": -2,
                         "Weakening": -0.5, "Strengthening": 0.5, "Bullish Reversal": 1.5,
                         "Bearish Reversal": -1.5}  # Added new sentiments
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

        sentiment_map = {"Strong Bullish": 2, "Mild Bullish": 1, "Neutral": 0, "Mild Bearish": -1, "Strong Bearish": -2,
                         "Weakening": -0.5, "Strengthening": 0.5, "Bullish Reversal": 1.5,
                         "Bearish Reversal": -1.5}  # Added new sentiments
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
                    # Original complex summary for non-ticker-only symbols (like SENSEX, INDIAVIX)
                    sentiment = "Mild Bearish" if change < 0 else "Mild Bullish" if change > 0 else "Neutral"
                    summary = {'time': self._get_ist_time().strftime("%H:%M"), 'sp': round(change, 2),
                               'value': round(current_price, 2), 'pcr': round(pct_change, 2), 'sentiment': sentiment,
                               'call_oi': 0, 'put_oi': 0, 'add_exit': "Live Price", 'pcr_change': 0, 'intraday_pcr': 0,
                               'expiry': 'N/A'}
                    shared_data[sym]['summary'] = summary
                    shared_data[sym]['strikes'], shared_data[sym]['max_pain_chart_data'], shared_data[sym][
                        'ce_oi_chart_data'], shared_data[sym]['pe_oi_chart_data'], shared_data[sym][
                        'pcr_chart_data'] = [], [], [], [], []
                else:  # Simplified summary for TICKER_ONLY_SYMBOLS
                    sentiment = "Mild Bearish" if change < 0 else "Mild Bullish" if change > 0 else "Neutral"
                    summary = {'time': self._get_ist_time().strftime("%H:%M"), 'sp': round(change, 2),
                               'value': round(current_price, 2), 'pcr': round(pct_change, 2), 'sentiment': sentiment,
                               'call_oi': 0, 'put_oi': 0, 'add_exit': "Live Price", 'pcr_change': 0, 'intraday_pcr': 0,
                               'expiry': 'N/A'}
                    # Store this simplified summary
                    shared_data[sym]['summary'] = summary
                    # Ensure chart data lists are initialized even if empty
                    shared_data[sym]['strikes'] = []
                    shared_data[sym]['max_pain_chart_data'] = []
                    shared_data[sym]['ce_oi_chart_data'] = []
                    shared_data[sym]['pe_oi_chart_data'] = []
                    shared_data[sym]['pcr_chart_data'] = []

            print(f"{sym} YFINANCE DATA UPDATED | Value: {current_price:.2f}")
            broadcast_live_update()

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
            # Try fetching data, if 403, refresh cookies and retry once
            for attempt in range(2):
                response = self.session.get(url, headers=self.nse_headers, timeout=10, verify=False)
                if response.status_code == 403 and attempt == 0:
                    print(f"403 Forbidden for {sym}, refreshing cookies and retrying...")
                    self._set_nse_session_cookies()
                    time.sleep(1)  # Small delay before retry
                elif response.status_code == 200:
                    break
                else:
                    response.raise_for_status()  # Raise for other HTTP errors

            response.raise_for_status()  # Ensure success after retries
            data = response.json()

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
                put_action = "ADD" if put_oi > 0 else "EXIT" if put_coi < 0 else ""
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

            # --- MODIFICATION START ---
            # Line 1: Call get_sentiment with intraday_pcr, symbol and history
            current_history_for_sym = todays_history.get(sym, [])
            sentiment = self.get_sentiment(pcr, intraday_pcr, total_call_coi, total_put_coi, sym,
                                           current_history_for_sym)
            # --- MODIFICATION END ---

            add_exit_str = " | ".join(filter(None,
                                             [f"CE Add: {', '.join(sorted(ce_add_strikes))}" if ce_add_strikes else "",
                                              f"PE Add: {', '.join(sorted(pe_add_strikes))}" if pe_add_strikes else "",
                                              f"CE Exit: {', '.join(sorted(ce_exit_strikes))}" if ce_exit_strikes else "",
                                              f"PE Exit: {', '.join(sorted(pe_exit_strikes))}" if pe_exit_strikes else ""])) or "No Change"
            summary = {'time': self._get_ist_time().strftime("%H:%M"), 'sp': int(sp), 'value': int(round(underlying)),
                       'call_oi': round(atm_call_coi / 1000, 1), 'put_oi': round(atm_put_coi / 1000, 1), 'pcr': pcr,
                       'sentiment': sentiment, 'expiry': expiry, 'add_exit': add_exit_str,
                       'pcr_change': pcr_change, 'intraday_pcr': intraday_pcr,
                       'total_call_coi': total_call_coi, 'total_put_coi': total_put_coi  # Added for history analysis
                       }
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
        except requests.exceptions.RequestException as e:
            print(f"{sym} processing error (RequestException): {e}")
            # This will be caught by the outer try-except in fetch_and_process_symbol
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
            # Try fetching data, if 403, refresh cookies and retry once
            for attempt in range(2):
                response = self.session.get(url, headers=self.nse_headers, timeout=10, verify=False)
                if response.status_code == 403 and attempt == 0:
                    print(f"403 Forbidden for {sym} equity, refreshing cookies and retrying...")
                    self._set_nse_session_cookies()
                    time.sleep(1)  # Small delay before retry
                elif response.status_code == 200:
                    break
                else:
                    response.raise_for_status()  # Raise for other HTTP errors

            response.raise_for_status()  # Ensure success after retries <-- FIX: Changed from raise_status to raise_for_status
            data = response.json()

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
                put_action = "ADD" if put_oi > 0 else "EXIT" if put_coi < 0 else ""
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

            # --- MODIFICATION START ---
            # Line 3: Call get_sentiment with intraday_pcr, symbol and history
            current_history_for_sym = todays_history.get(sym, [])
            sentiment = self.get_sentiment(pcr, intraday_pcr, total_call_coi, total_put_coi, sym,
                                           current_history_for_sym)
            # --- MODIFICATION END ---

            summary = {'sp': int(sp), 'value': float(underlying), 'pcr': pcr, 'sentiment': sentiment,
                       'intraday_pcr': intraday_pcr,
                       'total_call_coi': total_call_coi, 'total_put_coi': total_put_coi  # Added for history analysis
                       }
            pulse_summary = {'total_call_oi': total_call_oi, 'total_put_oi': total_put_oi,
                             'total_call_coi': total_call_coi, 'total_put_coi': total_put_coi}

            return {'summary': summary, 'strikes': strikes_data, 'pulse_summary': pulse_summary}
        except requests.exceptions.RequestException as e:
            print(f"Error processing equity data for {sym} (RequestException): {e}")
            return None
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

    # --- MODIFICATION START ---
    # Line 4: Updated get_sentiment signature and logic
    def get_sentiment(self, pcr: float, intraday_pcr: float, total_call_coi: int, total_put_coi: int, sym: str,
                      history: List[Dict[str, Any]]) -> str:
        """
        Calculates sentiment based on PCR, Intraday PCR (PE OI Delta), and recent trends in OI.
        """
        # Define thresholds for PCR
        PCR_BULLISH_THRESHOLD = 1.2
        PCR_BEARISH_THRESHOLD = 0.8
        PCR_STABLE_THRESHOLD = 0.05  # Max change for PCR to be considered "stable" over history
        OI_CHANGE_THRESHOLD_FOR_TREND = 50000  # Minimum cumulative OI change to consider an "add" or "exit" significant

        # --- 1. Determine PE OI Delta direction based on intraday_pcr ---
        # Using the sign of total_put_coi directly for PE OI Delta (more accurate to your table's implication)
        pe_oi_delta_positive = total_put_coi > 0
        pe_oi_delta_negative = total_put_coi < 0

        # Using the sign of total_call_coi directly for CE OI Delta
        ce_oi_delta_positive = total_call_coi > 0
        ce_oi_delta_negative = total_call_coi < 0

        # --- 2. Apply the Writer-Aware PCR logic (Strong Sentiments) ---
        if pcr > PCR_BULLISH_THRESHOLD:  # PCR > 1.2
            if pe_oi_delta_positive:
                return "Strong Bullish"  # Bullish – Put Writers in control
            elif pe_oi_delta_negative:
                return "Strong Bearish"  # Bearish – Put Buying fear
        elif pcr < PCR_BEARISH_THRESHOLD:  # PCR < 0.8
            if pe_oi_delta_positive:
                return "Strong Bearish"  # Bearish – Call Writers capping
            elif pe_oi_delta_negative:
                return "Strong Bullish"  # Bullish – Call Buying FOMO

        # --- 3. Analyze trends from history for "Weakening" / "Strengthening" / "Reversal" ---
        TREND_LOOKBACK = 5  # Number of recent updates to analyze

        # Ensure we have enough history to analyze trends
        if len(history) >= TREND_LOOKBACK:
            recent_history = history[:TREND_LOOKBACK]  # Get the most recent N updates

            # Calculate PCR trend
            first_pcr = recent_history[-1]['pcr']  # Oldest PCR in the lookback
            last_pcr = recent_history[0]['pcr']  # Newest PCR in the lookback (current pcr)

            pcr_trend_change = last_pcr - first_pcr
            pcr_is_stable = abs(pcr_trend_change) < PCR_STABLE_THRESHOLD
            pcr_is_rising = pcr_trend_change > PCR_STABLE_THRESHOLD
            pcr_is_falling = pcr_trend_change < -PCR_STABLE_THRESHOLD

            # Calculate cumulative OI changes over the lookback period
            cumulative_call_coi_history = sum(h.get('total_call_coi', 0) for h in recent_history)
            cumulative_put_coi_history = sum(h.get('total_put_coi', 0) for h in recent_history)

            ce_oi_adding_trend = cumulative_call_coi_history > OI_CHANGE_THRESHOLD_FOR_TREND
            ce_oi_exiting_trend = cumulative_call_coi_history < -OI_CHANGE_THRESHOLD_FOR_TREND
            pe_oi_adding_trend = cumulative_put_coi_history > OI_CHANGE_THRESHOLD_FOR_TREND
            pe_oi_exiting_trend = cumulative_put_coi_history < -OI_CHANGE_THRESHOLD_FOR_TREND

            # Apply trend-based rules
            # Weakening: PCR stable/falling AND CE OI adding
            if (pcr_is_stable or pcr_is_falling) and ce_oi_adding_trend:
                return "Weakening"

            # Strengthening: PCR stable/rising AND PE OI adding
            if (pcr_is_stable or pcr_is_rising) and pe_oi_adding_trend:
                return "Strengthening"

            # Bullish Reversal: PCR falling AND PE OI exiting
            if pcr_is_falling and pe_oi_exiting_trend:
                return "Bullish Reversal"

            # Bearish Reversal: PCR rising AND CE OI exiting
            if pcr_is_rising and ce_oi_exiting_trend:
                return "Bearish Reversal"

        # --- 4. Apply intraday_pcr influence for Mild Sentiments (when pcr is between 0.8 and 1.2) ---
        # This block only executes if no "Strong" or "Trend-based" sentiment was returned.
        # It handles cases where pcr is in the middle range, using intraday_pcr for direction.

        if pcr >= 1.0:  # Generally bullish or neutral PCR
            if pe_oi_delta_positive:  # Intraday action is also bullish (put writers active)
                return "Mild Bullish"
            elif ce_oi_delta_positive:  # Intraday action is bearish (call writers active)
                return "Mild Bearish"  # If CE OI adds when PCR is somewhat bullish, it's a bearish sign

        elif pcr < 1.0:  # Generally bearish or neutral PCR
            if ce_oi_delta_negative:  # Intraday action is bullish (call writers unwinding or call buyers active)
                return "Mild Bullish"
            elif pe_oi_delta_negative:  # Intraday action is bearish (put buyers active)
                return "Mild Bearish"

        # --- 5. Fallback to general PCR-based sentiment if other specific conditions not met ---
        if pcr >= 1.1:  # Changed from > 1.1 to >= 1.1 as discussed
            return "Mild Bullish"
        elif pcr < 0.9:
            return "Mild Bearish"

        return "Neutral"

    # --- MODIFICATION END ---

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

