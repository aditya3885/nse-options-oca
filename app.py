#!/usr/bin/env python3

# -- coding: utf-8 --

# === 1. MONKEY PATCH FIRST ===

import eventlet

import os

os.environ["EVENTLET_NO_GREENDNS"] = "YES"  # disable greendns to avoid DNS stalls

# Re-import sqlite3 instead of psycopg2
# import BEFORE eventlet monkey_patch to avoid eventlet's sqlite3 patcher
import sqlite3

import eventlet

# FIX: Removed the problematic 'dns=False' argument.
eventlet.monkey_patch()

# === 2. IMPORTS ===

import datetime
import time
import threading
import requests
import requests.packages.urllib3
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import random
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
import yfinance as yf
import numpy as np  # Import numpy
import pytz
from zipfile import ZipFile
from io import BytesIO
import joblib
import re
import feedparser
from dotenv import load_dotenv

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning)

# --------------------------------------------------------------------------- #
# CONFIG
# --------------------------------------------------------------------------- #

TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"  # Replace with your bot token
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"  # Replace with your chat ID
SEND_TEXT_UPDATES = True
UPDATE_INTERVAL = 180
MAX_HISTORY_ROWS_DB = 10000
LIVE_DATA_INTERVAL = 180
EQUITY_FETCH_INTERVAL = 300  # 5 minutes

# AI Bot Configuration
AI_BOT_UPDATE_INTERVAL = 60  # Check for new AI Bot trades every 1 minute
AI_BOT_TRADING_START_TIME = datetime.time(9, 15)
AI_BOT_TRADING_END_TIME = datetime.time(23, 15)  # Bot active until 3:15 PM IST
AI_BOT_HISTORY_DAYS = 2  # Keep AI Bot trade history for the last 2 days
AI_BOT_MIN_TRADE_INTERVAL = 15 * 60  # Minimum time (seconds) between new trades for a symbol

NSE_FETCH_START_TIME = datetime.time(9, 15)
NSE_FETCH_END_TIME = datetime.time(15, 31)  # NSE data fetch active until 3:31 PM IST

AUTO_SYMBOLS = ["NIFTY", "FINNIFTY", "BANKNIFTY", "SENSEX", "INDIAVIX", "GOLD", "SILVER", "BTC-USD", "USD-INR"]
# Define NSE indices you want to track historical Bhavcopy for
NSE_INDEX_BHAVCOPY_SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "INDIAVIX"]

# --- NEWS CONFIGURATION ---
NEWS_FETCH_INTERVAL = 300  # Fetch news every 5 minutes (300 seconds)
NEWS_RETENTION_DAYS = 3  # Keep news for the last 3 days
# Public RSS feeds for Indian financial news (can be volatile, verify URLs)
# Using more general feeds or known reliable ones. Some specific ones might be flaky.
RSS_FEEDS = [
    "https://www.moneycontrol.com/rss/markets.xml",
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://www.cnbctv18.com/rss/markets.xml",
    "https://www.livemint.com/rss/markets",
    "https://www.business-standard.com/rss/markets-122",
    "https://zeenews.india.com/markets.rss",
]

# --------------------------------------------------------------------------- #
# NEWS UTILITIES (from news_fetcher.py)
# --------------------------------------------------------------------------- #

POSITIVE_WORDS = {
    "rise", "gain", "up", "rally", "beat", "surge", "jump", "boost",
    "strong", "profit", "record", "high", "bullish", "ipo", "listing",
    "growth", "buy", "recommend", "upgrade", "expands", "acquires", "deal", "positive"
}

NEGATIVE_WORDS = {
    "fall", "drop", "down", "crash", "miss", "weak", "loss",
    "decline", "bearish", "sell", "downgrade", "caution", "slump", "cuts", "negative", "warns"
}


def _estimate_impact(title: str, summary: str) -> str:
    txt = (title + " " + summary).lower()
    pos = sum(1 for w in POSITIVE_WORDS if w in txt)
    neg = sum(1 for w in NEGATIVE_WORDS if w in txt)
    if pos > neg:
        return "+ (Bullish)"
    if neg > pos:
        return "- (Bearish)"
    return "Neutral"


def fetch_market_news(top_n: int = 20) -> List[Dict[str, Any]]:
    """
    Fetches market news from RSS feeds and returns a list of dictionaries.
    Each dictionary represents a news item.
    """
    entries = []
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/555.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/555.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    })
    for url in RSS_FEEDS:
        try:
            response = session.get(url, timeout=10)
            response.raise_for_status()
            feed = feedparser.parse(response.content)
            source = feed.feed.get("title", "Unknown")
            for e in feed.entries:
                published_dt_utc = None
                if hasattr(e, 'published_parsed') and e.published_parsed:
                    published_dt_utc = datetime.datetime(*e.published_parsed[:6], tzinfo=pytz.utc)
                elif hasattr(e, 'updated_parsed') and e.updated_parsed:
                    published_dt_utc = datetime.datetime(*e.updated_parsed[:6], tzinfo=pytz.utc)
                else:
                    pub_str = e.get("published", e.get("updated", ""))
                    if pub_str:
                        for fmt in ["%a, %d %b %Y %H:%M:%S %Z", "%Y-%m-%dT%H:%M:%S%z", "%a, %d %b %Y %H:%M:%S GMT",
                                    "%Y-%m-%d %H:%M:%S"]:
                            try:
                                if "%z" in fmt or "%Z" in fmt:
                                    parsed_dt = datetime.datetime.strptime(pub_str, fmt)
                                    if parsed_dt.tzinfo is None:
                                        published_dt_utc = pytz.utc.localize(parsed_dt)
                                    else:
                                        published_dt_utc = parsed_dt.astimezone(pytz.utc)
                                else:
                                    published_dt_utc = pytz.utc.localize(datetime.datetime.strptime(pub_str, fmt))
                                break
                            except ValueError:
                                continue
                if published_dt_utc is None:
                    published_dt_utc = datetime.datetime.now(pytz.utc)
                time_ist_display = published_dt_utc.astimezone(pytz.timezone('Asia/Kolkata')).strftime("%H:%M")
                title = e.title.strip()
                summary_raw = (e.get("summary") or e.get("description") or "")
                clean_summary = re.sub(r'<.*?>', '', summary_raw).strip()
                display_summary = (clean_summary[:120] + "...") if len(clean_summary) > 120 else clean_summary
                impact = _estimate_impact(title, clean_summary)
                link = e.link if hasattr(e, 'link') else '#'
                entries.append({
                    "timestamp": published_dt_utc.isoformat(),
                    "Time (IST)": time_ist_display,
                    "Headline": title,
                    "Source": source,
                    "Key Details": display_summary,
                    "Impact": impact,
                    "URL": link
                })
        except requests.exceptions.RequestException as exc:
            print(f"[WARN] RSS {url} failed (network/HTTP error): {exc}")
        except Exception as exc:
            print(f"[WARN] RSS {url} failed (parsing/other error): {exc}")
        time.sleep(0.5)
    if entries:
        df = pd.DataFrame(entries).drop_duplicates(subset=["Headline", "Source"])
        df['timestamp_dt'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by='timestamp_dt', ascending=False)
        return df.head(top_n).drop(columns=['timestamp_dt']).to_dict(orient='records')
    else:
        return []


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
latest_bhavcopy_data: Dict[str, Any] = {"equities": [], "indices": [], "date": None}
ai_bot_trades: Dict[str, Any] = {}
ai_bot_trade_history: List[Dict[str, Any]] = []
last_ai_bot_run_time: Dict[str, float] = {}
news_alerts: List[Dict[str, Any]] = []


# Helper to convert NumPy types to standard Python types for JSON serialization
def convert_numpy_types(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(elem) for elem in obj]
    else:
        return obj


@app.route('/')
def index():
    return render_template('dashboard.html')


@app.route('/api/stocks')
def get_stocks():
    return jsonify(fno_stocks_list)


@app.route('/api/bhavcopy')
def get_bhavcopy_data():
    return jsonify(convert_numpy_types(latest_bhavcopy_data))  # Convert here


@app.route('/api/bhavcopy/<date_str>')
def get_historical_bhavcopy(date_str: str):
    if not analyzer.conn:
        return jsonify({"error": "Database connection not available."}), 500
    try:
        datetime.datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date format. Please use YYYY-MM-DD."}), 400
    try:
        data = analyzer._get_bhavcopy_for_date(date_str)
        if not data["equities"] and not data["indices"] and not data["block_deals"] and not data["fno_data"]:
            return jsonify(
                {"error": f"No Bhavcopy data (equity, index, block deals, or F&O) found for {date_str}."}), 404
        data["date"] = date_str
        return jsonify(convert_numpy_types(data))  # Convert here
    except Exception as e:
        print(f"Error fetching historical bhavcopy for {date_str}: {e}")
        return jsonify({"error": "An internal error occurred while fetching data."}), 500


@app.route('/api/bhavcopy/strategies/<date_str>')
def get_bhavcopy_strategies(date_str: str) -> Dict[str, Any]:
    if not analyzer.conn:
        return {"error": "Database connection not available."}
    try:
        datetime.datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return {"error": "Invalid date format. Please use YYYY-MM-DD."}
    results = {}
    try:
        results['FDDT'] = analyzer.analyze_fii_dii_delivery_divergence(date_str)
        results['ZDFT'] = analyzer.analyze_zero_delivery_future_roll(date_str)
        results['BDGP'] = analyzer.analyze_block_deal_ghost_pump(date_str)
        results['VAR7'] = analyzer.analyze_vwap_anchor_reversion(date_str)
        results['HBA21'] = analyzer.analyze_hidden_bonus_arbitrage(date_str)
        results['OIMT'] = analyzer.analyze_oi_momentum_trap(date_str)
        results['VSSB'] = analyzer.analyze_volume_surge_scanner(date_str)
        results['OOAD'] = analyzer.analyze_options_oi_anomaly(date_str)
        results['CAADT'] = analyzer.analyze_corporate_action_arbitrage(date_str)
    except Exception as e:
        print(f"Error running strategies for {date_str}: {e}")
        return {"error": f"An internal error occurred while running strategies: {e}"}
    return convert_numpy_types(results)  # Convert here


@socketio.on('connect')
def handle_connect(sid):  # Added sid argument
    global site_visits
    with data_lock:
        site_visits += 1
        socketio.emit('update_visits', {'count': site_visits})
    with data_lock:
        live_feed_summary = {sym: data.get('live_feed_summary', {}) for sym, data in shared_data.items()}

        # For OI_SYMBOLS, create a live_feed_summary from their 'summary' for consistency
        for sym in AUTO_SYMBOLS:
            if sym not in analyzer.YFINANCE_SYMBOLS and sym in shared_data and 'summary' in shared_data[sym]:
                summary = shared_data[sym]['summary']
                live_feed_summary[sym] = {
                    'current_value': summary.get('value'),
                    'change': summary.get('change'),
                    'percentage_change': summary.get('percentage_change'),
                    'pcr': summary.get('pcr')  # Include PCR for potential display in ticker
                }

        # Convert NumPy types before emitting
        serializable_shared_data = convert_numpy_types(shared_data)
        serializable_live_feed_summary = convert_numpy_types(live_feed_summary)
        serializable_ai_bot_trades = convert_numpy_types(ai_bot_trades)
        serializable_ai_bot_trade_history = convert_numpy_types(ai_bot_trade_history)
        serializable_todays_history = convert_numpy_types(todays_history)
        serializable_news_alerts = convert_numpy_types(news_alerts)

        emit('update', {
            'live': serializable_shared_data,
            'live_feed_summary': serializable_live_feed_summary,
            'ai_bot_trades': serializable_ai_bot_trades,
            'ai_bot_trade_history': serializable_ai_bot_trade_history
        }, to=sid)  # Used sid
        emit('initial_todays_history', {'history': serializable_todays_history}, to=sid)  # Used sid
        analyzer.rank_and_emit_movers()  # This emits separately, ensure its data is clean
        emit('news_update', {'news': serializable_news_alerts}, to=sid)  # Used sid


@socketio.on('fetch_equity_data')
def handle_fetch_equity(data):
    stock_symbol = data.get('symbol')
    if stock_symbol and stock_symbol in fno_stocks_list:
        print(f"Received request to fetch data for equity: {stock_symbol}")
        # sid is passed by Flask-SocketIO automatically to the background task if it's the last argument
        socketio.start_background_task(target=analyzer.process_and_emit_equity_data, symbol=stock_symbol,
                                       sid=request.sid)


@socketio.on('run_bhavcopy_manually')
def handle_manual_bhavcopy_run():
    print("--- Manual Bhavcopy scan triggered by user ---")

    def manual_scan_wrapper():
        now_ist = analyzer._get_ist_time()
        scan_successful = False
        for i in range(5):
            target_day = now_ist - datetime.timedelta(days=i)
            if target_day.weekday() >= 5:
                continue
            if analyzer.run_bhavcopy_for_date(target_day, trigger_manual=True):
                print(f"Successfully processed Bhavcopy for date: {target_day.strftime('%Y-%m-%d')}")
                analyzer.send_telegram_message(
                    f"✅ Manual Scan: Bhavcopy for {target_day.strftime('%d-%b-%Y')} processed and loaded.")
                scan_successful = True
                break
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
        # sid is passed by Flask-SocketIO automatically to the background task if it's the last argument
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

        # For OI_SYMBOLS, create a live_feed_summary from their 'summary' for consistency
        for sym in AUTO_SYMBOLS:
            if sym not in analyzer.YFINANCE_SYMBOLS and sym in shared_data and 'summary' in shared_data[sym]:
                summary = shared_data[sym]['summary']
                live_feed_summary[sym] = {
                    'current_value': summary.get('value'),
                    'change': summary.get('change'),
                    'percentage_change': summary.get('percentage_change'),
                    'pcr': summary.get('pcr')  # Include PCR for potential display in ticker
                }

        # Convert NumPy types before emitting
        serializable_shared_data = convert_numpy_types(shared_data)
        serializable_live_feed_summary = convert_numpy_types(live_feed_summary)
        serializable_ai_bot_trades = convert_numpy_types(ai_bot_trades)
        serializable_ai_bot_trade_history = convert_numpy_types(ai_bot_trade_history)

        socketio.emit('update',
                      {'live': serializable_shared_data,
                       'live_feed_summary': serializable_live_feed_summary,
                       'ai_bot_trades': serializable_ai_bot_trades,
                       'ai_bot_trade_history': serializable_ai_bot_trade_history})


def broadcast_history_append(sym: str, new_history_item: Dict[str, Any]):
    with data_lock:
        serializable_new_history_item = convert_numpy_types(new_history_item)
        socketio.emit('todays_history_append', {'symbol': sym, 'item': serializable_new_history_item})


def broadcast_news_update():
    with data_lock:
        serializable_news_alerts = convert_numpy_types(news_alerts)
        socketio.emit('news_update', {'news': serializable_news_alerts})


@app.route('/history/<symbol>/<date_str>')
def get_historical_data(symbol: str, date_str: str):
    if symbol not in AUTO_SYMBOLS and symbol not in fno_stocks_list:
        return jsonify({"error": f"Invalid symbol: {symbol}"}), 400
    history_for_date: List[Dict[str, Any]] = []
    if analyzer.conn:
        cur = None
        try:
            cur = analyzer.conn.cursor()
            ist_day_start = datetime.datetime.strptime(date_str, "%Y-%m-%d")
            # Convert IST day start to UTC for comparison with DB timestamps
            # SQLite stores datetime as strings, so we convert directly to string format
            utc_day_start_str = ist_day_start.replace(tzinfo=analyzer.ist_timezone).astimezone(
                pytz.utc).isoformat()
            utc_day_end_str = (ist_day_start + datetime.timedelta(days=1)).replace(
                tzinfo=analyzer.ist_timezone).astimezone(pytz.utc).isoformat()
            cur.execute(
                """SELECT timestamp, sp, value, call_oi, put_oi, pcr, sentiment, add_exit, intraday_pcr, ml_sentiment, sentiment_reason, implied_volatility
                   FROM history WHERE symbol = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp DESC""",
                (symbol, utc_day_start_str, utc_day_end_str))
            rows = cur.fetchall()
            for r in rows:
                # Convert stored ISO string back to datetime object for timezone conversion
                db_timestamp_utc = datetime.datetime.fromisoformat(r["timestamp"])
                history_for_date.append(
                    {'time': analyzer._convert_utc_to_ist_display(db_timestamp_utc), 'sp': float(r["sp"]),
                     # Cast to float
                     'value': float(r["value"]),  # Cast to float
                     'call_oi': float(r["call_oi"]),  # Cast to float
                     'put_oi': float(r["put_oi"]),  # Cast to float
                     'pcr': float(r["pcr"]),  # Cast to float
                     'sentiment': r["sentiment"], 'add_exit': r["add_exit"],
                     'intraday_pcr': float(r["intraday_pcr"]) if r["intraday_pcr"] is not None else 0.0,
                     # Cast to float
                     'ml_sentiment': r["ml_sentiment"] if r["ml_sentiment"] is not None else 'N/A',
                     'sentiment_reason': r["sentiment_reason"] if r["sentiment_reason"] is not None else 'N/A',
                     'implied_volatility': float(r["implied_volatility"]) if r[
                                                                                 "implied_volatility"] is not None else 0.0
                     # Cast to float
                     })
        except sqlite3.Error as e:  # Catch sqlite3.Error
            print(f"SQLite Error fetching historical data: {e}")
            return jsonify({"error": "Failed to query database."}), 500
        finally:
            if cur:
                cur.close()
    return jsonify({"history": convert_numpy_types(history_for_date)})  # Convert here


@app.route('/clear_todays_history', methods=['POST'])
def clear_history_endpoint():
    symbol_to_clear = request.json.get('symbol')
    analyzer.clear_todays_history_db(symbol_to_clear)
    return jsonify({"status": "success", "message": "Today's history cleared."})


class DeepSeekBot:
    def __init__(self):
        self.recommendations: Dict[str, Dict[str, Any]] = {}
        self.active_trades: Dict[str, Dict[str, Any]] = {}
        self.last_vix: float = 15.0

    def _calculate_max_pain_for_bot(self, df_ce: pd.DataFrame, df_pe: pd.DataFrame) -> Optional[float]:
        if df_ce.empty or df_pe.empty:
            return None
        if 'strikePrice' not in df_ce.columns or 'openInterest' not in df_ce.columns or \
                'strikePrice' not in df_pe.columns or 'openInterest' not in df_pe.columns:
            print(
                "Warning: Missing 'strikePrice' or 'openInterest' columns in CE/PE dataframes for Max Pain calculation.")
            return None
        MxPn_CE = df_ce[['strikePrice', 'openInterest']]
        MxPn_PE = df_pe[['strikePrice', 'openInterest']]
        MxPn_Df = pd.merge(MxPn_CE, MxPn_PE, on=['strikePrice'], how='outer', suffixes=('_CE', '_PE')).fillna(0)

        # Ensure these are standard Python types
        StrikePriceList = [float(x) for x in MxPn_Df['strikePrice'].values.tolist()]
        OiCallList = [int(x) for x in MxPn_Df['openInterest_CE'].values.tolist()]
        OiPutList = [int(x) for x in MxPn_Df['openInterest_PE'].values.tolist()]

        min_pain = float('inf')
        max_pain_strike = None

        for current_strike_price in StrikePriceList:
            call_writer_loss = sum(
                (max(0, current_strike_price - strike) * call_oi)
                for strike, call_oi in zip(StrikePriceList, OiCallList) if current_strike_price > strike
            )
            put_writer_loss = sum(
                (max(0, strike - current_strike_price) * put_oi)
                for strike, put_oi in zip(StrikePriceList, OiPutList) if current_strike_price < strike
            )
            total_pain = call_writer_loss + put_writer_loss
            if total_pain < min_pain:
                min_pain = total_pain
                max_pain_strike = current_strike_price
        return float(max_pain_strike) if max_pain_strike is not None else None  # Ensure float

    def analyze_and_recommend(self, symbol: str, history: List[Dict[str, Any]], current_vix: float, df_ce: pd.DataFrame,
                              df_pe: pd.DataFrame) -> Dict[str, Any]:
        global ai_bot_trade_history
        now = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
        current_time_only = now.time()

        # --- MODIFICATION START (to run bot always) ---
        # Removed the market hours check from here to allow continuous bot operation
        # if not (AI_BOT_TRADING_START_TIME <= current_time_only <= AI_BOT_TRADING_END_TIME):
        #     if symbol in self.active_trades:
        #         entry_premium_ce = float(self.active_trades[symbol].get('entry_premium_ce', 0))
        #         entry_premium_pe = float(self.active_trades[symbol].get('entry_premium_pe', 0))

        #         lot_size = 50
        #         if symbol == "NIFTY":
        #             lot_size = 50
        #         elif symbol == "BANKNIFTY":
        #             lot_size = 15
        #         elif symbol == "FINNIFTY":
        #             lot_size = 40

        #         active_otm_ce_strike = float(self.active_trades[symbol].get('otm_ce_strike', 0))  # Cast
        #         active_otm_pe_strike = float(self.active_trades[symbol].get('otm_pe_strike', 0))  # Cast

        #         premium_ce_exit = float(df_ce[df_ce['strikePrice'] == active_otm_ce_strike]['lastPrice'].iloc[0]) if not \
        #         df_ce[
        #             df_ce['strikePrice'] == active_otm_ce_strike].empty else 0.0  # Cast
        #         premium_pe_exit = float(df_pe[df_pe['strikePrice'] == active_otm_pe_strike]['lastPrice'].iloc[0]) if not \
        #         df_pe[
        #             df_pe['strikePrice'] == active_otm_pe_strike].empty else 0.0  # Cast

        #         current_pnl = 0.0
        #         if "CE" in self.active_trades[symbol].get('strikes', '') and entry_premium_ce > 0:
        #             current_pnl += (entry_premium_ce - premium_ce_exit) * lot_size
        #         if "PE" in self.active_trades[symbol].get('strikes', '') and entry_premium_pe > 0:
        #             current_pnl += (entry_premium_pe - premium_pe_exit) * lot_size

        #         final_exit_rec = {
        #             "recommendation": "EXIT", "rationale": "Market closed, exiting active trade.",
        #             "timestamp": now.isoformat(),
        #             "trade": "Exit " + self.active_trades[symbol].get('trade', 'previous trade'),
        #             "strikes": self.active_trades[symbol].get('strikes', '-'),
        #             "type": "Exit", "risk_pct": "-", "exit_rule": "Market Close",
        #             "spot": float(self.active_trades[symbol].get('spot')),  # Cast
        #             "pcr": float(self.active_trades[symbol].get('pcr')),  # Cast
        #             "intraday_pcr": float(self.active_trades[symbol].get('intraday_pcr')),  # Cast
        #             "status": "Exit", "pnl": round(float(current_pnl), 2),  # Cast
        #             "action_price": round(float(premium_ce_exit + premium_pe_exit), 2),  # Cast
        #             "symbol": symbol
        #         }
        #         ai_bot_trade_history.append(final_exit_rec)
        #         self.active_trades.pop(symbol, None)
        #         return final_exit_rec
        #     else:
        #         return {"recommendation": "Neutral", "rationale": "Outside trading hours.",
        #                 "timestamp": now.isoformat(),
        #                 "trade": "-", "strikes": "-", "type": "-",
        #                 "risk_pct": "-",
        #                 "exit_rule": "-", "spot": 0.0, "pcr": 0.0, "intraday_pcr": 0.0, "status": "No Trade",
        #                 "pnl": 0.0, "action_price": 0.0,
        #                 "symbol": symbol
        #                 }
        # --- MODIFICATION END (to run bot always) ---

        if not history:
            return {"recommendation": "Neutral", "rationale": "No history data available.",
                    "timestamp": now.isoformat(),
                    "trade": "-", "strikes": "-", "type": "-", "risk_pct": "-",
                    "exit_rule": "-", "spot": 0.0, "pcr": 0.0, "intraday_pcr": 0.0, "status": "No Data", "pnl": 0.0,
                    "action_price": 0.0,
                    "symbol": symbol
                    }

        latest = history[0]
        pcr = float(latest.get('pcr', 1.0))  # Cast
        sentiment = latest.get('sentiment', 'Neutral')
        ml_sentiment = latest.get('ml_sentiment', 'N/A')
        spot = float(latest.get('value', 0))  # Cast
        intraday_pcr = float(latest.get('intraday_pcr', 1.0))  # Cast
        total_call_coi_actual = int(latest.get('total_call_coi', 0))  # Cast
        total_put_coi_actual = int(latest.get('total_put_coi', 0))  # Cast

        current_hour = now.hour
        current_minute = now.minute

        trade_logic_override = None
        if current_hour == 9 and current_minute >= 30 and current_minute <= 59:
            if pcr < 0.7:
                trade_logic_override = "Prepare to write puts if price holds"
        elif current_hour >= 10 and current_hour < 12:
            if pcr > 1.3:
                trade_logic_override = "Peak writing window - Write OTM calls — bounce likely"
        elif current_hour >= 13 and current_hour <= 14 and current_minute <= 30:
            if len(history) > 1:
                prev_pcr = float(history[1].get('pcr', pcr))  # Cast
                if prev_pcr > 1.4 and pcr < 0.6:
                    trade_logic_override = "Reversal hunting - Write puts aggressively (reversal down)"
        elif current_hour == 14 and current_minute >= 30 and current_minute <= 59:
            if pcr < 0.8:
                trade_logic_override = "Expiry theta play - Short strangle — theta burn max"

        recommendation = "Neutral"
        rationale = "Observing market."
        trade_summary = "-"
        strikes_selected = "-"
        trade_type = "-"
        risk_pct_display = "-"
        exit_rule_display = "Standard exit logic"
        status = "Hold"
        pnl = 0.0
        action_price = 0.0
        # capital = 500000 # Not used
        lot_size = 50
        if symbol == "NIFTY":
            lot_size = 50
        elif symbol == "BANKNIFTY":
            lot_size = 15
        elif symbol == "FINNIFTY":
            lot_size = 40

        strike_step = 50
        if symbol == "BANKNIFTY":
            strike_step = 100
        elif symbol == "FINNIFTY":
            strike_step = 50

        atm_strike = int(round(spot / strike_step) * strike_step)  # Cast
        otm_ce_strike = int(atm_strike + (3 * strike_step))  # Cast
        otm_pe_strike = int(atm_strike - (3 * strike_step))  # Cast
        # far_otm_ce_strike = atm_strike + (6 * strike_step) # Not used
        # far_otm_pe_strike = atm_strike - (6 * strike_step) # Not used

        premium_ce = float(df_ce[df_ce['strikePrice'] == otm_ce_strike]['lastPrice'].iloc[0]) if not df_ce[
            df_ce['strikePrice'] == otm_ce_strike].empty else 0.0  # Cast
        premium_pe = float(df_pe[df_pe['strikePrice'] == otm_pe_strike]['lastPrice'].iloc[0]) if not df_pe[
            df_pe['strikePrice'] == otm_pe_strike].empty else 0.0  # Cast

        is_ce_add_heavy = total_call_coi_actual > (2 * total_put_coi_actual) and total_call_coi_actual > 20000
        is_pe_add_heavy = total_put_coi_actual > (2 * total_call_coi_actual) and total_put_coi_actual > 20000

        if symbol in self.active_trades:
            prev_trade = self.active_trades[symbol]
            entry_spot = float(prev_trade.get('entry_spot', spot))  # Cast
            entry_pcr = float(prev_trade.get('entry_pcr', pcr))  # Cast
            entry_premium_ce = float(prev_trade.get('entry_premium_ce', 0))  # Cast
            entry_premium_pe = float(prev_trade.get('entry_premium_pe', 0))  # Cast

            exit_triggered = False
            exit_reason = ""

            if prev_trade['type'] == 'Credit':
                if "CE" in prev_trade.get('strikes', '') and spot > float(prev_trade.get('otm_ce_strike', 0)):  # Cast
                    exit_triggered = True
                    exit_reason = f"Spot ({spot:.2f}) breached CE strike ({float(prev_trade.get('otm_ce_strike', 0)):.2f})."  # Cast
                elif "PE" in prev_trade.get('strikes', '') and spot < float(prev_trade.get('otm_pe_strike', 0)):  # Cast
                    exit_triggered = True
                    exit_reason = f"Spot ({spot:.2f}) breached PE strike ({float(prev_trade.get('otm_pe_strike', 0)):.2f})."  # Cast

            if abs(pcr - entry_pcr) > 0.3:
                exit_triggered = True
                exit_reason = f"PCR reversed {abs(pcr - entry_pcr):.2f} from entry ({entry_pcr:.2f})."

            # --- MODIFICATION START (to run bot always) ---
            # Removed the market hours exit logic here
            # if now.hour >= 15 and now.minute >= 15:
            #     exit_triggered = True
            #     exit_reason = "Approaching end of day (3:15 PM), time to exit."
            # --- MODIFICATION END (to run bot always) ---

            if exit_triggered:
                recommendation = "EXIT"
                status = "Exit"
                trade_summary = "Exit " + prev_trade.get('trade', 'previous trade')
                strikes_selected = prev_trade.get('strikes', '-')
                current_pnl = 0.0

                if "CE" in prev_trade.get('strikes', '') and entry_premium_ce > 0:
                    current_pnl += (entry_premium_ce - premium_ce) * lot_size
                if "PE" in prev_trade.get('strikes', '') and entry_premium_pe > 0:
                    current_pnl += (entry_premium_pe - premium_pe) * lot_size

                pnl = round(float(current_pnl), 2)  # Cast
                action_price = float(premium_ce + premium_pe) if "Strangle" in prev_trade.get('trade', '') else (
                    # Cast
                    float(premium_ce) if "CE" in prev_trade.get('strikes', '') else float(premium_pe))  # Cast

                self.active_trades.pop(symbol, None)

                final_recommendation = {"recommendation": recommendation, "rationale": exit_reason,
                                        "timestamp": now.isoformat(),
                                        "trade": trade_summary, "strikes": strikes_selected, "type": "Exit",
                                        "risk_pct": "-",
                                        "exit_rule": "Triggered exit logic", "spot": float(spot), "pcr": float(pcr),
                                        # Cast
                                        "intraday_pcr": float(intraday_pcr),  # Cast
                                        "status": "Exit", "pnl": pnl, "action_price": round(action_price, 2),
                                        "symbol": symbol
                                        }
                ai_bot_trade_history.append(final_recommendation)
                return final_recommendation

        if symbol in self.active_trades:
            active_trade_info = self.active_trades[symbol]
            status = "Hold"
            current_pnl = 0.0

            if "CE" in active_trade_info.get('strikes', '') and float(
                    active_trade_info.get('entry_premium_ce', 0)) > 0:  # Cast
                current_pnl += (float(active_trade_info.get('entry_premium_ce', 0)) - premium_ce) * lot_size  # Cast
            if "PE" in active_trade_info.get('strikes', '') and float(
                    active_trade_info.get('entry_premium_pe', 0)) > 0:  # Cast
                current_pnl += (float(active_trade_info.get('entry_premium_pe', 0)) - premium_pe) * lot_size  # Cast

            pnl = round(float(current_pnl), 2)  # Cast
            hold_recommendation = {
                "recommendation": "HOLD",
                "rationale": f"Holding active {active_trade_info['trade']} trade. Current P/L: {pnl:.2f}.",
                "timestamp": now.isoformat(),
                "trade": active_trade_info['trade'],
                "strikes": active_trade_info['strikes'],
                "type": active_trade_info['type'],
                "risk_pct": active_trade_info['risk_pct'],
                "exit_rule": active_trade_info['exit_rule'],
                "spot": float(spot),  # Cast
                "pcr": float(pcr),  # Cast
                "intraday_pcr": float(intraday_pcr),  # Cast
                "status": status,
                "pnl": pnl,
                "action_price": float(active_trade_info['action_price']),  # Cast
                "symbol": symbol
            }
            ai_bot_trade_history.append(hold_recommendation)
            return hold_recommendation

        if symbol not in self.active_trades:
            last_trade_time = None
            for trade in reversed(ai_bot_trade_history):
                if trade.get('symbol') == symbol and (trade['status'] == 'Entry' or trade['status'] == 'Exit'):
                    last_trade_time = datetime.datetime.fromisoformat(trade['timestamp'])
                    break
            if last_trade_time and (now - last_trade_time).total_seconds() < AI_BOT_MIN_TRADE_INTERVAL:
                return {"recommendation": "Neutral",
                        "rationale": f"Waiting for {AI_BOT_MIN_TRADE_INTERVAL / 60:.0f} min interval before new trade for {symbol}.",
                        "timestamp": now.isoformat(), "trade": "-", "strikes": "-", "type": "-",
                        "risk_pct": "-", "exit_rule": "-", "spot": float(spot), "pcr": float(pcr),
                        "intraday_pcr": float(intraday_pcr),  # Cast
                        "status": "No Trade", "pnl": 0.0, "action_price": 0.0,
                        "symbol": symbol
                        }

        if trade_logic_override:
            if "Prepare to write puts" in trade_logic_override:
                recommendation = "SELL"
                trade_summary = "Naked PE Write"
                strikes_selected = f"Sell {otm_pe_strike} PE @ {premium_pe:.2f}"
                trade_type = "Credit"
                action_price = float(premium_pe)  # Cast
                rationale = f"{trade_logic_override}. PCR {pcr:.2f} (Intraday PCR {intraday_pcr:.2f}). "
            elif "Write OTM calls" in trade_logic_override:
                recommendation = "SELL"
                trade_summary = "Naked CE Write"
                strikes_selected = f"Sell {otm_ce_strike} CE @ {premium_ce:.2f}"
                trade_type = "Credit"
                action_price = float(premium_ce)  # Cast
                rationale = f"{trade_logic_override}. PCR {pcr:.2f} (Intraday PCR {intraday_pcr:.2f}). "
            elif "Write puts aggressively" in trade_logic_override:
                recommendation = "SELL"
                trade_summary = "Naked PE Write"
                strikes_selected = f"Sell {otm_pe_strike} PE @ {premium_pe:.2f}"
                trade_type = "Credit"
                action_price = float(premium_pe)  # Cast
                rationale = f"{trade_logic_override}. PCR {pcr:.2f} (Intraday PCR {intraday_pcr:.2f}). "
            elif "Short strangle" in trade_logic_override:
                recommendation = "SELL"
                trade_summary = "Short Strangle"
                action_price = float(premium_ce + premium_pe)  # Cast
                strikes_selected = f"Sell {otm_ce_strike} CE + {otm_pe_strike} PE (Total Premium: {action_price:.2f})"
                trade_type = "Credit"
                rationale = f"{trade_logic_override}. PCR {pcr:.2f} (Intraday PCR {intraday_pcr:.2f}). "

            risk_pct_val = random.uniform(0.5, 2.0)
            risk_pct_display = f"{risk_pct_val:.2f}%"
            status = "Entry"

            final_recommendation = {
                "recommendation": recommendation, "rationale": rationale, "timestamp": now.isoformat(),
                "trade": trade_summary, "strikes": strikes_selected, "type": trade_type, "risk_pct": risk_pct_display,
                "exit_rule": exit_rule_display,
                "spot": float(spot), "pcr": float(pcr), "intraday_pcr": float(intraday_pcr), "status": status,
                "pnl": 0.0,  # Cast
                "action_price": round(action_price, 2),
                "entry_spot": float(spot), "entry_pcr": float(pcr), "entry_premium_ce": float(premium_ce),
                "entry_premium_pe": float(premium_pe),  # Cast
                "otm_ce_strike": float(otm_ce_strike), "otm_pe_strike": float(otm_pe_strike),  # Cast
                "symbol": symbol
            }
            if recommendation != "Neutral":
                self.active_trades[symbol] = final_recommendation
            ai_bot_trade_history.append(final_recommendation)
            return final_recommendation

        if symbol not in self.active_trades:
            max_pain_strike = self._calculate_max_pain_for_bot(df_ce, df_pe)
            if max_pain_strike:
                if spot > max_pain_strike + 100 and pcr < 0.9:
                    recommendation = "SELL"
                    trade_summary = "Naked CE Write"
                    strikes_selected = f"Sell {otm_ce_strike} CE @ {premium_ce:.2f}"
                    trade_type = "Credit"
                    action_price = float(premium_ce)  # Cast
                    rationale = f"Spot ({spot:.2f}) > Max Pain ({max_pain_strike:.2f}) + 100 and PCR ({pcr:.2f}) < 0.9. Pinning down. (Intraday PCR {intraday_pcr:.2f})."
                elif spot < max_pain_strike - 100 and pcr > 1.2:
                    recommendation = "SELL"
                    trade_summary = "Naked PE Write"
                    strikes_selected = f"Sell {otm_pe_strike} PE @ {premium_pe:.2f}"
                    trade_type = "Credit"
                    action_price = float(premium_pe)  # Cast
                    rationale = f"Spot ({spot:.2f}) < Max Pain ({max_pain_strike:.2f}) - 100 and PCR ({pcr:.2f}) > 1.2. Pinning up. (Intraday PCR {intraday_pcr:.2f})."
                if recommendation != "Neutral":
                    risk_pct_val = random.uniform(0.5, 2.0)
                    risk_pct_display = f"{risk_pct_val:.2f}%"
                    exit_rule_display = "Max Pain/PCR Divergence exit."
                    status = "Entry"

                    final_recommendation = {
                        "recommendation": recommendation, "rationale": rationale, "timestamp": now.isoformat(),
                        "trade": trade_summary, "strikes": strikes_selected, "type": trade_type,
                        "risk_pct": risk_pct_display, "exit_rule": exit_rule_display,
                        "spot": float(spot), "pcr": float(pcr), "intraday_pcr": float(intraday_pcr), "status": status,
                        "pnl": 0.0,  # Cast
                        "action_price": round(action_price, 2),
                        "entry_spot": float(spot), "entry_pcr": float(pcr), "entry_premium_ce": float(premium_ce),
                        "entry_premium_pe": float(premium_pe),
                        "otm_ce_strike": float(otm_ce_strike), "otm_pe_strike": float(otm_pe_strike),
                        "symbol": symbol
                    }
                    self.active_trades[symbol] = final_recommendation
                    ai_bot_trade_history.append(final_recommendation)
                    return final_recommendation

        if symbol not in self.active_trades:
            if pcr >= 1.0 and pcr <= 1.2:
                recommendation = "SELL"
                trade_summary = "Short Strangle"
                action_price = float(premium_ce + premium_pe)  # Cast
                strikes_selected = f"Sell {otm_ce_strike} CE + {otm_pe_strike} PE (Total Premium: {action_price:.2f})"
                trade_type = "Credit"
                rationale = f"PCR stable ({pcr:.2f}), suggesting short strangle for theta decay. (Intraday PCR {intraday_pcr:.2f})."
            elif pcr > 1.3:
                recommendation = "SELL"
                trade_summary = "Naked CE Write"
                strikes_selected = f"Sell {otm_ce_strike} CE @ {premium_ce:.2f}"
                trade_type = "Credit"
                action_price = float(premium_ce)  # Cast
                rationale = f"High PCR ({pcr:.2f}), indicating overbought or resistance, writing OTM CE. (Intraday PCR {intraday_pcr:.2f})."
            elif pcr < 0.8:
                recommendation = "SELL"
                trade_summary = "Naked PE Write"
                strikes_selected = f"Sell {otm_pe_strike} PE @ {premium_pe:.2f}"
                trade_type = "Credit"
                action_price = float(premium_pe)  # Cast
                rationale = f"Low PCR ({pcr:.2f}), indicating oversold or support, writing OTM PE. (Intraday PCR {intraday_pcr:.2f})."
            else:
                recommendation = "Neutral"
                rationale = f"PCR ({pcr:.2f}) is in a neutral range, awaiting clearer signals. (Intraday PCR {intraday_pcr:.2f})."

            if recommendation == "SELL":
                if is_ce_add_heavy:
                    recommendation = "SELL"
                    trade_summary = "Naked CE Write"
                    strikes_selected = f"Sell {otm_ce_strike} CE @ {premium_ce:.2f}"
                    action_price = float(premium_ce)  # Cast
                    rationale += " Confirmed by strong Call writing."
                elif is_pe_add_heavy:
                    recommendation = "SELL"
                    trade_summary = "Naked PE Write"
                    strikes_selected = f"Sell {otm_pe_strike} PE @ {premium_pe:.2f}"
                    action_price = float(premium_pe)  # Cast
                    rationale += " Confirmed by strong Put writing."
                elif not is_ce_add_heavy and not is_pe_add_heavy and "Short Strangle" not in trade_summary:
                    if pcr > 0.9 and pcr < 1.1:
                        recommendation = "SELL"
                        trade_summary = "Short Strangle"
                        action_price = float(premium_ce + premium_pe)  # Cast
                        strikes_selected = f"Sell {otm_ce_strike} CE + {otm_pe_strike} PE (Total Premium: {action_price:.2f})"
                        rationale += " Balanced OI activity, moving to short strangle."

            risk_pct_val = random.uniform(0.5, 2.0)
            risk_pct_display = f"{risk_pct_val:.2f}%"
            status = "Entry"

            final_recommendation = {
                "recommendation": recommendation,
                "rationale": rationale,
                "timestamp": now.isoformat(),
                "trade": trade_summary,
                "strikes": strikes_selected,
                "type": trade_type,
                "risk_pct": risk_pct_display,
                "exit_rule": exit_rule_display,
                "spot": float(spot), "pcr": float(pcr), "intraday_pcr": float(intraday_pcr), "status": status,
                "pnl": 0.0,  # Cast
                "action_price": round(action_price, 2),
                "entry_spot": float(spot), "entry_pcr": float(pcr), "entry_premium_ce": float(premium_ce),
                "entry_premium_pe": float(premium_pe),  # Cast
                "otm_ce_strike": float(otm_ce_strike), "otm_pe_strike": float(otm_pe_strike),  # Cast
                "symbol": symbol
            }
            if recommendation != "Neutral":
                self.active_trades[symbol] = final_recommendation
            ai_bot_trade_history.append(final_recommendation)
            return final_recommendation

        return {"recommendation": "Neutral", "rationale": "No new trade or active trade status.",
                "timestamp": now.isoformat(),
                "trade": "-", "strikes": "-", "type": "-", "risk_pct": "-",
                "exit_rule": "-", "spot": float(spot), "pcr": 0.0, "intraday_pcr": 0.0, "status": "No Trade",  # Cast
                "pnl": 0.0, "action_price": 0.0,
                "symbol": symbol
                }


class NewsAnalyzer:
    def __init__(self, nse_bse_analyzer_instance):
        self.analyzer = nse_bse_analyzer_instance
        self.last_news_check_time = 0

    def _load_recent_news_from_db(self):
        global news_alerts
        if not self.analyzer.conn:
            return
        cur = None
        try:
            cur = self.analyzer.conn.cursor()
            time_threshold = (
                    datetime.datetime.now(pytz.utc) - datetime.timedelta(days=NEWS_RETENTION_DAYS)).isoformat()
            cur.execute(
                """SELECT timestamp, Headline, Source, "Key Details", Impact, URL FROM news_alerts WHERE timestamp >= ? ORDER BY timestamp DESC""",
                (time_threshold,))
            rows = cur.fetchall()
            loaded_news = []
            for r in rows:
                loaded_news.append({
                    "timestamp": r[0],  # SQLite stores as string, keep as string
                    "Headline": r[1],
                    "Source": r[2],
                    "Key Details": r[3],
                    "Impact": r[4],
                    "URL": r[5]
                })
            with data_lock:
                news_alerts = loaded_news
            print(f"Loaded {len(news_alerts)} news alerts from DB.")
        except sqlite3.Error as e:
            print(f"SQLite Error loading news alerts from DB: {e}")
        finally:
            if cur:
                cur.close()

    def _save_news_alert_to_db(self, news_item: Dict[str, Any]):
        if not self.analyzer.conn:
            return
        cur = None
        try:
            cur = self.analyzer.conn.cursor()
            ts_iso = news_item.get('timestamp')  # Keep as ISO string for SQLite
            cur.execute(
                """INSERT INTO news_alerts (timestamp, Headline, Source, "Key Details", Impact, URL) VALUES (?,?,?,?,?,?)""",
                (ts_iso, news_item.get('Headline', ''), news_item.get('Source', ''),
                 news_item.get('Key Details', ''), news_item.get('Impact', ''), news_item.get('URL', ''))
            )
            time_threshold = (
                    datetime.datetime.now(pytz.utc) - datetime.timedelta(days=NEWS_RETENTION_DAYS)).isoformat()
            cur.execute("DELETE FROM news_alerts WHERE timestamp < ?",
                        (time_threshold,))
            self.analyzer.conn.commit()
        except sqlite3.Error as e:
            print(f"SQLite DB save error for news alert: {e}")
            self.analyzer.conn.rollback()
        finally:
            if cur:
                cur.close()

    def run_news_analysis(self):
        global news_alerts
        now_ts = time.time()
        current_time_only = self.analyzer._get_ist_time().time()

        # --- MODIFICATION START (to run news always) ---
        # Removed market hours check for news fetcher
        # if not (datetime.time(9, 0) <= current_time_only <= datetime.time(16, 0)):
        #     if self.analyzer.conn:
        #         cur = None
        #         try:
        #             cur = self.analyzer.conn.cursor()
        #             time_threshold = (datetime.datetime.now(pytz.utc) - datetime.timedelta(
        #                 days=NEWS_RETENTION_DAYS)).isoformat()
        #             cur.execute("DELETE FROM news_alerts WHERE timestamp < ?",
        #                         (time_threshold,))
        #             self.analyzer.conn.commit()
        #         except sqlite3.Error as e:
        #             print(f"SQLite Error clearing old news alerts: {e}")
        #             self.analyzer.conn.rollback()
        #         finally:
        #             if cur:
        #                 cur.close()
        #     return
        # --- MODIFICATION END (to run news always) ---

        if now_ts - self.last_news_check_time < NEWS_FETCH_INTERVAL:
            return

        print(f"--- Running RSS News Aggregator at {self.analyzer._get_ist_time().strftime('%H:%M:%S')} ---")
        self.last_news_check_time = now_ts

        newly_fetched_articles = fetch_market_news(top_n=20)
        if not newly_fetched_articles:
            print("No new articles fetched from RSS feeds.")
            return

        current_news_identifiers = set()
        for item in news_alerts:
            if 'Headline' in item and 'Source' in item:
                current_news_identifiers.add((item['Headline'], item['Source']))
            else:
                print(f"Warning: Malformed news alert in news_alerts, missing Headline or Source: {item}")

        unique_new_articles = []
        for article in newly_fetched_articles:
            if (article['Headline'], article['Source']) not in current_news_identifiers:
                unique_new_articles.append(article)
                current_news_identifiers.add(
                    (article['Headline'], article['Source']))

        if not unique_new_articles:
            print("No unique new articles found after filtering duplicates.")
            return

        print(f"Found {len(unique_new_articles)} unique new articles.")
        for article in unique_new_articles:
            with data_lock:
                news_alerts.insert(0, article)
            self._save_news_alert_to_db(article)

        with data_lock:
            time_threshold = datetime.datetime.now(self.analyzer.ist_timezone) - datetime.timedelta(
                days=NEWS_RETENTION_DAYS)
            cleaned_news_alerts = []
            for alert in news_alerts:
                if "timestamp" in alert:
                    try:
                        # SQLite timestamps are strings, convert to datetime for comparison
                        alert_dt = datetime.datetime.fromisoformat(alert["timestamp"])
                        if alert_dt.tzinfo is None:
                            alert_dt = pytz.utc.localize(alert_dt)
                        if alert_dt.date() >= time_threshold.date():
                            cleaned_news_alerts.append(alert)
                    except ValueError:
                        print(f"Warning: Could not parse timestamp '{alert['timestamp']}' for news alert. Skipping.")
                else:
                    print(f"Warning: News alert missing 'timestamp' key. Skipping: {alert.get('Headline', 'Unknown')}")
            news_alerts[:] = cleaned_news_alerts

        broadcast_news_update()
        print(f"Broadcasted {len(unique_new_articles)} new news alerts.")


# --------------------------------------------------------------------------- #
# ANALYZER CLASS
# --------------------------------------------------------------------------- #
class NseBseAnalyzer:
    def __init__(self):
        self.stop = threading.Event()
        self.session = requests.Session()
        self.nse_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'application/json, text/plain, */*',
            'Connection': 'keep-alive',
            'Referer': 'https://www.nseindia.com/option-chain'
        }
        self.url_oc = "https://www.nseindia.com/option-chain"
        self.url_indices = "https://www.nseindia.com/api/option-chain-indices?symbol="
        self.url_equities = "https://www.nseindia.com/api/option-chain-equities?symbol="
        self.url_symbols = "https://www.nseindia.com/api/underlying-information"
        self.db_path = 'nse_bse_data.db'  # SQLite DB path
        self.conn: Optional[sqlite3.Connection] = None  # Type hint for SQLite connection
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        self.YFINANCE_SYMBOLS = ["SENSEX", "INDIAVIX", "GOLD", "SILVER", "BTC-USD", "USD-INR"]
        self.TICKER_ONLY_SYMBOLS = ["GOLD", "SILVER", "BTC-USD", "USD-INR"]
        self.YFINANCE_TICKER_MAP = {"SENSEX": "^BSESN", "INDIAVIX": "^INDIAVIX", "GOLD": "GOLDBEES.NS",
                                    "SILVER": "SILVERBEES.NS", "BTC-USD": "BTC-USD", "USD-INR": "INR=X"}
        self.previous_data = {}
        load_dotenv()
        self._init_db()
        self.pcr_graph_data: Dict[str, List[Dict[str, Any]]] = {}
        self.previous_pcr: Dict[str, float] = {}
        self.bhavcopy_running = threading.Event()
        self.BHAVCOPY_INDICES_MAP = {"NIFTY": "NIFTY 50", "BANKNIFTY": "NIFTY BANK", "FINNIFTY": "NIFTY FIN SERVICE",
                                     "INDIAVIX": "INDIA VIX"}
        self.bhavcopy_last_run_date = None
        self.deepseek_bot = DeepSeekBot()
        self.sentiment_model = None
        self.sentiment_features = None
        self.sentiment_label_encoder = None
        self._load_ml_models()
        self.news_analyzer = NewsAnalyzer(self)
        self.news_analyzer._load_recent_news_from_db()
        self._set_nse_session_cookies()
        self.get_stock_symbols()
        self._load_todays_history_from_db()
        self._load_initial_underlying_values()  # IMPORTANT: Call after _load_todays_history_from_db
        self._populate_initial_shared_chart_data()
        self._load_latest_bhavcopy_from_db()

        # NEW: Dictionary to store historical OI/COI snapshots for 15/30 min calculations
        self.oi_coi_history: Dict[str, List[Dict[str, Any]]] = {}

        # New: Define mean IV ranges based on VIX for Nifty/Equity, FinNifty, BankNifty
        self.mean_iv_ranges = {
            "NIFTY": {
                (0, 10): (0, 10), (10, 13): (10, 13), (13, 14): (13, 14),
                (14, 15): (14, 15),  # Added
                (15, 17): (15, 17), (17, 20): (17, 20), (20, 25): (20, 25),
                (25, 30): (25, 30), (30, 100): (30, 40)  # Added upper bound for VIX
            },
            "FINNIFTY": {
                (0, 10): (0, 11), (10, 13): (11, 14.3), (13, 14): (14.3, 15.4),
                (14, 15): (15.4, 16.5),  # Added
                (15, 17): (16.5, 18.7), (17, 20): (18.7, 22), (20, 25): (22, 27.5),
                (25, 30): (27.5, 33), (30, 100): (33, 44)  # Added upper bound for VIX
            },
            "BANKNIFTY": {
                (0, 10): (0, 12.5), (10, 13): (12.5, 16.3), (13, 14): (16.3, 17.5),
                (14, 15): (17.5, 18.8),  # Added
                (15, 17): (18.8, 21.3), (17, 20): (21.3, 25), (20, 25): (25, 31.3),
                (25, 30): (31.3, 37.5), (30, 100): (37.5, 50)  # Added upper bound for VIX
            },
            # Default for F&O Equities if not explicitly defined
            "EQUITY": {
                (0, 10): (0, 10), (10, 13): (10, 13), (13, 14): (13, 14),
                (14, 15): (14, 15),  # Added
                (15, 17): (15, 17), (17, 20): (17, 20), (20, 25): (20, 25),
                (25, 30): (25, 30), (30, 100): (30, 40)  # Added upper bound for VIX
            }
        }

        threading.Thread(target=self.run_loop, daemon=True).start()
        threading.Thread(target=self.equity_fetcher_thread, daemon=True).start()
        threading.Thread(target=self.bhavcopy_scanner_thread, daemon=True).start()
        threading.Thread(target=self.news_analyzer_thread, daemon=True).start()

        self.send_telegram_message("NSE OCA PRO Bot is Online\n\nMonitoring will begin during market hours.")

    def news_analyzer_thread(self):
        print("News Analyzer thread started.")
        while not self.stop.is_set():
            self.news_analyzer.run_news_analysis()
            time.sleep(NEWS_FETCH_INTERVAL)

    def _set_nse_session_cookies(self):
        print("Attempting to refresh NSE session cookies...")
        try:
            response = self.session.get(self.url_oc, headers=self.nse_headers, timeout=10, verify=False)
            response.raise_for_status()
            print("NSE session cookies refreshed successfully.")
            return True
        except requests.exceptions.RequestException as e:
            print(f"Failed to refresh NSE session cookies: {e}")
            return False

    def get_stock_symbols(self):
        global fno_stocks_list
        global ai_bot_trades
        global last_ai_bot_run_time
        try:
            if not self._set_nse_session_cookies():
                print("Could not get fresh cookies, falling back to hardcoded symbols.")
                fno_stocks_list = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN"]
                for sym in fno_stocks_list:
                    if sym not in ai_bot_trades:
                        ai_bot_trades[sym] = {"recommendation": "Neutral", "rationale": "Waiting for data...",
                                              "timestamp": "", "trade": "-",
                                              "strikes": "-", "type": "-", "risk_pct": "-", "exit_rule": "-",
                                              "spot": 0.0, "pcr": 0.0, "intraday_pcr": 0.0,
                                              "symbol": sym}
                        last_ai_bot_run_time[sym] = 0
                return

            response = self.session.get(self.url_symbols, headers=self.nse_headers, timeout=10, verify=False)
            response.raise_for_status()  # Changed to raise_for_status
            json_data = response.json()
            fno_stocks_list = sorted([item['symbol'] for item in json_data['data']['UnderlyingList']])
            print(f"Successfully fetched {len(fno_stocks_list)} F&O stock symbols.")
            for sym in fno_stocks_list:
                if sym not in ai_bot_trades:
                    ai_bot_trades[sym] = {"recommendation": "Neutral", "rationale": "Waiting for data...",
                                          "timestamp": "", "trade": "-",
                                          "strikes": "-", "type": "-", "risk_pct": "-", "exit_rule": "-", "spot": 0.0,
                                          "pcr": 0.0, "intraday_pcr": 0.0,
                                          "symbol": sym}
                    last_ai_bot_run_time[sym] = 0
        except requests.exceptions.RequestException as e:
            print(f"Fatal error: Could not fetch stock symbols. {e}. Falling back to hardcoded symbols.")
            fno_stocks_list = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN"]
            for sym in fno_stocks_list:
                if sym not in ai_bot_trades:
                    ai_bot_trades[sym] = {"recommendation": "Neutral", "rationale": "Waiting for data...",
                                          "timestamp": "", "trade": "-",
                                          "strikes": "-", "type": "-", "risk_pct": "-", "exit_rule": "-", "spot": 0.0,
                                          "pcr": 0.0, "intraday_pcr": 0.0,
                                          "symbol": sym}
                    last_ai_bot_run_time[sym] = 0
        except Exception as e:
            print(f"An unexpected error occurred while fetching stock symbols: {e}. Falling back to hardcoded symbols.")
            fno_stocks_list = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN"]
            for sym in fno_stocks_list:
                if sym not in ai_bot_trades:
                    ai_bot_trades[sym] = {"recommendation": "Neutral", "rationale": "Waiting for data...",
                                          "timestamp": "", "trade": "-",
                                          "strikes": "-", "type": "-", "risk_pct": "-", "exit_rule": "-", "spot": 0.0,
                                          "pcr": 0.0, "intraday_pcr": 0.0,
                                          "symbol": sym}
                    last_ai_bot_run_time[sym] = 0

    def _init_db(self):
        cur = None
        try:
            # Use sqlite3.connect for SQLite
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # Allows accessing columns by name
            print(f"Connected to SQLite database: {self.db_path}")
            cur = self.conn.cursor()

            # --- Table: history ---
            # Use INTEGER PRIMARY KEY AUTOINCREMENT for auto-incrementing in SQLite
            # Use TEXT for timestamps to store ISO format
            cur.execute("""
                CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    symbol TEXT,
                    sp REAL,
                    value REAL,
                    call_oi REAL,
                    put_oi REAL,
                    pcr REAL,
                    sentiment TEXT,
                    add_exit TEXT,
                    intraday_pcr REAL,
                    ml_sentiment TEXT,
                    sentiment_reason TEXT,
                    implied_volatility REAL DEFAULT 0.0 -- Added new column for IV
                )
            """)

            # Check for existing columns and add if missing (SQLite compatible)
            cur.execute("PRAGMA table_info(history)")
            columns = [col[1] for col in cur.fetchall()]
            if 'ml_sentiment' not in columns:
                cur.execute("ALTER TABLE history ADD COLUMN ml_sentiment TEXT")
                print("Added 'ml_sentiment' column to 'history' table.")
            if 'sentiment_reason' not in columns:
                cur.execute("ALTER TABLE history ADD COLUMN sentiment_reason TEXT")
                print("Added 'sentiment_reason' column to 'history' table.")
            if 'implied_volatility' not in columns:  # Check and add IV column
                cur.execute("ALTER TABLE history ADD COLUMN implied_volatility REAL DEFAULT 0.0")
                print("Added 'implied_volatility' column to 'history' table.")

            # --- Table: bhavcopy_data ---
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bhavcopy_data (
                    date TEXT,
                    symbol TEXT,
                    close REAL,
                    volume INTEGER,
                    pct_change REAL,
                    delivery_pct REAL,
                    "open" REAL,
                    high REAL,
                    low REAL,
                    prev_close REAL,
                    trading_type TEXT,
                    PRIMARY KEY (date, symbol, trading_type)
                )
            """)

            # --- Table: index_bhavcopy_data ---
            cur.execute("""
                CREATE TABLE IF NOT EXISTS index_bhavcopy_data (
                    date TEXT,
                    symbol TEXT,
                    close REAL,
                    pct_change REAL,
                    "open" REAL,
                    high REAL,
                    low REAL,
                    PRIMARY KEY (date, symbol)
                )
            """)

            # --- Table: fno_bhavcopy_data ---
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fno_bhavcopy_data (
                    date TEXT,
                    symbol TEXT,
                    instrument TEXT,
                    expiry_date TEXT,
                    strike_price REAL,
                    option_type TEXT,
                    open_interest INTEGER,
                    change_in_oi INTEGER,
                    volume INTEGER,
                    close REAL,
                    delivery_pct REAL,
                    PRIMARY KEY (date, symbol, instrument, expiry_date, strike_price, option_type)
                )
            """)

            # --- Table: block_deal_data ---
            cur.execute("""
                CREATE TABLE IF NOT EXISTS block_deal_data (
                    date TEXT,
                    symbol TEXT,
                    trade_type TEXT,
                    quantity INTEGER,
                    price REAL,
                    PRIMARY KEY (date, symbol, quantity, price)
                )
            """)

            # --- Table: news_alerts ---
            cur.execute("""
                CREATE TABLE IF NOT EXISTS news_alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    Headline TEXT,
                    Source TEXT,
                    "Key Details" TEXT,
                    Impact TEXT,
                    URL TEXT
                )
            """)
            self.conn.commit()
        except sqlite3.Error as e:  # Catch sqlite3.Error
            print(f"SQLite DB error: {e}")
            raise
        finally:
            if cur:
                cur.close()

    def _load_latest_bhavcopy_from_db(self):
        global latest_bhavcopy_data
        if not self.conn:
            return
        cur = None
        try:
            cur = self.conn.cursor()
            latest_dates = []
            cur.execute("SELECT date FROM bhavcopy_data ORDER BY date DESC LIMIT 1")
            eq_date = cur.fetchone()
            if eq_date:
                latest_dates.append(eq_date[0])
            cur.execute("SELECT date FROM index_bhavcopy_data ORDER BY date DESC LIMIT 1")
            idx_date = cur.fetchone()
            if idx_date:
                latest_dates.append(idx_date[0])
            cur.execute("SELECT date FROM fno_bhavcopy_data ORDER BY date DESC LIMIT 1")
            fno_date = cur.fetchone()
            if fno_date:
                latest_dates.append(fno_date[0])

            latest_date = max(latest_dates) if latest_dates else None

            if not latest_date:
                print("No previous Bhavcopy data found in the database.")
                return

            latest_date_str = latest_date  # SQLite date is already string YYYY-MM-DD
            cur.execute(
                """SELECT symbol, close, volume, pct_change, delivery_pct, "open", high, low FROM bhavcopy_data WHERE date = ? AND trading_type = 'EQ'""",
                (latest_date_str,)
            )
            equity_rows = cur.fetchall()
            equities = []
            for row in equity_rows:
                equities.append({
                    "Symbol": row["symbol"],
                    "Close": float(row["close"]),  # Cast to float
                    "Volume": int(row["volume"]),  # Cast to int
                    "Pct Change": float(row["pct_change"]),  # Cast to float
                    "Delivery %": float(row["delivery_pct"]) if row["delivery_pct"] is not None else None,
                    # Cast to float
                    "Open": float(row["open"]),  # Cast to float
                    "High": float(row["high"]),  # Cast to float
                    "Low": float(row["low"])  # Cast to float
                })

            cur.execute(
                """SELECT symbol, close, pct_change, open, high, low FROM index_bhavcopy_data WHERE date = ?""",
                (latest_date_str,)
            )
            fetched_index_rows = cur.fetchall()
            indices = [{
                "Symbol": row["symbol"],
                "Close": float(row["close"]),  # Cast to float
                "Pct Change": float(row["pct_change"]),  # Cast to float
                "Open": float(row["open"]),  # Cast to float
                "High": float(row["high"]),  # Cast to float
                "Low": float(row["low"])  # Cast to float
            } for row in fetched_index_rows]
            print(f"DEBUG: _get_bhavcopy_for_date - Found {len(indices)} Index records for {latest_date_str}")

            if equities or indices:
                latest_bhavcopy_data["equities"] = equities
                latest_bhavcopy_data["indices"] = indices
                latest_bhavcopy_data["date"] = latest_date_str
                print(f"Successfully loaded Bhavcopy data for {latest_date_str} from database.")
        except sqlite3.Error as e:
            print(f"SQLite Error loading Bhavcopy from DB: {e}")
        finally:
            if cur:
                cur.close()

    def _populate_initial_shared_chart_data(self):
        with data_lock:
            for sym in AUTO_SYMBOLS:
                if sym not in self.YFINANCE_SYMBOLS:
                    if sym not in shared_data:
                        shared_data[sym] = {}
                    shared_data[sym]['pcr_chart_data'] = self.pcr_graph_data.get(sym, [])
                    # Initialize new chart data keys to empty lists
                    shared_data[sym]['ce_coi_day_chart_data'] = []
                    shared_data[sym]['pe_coi_day_chart_data'] = []
                    shared_data[sym]['ce_coi_15min_chart_data'] = []
                    shared_data[sym]['pe_coi_15min_chart_data'] = []
                    shared_data[sym]['ce_coi_30min_chart_data'] = []
                    shared_data[sym]['pe_coi_30min_chart_data'] = []

    def _load_todays_history_from_db(self):
        if not self.conn:
            return
        cur = None
        try:
            cur = self.conn.cursor()
            ist_now = self._get_ist_time()
            # SQLite stores datetime as strings, so convert to ISO format for comparison
            utc_start_dt_str = ist_now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(
                pytz.utc).isoformat()

            all_symbols_to_load = AUTO_SYMBOLS + fno_stocks_list
            for sym in all_symbols_to_load:
                cur.execute(
                    """SELECT timestamp, sp, value, call_oi, put_oi, pcr, sentiment, add_exit, intraday_pcr, ml_sentiment, sentiment_reason, implied_volatility
                       FROM history WHERE symbol = ? AND timestamp >= ? ORDER BY timestamp DESC""",
                    (sym, utc_start_dt_str))
                rows = cur.fetchall()
                with data_lock:
                    todays_history[sym] = []
                    for r in rows:
                        # Convert stored ISO string back to datetime object for timezone conversion
                        db_timestamp_utc = datetime.datetime.fromisoformat(r["timestamp"])
                        display_time = db_timestamp_utc.astimezone(self.ist_timezone).strftime("%H:%M")
                        history_item = {
                            'time': display_time,
                            'sp': float(r["sp"]),  # Cast to float
                            'value': float(r["value"]),  # Cast to float
                            'call_oi': float(r["call_oi"]),  # Cast to float
                            'put_oi': float(r["put_oi"]),  # Cast to float
                            'pcr': float(r["pcr"]),  # Cast to float
                            'sentiment': r["sentiment"],
                            'add_exit': r["add_exit"],
                            'intraday_pcr': float(r["intraday_pcr"]) if r["intraday_pcr"] is not None else 0.0,
                            # Cast to float
                            'ml_sentiment': r["ml_sentiment"] if r["ml_sentiment"] is not None else 'N/A',
                            'sentiment_reason': r["sentiment_reason"] if r["sentiment_reason"] is not None else 'N/A',
                            'implied_volatility': float(r["implied_volatility"]) if r[
                                                                                        "implied_volatility"] is not None else 0.0
                            # Cast to float
                        }
                        todays_history[sym].append(history_item)
                    if sym not in self.YFINANCE_SYMBOLS:
                        if todays_history.get(sym) and todays_history[sym]:
                            self.previous_pcr[sym] = float(todays_history[sym][0]['pcr'])  # Cast
                        self.pcr_graph_data[sym] = [
                            {"TIME": item['time'], "PCR": float(item['pcr']),
                             "IntradayPCR": float(item['intraday_pcr'])}  # Cast
                            for item in reversed(todays_history.get(sym, []))
                        ]
        except sqlite3.Error as e:
            print(f"SQLite History load error: {e}")
        finally:
            if cur:
                cur.close()

    def _load_initial_underlying_values(self):
        # When loading history, also populate initial_underlying_values
        for sym in AUTO_SYMBOLS + fno_stocks_list:
            if todays_history.get(sym):
                latest_summary = todays_history[sym][0]
                if 'value' in latest_summary:
                    initial_underlying_values[sym] = float(latest_summary['value'])

    def _get_ist_time(self) -> datetime.datetime:
        return datetime.datetime.now(self.ist_timezone)

    def _convert_utc_to_ist_display(self, utc_timestamp_obj: datetime.datetime) -> str:
        try:
            return (utc_timestamp_obj.astimezone(self.ist_timezone)).strftime("%H:%M")
        except (ValueError, TypeError):
            return "00:00"

    def clear_todays_history_db(self, sym: Optional[str] = None):
        if not self.conn:
            return
        cur = None
        try:
            cur = self.conn.cursor()
            ist_now = self._get_ist_time()
            utc_start_dt_str = ist_now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(
                pytz.utc).isoformat()

            all_symbols_to_load = AUTO_SYMBOLS + fno_stocks_list
            for s in all_symbols_to_load:
                if s not in todays_history:
                    todays_history[s] = []

            if sym:
                cur.execute("DELETE FROM history WHERE symbol = ? AND timestamp >= ?",
                            (sym, utc_start_dt_str))
                if sym in todays_history:
                    todays_history[sym] = []
            else:
                cur.execute("DELETE FROM history WHERE timestamp >= ?", (utc_start_dt_str,))
                for key in todays_history:
                    todays_history[key] = []
            self.conn.commit()
            print(f"Cleared today's history for: {'All' if not sym else sym}")
        except sqlite3.Error as e:
            print(f"SQLite Error clearing history: {e}")
            self.conn.rollback()
        finally:
            if cur:
                cur.close()

    def run_loop(self):
        try:
            self._set_nse_session_cookies()
        except requests.exceptions.RequestException as e:
            print(f"Initial NSE session setup failed: {e}")

        while not self.stop.is_set():
            now_ist = self._get_ist_time()
            current_time_only = now_ist.time()
            symbols_to_process = AUTO_SYMBOLS

            # --- MODIFICATION START (to run data fetch always) ---
            # Removed the market hours check from here to allow continuous data fetching
            # for sym in symbols_to_process:
            #     if sym in ["NIFTY", "FINNIFTY", "BANKNIFTY"] + fno_stocks_list:
            #         if not (NSE_FETCH_START_TIME <= current_time_only <= NSE_FETCH_END_TIME):
            #             if sym in self.deepseek_bot.active_trades:
            #                 current_vix_value = float(
            #                     shared_data.get("INDIAVIX", {}).get("live_feed_summary", {}).get(  # Cast
            #                         "current_value", 15.0))
            #                 active_trade_info = self.deepseek_bot.active_trades[sym]
            #                 dummy_df_ce = pd.DataFrame(
            #                     [{'strikePrice': float(active_trade_info.get('otm_ce_strike', 0)), 'openInterest': 0,
            #                       # Cast
            #                       'lastPrice': float(active_trade_info.get('entry_premium_ce', 0))}])  # Cast
            #                 dummy_df_pe = pd.DataFrame(
            #                     [{'strikePrice': float(active_trade_info.get('otm_pe_strike', 0)), 'openInterest': 0,
            #                       # Cast
            #                       'lastPrice': float(active_trade_info.get('entry_premium_pe', 0))}])  # Cast
            #                 self.deepseek_bot.analyze_and_recommend(sym, todays_history.get(sym, []),
            #                                                         current_vix_value,
            #                                                         dummy_df_ce, dummy_df_pe)
            #                 broadcast_live_update()
            #             print(
            #                 f"Skipping NSE data fetch for {sym} outside of market hours ({current_time_only.strftime('%H:%M')}). Retaining last data.")
            #             continue
            # --- MODIFICATION END (to run data fetch always) ---
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
            current_time_only = now_ist.time()

            # --- MODIFICATION START (to run equity fetch always) ---
            # Removed the market hours check from here to allow continuous data fetching
            # if NSE_FETCH_START_TIME <= current_time_only <= NSE_FETCH_END_TIME:
            print(f"Starting F&O equity fetch cycle at {now_ist.strftime('%H:%M:%S')}")
            self._set_nse_session_cookies()
            for i, symbol in enumerate(fno_stocks_list):
                try:
                    print(f"Fetching F&O Equity {i + 1}/{len(fno_stocks_list)}: {symbol}")
                    equity_data = self._process_equity_data(symbol)
                    if equity_data:
                        previous_data = equity_data_cache.get(symbol, {}).get('current')
                        equity_data_cache[symbol] = {'current': equity_data, 'previous': previous_data}
                    time.sleep(1)
                except Exception as e:
                    print(f"Error fetching {symbol} in equity loop: {e}")
            self.rank_and_emit_movers()
            print(f"F&O Equity fetch cycle finished. Sleeping for {EQUITY_FETCH_INTERVAL} seconds.")
            time.sleep(EQUITY_FETCH_INTERVAL)
            # else:
            #     print(
            #         f"Market is closed. F&O Equity fetcher sleeping. Current time: {now_ist.strftime('%H:%M:%S')}. Retaining last data.")
            #     for sym in fno_stocks_list:
            #         if sym in self.deepseek_bot.active_trades:
            #             current_vix_value = float(
            #                 shared_data.get("INDIAVIX", {}).get("live_feed_summary", {}).get(  # Cast
            #                     "current_value", 15.0))
            #             active_trade_info = self.deepseek_bot.active_trades[sym]
            #             dummy_df_ce = pd.DataFrame(
            #                 [{'strikePrice': float(active_trade_info.get('otm_ce_strike', 0)), 'openInterest': 0,
            #                   # Cast
            #                   'lastPrice': float(active_trade_info.get('entry_premium_ce', 0))}])  # Cast
            #             dummy_df_pe = pd.DataFrame(
            #                 [{'strikePrice': float(active_trade_info.get('otm_pe_strike', 0)), 'openInterest': 0,
            #                   # Cast
            #                   'lastPrice': float(active_trade_info.get('entry_premium_pe', 0))}])  # Cast
            #             self.deepseek_bot.analyze_and_recommend(sym, todays_history.get(sym, []), current_vix_value,
            #                                                     dummy_df_ce, dummy_df_pe)
            #             broadcast_live_update()
            #     time.sleep(60)
            # --- MODIFICATION END (to run equity fetch always) ---

    def bhavcopy_scanner_thread(self):
        print("Bhavcopy scanner thread started.")
        while not self.stop.is_set():
            now_ist = self._get_ist_time()
            if now_ist.hour >= 21 and (self.bhavcopy_last_run_date != now_ist.date().isoformat()):
                print(f"--- Triggering daily Bhavcopy scan for {now_ist.date().isoformat()} ---")
                scan_successful = False
                for i in range(5):
                    target_day = now_ist - datetime.timedelta(days=i)
                    if target_day.weekday() >= 5:
                        continue
                    if self.run_bhavcopy_for_date(target_day, trigger_manual=True):
                        print(f"Successfully processed Bhavcopy for date: {target_day.strftime('%Y-%m-%d')}")
                        self.send_telegram_message(
                            f"✅ Daily Bhavcopy scan complete.")
                        scan_successful = True
                        break
                self.bhavcopy_last_run_date = now_ist.date().isoformat()
                if scan_successful:
                    self.send_telegram_message(f"✅ Daily Bhavcopy scan complete.")
                else:
                    self.send_telegram_message(
                        f"❌ Daily Bhavcopy scan: Failed to process Bhavcopy for the last few trading days.")
            time.sleep(900)

    def download_bhavcopy_file_by_name(self, date_obj, file_pattern_or_name, target_folder, is_zip=False):
        """
        Attempts to download a bhavcopy file given a date and a file pattern/name.
        It tries a few common NSE base URLs.
        """
        date_dmy = date_obj.strftime('%d%m%Y')
        date_ymd = date_obj.strftime('%Y-%m-%d')
        month_upper = date_obj.strftime('%b').upper()
        year = date_obj.year

        possible_base_urls = [
            "https://nsearchives.nseindia.com/products/content/",
            f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/{year}/{month_upper}/",
            "https://nsearchives.nseindia.com/content/indices/",
            "https://nsearchives.nseindia.com/content/fo/",
            "https://nsearchives.nseindia.com/content/equities/",
            "https://nsearchives.nseindia.com/archives/fo/",
            "https://nsearchives.nseindia.com/archives/nsccl/sett/",
            "https://nsearchives.nseindia.com/content/cd/bhav/",
            "https://nsearchives.nseindia.com/content/com/",
            "https://nsearchives.nseindia.com/archives/ird/bhav/",
            f"https://www1.nseindia.com/content/historical/DERIVATIVES/{year}/{month_upper}/",
            "https://www1.nseindia.com/products/content/",
            "https://archives.nseindia.com/content/fo/bhav/",
            "https://archives.nseindia.com/content/equities/bulk_block/",
            "https://archives.nseindia.com/content/historical/equities/",
            "https://archives.nseindia.com/content/nsccl/fao_bhav/",
        ]

        file_candidates = []
        if "DDMMYYYY" in file_pattern_or_name:
            file_candidates.append(file_pattern_or_name.replace("DDMMYYYY", date_dmy))
        if "YYYY-MM-DD" in file_pattern_or_name:
            file_candidates.append(file_pattern_or_name.replace("YYYY-MM-DD", date_ymd))
        if "DDMMYY" in file_pattern_or_name:
            file_candidates.append(file_pattern_or_name.replace("DDMMYY", date_obj.strftime('%d%m%y')))
        if "YYYYMMDD" in file_pattern_or_name:
            file_candidates.append(file_pattern_or_name.replace("YYYYMMDD", date_obj.strftime('%Y%m%d')))
        if file_pattern_or_name not in file_candidates:
            file_candidates.append(file_pattern_or_name)

        final_file_candidates = []
        for fn in file_candidates:
            if is_zip and not fn.endswith(".zip"):
                final_file_candidates.append(f"{fn}.zip")
            elif not is_zip and not (fn.endswith(".csv") or fn.endswith(".DAT") or fn.endswith(".xls")):
                final_file_candidates.append(fn)
            else:
                final_file_candidates.append(fn)

        for base_url in possible_base_urls:
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
                        continue
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
        download_success = self.run_bhavcopy_for_date(date_obj, trigger_manual=True)
        if download_success:
            socketio.emit('bhavcopy_analysis_status',
                          {'success': True, 'message': f'Bhavcopy for {date_str} processed. Analyzing strategies...'},
                          to=sid)
            try:
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
                socketio.emit('bhavcopy_strategy_results', {'date': date_str, 'results': convert_numpy_types(results)},
                              to=sid)  # Convert here
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
            cm_file_path_local = None
            fno_file_path_local = None
            index_file_path_local = None
            block_file_path_local = None
            bulk_file_path_local = None

            print(f"Trying to find CM Bhavcopy for {target_date_str_dmy}...")
            cm_file_path_local = self.download_bhavcopy_file_by_name(date_obj,
                                                                     f"sec_bhavdata_full_{target_date_str_dmy}.csv",
                                                                     "Bhavcopy_Downloads/NSE")
            if not cm_file_path_local:
                cm_file_path_local = self.download_bhavcopy_file_by_name(date_obj,
                                                                         f"BhavCopy_NSE_CM_0_0_0_{date_obj.strftime('%Y%m%d')}_F_0000.csv.zip",
                                                                         "Bhavcopy_Downloads/NSE", is_zip=True)
                if cm_file_path_local:
                    print(f"Found alternative CM Bhavcopy: {os.path.basename(cm_file_path_local)}")

            print(f"Trying to find F&O Bhavcopy for {target_date_str_dmy}...")
            fno_file_path_local = self.download_bhavcopy_file_by_name(date_obj, f"fo{target_date_str_dmy}bhav.csv.zip",
                                                                      "Bhavcopy_Downloads/NSE", is_zip=True)
            if not fno_file_path_local:
                fno_file_path_local = self.download_bhavcopy_file_by_name(date_obj, f"fo{target_date_str_dmy}.zip",
                                                                          "Bhavcopy_Downloads/NSE", is_zip=True)
            if not fno_file_path_local:
                fno_file_path_local = self.download_bhavcopy_file_by_name(date_obj,
                                                                          f"PR{date_obj.strftime('%d%m%y')}.zip",
                                                                          "Bhavcopy_Downloads/NSE", is_zip=True)
            if not fno_file_path_local:
                fno_file_path_local = self.download_bhavcopy_file_by_name(date_obj,
                                                                          f"BhavCopy_NSE_FO_0_0_0_{date_obj.strftime('%Y%m%d')}_F_0000.csv.zip",
                                                                          "Bhavcopy_Downloads/NSE", is_zip=True)
            if not fno_file_path_local:
                fno_file_path_local = self.download_bhavcopy_file_by_name(date_obj,
                                                                          f"nsccl.{date_obj.strftime('%Y%m%d')}.s.zip",
                                                                          "Bhavcopy_Downloads/NSE", is_zip=True)
            if fno_file_path_local:
                print(f"Found F&O Bhavcopy: {os.path.basename(fno_file_path_local)}")

            print(f"Trying to find Index Bhavcopy for {target_date_str_dmy}...")
            index_file_path_local = self.download_bhavcopy_file_by_name(date_obj,
                                                                        f"ind_close_all_{target_date_str_dmy}.csv",
                                                                        "Bhavcopy_Downloads/NSE")
            if not index_file_path_local:
                print(
                    f"Standard Index Bhavcopy not found. Relying on process_and_save_bhavcopy_files to potentially extract from other files.")
                pass

            print(f"Trying to find Block Deals file for {target_date_str_dmy}...")
            block_file_path_local = self.download_bhavcopy_file_by_name(date_obj, "block.csv", "Bhavcopy_Downloads/NSE")
            if block_file_path_local:
                print(f"Found Block Deals file: {os.path.basename(block_file_path_local)}")

            print(f"Trying to find Bulk Deals file for {target_date_str_dmy}...")
            bulk_file_path_local = self.download_bhavcopy_file_by_name(date_obj, "bulk.csv", "Bhavcopy_Downloads/NSE")
            if bulk_file_path_local:
                print(f"Found Bulk Deals file: {os.path.basename(bulk_file_path_local)}")

            if cm_file_path_local or fno_file_path_local or index_file_path_local or block_file_path_local or bulk_file_path_local:
                downloaded_files_str = f"{'CM ' if cm_file_path_local else ''}{'F&O ' if fno_file_path_local else ''}{'Index ' if index_file_path_local else ''}{'Block ' if block_file_path_local else ''}{'Bulk ' if bulk_file_path_local else ''}"
                print(f"Bhavcopy files for {target_date_str_dmy} downloaded ({downloaded_files_str.strip()}).")

                extracted_data = self.process_and_save_bhavcopy_files(
                    target_date_str_dmy, cm_file_path_local, fno_file_path_local, index_file_path_local,
                    block_file_path_local, bulk_file_path_local
                )

                if extracted_data and (
                        extracted_data.get("equities") or extracted_data.get("indices") or extracted_data.get(
                    "fno_data") or extracted_data.get("block_deals")):
                    print(
                        f"Successfully extracted {len(extracted_data.get('equities', []))} equity, {len(extracted_data.get('fno_data', []))} F&O, {len(extracted_data.get('indices', []))} index, and {len(extracted_data.get('block_deals', []))} block deal records from Bhavcopy.")
                    latest_bhavcopy_data = extracted_data
                    print("Bhavcopy data extracted, stored in memory, and saved to DB.")
                    socketio.emit('bhavcopy_update', convert_numpy_types(latest_bhavcopy_data))  # Convert here
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
        all_block_deals_for_date = []

        if cm_file_path and os.path.exists(cm_file_path):
            try:
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
                    prev_close = float(r.get('PREV_CLOSE', 0.0))  # Cast to float
                    close_price = float(r.get('CLOSE_PRICE', 0.0))  # Cast to float
                    volume = int(r.get('TTL_TRD_QNTY', 0))  # Cast to int
                    open_price = float(r.get('OPEN_PRICE', 0.0))  # Cast to float
                    high_price = float(r.get('HIGH_PRICE', 0.0))  # Cast to float
                    low_price = float(r.get('LOW_PRICE', 0.0))  # Cast to float
                    trading_type = r.get('TRADING_TYPE', 'EQ')
                    chg = (((close_price - prev_close) / prev_close * 100)
                           if prev_close and prev_close > 0 and close_price is not None else 0.0)  # Ensure float
                    delivery_str = str(r.get('DELIV_PER', 'N/A')).strip()
                    try:
                        delivery = float(delivery_str.replace('%', '')) if delivery_str != 'N/A' else None
                    except ValueError:
                        delivery = None

                    all_equities_for_date.append({
                        "Symbol": sym,
                        "Close": round(close_price, 2) if close_price is not None else None,
                        "Volume": volume,  # Already int
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
                            pct_change = float(
                                str(pct_change).replace(',', '')) if pct_change is not None else 0.0  # Ensure float
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

        if block_file_path and os.path.exists(block_file_path):
            try:
                df_block = pd.read_csv(block_file_path, skipinitialspace=True)
                df_block.columns = [col.strip().upper() for col in df_block.columns]
                print(f"--- Block Deals (block.csv) for {db_date_str} loaded ---")

                for index, r in df_block.iterrows():
                    all_block_deals_for_date.append({
                        "Symbol": r['SYMBOL'],
                        "Trade Type": r.get('TRADE_TYPE', 'B'),
                        "Quantity": int(r['NO_OF_SHARES']),  # Cast to int
                        "Price": round(float(r['TRADE_PRICE']), 2)  # Cast to float
                    })
                print(f"Block Deals count from block.csv: {len(all_block_deals_for_date)}")
            except KeyError as e:
                print(
                    f"CRITICAL ERROR: A required column is missing in the Block Deal CSV (block.csv): {e}. Data extraction failed.")
            except Exception as e:
                print(f"Error reading Block Deal file (block.csv): {e}")

        if bulk_file_path and os.path.exists(bulk_file_path):
            try:
                df_bulk = pd.read_csv(bulk_file_path, skipinitialspace=True)
                df_bulk.columns = [col.strip().upper() for col in df_bulk.columns]
                print(f"--- Bulk Deals (bulk.csv) for {db_date_str} loaded ---")

                for index, r in df_bulk.iterrows():
                    all_block_deals_for_date.append({
                        "Symbol": r['SYMBOL'],
                        "Trade Type": r.get('TRADE_TYPE', 'K'),
                        "Quantity": int(r['NO_OF_SHARES']),  # Cast to int
                        "Price": round(float(r['TRADE_PRICE']), 2)  # Cast to float
                    })
                print(
                    f"Bulk Deals count from bulk.csv: {len(df_bulk)}. Total block/bulk deals: {len(all_block_deals_for_date)}")
            except KeyError as e:
                print(
                    f"CRITICAL ERROR: A required column is missing in the Bulk Deal CSV (bulk.csv): {e}. Data extraction failed.")
            except Exception as e:
                print(f"Error reading Bulk Deal file (bulk.csv): {e}")

        if self.conn:
            cur = None
            try:
                cur = self.conn.cursor()
                if all_equities_for_date:
                    cur.execute("DELETE FROM bhavcopy_data WHERE date = ? AND trading_type = 'EQ'", (db_date_str,))
                    for stock in all_equities_for_date:
                        cur.execute(
                            """INSERT INTO bhavcopy_data (date, symbol, close, volume, pct_change, delivery_pct, "open", high, low, prev_close, trading_type)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                db_date_str,
                                stock['Symbol'],
                                stock['Close'],
                                stock['Volume'],
                                stock['Pct Change'],
                                stock['Delivery %'],
                                stock['Open'],
                                stock['High'],
                                stock['Low'],
                                stock['Prev Close'],
                                stock['Trading Type']
                            )
                        )
                    print(
                        f"Saved {len(all_equities_for_date)} equity bhavcopy records for {db_date_str} to the database.")

                if all_indices_for_date:
                    cur.execute("DELETE FROM index_bhavcopy_data WHERE date = ?",
                                (db_date_str,))
                    for index_data in all_indices_for_date:
                        cur.execute(
                            """INSERT INTO index_bhavcopy_data (date, symbol, close, pct_change, "open", high, low)
                               VALUES (?, ?, ?, ?, ?, ?, ?)""",
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
                    print(
                        f"Saved {len(all_indices_for_date)} index bhavcopy records for {db_date_str} to the database.")

                if all_fno_data_for_date:
                    cur.execute("DELETE FROM fno_bhavcopy_data WHERE date = ?", (db_date_str,))
                    for fno_item in all_fno_data_for_date:
                        # SQLite stores date as TEXT, so store as YYYY-MM-DD string
                        expiry_date_str = datetime.datetime.strptime(fno_item['Expiry Date'],
                                                                     '%d-%b-%Y').strftime('%Y-%m-%d')
                        cur.execute(
                            """INSERT INTO fno_bhavcopy_data (date, symbol, instrument, expiry_date, strike_price, option_type, open_interest, change_in_oi, volume, close, delivery_pct)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                db_date_str,
                                fno_item['Symbol'],
                                fno_item['Instrument'],
                                expiry_date_str,  # Corrected to use string
                                float(fno_item['Strike Price']),  # Cast to float
                                fno_item['Option Type'],
                                int(fno_item['Open Interest']),  # Cast to int
                                int(fno_item['Change in OI']),  # Cast to int
                                int(fno_item['Volume']),  # Cast to int
                                float(fno_item['Close']),  # Cast to float
                                float(fno_item['Delivery %']) if fno_item['Delivery %'] is not None else None
                                # Cast to float
                            )
                        )
                    print(f"Saved {len(all_fno_data_for_date)} F&O bhavcopy records for {db_date_str} to the database.")

                if all_block_deals_for_date:
                    cur.execute("DELETE FROM block_deal_data WHERE date = ?", (db_date_str,))
                    for block_deal in all_block_deals_for_date:
                        cur.execute(
                            """INSERT INTO block_deal_data (date, symbol, trade_type, quantity, price)
                               VALUES (?, ?, ?, ?, ?)""",
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
            except sqlite3.Error as e:
                print(f"SQLite Error processing and saving Bhavcopy files for {db_date_str}: {e}")
                self.conn.rollback()
            finally:
                if cur:
                    cur.close()
        return {"equities": all_equities_for_date, "indices": all_indices_for_date, "fno_data": all_fno_data_for_date,
                "block_deals": all_block_deals_for_date, "date": db_date_str}

    def _get_bhavcopy_for_date(self, date_str: str) -> Dict[str, Any]:
        """Helper to fetch all relevant bhavcopy data for a given date."""
        print(f"DEBUG: _get_bhavcopy_for_date called for date: {date_str}")
        cur = None
        try:
            cur = self.conn.cursor()
            cur.execute(
                """SELECT symbol, close, volume, pct_change, delivery_pct, "open", high, low, prev_close, trading_type FROM bhavcopy_data WHERE date = ? AND trading_type = 'EQ'""",
                (date_str,)
            )
            equities = [{
                "Symbol": row["symbol"],
                "Close": float(row["close"]),
                "Volume": int(row["volume"]),
                "Pct Change": float(row["pct_change"]),
                "Delivery %": float(row["delivery_pct"]) if row["delivery_pct"] is not None else None,
                "Open": float(row["open"]),
                "High": float(row["high"]),
                "Low": float(row["low"]),
                "Prev Close": float(row["prev_close"]),
                "Trading Type": row["trading_type"]
            } for row in cur.fetchall()]
            print(f"DEBUG: _get_bhavcopy_for_date - Found {len(equities)} EQ records for {date_str}")

            cur.execute(
                """SELECT symbol, close, pct_change, open, high, low FROM index_bhavcopy_data WHERE date = ?""",
                (date_str,)
            )
            fetched_index_rows = cur.fetchall()
            indices = [{
                "Symbol": row["symbol"],
                "Close": float(row["close"]),
                "Pct Change": float(row["pct_change"]),
                "Open": float(row["open"]),
                "High": float(row["high"]),
                "Low": float(row["low"])
            } for row in fetched_index_rows]
            print(f"DEBUG: _get_bhavcopy_for_date - Found {len(indices)} Index records for {date_str}")

            cur.execute(
                """SELECT symbol, instrument, expiry_date, strike_price, option_type, open_interest, change_in_oi, volume, close, delivery_pct FROM fno_bhavcopy_data WHERE date = ?""",
                (date_str,)
            )
            fno_data = [{
                "Symbol": row["symbol"],
                "Instrument": row["instrument"],
                "Expiry Date": row["expiry_date"],
                "Strike Price": float(row["strike_price"]),
                "Option Type": row["option_type"],
                "Open Interest": int(row["open_interest"]),
                "Change in OI": int(row["change_in_oi"]),
                "Volume": int(row["volume"]),
                "Close": float(row["close"]),
                "Delivery %": float(row["delivery_pct"]) if row["delivery_pct"] is not None else None
            } for row in cur.fetchall()]
            print(f"DEBUG: _get_bhavcopy_for_date - Found {len(fno_data)} F&O records for {date_str}")

            cur.execute(
                """SELECT symbol, trade_type, quantity, price FROM block_deal_data WHERE date = ?""",
                (date_str,)
            )
            block_deals = [{
                "Symbol": row["symbol"],
                "Trade Type": row["trade_type"],
                "Quantity": int(row["quantity"]),
                "Price": float(row["price"])
            } for row in cur.fetchall()]
            print(f"DEBUG: _get_bhavcopy_for_date - Found {len(block_deals)} Block Deal records for {date_str}")

            return {"equities": equities, "indices": indices, "fno_data": fno_data, "block_deals": block_deals}
        finally:
            if cur:
                cur.close()

    def _get_historical_bhavcopy_for_stock(self, symbol: str, start_date_str: str, end_date_str: str, limit: int) -> \
            List[Dict[str, Any]]:
        """
        Fetches historical equity bhavcopy data for a specific stock over a period.
        `limit` is the maximum number of records to return.
        """
        cur = None
        try:
            cur = self.conn.cursor()
            cur.execute(
                """SELECT date, close, volume, delivery_pct, "open", high, low, prev_close FROM bhavcopy_data WHERE symbol = ? AND date BETWEEN ? AND ? AND trading_type = 'EQ' ORDER BY date DESC LIMIT ?""",
                (symbol, start_date_str, end_date_str, limit)
            )
            history = []
            for row in cur.fetchall():
                history.append({
                    "date": row["date"],
                    "Close": float(row["close"]),  # Cast to float
                    "Volume": int(row["volume"]),  # Cast to int
                    "Delivery %": float(row["delivery_pct"]) if row["delivery_pct"] is not None else None,
                    # Cast to float
                    "Open": float(row["open"]),  # Cast to float
                    "High": float(row["high"]),  # Cast to float
                    "Low": float(row["low"]),  # Cast to float
                    "Prev Close": float(row["prev_close"])  # Cast to float
                })
            return history
        finally:
            if cur:
                cur.close()

    def analyze_fii_dii_delivery_divergence(self, date_str: str) -> List[Dict[str, Any]]:
        return [{"Symbol": "N/A", "Signal": "Requires FII/DII data integration."}]

    def analyze_zero_delivery_future_roll(self, date_str: str) -> List[Dict[str, Any]]:
        signals = []
        try:
            current_day_data = self._get_bhavcopy_for_date(date_str)
            current_fno_df = pd.DataFrame(current_day_data['fno_data'])

            if current_fno_df.empty:
                print(f"DEBUG: ZDFT for {date_str} - No F&O data available.")
                return [{"Symbol": "N/A", "Near Month Delivery %": "N/A", "Next Month OI Increase %": "N/A",
                         "Signal": "No F&O data for " + date_str}]

            futures_df = current_fno_df[current_fno_df['Instrument'] == 'FUTSTK'].copy()
            if futures_df.empty:
                print(f"DEBUG: ZDFT for {date_str} - No FUTSTK data available.")
                return [{"Symbol": "N/A", "Near Month Delivery %": "N/A", "Next Month OI Increase %": "N/A",
                         "Signal": "No Futures (FUTSTK) data for " + date_str}]

            current_date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            prev_trading_day_str_1 = self._get_previous_trading_day(current_date_obj, 1)
            prev_trading_day_str_2 = self._get_previous_trading_day(current_date_obj, 2)

            if not prev_trading_day_str_1 or not prev_trading_day_str_2:
                print(f"DEBUG: ZDFT for {date_str} - Not enough previous trading days for OI comparison.")
                return [{"Symbol": "N/A", "Near Month Delivery %": "N/A", "Next Month OI Increase %": "N/A",
                         "Signal": "Not enough previous trading days for OI comparison."}]

            prev_day_data_2 = self._get_bhavcopy_for_date(prev_trading_day_str_2)
            prev_fno_df_2 = pd.DataFrame(prev_day_data_2['fno_data'])

            if prev_fno_df_2.empty:
                print(f"DEBUG: ZDFT for {date_str} - No F&O data from 2 days ago for OI comparison.")
                return [{"Symbol": "N/A", "Near Month Delivery %": "N/A", "Next Month OI Increase %": "N/A",
                         "Signal": "No F&O data from 2 days ago for OI comparison."}]

            prev_fno_df_2['Expiry Date'] = pd.to_datetime(prev_fno_df_2['Expiry Date'])
            futures_df['Expiry Date'] = pd.to_datetime(futures_df['Expiry Date'])
            prev_futures_df_2 = prev_fno_df_2[prev_fno_df_2['Instrument'] == 'FUTSTK']

            for symbol in futures_df['Symbol'].unique():
                stock_futures = futures_df[futures_df['Symbol'] == symbol].copy()
                stock_futures = stock_futures.sort_values(by='Expiry Date')

                if len(stock_futures) >= 2:
                    near_month_future = stock_futures.iloc[0]
                    next_month_future = stock_futures.iloc[1]

                    near_month_delivery_pct = float(near_month_future['Delivery %']) if near_month_future[
                                                                                            'Delivery %'] is not None else None  # Cast
                    next_month_oi_current = int(next_month_future['Open Interest'])  # Cast

                    prev_next_month_future_2_days_ago = prev_futures_df_2[
                        (prev_futures_df_2['Symbol'] == symbol) &
                        (prev_futures_df_2['Instrument'] == 'FUTSTK') &
                        (prev_futures_df_2['Expiry Date'] == next_month_future['Expiry Date'])
                        ]

                    if not prev_next_month_future_2_days_ago.empty:
                        prev_next_month_future_2_days_ago_row = prev_next_month_future_2_days_ago.iloc[0]
                        next_month_oi_2_days_ago = int(prev_next_month_future_2_days_ago_row['Open Interest'])  # Cast

                        oi_increase_pct = ((
                                                   next_month_oi_current - next_month_oi_2_days_ago) / next_month_oi_2_days_ago * 100) if next_month_oi_2_days_ago > 0 else float(
                            'inf')

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
        signals = []
        try:
            current_day_data = self._get_bhavcopy_for_date(date_str)
            equities_df = pd.DataFrame(current_day_data['equities'])
            block_deals_df = pd.DataFrame(current_day_data['block_deals'])
            print(
                f"DEBUG: BDGP for {date_str} - equities_df count: {len(equities_df)}, block_deals_df count: {len(block_deals_df)}")

            if equities_df.empty and block_deals_df.empty:
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
                bd_price = float(bd['Price'])  # Cast to float
                equity_data = equities_df[equities_df['Symbol'] == symbol]

                if not equity_data.empty:
                    eq = equity_data.iloc[0]
                    prev_close = float(eq['Prev Close'])  # Cast to float
                    current_open = float(eq['Open'])  # Cast to float

                    if prev_close is not None and current_open is not None and bd_price is not None:
                        is_discount_block_deal = (
                                bd_price < prev_close * 0.92) if prev_close and prev_close > 0 else False
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
                volume = int(eq['Volume'])  # Cast to int
                close_price = float(eq['Close'])  # Cast to float
                open_price = float(eq['Open'])  # Cast to float
                high_price = float(eq['High'])  # Cast to float
                low_price = float(eq['Low'])  # Cast to float
                delivery_pct = float(eq['Delivery %']) if eq['Delivery %'] is not None else None  # Cast to float

                if all(x is not None for x in [volume, close_price, open_price, high_price,
                                               low_price]) and delivery_pct is not None and volume > 0:
                    vwap = (high_price + low_price + close_price) / 3.0
                    if vwap > 0:
                        close_vs_vwap_pct = ((close_price - vwap) / vwap * 100)
                        if close_vs_vwap_pct > 7 and delivery_pct is not None and delivery_pct < 12:
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
        return [{"Symbol": "N/A", "Signal": "Requires Corporate Actions data and multi-day delivery trend analysis."}]

    def analyze_oi_momentum_trap(self, date_str: str) -> List[Dict[str, Any]]:
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

            current_date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            prev_trading_day_str = self._get_previous_trading_day(current_date_obj, 1)

            if not prev_trading_day_str:
                print(f"DEBUG: OIMT for {date_str} - Not enough previous trading days for OI comparison.")
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

            prev_fno_df['Expiry Date'] = pd.to_datetime(prev_fno_df['Expiry Date'])
            current_fno_df['Expiry Date'] = pd.to_datetime(current_fno_df['Expiry Date'])
            futures_df = current_fno_df[current_fno_df['Instrument'] == 'FUTSTK'].copy()
            prev_futures_df = prev_fno_df[prev_fno_df['Instrument'] == 'FUTSTK'].copy()

            for symbol in futures_df['Symbol'].unique():
                stock_futures = futures_df[futures_df['Symbol'] == symbol]
                prev_stock_futures = prev_futures_df[prev_futures_df['Symbol'] == symbol]
                current_eq = current_equities_df[current_equities_df['Symbol'] == symbol]
                prev_eq = prev_equities_df[prev_equities_df['Symbol'] == symbol]

                if not stock_futures.empty and not prev_stock_futures.empty and not current_eq.empty and not prev_eq.empty:
                    # Ensure we are comparing futures with the same expiry date
                    current_fut_near = stock_futures.sort_values(by='Expiry Date').iloc[0]
                    # Find the corresponding previous future with the same expiry date
                    prev_fut_near_matches = prev_stock_futures[
                        prev_stock_futures['Expiry Date'] == current_fut_near['Expiry Date']]

                    if not prev_fut_near_matches.empty:
                        prev_fut_near = prev_fut_near_matches.iloc[0]
                    else:
                        print(
                            f"DEBUG: OIMT for {date_str} - No matching previous day future for {symbol} with expiry {current_fut_near['Expiry Date']}. Skipping OI comparison.")
                        continue

                    current_eq_row = current_eq.iloc[0]
                    prev_eq_row = prev_eq.iloc[0]

                    price_change_pct = float((
                            (float(current_eq_row['Close']) - float(prev_eq_row['Close'])) / float(
                        prev_eq_row['Close']) * 100)) if \
                        float(prev_eq_row['Close']) > 0 else 0.0  # Casts
                    oi_change = int(current_fut_near['Open Interest']) - int(prev_fut_near['Open Interest'])  # Casts

                    signal_type = "N/A"
                    if price_change_pct > 0 and oi_change > 0:
                        signal_type = "Long Buildup"
                    elif price_change_pct > 0 and oi_change < 0:
                        signal_type = "Short Covering"
                    elif price_change_pct < 0 and oi_change > 0:
                        signal_type = "Short Buildup"
                    elif price_change_pct < 0 and oi_change < 0:
                        signal_type = "Long Unwinding"

                    delivery_pct = float(current_eq_row['Delivery %']) if current_eq_row[
                                                                              'Delivery %'] is not None else None  # Cast
                    institutional_conviction = "No"
                    if delivery_pct is not None and float(
                            delivery_pct) > 60:
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
        signals = []
        try:
            current_day_data = self._get_bhavcopy_for_date(date_str)
            equities_df = pd.DataFrame(current_day_data['equities'])
            print(f"DEBUG: VSSB for {date_str} - current_equities_df count: {len(equities_df)}")

            if equities_df.empty:
                return [{"Symbol": "N/A", "Today's Volume": "N/A", "10-Day Avg Volume": "N/A", "Volume Multiple": "N/A",
                         "Delivery %": "N/A", "Price Change %": "N/A", "Signal": "No Equity data for " + date_str}]

            current_date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()

            for index, eq in equities_df.iterrows():
                symbol = eq['Symbol']
                today_volume = int(eq['Volume'])  # Cast to int
                today_delivery_pct = float(eq['Delivery %']) if eq['Delivery %'] is not None else None  # Cast to float
                today_pct_change = float(eq['Pct Change'])  # Cast to float

                if all(x is not None for x in [today_volume, today_delivery_pct]) and today_volume > 0:
                    today_delivery_pct_float = float(
                        today_delivery_pct)

                    historical_data = self._get_historical_bhavcopy_for_stock(
                        symbol,
                        (current_date_obj - datetime.timedelta(days=30)).strftime('%Y-%m-%d'),
                        (current_date_obj - datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
                        10
                    )
                    historical_df = pd.DataFrame(historical_data)

                    if len(historical_df) >= 10:
                        avg_volume = float(historical_df['Volume'].mean())  # Cast to float
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
        return [{"Symbol": "N/A",
                 "Signal": "Requires detailed Options Bhavcopy parsing and storage for strike-wise OI analysis."}]

    def analyze_corporate_action_arbitrage(self, date_str: str) -> List[Dict[str, Any]]:
        return [{"Symbol": "N/A", "Signal": "Requires Corporate Actions data and multi-day delivery trend analysis."}]

    def _get_previous_trading_day(self, current_date: datetime.date, num_days_back: int) -> Optional[str]:
        if not self.conn:
            return None
        cur = None
        try:
            cur = self.conn.cursor()
            query = """
                    SELECT DISTINCT date FROM bhavcopy_data
                    WHERE date < ? AND trading_type = 'EQ'
                    ORDER BY date DESC
                    LIMIT ?
                """
            cur.execute(query, (current_date.strftime('%Y-%m-%d'), num_days_back))
            rows = cur.fetchall()
            if len(rows) < num_days_back:
                return None
            return rows[num_days_back - 1]["date"]  # SQLite date is already string YYYY-MM-DD
        except sqlite3.Error as e:
            print(f"SQLite Error getting previous trading day: {e}")
            return None
        finally:
            if cur:
                cur.close()

    def send_telegram_message(self, message):
        if not SEND_TEXT_UPDATES or TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN" or TELEGRAM_CHAT_ID == "YOUR_TELEGRAM_CHAT_ID":
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
                         "Bearish Reversal": -1.5, "Ultra Bullish": 2.5, "Strong Bearish Reversal": -2.5,
                         "Mild Bullish Trap": 1.5, "Ultra Bearish": -2.5,
                         "Weakening Bullish (Resistance Building)": -1.0,
                         "Strengthening Bullish (Support Building)": 1.0, "Bullish Reversal Confirmed": 2.0,
                         "Bearish Reversal Confirmed": -2.0, "Mild Bullish with Support": 1.0,
                         "Mild Bearish Emerging": -1.0, "Mild Bullish Relief": 0.8, "Mild Bearish Continuation": -0.8,
                         "Sustained Mild Bullish": 1.2, "Sustained Mild Bearish": -1.2,
                         "Volatile Neutral (Rangebound)": 0.0,
                         "Neutral - Monitor": 0.0}

        sentiment_to_use = summary.get('ml_sentiment', summary.get('sentiment', 'Neutral'))
        if sentiment_to_use == 'N/A':
            sentiment_to_use = summary.get('sentiment', 'Neutral')
        sentiment_score = sentiment_map.get(sentiment_to_use, 0)
        pcr = float(summary.get('pcr', 1.0))  # Cast
        intraday_pcr = float(summary.get('intraday_pcr', 1.0))  # Cast
        return round((sentiment_score * 1.5) + (pcr * 1.0) + (intraday_pcr * 1.2), 2)

    def calculate_reversal_score(self, current_data: Dict, previous_data: Optional[Dict]) -> float:
        if not previous_data:
            return 0.0

        current_summary = current_data.get('summary', {})
        previous_summary = previous_data.get('summary', {})

        current_pcr = float(current_summary.get('pcr', 1.0))  # Cast
        current_intraday_pcr = float(current_summary.get('intraday_pcr', 1.0))  # Cast
        previous_intraday_pcr = float(previous_summary.get('intraday_pcr', 1.0))  # Cast

        sentiment_map = {"Strong Bullish": 2, "Mild Bullish": 1, "Neutral": 0, "Mild Bearish": -1, "Strong Bearish": -2,
                         "Weakening": -0.5, "Strengthening": 0.5, "Bullish Reversal": 1.5,
                         "Bearish Reversal": -1.5, "Ultra Bullish": 2.5, "Strong Bearish Reversal": -2.5,
                         "Mild Bullish Trap": 1.5, "Ultra Bearish": -2.5,
                         "Weakening Bullish (Resistance Building)": -1.0,
                         "Strengthening Bullish (Support Building)": 1.0, "Bullish Reversal Confirmed": 2.0,
                         "Bearish Reversal Confirmed": -2.0, "Mild Bullish with Support": 1.0,
                         "Mild Bearish Emerging": -1.0, "Mild Bullish Relief": 0.8, "Mild Bearish Continuation": -0.8,
                         "Sustained Mild Bullish": 1.2, "Sustained Mild Bearish": -1.2,
                         "Volatile Neutral (Rangebound)": 0.0,
                         "Neutral - Monitor": 0.0}

        sentiment_to_use = current_summary.get('ml_sentiment', current_summary.get('sentiment', 'Neutral'))
        if sentiment_to_use == 'N/A':
            sentiment_to_use = current_summary.get('sentiment', 'Neutral')
        sentiment_score = sentiment_map.get(sentiment_to_use, 0)

        intraday_pcr_change = current_intraday_pcr - previous_intraday_pcr

        bullish_reversal_score = (1 / (current_pcr + 0.1)) * (current_intraday_pcr * 1.5) + (
                intraday_pcr_change * 10) + sentiment_score
        return round(float(bullish_reversal_score), 2)  # Cast

    def rank_and_emit_movers(self):
        global previous_improving_list, previous_worsening_list
        if not equity_data_cache:
            socketio.emit('top_movers_update', {
                'strongest': [], 'weakest': [], 'improving': [], 'worsening': [],
                'status': "Waiting for market data..."
            })
            return

        strength_scores = []
        reversal_scores = []
        for symbol, data_points in equity_data_cache.items():
            current_data = data_points.get('current')
            previous_data = data_points.get('previous')
            if not current_data:
                continue

            strength_score = self.calculate_strength_score(current_data)
            strength_scores.append(
                {'symbol': symbol, 'score': float(strength_score),  # Cast
                 'sentiment': current_data['summary'].get('ml_sentiment', current_data['summary']['sentiment']),
                 'pcr': float(current_data['summary']['pcr'])})  # Cast

            reversal_score = self.calculate_reversal_score(current_data, previous_data)
            reversal_scores.append(
                {'symbol': symbol, 'score': float(reversal_score),  # Cast
                 'sentiment': current_data['summary'].get('ml_sentiment', current_data['summary']['sentiment']),
                 'pcr': float(current_data['summary']['pcr'])})  # Cast

        strength_scores.sort(key=lambda x: x['score'], reverse=True)
        top_strongest = strength_scores[:10]
        top_weakest = strength_scores[-10:][::-1]

        improving_candidates = [s for s in reversal_scores if s['score'] > 0]
        worsening_candidates = [s for s in reversal_scores if s['score'] < 0]

        top_improving = sorted(improving_candidates, key=lambda x: x['score'], reverse=True)[:10]
        top_worsening = sorted(worsening_candidates, key=lambda x: x['score'])[:10]

        socketio.emit('top_movers_update', convert_numpy_types({  # Convert here
            'strongest': top_strongest,
            'weakest': top_weakest,
            'improving': top_improving,
            'worsening': top_worsening
        }))
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
            if not ticker_str:
                return

            current_price = 0.0
            change = 0.0
            pct_change = 0.0
            sentiment_yfinance = "Neutral"
            sentiment_reason_yfinance = "N/A - No OI Data"

            ticker = yf.Ticker(ticker_str)
            hist = ticker.history(period="5d")

            if hist.empty or len(hist) < 2:
                print(
                    f"Warning: Could not fetch sufficient data for {sym} ({ticker_str}). It might be temporarily unavailable or the ticker is incorrect. Displaying N/A.")
                current_price = 0.0
                change = 0.0
                pct_change = 0.0
                sentiment_yfinance = "N/A - No Data"
                sentiment_reason_yfinance = "Could not fetch sufficient historical data for YFinance symbol."
            else:
                current_price = float(hist['Close'].iloc[-1])  # Cast to float
                previous_close = float(hist['Close'].iloc[-2])  # Cast to float
                change = current_price - previous_close
                pct_change = (change / previous_close) * 100 if previous_close != 0 else 0

                # Store previous price for next calculation
                initial_underlying_values[sym] = current_price

                sentiment_yfinance = "Mild Bearish" if change < 0 else "Mild Bullish" if change > 0 else "Neutral"
                sentiment_reason_yfinance = "Price movement based on YFinance data."

            with data_lock:
                if sym not in shared_data:
                    shared_data[sym] = {}
                shared_data[sym]['live_feed_summary'] = {
                    'current_value': round(float(current_price), 4),  # Cast
                    'change': round(float(change), 4),  # Cast
                    'percentage_change': round(float(pct_change), 2)  # Cast
                }
                summary = {
                    'time': self._get_ist_time().strftime("%H:%M"),
                    'sp': round(float(change), 2),  # Cast (this is actually change for YFinance, not SP)
                    'value': round(float(current_price), 2),  # Cast
                    'pcr': round(float(pct_change), 2),  # Cast (this is actually pct_change for YFinance, not PCR)
                    'sentiment': sentiment_yfinance,
                    'call_oi': 0,
                    'put_oi': 0,
                    'add_exit': "Live Price",
                    'intraday_pcr': 0.0,  # Ensure float
                    'expiry': 'N/A',
                    'ml_sentiment': 'N/A',
                    'symbol': sym,
                    'sentiment_reason': sentiment_reason_yfinance,
                    'implied_volatility': 0.0,  # Placeholder for IV for YFinance symbols
                    'change': round(float(change), 2),  # Explicitly add change
                    'percentage_change': round(float(pct_change), 2)  # Explicitly add percentage change
                }
                shared_data[sym]['summary'] = summary
                shared_data[sym]['strikes'] = []
                shared_data[sym]['max_pain_chart_data'] = []
                shared_data[sym]['ce_oi_chart_data'] = []
                shared_data[sym]['pe_oi_chart_data'] = []
                shared_data[sym]['pcr_chart_data'] = []
                # Initialize new chart data keys to empty lists for YFinance symbols
                shared_data[sym]['ce_coi_day_chart_data'] = []
                shared_data[sym]['pe_coi_day_chart_data'] = []
                shared_data[sym]['ce_coi_15min_chart_data'] = []
                shared_data[sym]['pe_coi_15min_chart_data'] = []
                shared_data[sym]['ce_coi_30min_chart_data'] = []
                shared_data[sym]['pe_coi_30min_chart_data'] = []

            print(f"{sym} YFINANCE DATA UPDATED | Value: {current_price:.2f}")
            broadcast_live_update()

            now_ts_float = time.time()
            if now_ts_float - last_history_update.get(sym, 0) >= UPDATE_INTERVAL:
                with data_lock:
                    if sym not in todays_history:
                        todays_history[sym] = []
                    todays_history[sym].insert(0, summary)
                broadcast_history_append(sym, summary)
                last_history_update[sym] = now_ts_float
                self._save_db(sym, summary)

            if now_ts_float - last_alert.get(sym, 0) >= UPDATE_INTERVAL:
                self.send_alert(sym, summary)
                last_alert[sym] = now_ts_float

        except Exception as e:
            print(f"{sym} yfinance processing error: {e}")

    def _get_oi_buildup(self, price_change, oi_change):
        if abs(price_change) > 0.01:
            if price_change > 0 and oi_change > 0:
                return "Long Buildup"
            if price_change > 0 and oi_change < 0:
                return "Short Covering"
            if price_change < 0 and oi_change > 0:
                return "Short Buildup"
            if price_change < 0 and oi_change < 0:
                return "Long Unwinding"
        if oi_change > 100:
            return "Fresh OI Added"
        if oi_change < -100:
            return "OI Exited"
        return ""

    def _get_nse_option_chain_data(self, sym: str, is_equity: bool = False) -> Optional[Dict]:
        """
        Helper method to fetch and process NSE option chain data for both indices and F&O equities.
        Returns a dictionary containing summary, strikes, pulse_summary, chart data, and raw dataframes.
        """
        url = self.url_equities + sym if is_equity else self.url_indices + sym

        print(f"DEBUG: Attempting to fetch NSE Option Chain for {sym} from URL: {url}")

        response = None
        for attempt in range(2):
            try:
                response = self.session.get(url, headers=self.nse_headers, timeout=10, verify=False)
                if response.status_code == 403 and attempt == 0:
                    print(f"DEBUG: 403 Forbidden for {sym}, refreshing cookies and retrying...")
                    self._set_nse_session_cookies()
                    time.sleep(1)
                elif response.status_code == 200:
                    print(f"DEBUG: Successfully fetched data for {sym} (Attempt {attempt + 1}).")
                    break
                else:
                    print(f"DEBUG: HTTP Error {response.status_code} for {sym} (Attempt {attempt + 1}).")
                    if response is not None:
                        response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"DEBUG: RequestException for {sym} (Attempt {attempt + 1}): {e}")
                if attempt == 0:
                    print(f"DEBUG: Refreshing cookies and retrying for {sym}...")
                    self._set_nse_session_cookies()
                    time.sleep(1)
                else:
                    print(f"DEBUG: Failed to fetch {sym} after retry. Returning None.")
                    return None

        if response is None or response.status_code != 200:
            print(
                f"DEBUG: Final HTTP status for {sym} is {response.status_code if response else 'N/A'}. Returning None.")
            return None

        try:
            data = response.json()
        except ValueError as e:
            print(
                f"DEBUG: JSON decoding error for {sym}: {e}. Response content: {response.text[:200]}... Returning None.")
            return None

        if not data.get('records') or not data['records'].get('data'):
            print(f"DEBUG: No 'records' or 'data' found in JSON response for {sym}. Returning None.")
            return None

        expiry_dates = data['records']['expiryDates']
        if not expiry_dates:
            print(f"DEBUG: No expiry dates found for {sym} in JSON response. Returning None.")
            return None

        expiry = expiry_dates[0]
        if 'underlyingValue' not in data['records'] or data['records']['underlyingValue'] is None:
            print(f"DEBUG: 'underlyingValue' not found or is None for {sym}. Returning None.")
            return None

        underlying = float(data['records']['underlyingValue'])
        print(f"DEBUG: Found expiry: {expiry}, Underlying: {underlying} for {sym}.")

        prev_price = initial_underlying_values.get(sym)

        current_change = 0.0
        current_pct_change = 0.0
        if prev_price is None:
            initial_underlying_values[sym] = underlying
            print(f"DEBUG: Initializing underlying value for {sym}: {underlying}")
        else:
            current_change = underlying - prev_price
            current_pct_change = (current_change / prev_price) * 100 if prev_price != 0 else 0.0
            initial_underlying_values[sym] = underlying
            print(f"DEBUG: Calculated change for {sym}: {current_change:.2f} ({current_pct_change:.2f}%)")

        ce_values = [d['CE'] for d in data['records']['data'] if 'CE' in d and d['expiryDate'] == expiry]
        pe_values = [d['PE'] for d in data['records']['data'] if 'PE' in d and d['expiryDate'] == expiry]

        if not ce_values or not pe_values:
            print(
                f"DEBUG: No CE ({len(ce_values)}) or PE ({len(pe_values)}) values found for {sym} for expiry {expiry}. Returning None.")
            return None

        df_ce = pd.DataFrame(ce_values)
        df_pe = pd.DataFrame(pe_values)

        ce_dt_for_charts = pd.DataFrame()
        pe_dt_for_charts = pd.DataFrame()

        if not df_ce.empty:
            for col in ['strikePrice', 'impliedVolatility', 'openInterest', 'changeinOpenInterest', 'lastPrice',
                        'totalTradedVolume']:
                if col not in df_ce.columns:
                    df_ce[col] = 0.0
                df_ce[col] = pd.to_numeric(df_ce[col], errors='coerce').fillna(0)
            ce_dt_for_charts = df_ce.sort_values(by='strikePrice', ascending=True).copy()
        if not df_pe.empty:
            for col in ['strikePrice', 'impliedVolatility', 'openInterest', 'changeinOpenInterest', 'lastPrice',
                        'totalTradedVolume']:
                if col not in df_pe.columns:
                    df_pe[col] = 0.0
                df_pe[col] = pd.to_numeric(df_pe[col], errors='coerce').fillna(0)
            pe_dt_for_charts = df_pe.sort_values(by='strikePrice', ascending=True).copy()

        if ce_dt_for_charts.empty or pe_dt_for_charts.empty:
            print(f"DEBUG: CE or PE dataframes became empty after cleaning/conversion for {sym}. Returning None.")
            return None

        required_cols_ce = ['strikePrice', 'openInterest', 'changeinOpenInterest', 'lastPrice', 'impliedVolatility',
                            'totalTradedVolume']
        required_cols_pe = ['strikePrice', 'openInterest', 'changeinOpenInterest', 'lastPrice', 'impliedVolatility',
                            'totalTradedVolume']

        for col in required_cols_ce:
            if col not in ce_dt_for_charts.columns:
                ce_dt_for_charts[col] = 0.0
        for col in required_cols_pe:
            if col not in pe_dt_for_charts.columns:
                pe_dt_for_charts[col] = 0.0

        df = pd.merge(ce_dt_for_charts[required_cols_ce],
                      pe_dt_for_charts[required_cols_pe],
                      on='strikePrice', how='outer',
                      suffixes=('_call', '_put')).fillna(0)

        df['strikePrice'] = pd.to_numeric(df['strikePrice'], errors='coerce').fillna(0)

        sp = self.get_atm_strike(df, underlying)

        if not sp:
            print(f"DEBUG: Could not determine ATM strike for {sym}. Returning None.")
            return None

        df['strikePrice'] = pd.to_numeric(df['strikePrice'], errors='coerce')

        if sp not in df['strikePrice'].values:
            idx_series = (df['strikePrice'] - sp).abs().idxmin()
            idx = df.loc[idx_series].name
            print(
                f"DEBUG: Exact ATM strike {sp} not found for {sym}. Using closest strike {df.loc[idx, 'strikePrice']} at index {idx}.")
        else:
            idx_list = df[df['strikePrice'] == sp].index.tolist()
            if not idx_list:
                print(
                    f"DEBUG: ATM strike {sp} not found in dataframe after all checks for {sym}. This should not happen. Returning None.")
                return None
            idx = idx_list[0]

        strikes_data, ce_add_strikes, ce_exit_strikes, pe_add_strikes, pe_exit_strikes = [], [], [], [], []

        for i in range(-10, 11):
            if not (0 <= idx + i < len(df)):
                continue
            row = df.iloc[idx + i]
            strike = int(row['strikePrice'])
            call_oi = int(row.get('openInterest_call', 0))
            put_oi = int(row.get('openInterest_put', 0))
            # IMPORTANT: Use changeinOpenInterest_call/put from the API for the daily cumulative COI
            call_coi_daily = int(row.get('changeinOpenInterest_call', 0))
            put_coi_daily = int(row.get('changeinOpenInterest_put', 0))

            call_buildup = self._get_oi_buildup(current_change, call_coi_daily)
            put_buildup = self._get_oi_buildup(current_change, put_coi_daily)
            call_action = "ADD" if call_coi_daily > 0 else "EXIT" if call_coi_daily < 0 else ""
            # --- FIX: Changed 'put_coi' to 'put_coi_daily' ---
            put_action = "ADD" if put_oi > 0 else "EXIT" if put_coi_daily < 0 else ""
            # --- END FIX ---

            if call_action == "ADD":
                ce_add_strikes.append(str(strike))
            elif call_action == "EXIT":
                ce_exit_strikes.append(str(strike))
            if put_action == "ADD":
                pe_add_strikes.append(str(strike))
            elif put_action == "EXIT":
                pe_exit_strikes.append(str(strike))

            call_iv = float(row.get('impliedVolatility_call', 0.0))
            put_iv = float(row.get('impliedVolatility_put', 0.0))

            strikes_data.append(
                {'strike': strike, 'call_oi': call_oi, 'call_coi': call_coi_daily, 'call_action': call_action,
                 'put_oi': put_oi, 'put_coi': put_coi_daily, 'put_action': put_action, 'is_atm': i == 0,
                 'call_buildup': call_buildup, 'put_buildup': put_buildup,
                 'call_iv': round(call_iv, 2), 'put_iv': round(put_iv, 2)
                 })

        total_call_oi = int(df['openInterest_call'].sum())  # Total current OI, not change
        total_put_oi = int(df['openInterest_put'].sum())  # Total current OI, not change

        # --- MODIFICATION START: Renamed variables for clarity of daily COI ---
        total_call_coi_daily = int(df['changeinOpenInterest_call'].sum())  # This is the daily cumulative COI from API
        total_put_coi_daily = int(df['changeinOpenInterest_put'].sum())  # This is the daily cumulative COI from API

        # --- NEW: Calculate Diff. in OI for the day ---
        diff_oi_daily = total_call_coi_daily - total_put_coi_daily  # Calculate the difference
        # --- MODIFICATION END ---

        pcr = round(float(total_put_oi) / float(total_call_oi), 2) if total_call_oi else 0.0
        # --- MODIFICATION START: Use daily COI for intraday PCR if that's the intention ---
        # Assuming 'intraday_pcr' is meant to be based on the day's *change* in OI, not total OI
        intraday_pcr = round(float(total_put_coi_daily) / float(total_call_coi_daily),
                             2) if total_call_coi_daily != 0 else 0.0
        # --- MODIFICATION END ---

        atm_strike_data = next((s for s in strikes_data if s['is_atm']), None)
        atm_call_iv = float(atm_strike_data.get('call_iv', 0.0)) if atm_strike_data else 0.0
        atm_put_iv = float(atm_strike_data.get('put_iv', 0.0)) if atm_strike_data else 0.0

        average_iv = 0.0
        if atm_call_iv > 0 and atm_put_iv > 0:
            average_iv = (atm_call_iv + atm_put_iv) / 2
        elif atm_call_iv > 0:
            average_iv = atm_call_iv
        elif atm_put_iv > 0:
            average_iv = atm_put_iv

        max_pain_df_calc = self._calculate_max_pain(df_ce, df_pe)
        max_pain_strike = float(max_pain_df_calc.loc[max_pain_df_calc['TotalMaxPain'].idxmin()][
                                    'StrikePrice']) if not max_pain_df_calc.empty else None

        iv_skew = round(atm_put_iv - atm_call_iv, 2)

        highest_put_oi_strike = float(
            df_pe.loc[df_pe['openInterest'].idxmax()]['strikePrice']) if not df_pe.empty else None
        highest_call_oi_strike = float(
            df_ce.loc[df_ce['openInterest'].idxmax()]['strikePrice']) if not df_ce.empty else None

        total_call_volume = int(
            df['totalTradedVolume_call'].sum()) if 'totalTradedVolume_call' in df.columns else 0
        total_put_volume = int(
            df['totalTradedVolume_put'].sum()) if 'totalTradedVolume_put' in df.columns else 0
        volume_pcr = round(float(total_put_volume) / float(total_call_volume),
                           2) if total_call_volume > 0 else 0.0

        historical_avg_iv = 15.0
        current_vix = float(
            shared_data.get("INDIAVIX", {}).get("live_feed_summary", {}).get("current_value", 15.0))

        symbol_key_for_iv = sym if sym in ["NIFTY", "FINNIFTY", "BANKNIFTY"] else "EQUITY"
        vix_iv_ranges = self.mean_iv_ranges.get(symbol_key_for_iv, self.mean_iv_ranges["EQUITY"])

        effective_historical_avg_iv = historical_avg_iv
        for vix_range, iv_range in vix_iv_ranges.items():
            if float(vix_range[0]) <= float(current_vix) < float(vix_range[1]):
                effective_historical_avg_iv = (float(iv_range[0]) + float(
                    iv_range[1])) / 2
                break

        current_history_for_sym = todays_history.get(sym, [])

        # --- MODIFICATION START: Pass correct daily COI variables to get_sentiment ---
        rule_based_sentiment, sentiment_reason = self.get_sentiment(
            pcr, intraday_pcr, total_call_coi_daily, total_put_coi_daily, sym,
            current_history_for_sym, underlying, max_pain_strike,
            average_iv, iv_skew, highest_put_oi_strike, highest_call_oi_strike,
            volume_pcr, effective_historical_avg_iv, current_vix
        )

        ml_sentiment = self.get_ml_sentiment(pcr, intraday_pcr, total_call_coi_daily, total_put_coi_daily, sym,
                                             current_history_for_sym)
        # --- MODIFICATION END ---

        add_exit_str = " | ".join(filter(None,
                                         [f"CE Add: {', '.join(sorted(ce_add_strikes))}" if ce_add_strikes else "",
                                          f"PE Add: {', '.join(sorted(pe_add_strikes))}" if pe_add_strikes else "",
                                          f"CE Exit: {', '.join(sorted(ce_exit_strikes))}" if ce_exit_strikes else "",
                                          f"PE Exit: {', '.join(sorted(pe_exit_strikes))}" if pe_exit_strikes else ""])) or "No Change"

        # --- MODIFICATION START: The 'summary' dictionary ---
        summary = {'time': self._get_ist_time().strftime("%H:%M"),
                   'sp': int(sp),
                   'value': int(round(underlying)),
                   'call_oi': round(float(total_call_oi) / 1000, 1),  # Total OI for display
                   'put_oi': round(float(total_put_oi) / 1000, 1),  # Total OI for display
                   'call_coi_daily': round(float(total_call_coi_daily) / 1000, 1),  # Daily cumulative Call COI (in K)
                   'put_coi_daily': round(float(total_put_coi_daily) / 1000, 1),  # Daily cumulative Put COI (in K)
                   'pcr': float(pcr),
                   'sentiment': rule_based_sentiment,
                   'expiry': expiry,
                   'add_exit': add_exit_str,
                   'intraday_pcr': float(intraday_pcr),
                   'total_call_coi': int(total_call_coi_daily),  # Raw total daily cumulative Call COI
                   'total_put_coi': int(total_put_coi_daily),  # Raw total daily cumulative Put COI
                   'diff_oi_daily': int(diff_oi_daily),  # Explicitly add the daily OI difference
                   'ml_sentiment': ml_sentiment,
                   'sentiment_reason': sentiment_reason,
                   'symbol': sym,
                   'implied_volatility': round(float(average_iv), 2),
                   'change': round(current_change, 2),
                   'percentage_change': round(current_pct_change, 2)
                   }
        # --- MODIFICATION END ---

        # --- MODIFICATION START: The 'pulse_summary' dictionary ---
        pulse_summary = {'total_call_oi': int(total_call_coi_daily),
                         'total_put_oi': int(total_put_coi_daily),
                         'diff_oi_daily': int(diff_oi_daily),  # Add this here too if needed
                         'implied_volatility': round(float(average_iv), 2)
                         }
        # --- MODIFICATION END ---

        max_pain_chart_data = []
        if not max_pain_df_calc.empty:
            for _, row in max_pain_df_calc.iterrows():
                max_pain_chart_data.append({
                    'StrikePrice': float(row['StrikePrice']),
                    'TotalMaxPain': float(row['TotalMaxPain'])
                })

        final_ce_oi_data = []
        if not ce_dt_for_charts.empty:
            ce_dt_for_charts_sorted = ce_dt_for_charts.sort_values(by='strikePrice').copy()
            for _, row in ce_dt_for_charts_sorted.iterrows():
                final_ce_oi_data.append({
                    'strikePrice': float(row['strikePrice']),
                    'openInterest': int(row['openInterest'])
                })

        final_pe_oi_data = []
        if not pe_dt_for_charts.empty:
            pe_dt_for_charts_sorted = pe_dt_for_charts.sort_values(by='strikePrice').copy()
            for _, row in pe_dt_for_charts_sorted.iterrows():
                final_pe_oi_data.append({
                    'strikePrice': float(row['strikePrice']),
                    'openInterest': int(row['openInterest'])
                })

        # --- NEW CHARTS: COI for the Day ---
        # This should use the changeinOpenInterest directly from the API, which is daily cumulative
        final_ce_coi_day_data = []
        final_pe_coi_day_data = []
        if not df.empty:
            df_sorted = df.sort_values(by='strikePrice').copy()
            for _, row in df_sorted.iterrows():
                final_ce_coi_day_data.append({
                    'strikePrice': float(row['strikePrice']),
                    'changeinOpenInterest': int(row.get('changeinOpenInterest_call', 0))
                })
                final_pe_coi_day_data.append({
                    'strikePrice': float(row['strikePrice']),
                    'changeinOpenInterest': int(row.get('changeinOpenInterest_put', 0))
                })

        # --- NEW CHARTS: COI for Last 15/30 mins ---
        ce_coi_15min_chart_data = []
        pe_coi_15min_chart_data = []
        ce_coi_30min_chart_data = []
        pe_coi_30min_chart_data = []

        # Get the earliest snapshot for 15 and 30 min comparisons
        snapshot_15_min_ago = None
        snapshot_30_min_ago = None
        if sym in self.oi_coi_history:
            # Iterate from oldest to newest to find the first snapshot that meets the time criteria
            for snapshot in self.oi_coi_history[sym]:
                if self._get_ist_time() - snapshot['timestamp'] >= datetime.timedelta(
                        minutes=15) and snapshot_15_min_ago is None:
                    snapshot_15_min_ago = snapshot
                if self._get_ist_time() - snapshot['timestamp'] >= datetime.timedelta(
                        minutes=30) and snapshot_30_min_ago is None:
                    snapshot_30_min_ago = snapshot

        # Store current ABSOLUTE OI snapshot for future calculations
        current_oi_snapshot = {
            'timestamp': self._get_ist_time(),
            'data': {}  # {strike: {'call_oi': val, 'put_oi': val}}
        }

        if not df.empty:
            df_sorted = df.sort_values(by='strikePrice').copy()
            for _, row in df_sorted.iterrows():
                strike = int(row['strikePrice'])
                current_call_oi_abs = int(row.get('openInterest_call', 0))  # Absolute OI
                current_put_oi_abs = int(row.get('openInterest_put', 0))  # Absolute OI

                current_oi_snapshot['data'][strike] = {
                    'call_oi': current_call_oi_abs,
                    'put_oi': current_put_oi_abs
                }

                # Calculate 15 min COI using absolute OI
                prev_call_oi_abs_15m = snapshot_15_min_ago['data'].get(strike, {}).get('call_oi',
                                                                                       0) if snapshot_15_min_ago else 0
                prev_put_oi_abs_15m = snapshot_15_min_ago['data'].get(strike, {}).get('put_oi',
                                                                                      0) if snapshot_15_min_ago else 0

                ce_coi_15min_chart_data.append({
                    'strikePrice': float(strike),
                    'changeinOpenInterest': current_call_oi_abs - prev_call_oi_abs_15m
                })
                pe_coi_15min_chart_data.append({
                    'strikePrice': float(strike),
                    'changeinOpenInterest': current_put_oi_abs - prev_put_oi_abs_15m
                })

                # Calculate 30 min COI using absolute OI
                prev_call_oi_abs_30m = snapshot_30_min_ago['data'].get(strike, {}).get('call_oi',
                                                                                       0) if snapshot_30_min_ago else 0
                prev_put_oi_abs_30m = snapshot_30_min_ago['data'].get(strike, {}).get('put_oi',
                                                                                      0) if snapshot_30_min_ago else 0

                ce_coi_30min_chart_data.append({
                    'strikePrice': float(strike),
                    'changeinOpenInterest': current_call_oi_abs - prev_call_oi_abs_30m
                })
                pe_coi_30min_chart_data.append({
                    'strikePrice': float(strike),
                    'changeinOpenInterest': current_put_oi_abs - prev_put_oi_abs_30m
                })

        # Add the current snapshot to history
        if sym not in self.oi_coi_history:
            self.oi_coi_history[sym] = []
        self.oi_coi_history[sym].append(current_oi_snapshot)  # Append the absolute OI snapshot

        # Keep history clean (e.g., last 35 minutes, to allow for 30 min calculation)
        time_threshold_35_min = self._get_ist_time() - datetime.timedelta(minutes=35)
        self.oi_coi_history[sym] = [
            snapshot for snapshot in self.oi_coi_history[sym]
            if snapshot['timestamp'] >= time_threshold_35_min
        ]

        return {
            'summary': summary,
            'strikes': strikes_data,
            'pulse_summary': pulse_summary,
            'pcr_chart_data': [
                {"TIME": summary['time'], "PCR": float(pcr), "IntradayPCR": float(summary['intraday_pcr'])}],
            'max_pain_chart_data': max_pain_chart_data,
            'ce_oi_chart_data': final_ce_oi_data,
            'pe_oi_chart_data': final_pe_oi_data,
            'iv_skew_chart_data': strikes_data,
            'raw_df_ce': df_ce,
            'raw_df_pe': df_pe,
            'ce_coi_day_chart_data': final_ce_coi_day_data,  # Daily cumulative COI from API
            'pe_coi_day_chart_data': final_pe_coi_day_data,  # Daily cumulative COI from API
            'ce_coi_15min_chart_data': ce_coi_15min_chart_data,  # COI over last 15 mins
            'pe_coi_15min_chart_data': pe_coi_15min_chart_data,  # COI over last 15 mins
            'ce_coi_30min_chart_data': ce_coi_30min_chart_data,  # COI over last 30 mins
            'pe_coi_30min_chart_data': pe_coi_30min_chart_data,  # COI over last 30 mins
        }

    def _process_nse_data(self, sym: str):
        global ai_bot_trades
        global ai_bot_trade_history
        global last_ai_bot_run_time
        try:
            processed_data = self._get_nse_option_chain_data(sym, is_equity=False)

            if processed_data is None:
                print(f"INFO: _get_nse_option_chain_data returned None for {sym}. No data to update.")
                # The bot's active trade exit logic will still run even if no fresh OC data is available
                # This ensures active trades are handled at the end of the day or on specific exit triggers
                # regardless of new data availability.
                if sym in self.deepseek_bot.active_trades:
                    current_vix_value = float(shared_data.get("INDIAVIX", {}).get("live_feed_summary", {}).get(  # Cast
                        "current_value", 15.0))
                    active_trade_info = self.deepseek_bot.active_trades[sym]
                    # Pass empty dataframes to the bot if active trade exists, for exit logic
                    dummy_df_ce = pd.DataFrame(columns=['strikePrice', 'openInterest', 'lastPrice'])
                    dummy_df_pe = pd.DataFrame(columns=['strikePrice', 'openInterest', 'lastPrice'])
                    if 'otm_ce_strike' in active_trade_info:
                        dummy_df_ce.loc[0] = [float(active_trade_info.get('otm_ce_strike', 0)), 0,
                                              float(active_trade_info.get('entry_premium_ce', 0))]
                    if 'otm_pe_strike' in active_trade_info:
                        dummy_df_pe.loc[0] = [float(active_trade_info.get('otm_pe_strike', 0)), 0,
                                              float(active_trade_info.get('entry_premium_pe', 0))]

                    self.deepseek_bot.analyze_and_recommend(sym, todays_history.get(sym, []), current_vix_value,
                                                            dummy_df_ce, dummy_df_pe)
                    broadcast_live_update()
                return

            summary = processed_data['summary']
            strikes_data = processed_data['strikes']
            pulse_summary = processed_data['pulse_summary']
            pcr = float(summary['pcr'])  # Cast
            ml_sentiment = summary['ml_sentiment']
            rule_based_sentiment = summary['sentiment']
            sp = int(summary['sp'])  # Cast
            implied_volatility = float(summary.get('implied_volatility', 0.0))  # Cast

            with data_lock:
                if sym not in shared_data:
                    shared_data[sym] = {}
                shared_data[sym].update({
                    'summary': summary,
                    'strikes': strikes_data,
                    'pulse_summary': pulse_summary,
                    'max_pain_chart_data': processed_data['max_pain_chart_data'],
                    'ce_oi_chart_data': processed_data['ce_oi_chart_data'],
                    'pe_oi_chart_data': processed_data['pe_oi_chart_data'],
                    'iv_skew_chart_data': processed_data['iv_skew_chart_data'],
                    'ce_coi_day_chart_data': processed_data['ce_coi_day_chart_data'],
                    'pe_coi_day_chart_data': processed_data['pe_coi_day_chart_data'],
                    'ce_coi_15min_chart_data': processed_data['ce_coi_15min_chart_data'],
                    'pe_coi_15min_chart_data': processed_data['pe_coi_15min_chart_data'],
                    'ce_coi_30min_chart_data': processed_data['ce_coi_30min_chart_data'],
                    'pe_coi_30min_chart_data': processed_data['pe_coi_30min_chart_data'],
                })
                if sym not in self.pcr_graph_data:
                    self.pcr_graph_data[sym] = []
                # Ensure PCR and IntradayPCR are floats before appending
                self.pcr_graph_data[sym].append(
                    {"TIME": summary['time'], "PCR": float(pcr), "IntradayPCR": float(summary['intraday_pcr'])})
                if len(self.pcr_graph_data[sym]) > 180:
                    self.pcr_graph_data[sym].pop(0)
                shared_data[sym]['pcr_chart_data'] = self.pcr_graph_data[sym]

            print(
                f"{sym} LIVE DATA UPDATED | SP: {sp} | PCR: {pcr} | IV: {implied_volatility:.2f}% | Sentiment: {rule_based_sentiment} | ML Sentiment: {ml_sentiment}")
            broadcast_live_update()

            now_ts_float = time.time()
            if now_ts_float - last_history_update.get(sym, 0) >= UPDATE_INTERVAL:
                self.previous_pcr[sym] = float(pcr)  # Cast
                with data_lock:
                    if sym not in todays_history:
                        todays_history[sym] = []
                    todays_history[sym].insert(0, summary)
                broadcast_history_append(sym, summary)
                last_history_update[sym] = now_ts_float
                self._save_db(sym, summary)

            if (now_ts_float - last_ai_bot_run_time.get(sym, 0) >= AI_BOT_UPDATE_INTERVAL):
                current_vix_value = float(shared_data.get("INDIAVIX", {}).get("live_feed_summary", {}).get(  # Cast
                    "current_value", 15.0))
                bot_recommendation = self.deepseek_bot.analyze_and_recommend(sym, todays_history.get(sym, []),
                                                                             current_vix_value,
                                                                             processed_data['raw_df_ce'],
                                                                             processed_data['raw_df_pe'])
                with data_lock:
                    ai_bot_trades[sym] = bot_recommendation
                    now_dt = self._get_ist_time()
                    two_days_ago_dt = now_dt - datetime.timedelta(days=AI_BOT_HISTORY_DAYS)
                    ai_bot_trade_history[:] = [
                        entry for entry in ai_bot_trade_history
                        if datetime.datetime.fromisoformat(entry["timestamp"]).date() >= two_days_ago_dt.date()
                    ]
                print(
                    f"DeepSeekBot recommendation for {sym}: {bot_recommendation['recommendation']} - {bot_recommendation['trade']}")
                last_ai_bot_run_time[sym] = now_ts_float
                broadcast_live_update()

            if now_ts_float - last_alert.get(sym, 0) >= UPDATE_INTERVAL:
                self.send_alert(sym, summary)
                last_alert[sym] = now_ts_float

        except requests.exceptions.RequestException as e:
            print(f"{sym} processing error (RequestException): {e}")
        except Exception as e:
            import traceback
            print(f"{sym} processing error: {e}\n{traceback.format_exc()}")

    def process_and_emit_equity_data(self, symbol: str, sid: str):
        global ai_bot_trades
        global ai_bot_trade_history
        global last_ai_bot_run_time
        print(f"Processing equity data for {symbol} for client {sid}")
        try:
            processed_data = self._get_nse_option_chain_data(symbol, is_equity=True)
            if processed_data is None:
                print(
                    f"INFO: _get_nse_option_chain_data returned None for {symbol} (equity). No data to update.")
                if symbol in equity_data_cache:
                    socketio.emit('equity_data_update',
                                  {'symbol': symbol, 'data': convert_numpy_types(equity_data_cache[symbol]['current'])},
                                  to=sid)
                else:
                    socketio.emit('equity_data_update',
                                  {'symbol': symbol, 'error': 'No current data available or outside market hours.'},
                                  to=sid)
                return

            if processed_data:
                previous_data = equity_data_cache.get(symbol, {}).get('current')
                equity_data_cache[symbol] = {'current': processed_data, 'previous': previous_data}
            socketio.emit('equity_data_update', {'symbol': symbol, 'data': convert_numpy_types(processed_data)},
                          to=sid)

            with data_lock:
                if symbol not in shared_data:
                    shared_data[symbol] = {}
                shared_data[symbol].update({
                    'summary': processed_data['summary'],
                    'pulse_summary': processed_data['pulse_summary'],
                    'ce_oi_chart_data': processed_data.get('ce_oi_chart_data', []),
                    'pe_oi_chart_data': processed_data.get('pe_oi_chart_data', []),
                    'ce_coi_day_chart_data': processed_data.get('ce_coi_day_chart_data', []),
                    'pe_coi_day_chart_data': processed_data.get('pe_coi_day_chart_data', []),
                    'ce_coi_15min_chart_data': processed_data.get('ce_coi_15min_chart_data', []),
                    'pe_coi_15min_chart_data': processed_data.get('pe_coi_15min_chart_data', []),
                    'ce_coi_30min_chart_data': processed_data.get('ce_coi_30min_chart_data', []),
                    'pe_coi_30min_chart_data': processed_data.get('pe_coi_30min_chart_data', []),
                })
        except Exception as e:
            import traceback
            print(f"Error processing equity {symbol}: {e}\n{traceback.format_exc()}")
            socketio.emit('equity_data_update', {'symbol': symbol, 'error': str(e)}, to=sid)

    def _process_equity_data(self, sym: str) -> Optional[Dict]:
        """
        Fetches and processes equity option chain data, now using the common helper.
        """
        processed_data = self._get_nse_option_chain_data(sym, is_equity=True)
        if processed_data is None:
            print(
                f"INFO: _get_nse_option_chain_data returned None for {sym} (equity _process_equity_data). No data to update.")
            if sym in self.deepseek_bot.active_trades:
                current_vix_value = float(shared_data.get("INDIAVIX", {}).get("live_feed_summary", {}).get(  # Cast
                    "current_value", 15.0))
                active_trade_info = self.deepseek_bot.active_trades[sym]
                # Pass empty dataframes to the bot if active trade exists, for exit logic
                dummy_df_ce = pd.DataFrame(columns=['strikePrice', 'openInterest', 'lastPrice'])
                dummy_df_pe = pd.DataFrame(columns=['strikePrice', 'openInterest', 'lastPrice'])
                if 'otm_ce_strike' in active_trade_info:
                    dummy_df_ce.loc[0] = [float(active_trade_info.get('otm_ce_strike', 0)), 0,
                                          float(active_trade_info.get('entry_premium_ce', 0))]
                if 'otm_pe_strike' in active_trade_info:
                    dummy_df_pe.loc[0] = [float(active_trade_info.get('otm_pe_strike', 0)), 0,
                                          float(active_trade_info.get('entry_premium_pe', 0))]

                self.deepseek_bot.analyze_and_recommend(sym, todays_history.get(sym, []), current_vix_value,
                                                        dummy_df_ce, dummy_df_pe)
                broadcast_live_update()
            return None

        summary = processed_data['summary']
        now_ts_float = time.time()

        if now_ts_float - last_history_update.get(sym, 0) >= UPDATE_INTERVAL:
            self.previous_pcr[sym] = float(summary['pcr'])  # Cast
            with data_lock:
                if sym not in todays_history:
                    todays_history[sym] = []
                todays_history[sym].insert(0, summary)
            broadcast_history_append(sym, summary)
            last_history_update[sym] = now_ts_float
            self._save_db(sym, summary)

        if now_ts_float - last_alert.get(sym, 0) >= UPDATE_INTERVAL:
            self.send_alert(sym, summary)
            last_alert[sym] = now_ts_float

        global ai_bot_trades, ai_bot_trade_history, last_ai_bot_run_time
        if (now_ts_float - last_ai_bot_run_time.get(sym, 0) >= AI_BOT_UPDATE_INTERVAL):
            current_vix_value = float(shared_data.get("INDIAVIX", {}).get("live_feed_summary", {}).get(  # Cast
                "current_value", 15.0))
            bot_recommendation = self.deepseek_bot.analyze_and_recommend(sym, todays_history.get(sym, []),
                                                                         current_vix_value,
                                                                         processed_data['raw_df_ce'],
                                                                         processed_data['raw_df_pe'])
            with data_lock:
                ai_bot_trades[sym] = bot_recommendation
                now_dt = self._get_ist_time()
                two_days_ago_dt = now_dt - datetime.timedelta(days=AI_BOT_HISTORY_DAYS)
                ai_bot_trade_history[:] = [
                    entry for entry in ai_bot_trade_history
                    if datetime.datetime.fromisoformat(entry["timestamp"]).date() >= two_days_ago_dt.date()
                ]
            print(
                f"DeepSeekBot recommendation for {sym}: {bot_recommendation['recommendation']} - {bot_recommendation['trade']}")
            last_ai_bot_run_time[sym] = now_ts_float
            broadcast_live_update()
        return processed_data

    def get_atm_strike(self, df: pd.DataFrame, underlying: float) -> Optional[int]:
        try:
            # Ensure strikePrice is numeric and handle potential non-numeric values
            df_strikes = df['strikePrice'].dropna().astype(float)
            if df_strikes.empty:
                print(f"Warning: No valid strike prices found in DataFrame for underlying {underlying}.")
                return None

            strikes = df_strikes.unique()
            if len(strikes) > 0:
                # Find the closest strike from the *available* strikes
                closest_strike = min(strikes, key=lambda x: abs(x - underlying))

                # Keep your existing 20% check, it's a good sanity check
                if abs((closest_strike - underlying) / underlying) < 0.20:
                    return int(closest_strike)
                else:
                    print(
                        f"Warning: Closest strike {closest_strike} is too far from underlying {underlying} ({abs((closest_strike - underlying) / underlying) * 100:.2f}%). Check data for consistency.")
                    return None
            else:
                print(f"Warning: No unique strike prices found in DataFrame for underlying {underlying}.")
                return None
        except Exception as e:
            print(f"Error getting ATM strike for underlying {underlying}: {e}")
            return None

    def _load_ml_models(self):
        try:
            self.sentiment_model = joblib.load('sentiment_model.pkl')
            self.sentiment_features = joblib.load('sentiment_features.pkl')
            self.sentiment_label_encoder = joblib.load('sentiment_label_encoder.pkl')
            print("ML Sentiment model loaded successfully.")
        except FileNotFoundError:
            print(
                "ML Sentiment model files not found (sentiment_model.pkl, sentiment_features.pkl, sentiment_label_encoder.pkl). ML Sentiment will be 'N/A'.")
            self.sentiment_model = None
            self.sentiment_features = None
            self.sentiment_label_encoder = None
        except Exception as e:
            print(f"Error loading ML Sentiment model: {e}. ML Sentiment will be 'N/A'.")
            self.sentiment_model = None
            self.sentiment_features = None
            self.sentiment_label_encoder = None

    def get_ml_sentiment(self, pcr: float, intraday_pcr: float, total_call_coi: int, total_put_coi: int, sym: str,
                         history: List[Dict[str, Any]]) -> str:
        """
        Calculates sentiment using the loaded ML model. Returns 'N/A' if model is not loaded or an error occurs.
        """
        if self.sentiment_model and self.sentiment_features and self.sentiment_label_encoder and history:
            try:
                latest_data = history[0] if history else {}
                prev_data = history[1] if len(history) > 1 else {}

                feature_values = {
                    'pcr': float(pcr),  # Cast
                    'intraday_pcr': float(intraday_pcr),  # Cast
                    'value': float(latest_data.get('value', 0)),  # Cast
                    'call_oi': float(latest_data.get('call_oi', 0)),  # Cast
                    'put_oi': float(latest_data.get('put_oi', 0)),  # Cast
                    'pcr_lag1': float(prev_data.get('pcr', pcr)),  # Cast
                    'intraday_pcr_lag1': float(prev_data.get('intraday_pcr', intraday_pcr)),  # Cast
                    'value_lag1': float(prev_data.get('value', latest_data.get('value', 0))),  # Cast
                    'call_oi_lag1': float(prev_data.get('call_oi', latest_data.get('call_oi', 0))),  # Cast
                    'put_oi_lag1': float(prev_data.get('put_oi', latest_data.get('put_oi', 0))),  # Cast
                }

                feature_values['pcr_roc'] = float(pcr) - feature_values['pcr_lag1']  # Cast
                feature_values['intraday_pcr_roc'] = float(intraday_pcr) - feature_values['intraday_pcr_lag1']  # Cast
                feature_values['value_roc'] = float(latest_data.get('value', 0)) - feature_values['value_lag1']  # Cast
                feature_values['call_oi_roc'] = float(latest_data.get('call_oi', 0)) - feature_values[
                    'call_oi_lag1']  # Cast
                feature_values['put_oi_roc'] = float(latest_data.get('put_oi', 0)) - feature_values[
                    'put_oi_lag1']  # Cast
                feature_values['oi_spread'] = feature_values['put_oi'] - feature_values['call_oi']
                feature_values['oi_spread_roc'] = feature_values['oi_spread'] - (
                        float(prev_data.get('put_oi', 0)) - float(prev_data.get('call_oi', 0)))  # Cast

                input_df = pd.DataFrame([feature_values])

                for feature in self.sentiment_features:
                    if feature not in input_df.columns:
                        input_df[feature] = 0.0

                input_df = input_df[self.sentiment_features]
                predicted_label = int(self.sentiment_model.predict(input_df)[0])  # Cast to int
                predicted_sentiment_str = str(
                    self.sentiment_label_encoder.inverse_transform([predicted_label])[0])  # Cast to str
                return predicted_sentiment_str

            except Exception as e:
                print(f"Error during ML sentiment prediction for {sym}: {e}. Returning 'N/A'.")
                return 'N/A'
        return 'N/A'

    def get_sentiment(self, pcr: float, intraday_pcr: float, total_call_coi: int, total_put_coi: int, sym: str,
                      history: List[Dict[str, Any]], current_price: float, max_pain_strike: Optional[float],
                      implied_volatility: float, iv_skew: float, highest_put_oi_strike: Optional[float],
                      highest_call_oi_strike: Optional[float], volume_pcr: float, historical_avg_iv: float,
                      current_vix: float) -> Tuple[str, str]:
        """
        Calculates rule-based sentiment and its reason based on priority-based conditions
        with added factors like Max Pain, IV Skew, and Key OI Strikes.
        """
        PCR_BULLISH_THRESHOLD = 1.2
        PCR_BEARISH_THRESHOLD = 0.8
        PCR_STABLE_THRESHOLD = 0.05
        OI_CHANGE_THRESHOLD_FOR_TREND = 50000  # for Nifty, adjust for other symbols
        TREND_LOOKBACK = 5  # Number of historical periods for trend analysis

        pe_oi_delta_positive = total_put_coi > 0
        pe_oi_delta_negative = total_put_coi < 0
        ce_oi_delta_positive = total_call_coi > 0
        ce_oi_delta_negative = total_call_coi < 0

        pcr_is_stable = False
        pcr_is_rising = False
        pcr_is_falling = False
        cumulative_call_coi_history = 0
        cumulative_put_coi_history = 0
        cumulative_pcr_change = 0.0

        # Determine appropriate historical_avg_iv based on VIX range and symbol
        symbol_type = sym
        if sym not in ["NIFTY", "FINNIFTY", "BANKNIFTY"]:
            symbol_type = "EQUITY"  # Use generic EQUITY for stocks

        vix_iv_ranges = self.mean_iv_ranges.get(symbol_type, self.mean_iv_ranges["EQUITY"])

        effective_historical_avg_iv = historical_avg_iv  # Default if no match
        for vix_range, iv_range in vix_iv_ranges.items():
            if float(vix_range[0]) <= float(current_vix) < float(vix_range[1]):  # Cast
                effective_historical_avg_iv = (float(iv_range[0]) + float(
                    iv_range[1])) / 2  # Midpoint of the IV range, cast
                break

        if len(history) >= TREND_LOOKBACK:
            recent_history = history[:TREND_LOOKBACK]
            if recent_history and 'pcr' in recent_history[-1] and 'pcr' in recent_history[0]:
                first_pcr = float(recent_history[-1]['pcr'])  # Cast
                last_pcr = float(recent_history[0]['pcr'])  # Cast
                cumulative_pcr_change = last_pcr - first_pcr
                pcr_is_stable = abs(cumulative_pcr_change) < PCR_STABLE_THRESHOLD
                pcr_is_rising = cumulative_pcr_change > PCR_STABLE_THRESHOLD
                pcr_is_falling = cumulative_pcr_change < -PCR_STABLE_THRESHOLD

                cumulative_call_coi_history = sum(int(h.get('total_call_coi', 0)) for h in recent_history)  # Cast
                cumulative_put_coi_history = sum(int(h.get('total_put_coi', 0)) for h in recent_history)  # Cast

        # Helper for price near strike
        def is_near(price, strike, pct=0.005):
            if strike is None or price is None:
                return False
            return abs(float(price) - float(strike)) / float(price) < pct  # Cast

        # --- Priority-ordered Rules ---

        # 1. Ultra Bullish
        if (float(pcr) > 1.2 and pe_oi_delta_positive and float(current_price) > (
                float(max_pain_strike) if max_pain_strike is not None else float(current_price)) and  # Cast
                float(iv_skew) < 5.0):  # Low IV skew indicates less fear # Cast
            return "Ultra Bullish", "High PCR with put writing support, price above Max Pain magnet, low fear skew—strong upside momentum. Buy ATM calls or bull call spreads. SL below Max Pain."

        # 2. Strong Bearish Reversal
        if (float(pcr) > 1.2 and pe_oi_delta_negative and float(
                iv_skew) > 10.0):  # High IV skew indicates fear/put buying # Cast
            return "Strong Bearish Reversal", "Bullish PCR but unwinding puts and high put IV—signals weakening support, potential downside. Sell calls or buy puts; hedge with strangle if volume PCR <1. SL above recent high."

        # 3. Mild Bullish Trap
        if (float(pcr) < 0.8 and pe_oi_delta_positive and float(current_price) < (
                float(max_pain_strike) if max_pain_strike is not None else float(current_price))):  # Cast
            return "Mild Bullish Trap", "Bearish PCR but adding puts below Max Pain—possible short squeeze or reversal from oversold. Buy OTM calls; watch for breakout above Max Pain. Avoid if volume PCR >1 (conflicting)."

        # 4. Ultra Bearish
        if (float(pcr) < 0.8 and pe_oi_delta_negative and  # Cast
                highest_put_oi_strike is not None and float(current_price) < float(highest_put_oi_strike)):  # Cast
            return "Ultra Bearish", "Low PCR with unwinding puts above support strike—bearish continuation, short covering exhausted. Buy ATM puts or bear put spreads. SL above Max Pain."

        # 5. Weakening Bullish (Resistance Building)
        if (
                len(history) >= TREND_LOOKBACK and pcr_is_falling and cumulative_call_coi_history > OI_CHANGE_THRESHOLD_FOR_TREND and
                float(implied_volatility) > float(effective_historical_avg_iv) * 1.2):  # Cast
            return "Weakening Bullish (Resistance Building)", "Falling PCR with call OI buildup and elevated IV—growing overhead resistance, caution on longs. Reduce long positions; consider iron condor if rangebound (price near Max Pain ±1%)."

        # 6. Strengthening Bullish (Support Building)
        if (
                len(history) >= TREND_LOOKBACK and pcr_is_rising and cumulative_put_coi_history > OI_CHANGE_THRESHOLD_FOR_TREND and
                float(volume_pcr) > 1.0):  # Cast
            return "Strengthening Bullish (Support Building)", "Rising PCR with put OI accumulation and volume confirmation—solid floor forming. Add to longs on dips; bull put spread. SL at lowest put OI strike."

        # 7. Bullish Reversal Confirmed
        if (
                len(history) >= TREND_LOOKBACK and pcr_is_falling and cumulative_put_coi_history < -OI_CHANGE_THRESHOLD_FOR_TREND and
                float(current_price) > (
                float(max_pain_strike) if max_pain_strike is not None else float(current_price))):  # Cast
            return "Bullish Reversal Confirmed", "Falling PCR (less bearish) with put unwinding above Max Pain—shift to upside, fear reducing. Aggressive call buying; target 1–2% above current. Monitor IV crush post-reversal."

        # 8. Bearish Reversal Confirmed
        if (
                len(history) >= TREND_LOOKBACK and pcr_is_rising and cumulative_call_coi_history < -OI_CHANGE_THRESHOLD_FOR_TREND and
                float(iv_skew) > 5.0):  # Cast
            return "Bearish Reversal Confirmed", "Rising PCR (more bearish) with call unwinding and fear skew—downside shift, optimism fading. Put buying or short futures; hedge if near expiry. SL at highest call OI strike."

        # 9. Mild Bullish with Support
        if (float(pcr) >= 1.0 and pe_oi_delta_positive and  # Cast
                highest_put_oi_strike is not None and is_near(current_price, highest_put_oi_strike)):
            return "Mild Bullish with Support", "Neutral-high PCR reinforced by put writing at current levels—bias for bounce. Long straddle if IV low; otherwise, credit spreads."

        # 10. Mild Bearish Emerging
        if (float(pcr) >= 1.0 and ce_oi_delta_positive and float(volume_pcr) < 0.9):  # Cast
            return "Mild Bearish Emerging", "Bullish PCR but call writing and low volume PCR—resistance forming, cap on upside. Sell OTM calls; avoid if price breaks resistance."

        # 11. Mild Bullish Relief
        if (float(pcr) < 1.0 and ce_oi_delta_negative and float(implied_volatility) < float(
                effective_historical_avg_iv)):  # Cast
            return "Mild Bullish Relief", "Bearish PCR but call unwinding and low IV—potential short covering rally. Buy dips; calendar spreads for time decay."

        # 12. Mild Bearish Continuation
        if (float(pcr) < 1.0 and pe_oi_delta_negative and  # Cast
                highest_call_oi_strike is not None and float(current_price) < float(highest_call_oi_strike)):  # Cast
            return "Mild Bearish Continuation", "Bearish PCR with put unwinding below resistance—downside bias persists. Bear call spread; SL above resistance."

        # 13. Sustained Mild Bullish
        if (float(pcr) >= 1.1 and max_pain_strike is not None and float(current_price) > float(
                max_pain_strike) and pcr_is_stable):  # Cast
            return "Sustained Mild Bullish", "Leaning bullish PCR stable above Max Pain—range upside. Hold longs; add on pullbacks to support."

        # 14. Sustained Mild Bearish
        if (float(pcr) < 0.9 and max_pain_strike is not None and float(current_price) < float(
                max_pain_strike) and float(iv_skew) > 0):  # Cast
            return "Sustained Mild Bearish", "Leaning bearish PCR below Max Pain with put fear—range downside. Hold shorts; cover on bounces to resistance."

        # 15. Volatile Neutral (Rangebound)
        if (len(history) >= TREND_LOOKBACK and
                abs(cumulative_put_coi_history - cumulative_call_coi_history) < OI_CHANGE_THRESHOLD_FOR_TREND * 0.1 and
                float(implied_volatility) > 30.0):  # High IV for rangebound # Cast
            return "Volatile Neutral (Rangebound)", "Balanced OI history with high IV—no clear bias, expiry chop expected. Iron butterfly or straddle; profit from decay."

        # 16. Default Neutral
        return "Neutral - Monitor", "Lacking strong signals; wait for price confirmation or next data update. Stay sidelined; scalp with tight SL around Max Pain."

    def send_alert(self, sym: str, row: Dict[str, Any]):
        """
        Sends a Telegram alert with market data.
        """
        if not SEND_TEXT_UPDATES or TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN" or TELEGRAM_CHAT_ID == "YOUR_TELEGRAM_CHAT_ID":
            return
        try:
            # Use the 'change' and 'percentage_change' directly from the row/summary
            change = float(row.get('change', 0))  # Cast
            pct_change = float(row.get('percentage_change', 0))  # Cast
            implied_volatility = float(row.get('implied_volatility', 0.0))  # Cast

            change_str = f"+{change:.2f}" if change >= 0 else f"{change:.2f}"
            pct_str = f"+{pct_change:.2f}%" if pct_change >= 0 else f"{pct_change:.2f}%"

            message = f"* {sym.upper()} Update*\n\n"
            message += f"• Value: {float(row.get('value', 'N/A'))}\n"  # Cast
            message += f"• Change: {change_str} ({pct_str})\n"
            message += f"• PCR: {float(row.get('pcr', 'N/A'))}\n"  # Cast
            message += f"• Implied Volatility: {implied_volatility:.2f}%\n"
            message += f"• Sentiment (Rule-Based): {row.get('sentiment', 'N/A')}\n"
            message += f"• Reason: {row.get('sentiment_reason', 'N/A')}\n"
            message += f"• Sentiment (ML-Based): {row.get('ml_sentiment', 'N/A')}\n"
            if 'add_exit' in row and row['add_exit'] != "Live Price" and row['add_exit'] != "No Change":
                message += f"\nOI Changes:\n{row['add_exit']}"

            self.send_telegram_message(message)
        except Exception as e:
            print(f"Error formatting alert for {sym}: {e}")

    def _save_db(self, sym, row):
        if not self.conn:
            return
        cur = None
        try:
            cur = self.conn.cursor()
            ts = datetime.datetime.now(pytz.utc).isoformat()  # Store ISO formatted string for SQLite
            cur.execute(
                """INSERT INTO history (timestamp, symbol, sp, value, call_oi, put_oi, pcr, sentiment, add_exit, intraday_pcr, ml_sentiment, sentiment_reason, implied_volatility)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (ts, sym, float(row.get('sp', 0)), float(row.get('value', 0)), float(row.get('call_oi', 0)),
                 float(row.get('put_oi', 0)),  # Cast
                 float(row.get('pcr', 0)), row.get('sentiment', ''), row.get('add_exit', ''),  # Cast
                 float(row.get('intraday_pcr', 0.0)), row.get('ml_sentiment', 'N/A'),
                 row.get('sentiment_reason', 'N/A'),  # Cast
                 float(row.get('implied_volatility', 0.0))))  # Save IV, Cast
            # SQLite-compatible deletion for MAX_HISTORY_ROWS_DB
            cur.execute("""
                    DELETE FROM history
                    WHERE id NOT IN (
                        SELECT id FROM history
                        WHERE symbol = ?
                        ORDER BY timestamp DESC
                        LIMIT ?
                    ) AND symbol = ?
                """, (sym, MAX_HISTORY_ROWS_DB, sym))
            self.conn.commit()
        except sqlite3.Error as e:  # Catch sqlite3.Error
            print(f"SQLite DB save error for {sym}: {e}")
            self.conn.rollback()
        finally:
            if cur:
                cur.close()

    def _calculate_max_pain(self, ce_dt: pd.DataFrame, pe_dt: pd.DataFrame) -> pd.DataFrame:
        if ce_dt.empty or pe_dt.empty:
            return pd.DataFrame()

        MxPn_CE = ce_dt[['strikePrice', 'openInterest']]
        MxPn_PE = pe_dt[['strikePrice', 'openInterest']]

        MxPn_Df = pd.merge(MxPn_CE, MxPn_PE, on=['strikePrice'], how='outer', suffixes=('_call', '_Put')).fillna(0)

        # Ensure these are standard Python types
        StrikePriceList = [float(x) for x in MxPn_Df['strikePrice'].values.tolist()]
        OiCallList = [int(x) for x in MxPn_Df['openInterest_call'].values.tolist()]
        OiPutList = [int(x) for x in MxPn_Df['openInterest_Put'].values.tolist()]

        TCVSP = [int(self._total_option_pain_for_strike(StrikePriceList, OiCallList, OiPutList, sp_val)) for sp_val in
                 StrikePriceList]

        max_pain_df = pd.DataFrame({'StrikePrice': StrikePriceList, 'TotalMaxPain': TCVSP})

        if not max_pain_df.empty:
            min_pain_strike = float(
                max_pain_df.loc[max_pain_df['TotalMaxPain'].idxmin()]['StrikePrice'])  # Cast to float
            strikes_sorted = sorted(StrikePriceList)
            try:
                center_idx = strikes_sorted.index(min_pain_strike)
            except ValueError:
                # If min_pain_strike is not exactly in the list, find the closest
                center_idx = (pd.Series(strikes_sorted) - min_pain_strike).abs().idxmin()

            start_idx = max(0, center_idx - 8)
            end_idx = min(len(strikes_sorted), center_idx + 8)

            if start_idx >= end_idx:  # Ensure there's a valid range
                return pd.DataFrame()

            strikes_in_window = strikes_sorted[start_idx:end_idx]
            return max_pain_df[max_pain_df['StrikePrice'].isin(strikes_in_window)]
        return pd.DataFrame()

    def _total_option_pain_for_strike(self, strike_price_list: List[float], oi_call_list: List[int],
                                      oi_put_list: List[int], mxpn_strike: float) -> float:
        total_cash_value = sum((max(0, float(mxpn_strike) - float(strike)) * int(call_oi)) + (
                max(0, float(strike) - float(mxpn_strike)) * int(put_oi)) for  # Cast
                               strike, call_oi, put_oi in zip(strike_price_list, oi_call_list, oi_put_list))
        return float(total_cash_value)  # Cast


if __name__ == '__main__':
    analyzer = NseBseAnalyzer()
    print("WEB DASHBOARD LIVE → http://127.0.0.1:5000")
    socketio.run(app, host='0.0.0.0', port=5000)
