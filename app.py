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
import yfinance as yf
import numpy as np
import pytz
from zipfile import ZipFile  # Used for F&O Bhavcopy
from io import BytesIO
import joblib  # ADDED for ML model loading
import re  # ADDED for regex in news parsing
import feedparser  # ADDED for RSS feed parsing

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning)

# --------------------------------------------------------------------------- #
# CONFIG
# --------------------------------------------------------------------------- #
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"  # Replace with your bot token
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"  # Replace with your chat ID
SEND_TEXT_UPDATES = True
UPDATE_INTERVAL = 120
MAX_HISTORY_ROWS_DB = 10000
LIVE_DATA_INTERVAL = 15
EQUITY_FETCH_INTERVAL = 300  # 5 minutes

# AI Bot Configuration
AI_BOT_UPDATE_INTERVAL = 60  # Check for new AI Bot trades every 1 minute
AI_BOT_TRADING_START_TIME = datetime.time(9, 15)
AI_BOT_TRADING_END_TIME = datetime.time(15, 15)  # Bot active until 3:15 PM IST
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
    "https://www.cnbctv18.com/rss/markets.xml",  # This one was 404, but including as per request
    "https://www.livemint.com/rss/markets",
    "https://www.business-standard.com/rss/markets-122",
    "https://zeenews.india.com/markets.rss",  # Added a new one for variety
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
    # Using a requests.Session for potentially better performance and connection reuse
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
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

                # feedparser often provides a parsed_published_parsed or updated_parsed tuple
                if hasattr(e, 'published_parsed') and e.published_parsed:
                    published_dt_utc = datetime.datetime(*e.published_parsed[:6], tzinfo=pytz.utc)
                elif hasattr(e, 'updated_parsed') and e.updated_parsed:
                    published_dt_utc = datetime.datetime(*e.updated_parsed[:6], tzinfo=pytz.utc)
                else:  # Fallback for other formats if parsed_published_parsed is not available
                    pub_str = e.get("published", e.get("updated", ""))
                    if pub_str:
                        # Try common formats
                        for fmt in ["%a, %d %b %Y %H:%M:%S %Z", "%Y-%m-%dT%H:%M:%S%z", "%a, %d %b %Y %H:%M:%S GMT",
                                    "%Y-%m-%d %H:%M:%S"]:
                            try:
                                # Handle timezone offset if present, otherwise assume UTC
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

                # FIX: Change === to is None
                if published_dt_utc is None:
                    # If all parsing fails, use current UTC time
                    published_dt_utc = datetime.datetime.now(pytz.utc)

                # Convert to IST for display in the table, but store UTC for consistency
                time_ist_display = published_dt_utc.astimezone(pytz.timezone('Asia/Kolkata')).strftime("%H:%M")

                title = e.title.strip()
                summary_raw = (e.get("summary") or e.get("description") or "")
                # Clean HTML tags and limit length
                clean_summary = re.sub(r'<.*?>', '', summary_raw).strip()
                display_summary = (clean_summary[:120] + "...") if len(clean_summary) > 120 else clean_summary

                impact = _estimate_impact(title, clean_summary)
                link = e.link if hasattr(e, 'link') else '#'

                entries.append({
                    "timestamp": published_dt_utc.isoformat(),  # Store UTC ISO format for sorting/filtering
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
        time.sleep(0.5)  # be gentle to RSS servers

    # Dedupe + sort newest first
    # Only create DataFrame if entries is not empty to avoid KeyError on empty 'timestamp' column
    if entries:
        df = pd.DataFrame(entries).drop_duplicates(
            subset=["Headline", "Source"])  # Deduplicate based on title and source
        df['timestamp_dt'] = pd.to_datetime(df['timestamp'])  # Convert to datetime for proper sorting
        df = df.sort_values(by='timestamp_dt', ascending=False)
        return df.head(top_n).drop(columns=['timestamp_dt']).to_dict(orient='records')  # Return as list of dicts
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
# latest_bhavcopy_data will now combine both equity and index data for display
latest_bhavcopy_data: Dict[str, Any] = {"equities": [], "indices": [], "date": None}

# NEW: Global variable for AI Bot trades (now includes equities)
ai_bot_trades: Dict[str, Any] = {}
# NEW: Global variable for AI Bot trade history
ai_bot_trade_history: List[Dict[str, Any]] = []

last_ai_bot_run_time: Dict[str, float] = {}

# NEW: Global variable for news alerts (simplified structure)
news_alerts: List[Dict[str, Any]] = []


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
        # NEW: Also emit AI bot trades and history on connect
        emit('update', {'live': shared_data, 'live_feed_summary': live_feed_summary, 'ai_bot_trades': ai_bot_trades,
                        'ai_bot_trade_history': ai_bot_trade_history},
             to=request.sid)
        emit('initial_todays_history', {'history': todays_history}, to=request.sid)
        # Emit last known top movers on connect
        analyzer.rank_and_emit_movers()
        # NEW: Emit current news alerts on connect
        emit('news_update', {'news': news_alerts}, to=request.sid)


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
                print(f"Successfully processed Bhavcopy for date: {target_day.strftime('%Y-%m-%d')}")
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
        # NEW: Include AI bot trades and history in the broadcast
        socketio.emit('update',
                      {'live': shared_data, 'live_feed_summary': live_feed_summary, 'ai_bot_trades': ai_bot_trades,
                       'ai_bot_trade_history': ai_bot_trade_history})


def broadcast_history_append(sym: str, new_history_item: Dict[str, Any]):
    with data_lock: socketio.emit('todays_history_append', {'symbol': sym, 'item': new_history_item})


# NEW: Function to broadcast news updates
def broadcast_news_update():
    with data_lock:
        socketio.emit('news_update', {'news': news_alerts})


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
                "SELECT timestamp, sp, value, call_oi, put_oi, pcr, sentiment, add_exit, intraday_pcr, ml_sentiment FROM history WHERE symbol = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp DESC",
                (symbol, utc_day_start.strftime('%Y-%m-%d %H:%M:%S'), utc_day_end.strftime('%Y-%m-%d %H:%M:%S')))
            rows = cur.fetchall()
            for r in rows:
                history_for_date.append(
                    {'time': analyzer._convert_utc_to_ist_display(r[0]), 'sp': r[1], 'value': r[2], 'call_oi': r[3],
                     'put_oi': r[4], 'pcr': r[5], 'sentiment': r[6], 'add_exit': r[7],
                     'intraday_pcr': r[8] if r[8] is not None else 0.0,
                     'ml_sentiment': r[9] if r[9] is not None else 'N/A'})
        except Exception as e:
            return jsonify({"error": "Failed to query database."}), 500
    return jsonify({"history": history_for_date})


@app.route('/clear_todays_history', methods=['POST'])
def clear_history_endpoint():
    symbol_to_clear = request.json.get('symbol')
    analyzer.clear_todays_history_db(symbol_to_clear)
    return jsonify({"status": "success", "message": "Today's history cleared."})


# NEW: DeepSeekBot Class (ML Sentiment will be integrated into summary, not directly here)
class DeepSeekBot:
    def __init__(self):
        self.recommendations: Dict[str, Dict[str, Any]] = {}
        self.active_trades: Dict[str, Dict[str, Any]] = {}
        self.last_vix: float = 15.0

    def _calculate_max_pain_for_bot(self, df_ce: pd.DataFrame, df_pe: pd.DataFrame) -> Optional[float]:
        """Calculates Max Pain from the provided CE and PE DataFrames."""
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

        StrikePriceList = MxPn_Df['strikePrice'].values.tolist()
        OiCallList = MxPn_Df['openInterest_CE'].values.tolist()
        OiPutList = MxPn_Df['openInterest_PE'].values.tolist()

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

        return max_pain_strike

    def analyze_and_recommend(self, symbol: str, history: List[Dict[str, Any]], current_vix: float, df_ce: pd.DataFrame,
                              df_pe: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyzes the given history for a symbol and provides a trade recommendation based on prompts.
        """
        # --- FIX: Move global declaration to the top ---
        global ai_bot_trade_history

        now = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))

        # --- NEW: AI Bot Trading Time Restriction ---
        current_time_only = now.time()
        if not (AI_BOT_TRADING_START_TIME <= current_time_only <= AI_BOT_TRADING_END_TIME):
            # If outside trading hours, clear any active trades and return Neutral
            if symbol in self.active_trades:
                # Need to get current premiums for P/L calculation at market close
                entry_premium_ce = self.active_trades[symbol].get('entry_premium_ce', 0)
                entry_premium_pe = self.active_trades[symbol].get('entry_premium_pe', 0)
                lot_size = 50  # Default, will be overridden for specific symbols
                if symbol == "NIFTY":
                    lot_size = 50
                elif symbol == "BANKNIFTY":
                    lot_size = 15
                elif symbol == "FINNIFTY":
                    lot_size = 40

                # Get current premiums for exit calculation
                active_otm_ce_strike = self.active_trades[symbol].get('otm_ce_strike')
                active_otm_pe_strike = self.active_trades[symbol].get('otm_pe_strike')

                premium_ce_exit = df_ce[df_ce['strikePrice'] == active_otm_ce_strike]['lastPrice'].iloc[0] if not df_ce[
                    df_ce['strikePrice'] == active_otm_ce_strike].empty else 0
                premium_pe_exit = df_pe[df_pe['strikePrice'] == active_otm_pe_strike]['lastPrice'].iloc[0] if not df_pe[
                    df_pe['strikePrice'] == active_otm_pe_strike].empty else 0

                current_pnl = 0.0
                if "CE" in self.active_trades[symbol].get('strikes', '') and entry_premium_ce > 0:
                    current_pnl += (entry_premium_ce - premium_ce_exit) * lot_size
                if "PE" in self.active_trades[symbol].get('strikes', '') and entry_premium_pe > 0:
                    current_pnl += (entry_premium_pe - premium_pe_exit) * lot_size

                final_exit_rec = {
                    "recommendation": "EXIT", "rationale": "Market closed, exiting active trade.",
                    "timestamp": now.isoformat(),
                    "trade": "Exit " + self.active_trades[symbol].get('trade', 'previous trade'),
                    "strikes": self.active_trades[symbol].get('strikes', '-'),
                    "type": "Exit", "risk_pct": "-", "exit_rule": "Market Close",
                    "spot": self.active_trades[symbol].get('spot'), "pcr": self.active_trades[symbol].get('pcr'),
                    "intraday_pcr": self.active_trades[symbol].get('intraday_pcr'),
                    "status": "Exit", "pnl": round(current_pnl, 2),
                    "action_price": round(premium_ce_exit + premium_pe_exit, 2),
                    "symbol": symbol  # Added symbol
                }
                ai_bot_trade_history.append(final_exit_rec)
                self.active_trades.pop(symbol, None)  # Clear active trade
                return final_exit_rec
            else:
                return {"recommendation": "Neutral", "rationale": "Outside trading hours.",
                        "timestamp": now.isoformat(),
                        "trade": "-", "strikes": "-", "type": "-",
                        "risk_pct": "-",
                        "exit_rule": "-", "spot": 0.0, "pcr": 0.0, "intraday_pcr": 0.0, "status": "No Trade",
                        "pnl": 0.0, "action_price": 0.0,
                        "symbol": symbol  # Added symbol
                        }

        if not history:
            return {"recommendation": "Neutral", "rationale": "No history data available.",
                    "timestamp": now.isoformat(),
                    "trade": "-", "strikes": "-", "type": "-", "risk_pct": "-",
                    "exit_rule": "-", "spot": 0.0, "pcr": 0.0, "intraday_pcr": 0.0, "status": "No Data", "pnl": 0.0,
                    "action_price": 0.0,
                    "symbol": symbol  # Added symbol
                    }

        latest = history[0]
        pcr = latest.get('pcr', 1.0)
        sentiment = latest.get('sentiment', 'Neutral')  # This is the rule-based sentiment
        ml_sentiment = latest.get('ml_sentiment', 'N/A')  # NEW: Get ML sentiment from history
        spot = latest.get('value', 0)
        intraday_pcr = latest.get('intraday_pcr', 1.0)

        total_call_coi_actual = latest.get('total_call_coi', 0)
        total_put_coi_actual = latest.get('total_put_coi', 0)

        # --- Prompt 2: Time-Based Writing Windows ---
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
                prev_pcr = history[1].get('pcr', pcr)
                if prev_pcr > 1.4 and pcr < 0.6:
                    trade_logic_override = "Reversal hunting - Write puts aggressively (reversal down)"
        elif current_hour == 14 and current_minute >= 30 and current_minute <= 59:
            if pcr < 0.8:
                trade_logic_override = "Expiry theta play - Short strangle — theta burn max"

        # --- Initial Trade Parameters ---
        recommendation = "Neutral"
        rationale = "Observing market."
        trade_summary = "-"
        strikes_selected = "-"
        trade_type = "-"
        risk_pct_display = "-"
        exit_rule_display = "Standard exit logic"
        status = "Hold"  # Default status if no action is taken or trade is taken
        pnl = 0.0
        action_price = 0.0

        capital = 500000
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

        atm_strike = round(spot / strike_step) * strike_step
        otm_ce_strike = atm_strike + (3 * strike_step)
        otm_pe_strike = atm_strike - (3 * strike_step)
        far_otm_ce_strike = atm_strike + (6 * strike_step)
        far_otm_pe_strike = atm_strike - (6 * strike_step)

        premium_ce = df_ce[df_ce['strikePrice'] == otm_ce_strike]['lastPrice'].iloc[0] if not df_ce[
            df_ce['strikePrice'] == otm_ce_strike].empty else 0
        premium_pe = df_pe[df_pe['strikePrice'] == otm_pe_strike]['lastPrice'].iloc[0] if not df_pe[
            df_pe['strikePrice'] == otm_pe_strike].empty else 0

        is_ce_add_heavy = total_call_coi_actual > (2 * total_put_coi_actual) and total_call_coi_actual > 20000
        is_pe_add_heavy = total_put_coi_actual > (2 * total_call_coi_actual) and total_put_coi_actual > 20000

        # --- Exit logic check first (Prompt 4) ---
        if symbol in self.active_trades:
            prev_trade = self.active_trades[symbol]
            entry_spot = prev_trade.get('entry_spot', spot)
            entry_pcr = prev_trade.get('entry_pcr', pcr)
            entry_premium_ce = prev_trade.get('entry_premium_ce', 0)
            entry_premium_pe = prev_trade.get('entry_premium_pe', 0)

            exit_triggered = False
            exit_reason = ""

            # Stop Loss (Spot moved too much against us)
            if prev_trade['type'] == 'Credit':  # For option writing
                if "CE" in prev_trade.get('strikes', '') and spot > prev_trade.get('otm_ce_strike',
                                                                                   0):  # CE written, spot goes above strike
                    exit_triggered = True
                    exit_reason = f"Spot ({spot:.2f}) breached CE strike ({prev_trade.get('otm_ce_strike', 0):.2f})."
                elif "PE" in prev_trade.get('strikes', '') and spot < prev_trade.get('otm_pe_strike',
                                                                                     0):  # PE written, spot goes below strike
                    exit_triggered = True
                    exit_reason = f"Spot ({spot:.2f}) breached PE strike ({prev_trade.get('otm_pe_strike', 0):.2f})."

            # PCR Reversal (Significant change in PCR)
            if abs(pcr - entry_pcr) > 0.3:
                exit_triggered = True
                exit_reason = f"PCR reversed {abs(pcr - entry_pcr):.2f} from entry ({entry_pcr:.2f})."

            # End of Day Exit
            if now.hour >= 15 and now.minute >= 15:
                exit_triggered = True
                exit_reason = "Approaching end of day (3:15 PM), time to exit."

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

                pnl = round(current_pnl, 2)
                action_price = premium_ce + premium_pe if "Strangle" in prev_trade.get('trade', '') else (
                    premium_ce if "CE" in prev_trade.get('strikes', '') else premium_pe)

                self.active_trades.pop(symbol, None)

                final_recommendation = {"recommendation": recommendation, "rationale": exit_reason,
                                        "timestamp": now.isoformat(),
                                        "trade": trade_summary, "strikes": strikes_selected, "type": "Exit",
                                        "risk_pct": "-",
                                        "exit_rule": "Triggered exit logic", "spot": spot, "pcr": pcr,
                                        "intraday_pcr": intraday_pcr,
                                        "status": status, "pnl": pnl, "action_price": round(action_price, 2),
                                        "symbol": symbol  # Added symbol
                                        }

                ai_bot_trade_history.append(final_recommendation)
                return final_recommendation

        # If no exit, and there is an active trade, it's a HOLD
        if symbol in self.active_trades:
            active_trade_info = self.active_trades[symbol]
            status = "Hold"

            # Calculate current P/L for display
            current_pnl = 0.0
            if "CE" in active_trade_info.get('strikes', '') and active_trade_info.get('entry_premium_ce', 0) > 0:
                current_pnl += (active_trade_info.get('entry_premium_ce', 0) - premium_ce) * lot_size
            if "PE" in active_trade_info.get('strikes', '') and active_trade_info.get('entry_premium_pe', 0) > 0:
                current_pnl += (active_trade_info.get('entry_premium_pe', 0) - premium_pe) * lot_size
            pnl = round(current_pnl, 2)

            hold_recommendation = {
                "recommendation": "HOLD",
                "rationale": f"Holding active {active_trade_info['trade']} trade. Current P/L: {pnl:.2f}.",
                "timestamp": now.isoformat(),
                "trade": active_trade_info['trade'],
                "strikes": active_trade_info['strikes'],
                "type": active_trade_info['type'],
                "risk_pct": active_trade_info['risk_pct'],
                "exit_rule": active_trade_info['exit_rule'],
                "spot": spot,
                "pcr": pcr,
                "intraday_pcr": intraday_pcr,
                "status": status,
                "pnl": pnl,
                "action_price": active_trade_info['action_price'],  # Action price for entry
                "symbol": symbol  # Added symbol
            }
            ai_bot_trade_history.append(hold_recommendation)
            return hold_recommendation

        # If no active trade, check if a new trade can be initiated based on AI_BOT_MIN_TRADE_INTERVAL
        if symbol not in self.active_trades:
            last_trade_time = None
            for trade in reversed(ai_bot_trade_history):  # Look for the last entry or exit for this symbol
                if trade.get('symbol') == symbol and (
                        trade['status'] == 'Entry' or trade['status'] == 'Exit'):  # Used .get() for safety
                    last_trade_time = datetime.datetime.fromisoformat(trade['timestamp'])
                    break

            if last_trade_time and (now - last_trade_time).total_seconds() < AI_BOT_MIN_TRADE_INTERVAL:
                return {"recommendation": "Neutral",
                        "rationale": f"Waiting for {AI_BOT_MIN_TRADE_INTERVAL / 60:.0f} min interval before new trade for {symbol}.",
                        "timestamp": now.isoformat(), "trade": "-", "strikes": "-", "type": "-",
                        "risk_pct": "-", "exit_rule": "-", "spot": spot, "pcr": pcr, "intraday_pcr": intraday_pcr,
                        "status": "No Trade", "pnl": 0.0, "action_price": 0.0,
                        "symbol": symbol  # Added symbol
                        }

        # If no active trade and interval passed, try to generate a new entry

        # Prioritize time-based logic if present
        if trade_logic_override:
            if "Prepare to write puts" in trade_logic_override:
                recommendation = "SELL"
                trade_summary = "Naked PE Write"
                strikes_selected = f"Sell {otm_pe_strike} PE @ {premium_pe:.2f}"
                trade_type = "Credit"
                action_price = premium_pe
                rationale = f"{trade_logic_override}. PCR {pcr:.2f} (Intraday PCR {intraday_pcr:.2f}). "
            elif "Write OTM calls" in trade_logic_override:
                recommendation = "SELL"
                trade_summary = "Naked CE Write"
                strikes_selected = f"Sell {otm_ce_strike} CE @ {premium_ce:.2f}"
                trade_type = "Credit"
                action_price = premium_ce
                rationale = f"{trade_logic_override}. PCR {pcr:.2f} (Intraday PCR {intraday_pcr:.2f}). "
            elif "Write puts aggressively" in trade_logic_override:
                recommendation = "SELL"
                trade_summary = "Naked PE Write"
                strikes_selected = f"Sell {otm_pe_strike} PE @ {premium_pe:.2f}"
                trade_type = "Credit"
                action_price = premium_pe
                rationale = f"{trade_logic_override}. PCR {pcr:.2f} (Intraday PCR {intraday_pcr:.2f}). "
            elif "Short strangle" in trade_logic_override:
                recommendation = "SELL"
                trade_summary = "Short Strangle"
                action_price = premium_ce + premium_pe
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
                "spot": spot, "pcr": pcr, "intraday_pcr": intraday_pcr, "status": status, "pnl": 0.0,
                "action_price": round(action_price, 2),
                "entry_spot": spot, "entry_pcr": pcr, "entry_premium_ce": premium_ce, "entry_premium_pe": premium_pe,
                "otm_ce_strike": otm_ce_strike, "otm_pe_strike": otm_pe_strike,  # Store these for exit logic
                "symbol": symbol  # Added symbol
            }
            if recommendation != "Neutral":
                self.active_trades[symbol] = final_recommendation
            ai_bot_trade_history.append(final_recommendation)
            return final_recommendation

        # --- Max Pain & PCR Divergence (Prompt 5) ---
        if symbol not in self.active_trades:
            max_pain_strike = self._calculate_max_pain_for_bot(df_ce, df_pe)
            if max_pain_strike:
                if spot > max_pain_strike + 100 and pcr < 0.9:
                    recommendation = "SELL"
                    trade_summary = "Naked CE Write"
                    strikes_selected = f"Sell {otm_ce_strike} CE @ {premium_ce:.2f}"
                    trade_type = "Credit"
                    action_price = premium_ce
                    rationale = f"Spot ({spot:.2f}) > Max Pain ({max_pain_strike:.2f}) + 100 and PCR ({pcr:.2f}) < 0.9. Pinning down. (Intraday PCR {intraday_pcr:.2f})."
                elif spot < max_pain_strike - 100 and pcr > 1.2:
                    recommendation = "SELL"
                    trade_summary = "Naked PE Write"
                    strikes_selected = f"Sell {otm_pe_strike} PE @ {premium_pe:.2f}"
                    trade_type = "Credit"
                    action_price = premium_pe
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
                        "spot": spot, "pcr": pcr, "intraday_pcr": intraday_pcr, "status": status, "pnl": 0.0,
                        "action_price": round(action_price, 2),
                        "entry_spot": spot, "entry_pcr": pcr, "entry_premium_ce": premium_ce,
                        "entry_premium_pe": premium_pe,
                        "otm_ce_strike": otm_ce_strike, "otm_pe_strike": otm_pe_strike,  # Store these for exit logic
                        "symbol": symbol  # Added symbol
                    }
                    self.active_trades[symbol] = final_recommendation
                    ai_bot_trade_history.append(final_recommendation)
                    return final_recommendation

        # General rules from Prompt 3 & 8 if no time-based or max-pain override and no active trade
        if symbol not in self.active_trades:
            if pcr >= 1.0 and pcr <= 1.2:
                recommendation = "SELL"
                trade_summary = "Short Strangle"
                action_price = premium_ce + premium_pe
                strikes_selected = f"Sell {otm_ce_strike} CE + {otm_pe_strike} PE (Total Premium: {action_price:.2f})"
                trade_type = "Credit"
                rationale = f"PCR stable ({pcr:.2f}), suggesting short strangle for theta decay. (Intraday PCR {intraday_pcr:.2f})."
            elif pcr > 1.3:
                recommendation = "SELL"
                trade_summary = "Naked CE Write"
                strikes_selected = f"Sell {otm_ce_strike} CE @ {premium_ce:.2f}"
                trade_type = "Credit"
                action_price = premium_ce
                rationale = f"High PCR ({pcr:.2f}), indicating overbought or resistance, writing OTM CE. (Intraday PCR {intraday_pcr:.2f})."
            elif pcr < 0.8:
                recommendation = "SELL"
                trade_summary = "Naked PE Write"
                strikes_selected = f"Sell {otm_pe_strike} PE @ {premium_pe:.2f}"
                trade_type = "Credit"
                action_price = premium_pe
                rationale = f"Low PCR ({pcr:.2f}), indicating oversold or support, writing OTM PE. (Intraday PCR {intraday_pcr:.2f})."
            else:
                recommendation = "Neutral"
                rationale = f"PCR ({pcr:.2f}) is in a neutral range, awaiting clearer signals. (Intraday PCR {intraday_pcr:.2f})."

            # Refine trade based on OI Add/Exit for confirmation (Prompt 8)
            if recommendation == "SELL":
                if is_ce_add_heavy:
                    recommendation = "SELL"
                    trade_summary = "Naked CE Write"
                    strikes_selected = f"Sell {otm_ce_strike} CE @ {premium_ce:.2f}"
                    action_price = premium_ce
                    rationale += " Confirmed by strong Call writing."
                elif is_pe_add_heavy:
                    recommendation = "SELL"
                    trade_summary = "Naked PE Write"
                    strikes_selected = f"Sell {otm_pe_strike} PE @ {premium_pe:.2f}"
                    action_price = premium_pe
                    rationale += " Confirmed by strong Put writing."
                elif not is_ce_add_heavy and not is_pe_add_heavy and "Short Strangle" not in trade_summary:
                    if pcr > 0.9 and pcr < 1.1:
                        recommendation = "SELL"
                        trade_summary = "Short Strangle"
                        action_price = premium_ce + premium_pe
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
                "spot": spot,
                "pcr": pcr,
                "intraday_pcr": intraday_pcr,
                "status": status,
                "pnl": 0.0,
                "action_price": round(action_price, 2),
                "entry_spot": spot, "entry_pcr": pcr, "entry_premium_ce": premium_ce, "entry_premium_pe": premium_pe,
                "otm_ce_strike": otm_ce_strike, "otm_pe_strike": otm_pe_strike,  # Store these for exit logic
                "symbol": symbol  # Added symbol
            }
            if recommendation != "Neutral":
                self.active_trades[symbol] = final_recommendation
            ai_bot_trade_history.append(final_recommendation)
            return final_recommendation

        # This part should be unreachable if the logic above is sound, but as a final fallback
        return {"recommendation": "Neutral", "rationale": "No new trade or active trade status.",
                "timestamp": now.isoformat(),
                "trade": "-", "strikes": "-", "type": "-", "risk_pct": "-",
                "exit_rule": "-", "spot": spot, "pcr": 0.0, "intraday_pcr": 0.0, "status": "No Trade",
                "pnl": 0.0, "action_price": 0.0,
                "symbol": symbol  # Added symbol
                }


# REMOVED: DeepSeekClient class as it's not being used for LLM interaction


# REMOVED: NewsFetcher Class (replaced by fetch_market_news function)


# NEW: NewsAnalyzer Class (simplified)
class NewsAnalyzer:
    def __init__(self, nse_bse_analyzer_instance):  # Removed news_aggregator_instance
        self.analyzer = nse_bse_analyzer_instance
        self.last_news_check_time = 0

    def _load_recent_news_from_db(self):
        global news_alerts
        if not self.analyzer.conn: return
        try:
            cur = self.analyzer.conn.cursor()
            # Load news from the last NEWS_RETENTION_DAYS
            time_threshold = datetime.datetime.now(pytz.utc) - datetime.timedelta(days=NEWS_RETENTION_DAYS)
            cur.execute(
                "SELECT timestamp, Headline, Source, \"Key Details\", Impact, URL FROM news_alerts WHERE timestamp >= ? ORDER BY timestamp DESC",
                (time_threshold.strftime("%Y-%m-%d %H:%M:%S"),))
            rows = cur.fetchall()
            loaded_news = []
            for r in rows:
                loaded_news.append({
                    "timestamp": r[0],
                    "Headline": r[1],
                    "Source": r[2],
                    "Key Details": r[3],
                    "Impact": r[4],
                    "URL": r[5]
                })
            with data_lock:
                news_alerts = loaded_news
            print(f"Loaded {len(news_alerts)} news alerts from DB.")
        except Exception as e:
            print(f"Error loading news alerts from DB: {e}")

    def _save_news_alert_to_db(self, news_item: Dict[str, Any]):
        if not self.analyzer.conn: return
        try:
            ts = news_item.get('timestamp')  # Use timestamp from the news item itself

            self.analyzer.conn.execute(
                "INSERT INTO news_alerts (timestamp, Headline, Source, \"Key Details\", Impact, URL) VALUES (?,?,?,?,?,?)",
                (ts, news_item.get('Headline', ''), news_item.get('Source', ''),
                 news_item.get('Key Details', ''), news_item.get('Impact', ''), news_item.get('URL', ''))
            )
            # Clean up old news alerts
            time_threshold = datetime.datetime.now(pytz.utc) - datetime.timedelta(days=NEWS_RETENTION_DAYS)
            self.analyzer.conn.execute("DELETE FROM news_alerts WHERE timestamp < ?",
                                       (time_threshold.strftime("%Y-%m-%d %H:%M:%S"),))
            self.analyzer.conn.commit()
        except Exception as e:
            print(f"DB save error for news alert: {e}")

    def run_news_analysis(self):
        global news_alerts
        now_ts = time.time()
        # Only run during market hours (or slightly before/after)
        current_time_only = self.analyzer._get_ist_time().time()
        if not (datetime.time(9, 0) <= current_time_only <= datetime.time(16, 0)):  # Run from 9 AM to 4 PM IST
            # print(f"News analysis skipped outside market hours. Current time: {current_time_only.strftime('%H:%M')}")
            # Ensure old news is still cleaned up, even if not generating new news
            if self.analyzer.conn:
                time_threshold = datetime.datetime.now(pytz.utc) - datetime.timedelta(days=NEWS_RETENTION_DAYS)
                self.analyzer.conn.execute("DELETE FROM news_alerts WHERE timestamp < ?",
                                           (time_threshold.strftime("%Y-%m-%d %H:%M:%S"),))
                self.analyzer.conn.commit()
            return

        if now_ts - self.last_news_check_time < NEWS_FETCH_INTERVAL:
            return

        print(f"--- Running RSS News Aggregator at {self.analyzer._get_ist_time().strftime('%H:%M:%S')} ---")
        self.last_news_check_time = now_ts

        # 1. Fetch Raw News from RSS
        newly_fetched_articles = fetch_market_news(top_n=20)  # Use the global fetch_market_news function

        if not newly_fetched_articles:
            print("No new articles fetched from RSS feeds.")
            return

        # Filter out duplicates based on Headline and Source
        # Make sure news_alerts is not empty before creating the set, or handle the case where it might be.
        # It's safer to use a try-except or check for existence of 'Headline' and 'Source'.
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
                    (article['Headline'], article['Source']))  # Add to set to prevent future duplicates in this run

        if not unique_new_articles:
            print("No unique new articles found after filtering duplicates.")
            return

        print(f"Found {len(unique_new_articles)} unique new articles.")

        for article in unique_new_articles:
            with data_lock:
                news_alerts.insert(0, article)  # Add to the global list

            self._save_news_alert_to_db(article)  # Save to DB

        # Clean up old news alerts from memory (already done by _save_news_alert_to_db for DB)
        with data_lock:
            # Re-filter news_alerts to ensure it only contains recent items
            time_threshold = datetime.datetime.now(self.analyzer.ist_timezone) - datetime.timedelta(
                days=NEWS_RETENTION_DAYS)
            # Ensure all timestamps in news_alerts are properly parsed
            cleaned_news_alerts = []
            for alert in news_alerts:
                if "timestamp" in alert:
                    try:
                        alert_dt = datetime.datetime.fromisoformat(alert["timestamp"])
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
        self.db_path = 'nse_bse_data.db'
        self.conn: Optional[sqlite3.Connection] = None
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        self.YFINANCE_SYMBOLS = ["SENSEX", "INDIAVIX", "GOLD", "SILVER", "BTC-USD", "USD-INR"]
        self.TICKER_ONLY_SYMBOLS = ["GOLD", "SILVER", "BTC-USD", "USD-INR"]
        self.YFINANCE_TICKER_MAP = {"SENSEX": "^BSESN", "INDIAVIX": "^INDIAVIX", "GOLD": "GOLDBEES.NS",
                                    "SILVER": "SILVERBEES.NS", "BTC-USD": "BTC-USD", "USD-INR": "INR=X"}
        self.previous_data = {}

        # FIX: Initialize DB first
        self._init_db()

        self.pcr_graph_data: Dict[str, List[Dict[str, Any]]] = {}
        self.previous_pcr: Dict[str, float] = {}

        self.bhavcopy_running = threading.Event()

        self.BHAVCOPY_INDICES_MAP = {"NIFTY": "NIFTY 50", "BANKNIFTY": "NIFTY BANK", "FINNIFTY": "NIFTY FIN SERVICE",
                                     "INDIAVIX": "INDIA VIX"}
        self.bhavcopy_last_run_date = None

        self.deepseek_bot = DeepSeekBot()

        # ML Model attributes
        self.sentiment_model = None
        self.sentiment_features = None
        self.sentiment_label_encoder = None
        self._load_ml_models()  # Call this method to load ML models

        # News analysis components (simplified)
        self.news_analyzer = NewsAnalyzer(self)  # Pass analyzer instance
        self.news_analyzer._load_recent_news_from_db()  # Load news history after DB init

        self._set_nse_session_cookies()
        self.get_stock_symbols()
        self._load_todays_history_from_db()
        self._load_initial_underlying_values()
        self._populate_initial_shared_chart_data()
        self._load_latest_bhavcopy_from_db()

        threading.Thread(target=self.run_loop, daemon=True).start()
        threading.Thread(target=self.equity_fetcher_thread, daemon=True).start()
        threading.Thread(target=self.bhavcopy_scanner_thread, daemon=True).start()
        threading.Thread(target=self.news_analyzer_thread, daemon=True).start()  # NEW: News Analyzer Thread

        # Moved send_alert to be a method of NseBseAnalyzer
        self.send_telegram_message("NSE OCA PRO Bot is Online\n\nMonitoring will begin during market hours.")

    def news_analyzer_thread(self):
        print("News Analyzer thread started.")
        while not self.stop.is_set():
            self.news_analyzer.run_news_analysis()
            time.sleep(NEWS_FETCH_INTERVAL)  # Sleep for the configured interval

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
                                              "symbol": sym}  # Added symbol
                        last_ai_bot_run_time[sym] = 0
                return

            response = self.session.get(self.url_symbols, headers=self.nse_headers, timeout=10, verify=False)
            response.raise_for_status()
            json_data = response.json()
            fno_stocks_list = sorted([item['symbol'] for item in json_data['data']['UnderlyingList']])
            print(f"Successfully fetched {len(fno_stocks_list)} F&O stock symbols.")

            for sym in fno_stocks_list:
                if sym not in ai_bot_trades:
                    ai_bot_trades[sym] = {"recommendation": "Neutral", "rationale": "Waiting for data...",
                                          "timestamp": "", "trade": "-",
                                          "strikes": "-", "type": "-", "risk_pct": "-", "exit_rule": "-", "spot": 0.0,
                                          "pcr": 0.0, "intraday_pcr": 0.0,
                                          "symbol": sym}  # Added symbol
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
                                          "symbol": sym}  # Added symbol
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
                                          "symbol": sym}  # Added symbol
                    last_ai_bot_run_time[sym] = 0

    def _init_db(self):
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            # Ensure history table exists with ml_sentiment column
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, symbol TEXT, sp REAL, value REAL, call_oi REAL, put_oi REAL, pcr REAL, sentiment TEXT, add_exit TEXT, intraday_pcr REAL, ml_sentiment TEXT)")
            # Check if ml_sentiment column exists and add it if not
            # Moved this check AFTER table creation to prevent "no such table" error
            cursor = self.conn.execute("PRAGMA table_info(history)")
            columns = [col[1] for col in cursor.fetchall()]
            if 'ml_sentiment' not in columns:
                self.conn.execute("ALTER TABLE history ADD COLUMN ml_sentiment TEXT")
                print("Added 'ml_sentiment' column to 'history' table.")

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
                    trading_type TEXT,
                    PRIMARY KEY (date, symbol, trading_type)
                )"""
            )
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
            self.conn.execute(
                """CREATE TABLE IF NOT EXISTS fno_bhavcopy_data (
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
                )"""
            )
            self.conn.execute(
                """CREATE TABLE IF NOT EXISTS block_deal_data (
                    date TEXT,
                    symbol TEXT,
                    trade_type TEXT,
                    quantity INTEGER,
                    price REAL,
                    PRIMARY KEY (date, symbol, quantity, price)
                )"""
            )
            # NEW: News Alerts table (simplified columns)
            self.conn.execute(
                """CREATE TABLE IF NOT EXISTS news_alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    Headline TEXT,
                    Source TEXT,
                    "Key Details" TEXT,
                    Impact TEXT,
                    URL TEXT
                )"""
            )
            self.conn.commit()
        except Exception as e:
            print(f"DB error: {e}")  # Keep this for debugging if there are other DB issues

    def _load_latest_bhavcopy_from_db(self):
        global latest_bhavcopy_data
        if not self.conn: return
        try:
            cur = self.conn.cursor()
            latest_dates = []
            # Check if table is empty or doesn't exist before querying for max date
            table_exists_and_not_empty = False
            try:
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='bhavcopy_data'")
                if cur.fetchone():
                    cur.execute("SELECT COUNT(*) FROM bhavcopy_data")
                    if cur.fetchone()[0] > 0:
                        table_exists_and_not_empty = True
            except sqlite3.OperationalError:
                pass  # Table might not exist yet if init_db failed or is not complete

            if table_exists_and_not_empty:
                cur.execute("SELECT date FROM bhavcopy_data ORDER BY date DESC LIMIT 1")
                eq_date = cur.fetchone()
                if eq_date: latest_dates.append(eq_date[0])

            table_exists_and_not_empty = False
            try:
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='index_bhavcopy_data'")
                if cur.fetchone():
                    cur.execute("SELECT COUNT(*) FROM index_bhavcopy_data")
                    if cur.fetchone()[0] > 0:
                        table_exists_and_not_empty = True
            except sqlite3.OperationalError:
                pass

            if table_exists_and_not_empty:
                cur.execute("SELECT date FROM index_bhavcopy_data ORDER BY date DESC LIMIT 1")
                idx_date = cur.fetchone()
                if idx_date: latest_dates.append(idx_date[0])

            table_exists_and_not_empty = False
            try:
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fno_bhavcopy_data'")
                if cur.fetchone():
                    cur.execute("SELECT COUNT(*) FROM fno_bhavcopy_data")
                    if cur.fetchone()[0] > 0:
                        table_exists_and_not_empty = True
            except sqlite3.OperationalError:
                pass

            if table_exists_and_not_empty:
                cur.execute("SELECT date FROM fno_bhavcopy_data ORDER BY date DESC LIMIT 1")
                fno_date = cur.fetchone()
                if fno_date: latest_dates.append(fno_date[0])

            latest_date = max(latest_dates) if latest_dates else None

            if not latest_date:
                print("No previous Bhavcopy data found in the database.")
                return

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

            cur.execute(
                "SELECT symbol, close, pct_change, open, high, low FROM index_bhavcopy_data WHERE date = ?",
                (latest_date,)
            )
            # FIX: Assign fetchall to a variable before list comprehension
            fetched_index_rows = cur.fetchall()
            indices = [{
                "Symbol": row[0], "Close": row[1], "Pct Change": row[2],
                "Open": row[3], "High": row[4], "Low": row[5]
            } for row in fetched_index_rows]  # Use fetched_index_rows here
            print(f"DEBUG: _get_bhavcopy_for_date - Found {len(indices)} Index records for {latest_date}")

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
                # FIX: Remove this condition - YFinance symbols also need history for AI Bot
                # if sym in self.TICKER_ONLY_SYMBOLS: continue
                # Fetching ml_sentiment from DB
                cur.execute(
                    "SELECT timestamp, sp, value, call_oi, put_oi, pcr, sentiment, add_exit, intraday_pcr, ml_sentiment FROM history WHERE symbol = ? AND timestamp >= ? ORDER BY timestamp DESC",
                    (sym, utc_start_str))
                rows = cur.fetchall()
                with data_lock:
                    todays_history[sym] = []
                    for r in rows:
                        history_item = {
                            'time': self._convert_utc_to_ist_display(r[0]), 'sp': r[1], 'value': r[2], 'call_oi': r[3],
                            'put_oi': r[4], 'pcr': r[5], 'sentiment': r[6], 'add_exit': r[7],
                            'intraday_pcr': r[8] if r[8] is not None else 0.0,
                            'ml_sentiment': r[9] if r[9] is not None else 'N/A'  # Added ml_sentiment
                        }
                        todays_history[sym].append(history_item)

                    # FIX: All symbols need pcr_graph_data for historical display, even if it's just a placeholder
                    # The `if sym not in self.YFINANCE_SYMBOLS:` condition here is fine as YFinance symbols don't have PCR.
                    if sym not in self.YFINANCE_SYMBOLS:
                        if todays_history.get(sym) and todays_history[sym]:
                            self.previous_pcr[sym] = todays_history[sym][0]['pcr']
                        self.pcr_graph_data[sym] = [
                            {"TIME": item['time'], "PCR": item['pcr'], "IntradayPCR": item['intraday_pcr']}
                            for item in reversed(todays_history.get(sym, []))
                        ]
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
            all_symbols_to_load = AUTO_SYMBOLS + fno_stocks_list
            for s in all_symbols_to_load:
                if s not in todays_history: todays_history[s] = []  # Ensure dict entry exists for all potential symbols

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
            self._set_nse_session_cookies()
        except requests.exceptions.RequestException as e:
            print(f"Initial NSE session setup failed: {e}")
        while not self.stop.is_set():
            now_ist = self._get_ist_time()
            current_time_only = now_ist.time()

            # Process all AUTO_SYMBOLS, including YFinance ones
            symbols_to_process = AUTO_SYMBOLS
            for sym in symbols_to_process:
                # NEW: Apply time restriction for NSE index data fetching
                if sym in ["NIFTY", "FINNIFTY", "BANKNIFTY"] + fno_stocks_list:  # This now applies to F&O stocks too
                    if not (NSE_FETCH_START_TIME <= current_time_only <= NSE_FETCH_END_TIME):
                        # If outside market hours, and there's an active trade, clear it
                        if sym in self.deepseek_bot.active_trades:
                            current_vix_value = shared_data.get("INDIAVIX", {}).get("live_feed_summary", {}).get(
                                "current_value", 15.0)
                            # We need some dummy df_ce/df_pe to call analyze_and_recommend for market close exit logic
                            # This is a bit of a hack, but necessary if no real data is available.
                            active_trade_info = self.deepseek_bot.active_trades[sym]
                            dummy_df_ce = pd.DataFrame(
                                [{'strikePrice': active_trade_info.get('otm_ce_strike', 0), 'openInterest': 0,
                                  'lastPrice': active_trade_info.get('entry_premium_ce', 0)}])
                            dummy_df_pe = pd.DataFrame(
                                [{'strikePrice': active_trade_info.get('otm_pe_strike', 0), 'openInterest': 0,
                                  'lastPrice': active_trade_info.get('entry_premium_pe', 0)}])
                            self.deepseek_bot.analyze_and_recommend(sym, todays_history.get(sym, []), current_vix_value,
                                                                    dummy_df_ce, dummy_df_pe)
                            broadcast_live_update()  # Broadcast the exit
                        print(
                            f"Skipping NSE data fetch for {sym} outside of market hours ({current_time_only.strftime('%H:%M')}). Retaining last data.")
                        continue  # Skip to next symbol, do not clear previous data
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

            # FIX: The AI Bot logic for F&O stocks is now integrated into _get_nse_option_chain_data
            # We just need to trigger the data fetch for F&O stocks here
            if NSE_FETCH_START_TIME <= current_time_only <= NSE_FETCH_END_TIME:
                print(f"Market is open. Starting F&O equity fetch cycle at {now_ist.strftime('%H:%M:%S')}")
                self._set_nse_session_cookies()
                for i, symbol in enumerate(fno_stocks_list):
                    try:
                        print(f"Fetching F&O Equity {i + 1}/{len(fno_stocks_list)}: {symbol}")
                        # _process_equity_data will now handle AI Bot recommendations internally
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
            else:
                print(
                    f"Market is closed. F&O Equity fetcher sleeping. Current time: {now_ist.strftime('%H:%M:%S')}. Retaining last data.")
                # NEW: Clear active bot trades for equities when market closes - this is now handled in _get_nse_option_chain_data
                time.sleep(60)

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

                    if self.run_bhavcopy_for_date(target_day):
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
            "https://nsearchives.nseindia.com/content/cd/bhav/",  # Corrected from 'archives/cd/bhav/'
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
                final_file_candidates.append(f"{fn}.csv")
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

            # Renamed local variables to match parameter names for clarity and to avoid linter warnings
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
                if cm_file_path_local: print(f"Found alternative CM Bhavcopy: {os.path.basename(cm_file_path_local)}")

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
            if fno_file_path_local: print(f"Found F&O Bhavcopy: {os.path.basename(fno_file_path_local)}")

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
            if block_file_path_local: print(f"Found Block Deals file: {os.path.basename(block_file_path_local)}")

            print(f"Trying to find Bulk Deals file for {target_date_str_dmy}...")
            bulk_file_path_local = self.download_bhavcopy_file_by_name(date_obj, "bulk.csv", "Bhavcopy_Downloads/NSE")
            if bulk_file_path_local: print(f"Found Bulk Deals file: {os.path.basename(bulk_file_path_local)}")

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

        if block_file_path and os.path.exists(block_file_path):
            try:
                df_block = pd.read_csv(block_file_path, skipinitialspace=True)
                df_block.columns = [col.strip().upper() for col in df_block.columns]

                print(f"--- Block Deals (block.csv) for {db_date_str} loaded ---")
                for index, r in df_block.iterrows():
                    all_block_deals_for_date.append({
                        "Symbol": r['SYMBOL'],
                        "Trade Type": r.get('TRADE_TYPE', 'B'),
                        "Quantity": int(r['NO_OF_SHARES']),
                        "Price": round(float(r['TRADE_PRICE']), 2)
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
                        "Quantity": int(r['NO_OF_SHARES']),
                        "Price": round(float(r['TRADE_PRICE']), 2)
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
            if all_equities_for_date:
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

        cur.execute(
            "SELECT symbol, close, volume, pct_change, delivery_pct, open, high, low, prev_close, trading_type FROM bhavcopy_data WHERE date = ? AND trading_type = 'EQ'",
            (date_str,)
        )
        equities = [{
            "Symbol": row[0], "Close": row[1], "Volume": row[2], "Pct Change": row[3],
            "Delivery %": float(str(row[4]).replace('%', '')) if isinstance(row[4], str) and str(
                row[4]).strip() != 'N/A' else None,
            "Open": row[5], "High": row[6], "Low": row[7], "Prev Close": row[8], "Trading Type": row[9]
        } for row in cur.fetchall()]
        print(f"DEBUG: _get_bhavcopy_for_date - Found {len(equities)} EQ records for {date_str}")

        cur.execute(
            "SELECT symbol, close, pct_change, open, high, low FROM index_bhavcopy_data WHERE date = ?",
            (date_str,)
        )
        # FIX: Corrected variable name to match the loop, previously "index_rows" was used without assignment
        fetched_index_data = cur.fetchall()
        indices = [{
            "Symbol": row[0], "Close": row[1], "Pct Change": row[2],
            "Open": row[3], "High": row[4], "Low": row[5]
        } for row in fetched_index_data]
        print(f"DEBUG: _get_bhavcopy_for_date - Found {len(indices)} Index records for {date_str}")

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

            prev_futures_df_2 = prev_fno_df_2[prev_fno_df_2['Instrument'] == 'FUTSTK']

            for symbol in futures_df['Symbol'].unique():
                stock_futures = futures_df[futures_df['Symbol'] == symbol].copy()

                stock_futures['Expiry Date'] = pd.to_datetime(stock_futures['Expiry Date'])
                stock_futures = stock_futures.sort_values(by='Expiry Date')

                if len(stock_futures) >= 2:
                    near_month_future = stock_futures.iloc[0]
                    next_month_future = stock_futures.iloc[1]

                    near_month_delivery_pct = near_month_future['Delivery %']

                    next_month_oi_current = next_month_future['Open Interest']

                    prev_next_month_future_2_days_ago = prev_futures_df_2[
                        (prev_futures_df_2['Symbol'] == symbol) &
                        (prev_futures_df_2['Instrument'] == 'FUTSTK') &
                        (pd.to_datetime(prev_futures_df_2['Expiry Date']) == next_month_future['Expiry Date'])
                        ]

                    if not prev_next_month_future_2_days_ago.empty:
                        next_month_oi_2_days_ago = prev_next_month_future_2_days_ago.iloc[0]['Open Interest']
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
                bd_price = bd['Price']

                equity_data = equities_df[equities_df['Symbol'] == symbol]
                if not equity_data.empty:
                    eq = equity_data.iloc[0]
                    prev_close = eq['Prev Close']
                    current_open = eq['Open']

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
                volume = eq['Volume']
                close_price = eq['Close']
                open_price = eq['Open']
                high_price = eq['High']
                low_price = eq['Low']
                delivery_pct = eq['Delivery %']

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

            futures_df = current_fno_df[current_fno_df['Instrument'] == 'FUTSTK'].copy()
            prev_futures_df = prev_fno_df[prev_fno_df['Instrument'] == 'FUTSTK'].copy()

            for symbol in futures_df['Symbol'].unique():
                stock_futures = futures_df[futures_df['Symbol'] == symbol]
                prev_stock_futures = prev_futures_df[prev_futures_df['Symbol'] == symbol]

                current_eq = current_equities_df[current_equities_df['Symbol'] == symbol]
                prev_eq = prev_equities_df[prev_equities_df['Symbol'] == symbol]

                if not stock_futures.empty and not prev_stock_futures.empty and not current_eq.empty and not prev_eq.empty:
                    current_fut_near = stock_futures.sort_values(by='Expiry Date').iloc[0]
                    prev_fut_near = prev_stock_futures.sort_values(by='Expiry Date').iloc[
                        0]

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
                today_volume = eq['Volume']
                today_delivery_pct = eq['Delivery %']
                today_pct_change = eq['Pct Change']

                if all(x is not None for x in [today_volume, today_delivery_pct]) and today_volume > 0 and str(
                        today_delivery_pct).strip() != 'N/A':  # Ensure delivery_pct is not None or 'N/A'
                    today_delivery_pct_float = float(str(today_delivery_pct).replace('%', ''))

                    historical_data = self._get_historical_bhavcopy_for_stock(
                        symbol,
                        (current_date_obj - datetime.timedelta(days=30)).strftime('%Y-%m-%d'),
                        (current_date_obj - datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
                        10
                    )
                    historical_df = pd.DataFrame(historical_data)

                    if len(historical_df) >= 10:
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
        return [{"Symbol": "N/A",
                 "Signal": "Requires detailed Options Bhavcopy parsing and storage for strike-wise OI analysis."}]

    def analyze_corporate_action_arbitrage(self, date_str: str) -> List[Dict[str, Any]]:
        return [{"Symbol": "N/A", "Signal": "Requires Corporate Actions data and multi-day delivery trend analysis."}]

    def _get_previous_trading_day(self, current_date: datetime.date, num_days_back: int) -> Optional[str]:
        if not self.conn:
            return None

        cur = self.conn.cursor()
        query = """
            SELECT DISTINCT date FROM bhavcopy_data
            WHERE date < ? AND trading_type = 'EQ'
            ORDER BY date DESC
            LIMIT ?
        """
        # We need to fetch num_days_back records to get the Nth previous day
        cur.execute(query, (current_date.strftime('%Y-%m-%d'), num_days_back))
        rows = cur.fetchall()

        if len(rows) < num_days_back:
            # print(f"DEBUG: _get_previous_trading_day for {current_date} (back {num_days_back}) - Only found {len(rows)} previous trading days.")
            return None

        # The Nth previous day is the last one in the limited sorted list
        return rows[num_days_back - 1][0]

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
                         "Bearish Reversal": -1.5}
        # Use ML sentiment if available and not 'N/A' for scoring, otherwise fallback to rule-based
        sentiment_to_use = summary.get('ml_sentiment', summary.get('sentiment', 'Neutral'))
        if sentiment_to_use == 'N/A':  # Fallback to rule-based if ML_sentiment is 'N/A'
            sentiment_to_use = summary.get('sentiment', 'Neutral')

        sentiment_score = sentiment_map.get(sentiment_to_use, 0)
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
                         "Bearish Reversal": -1.5}
        # Use ML sentiment if available and not 'N/A' for scoring, otherwise fallback to rule-based
        sentiment_to_use = current_summary.get('ml_sentiment', current_summary.get('sentiment', 'Neutral'))
        if sentiment_to_use == 'N/A':  # Fallback to rule-based if ML_sentiment is 'N/A'
            sentiment_to_use = current_summary.get('sentiment', 'Neutral')

        sentiment_score = sentiment_map.get(sentiment_to_use, 0)

        intraday_pcr_change = current_intraday_pcr - previous_intraday_pcr

        bullish_reversal_score = (1 / (current_pcr + 0.1)) * (current_intraday_pcr * 1.5) + (
                intraday_pcr_change * 10) + sentiment_score

        return round(bullish_reversal_score, 2)

    def rank_and_emit_movers(self):
        global previous_improving_list, previous_worsening_list
        if not equity_data_cache:
            # Emit an empty state or "Waiting for data..." if no equity data has been processed yet
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
            if not current_data: continue  # Skip if no current data

            strength_score = self.calculate_strength_score(current_data)
            strength_scores.append(
                {'symbol': symbol, 'score': strength_score,
                 'sentiment': current_data['summary'].get('ml_sentiment', current_data['summary']['sentiment']),
                 'pcr': current_data['summary']['pcr']})

            reversal_score = self.calculate_reversal_score(current_data, previous_data)
            reversal_scores.append(
                {'symbol': symbol, 'score': reversal_score,
                 'sentiment': current_data['summary'].get('ml_sentiment', current_data['summary']['sentiment']),
                 'pcr': current_data['summary']['pcr']})

        strength_scores.sort(key=lambda x: x['score'], reverse=True)
        reversal_scores.sort(key=lambda x: x['score'], reverse=True)

        top_strongest = strength_scores[:10]
        top_weakest = strength_scores[-10:][::-1]
        top_improving = sorted([s for s in reversal_scores if s['score'] > 0], key=lambda x: x['score'], reverse=True)[
            :10]
        top_worsening = sorted([s for s in reversal_scores if s['score'] < 0], key=lambda x: x['score'])[:10]

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

            current_price = 0.0
            change = 0.0
            pct_change = 0.0
            sentiment_yfinance = "Neutral"

            # FIX: Use a longer period for YFinance to increase chance of getting data
            ticker = yf.Ticker(ticker_str)
            hist = ticker.history(period="5d")  # Changed from "2d" to "5d"

            if hist.empty or len(hist) < 2:
                print(
                    f"Warning: Could not fetch sufficient data for {sym} ({ticker_str}). It might be temporarily unavailable or the ticker is incorrect. Displaying N/A.")
                # Set values to N/A or 0 for display
                # We need to ensure these are actual numbers if they're used in calculations later.
                # For display purposes, we can keep them as 0.0 and sentiment as 'N/A - No Data'.
                current_price = 0.0
                change = 0.0
                pct_change = 0.0
                sentiment_yfinance = "N/A - No Data"
            else:
                current_price = hist['Close'].iloc[-1]
                previous_close = hist['Close'].iloc[-2]
                change = current_price - previous_close
                pct_change = (change / previous_close) * 100 if previous_close != 0 else 0
                sentiment_yfinance = "Mild Bearish" if change < 0 else "Mild Bullish" if change > 0 else "Neutral"

            with data_lock:
                if sym not in shared_data: shared_data[sym] = {}
                shared_data[sym]['live_feed_summary'] = {
                    'current_value': round(current_price, 4),
                    'change': round(change, 4),
                    'percentage_change': round(pct_change, 2)
                }

                # Create a summary object for YFinance symbols
                summary = {
                    'time': self._get_ist_time().strftime("%H:%M"),
                    'sp': round(change, 2),  # Using change as 'sp' for YFinance
                    'value': round(current_price, 2),
                    'pcr': round(pct_change, 2),  # Using pct_change as 'pcr' for YFinance
                    'sentiment': sentiment_yfinance,
                    'call_oi': 0,  # Not applicable for YFinance spot prices
                    'put_oi': 0,  # Not applicable for YFinance spot prices
                    'add_exit': "Live Price",
                    'intraday_pcr': 0,  # Not applicable for YFinance spot prices
                    'expiry': 'N/A',
                    'ml_sentiment': 'N/A',  # No ML sentiment for YFinance symbols
                    'symbol': sym  # Added symbol for consistency
                }

                shared_data[sym]['summary'] = summary
                # Clear option chain related data for YFinance symbols
                shared_data[sym]['strikes'] = []
                shared_data[sym]['max_pain_chart_data'] = []
                shared_data[sym]['ce_oi_chart_data'] = []
                shared_data[sym]['pe_oi_chart_data'] = []
                shared_data[sym]['pcr_chart_data'] = []  # This might contain previous data, clear it.

            print(f"{sym} YFINANCE DATA UPDATED | Value: {current_price:.2f}")
            broadcast_live_update()

            now_ts_float = time.time()
            # Always update todays_history and save to DB for YFinance symbols
            if now_ts_float - last_history_update.get(sym, 0) >= UPDATE_INTERVAL:
                with data_lock:
                    if sym not in todays_history: todays_history[sym] = []
                    todays_history[sym].insert(0, summary)
                broadcast_history_append(sym, summary)
                last_history_update[sym] = now_ts_float
                self._save_db(sym, summary)
            if now_ts_float - last_alert.get(sym, 0) >= UPDATE_INTERVAL:
                self.send_alert(sym, summary)  # Use self.send_alert
                last_alert[sym] = now_ts_float
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

    def _get_nse_option_chain_data(self, sym: str, is_equity: bool = False) -> Optional[Dict]:
        """
        Helper method to fetch and process NSE option chain data for both indices and F&O equities.
        Returns a dictionary containing summary, strikes, pulse_summary, chart data, and raw dataframes.
        """
        now_ist = self._get_ist_time()
        current_time_only = now_ist.time()

        if not (NSE_FETCH_START_TIME <= current_time_only <= NSE_FETCH_END_TIME):
            # If outside market hours, and there's an active trade, trigger exit logic for bot
            if sym in self.deepseek_bot.active_trades:
                current_vix_value = shared_data.get("INDIAVIX", {}).get("live_feed_summary", {}).get(
                    "current_value", 15.0)
                active_trade_info = self.deepseek_bot.active_trades[sym]
                # Dummy DataFrames for bot's exit logic when no real data is available
                dummy_df_ce = pd.DataFrame(
                    [{'strikePrice': active_trade_info.get('otm_ce_strike', 0), 'openInterest': 0,
                      'lastPrice': active_trade_info.get('entry_premium_ce', 0)}])
                dummy_df_pe = pd.DataFrame(
                    [{'strikePrice': active_trade_info.get('otm_pe_strike', 0), 'openInterest': 0,
                      'lastPrice': active_trade_info.get('entry_premium_pe', 0)}])

                self.deepseek_bot.analyze_and_recommend(sym, todays_history.get(sym, []), current_vix_value,
                                                        dummy_df_ce, dummy_df_pe)
                broadcast_live_update()
            print(
                f"Skipping NSE data fetch for {sym} outside of market hours ({current_time_only.strftime('%H:%M')}). Retaining last data.")
            return None  # Return None as no new data was fetched

        url = self.url_equities + sym if is_equity else self.url_indices + sym
        for attempt in range(2):
            response = self.session.get(url, headers=self.nse_headers, timeout=10, verify=False)
            if response.status_code == 403 and attempt == 0:
                print(f"403 Forbidden for {sym}, refreshing cookies and retrying...")
                self._set_nse_session_cookies()
                time.sleep(1)
            elif response.status_code == 200:
                break
            else:
                response.raise_for_status()

        response.raise_for_status()
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
            return None

        df_ce = pd.DataFrame(ce_values)
        df_pe = pd.DataFrame(pe_values)
        df = pd.merge(df_ce[['strikePrice', 'openInterest', 'changeinOpenInterest', 'lastPrice']],
                      df_pe[['strikePrice', 'openInterest', 'changeinOpenInterest', 'lastPrice']],
                      on='strikePrice', how='outer',
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
        intraday_pcr = round(total_put_coi / total_call_coi, 2) if total_call_coi != 0 else 0.0

        atm_index_in_strikes_data = next((j for j, s in enumerate(strikes_data) if s['is_atm']), 10)
        atm_call_coi = strikes_data[atm_index_in_strikes_data]['call_coi']
        atm_put_coi = strikes_data[atm_index_in_strikes_data]['put_coi']
        diff = round((atm_call_coi - atm_put_coi) / 1000, 1)

        current_history_for_sym = todays_history.get(sym, [])
        rule_based_sentiment = self.get_sentiment(pcr, intraday_pcr, total_call_coi, total_put_coi, sym,
                                                  current_history_for_sym)
        ml_sentiment = self.get_ml_sentiment(pcr, intraday_pcr, total_call_coi, total_put_coi, sym,
                                             current_history_for_sym)  # NEW: Get ML sentiment

        add_exit_str = " | ".join(filter(None,
                                         [f"CE Add: {', '.join(sorted(ce_add_strikes))}" if ce_add_strikes else "",
                                          f"PE Add: {', '.join(sorted(pe_add_strikes))}" if pe_add_strikes else "",
                                          f"CE Exit: {', '.join(sorted(ce_exit_strikes))}" if ce_exit_strikes else "",
                                          f"PE Exit: {', '.join(sorted(pe_exit_strikes))}" if pe_exit_strikes else ""])) or "No Change"
        summary = {'time': self._get_ist_time().strftime("%H:%M"), 'sp': int(sp), 'value': int(round(underlying)),
                   'call_oi': round(atm_call_coi / 1000, 1), 'put_oi': round(atm_put_coi / 1000, 1), 'pcr': pcr,
                   'sentiment': rule_based_sentiment, 'expiry': expiry, 'add_exit': add_exit_str,
                   'intraday_pcr': intraday_pcr,
                   'total_call_coi': total_call_coi, 'total_put_coi': total_put_coi,
                   'ml_sentiment': ml_sentiment,  # ADDED ml_sentiment to summary
                   'symbol': sym  # Added symbol for consistency
                   }
        pulse_summary = {'total_call_oi': total_call_oi, 'total_put_oi': total_put_oi,
                         'total_call_coi': total_call_coi, 'total_put_coi': total_put_coi}
        max_pain_df = self._calculate_max_pain(df_ce, df_pe)
        ce_dt_for_charts = df_ce.sort_values(['openInterest'], ascending=False)
        pe_dt_for_charts = df_pe.sort_values(['openInterest'], ascending=False)
        final_ce_data = ce_dt_for_charts[['strikePrice', 'openInterest']].iloc[:10].to_dict(orient='records')
        final_pe_data = pe_dt_for_charts[['strikePrice', 'openInterest']].iloc[:10].to_dict(orient='records')

        return {
            'summary': summary,
            'strikes': strikes_data,
            'pulse_summary': pulse_summary,
            'pcr_chart_data': [{"TIME": summary['time'], "PCR": pcr, "IntradayPCR": intraday_pcr}],
            'max_pain_chart_data': max_pain_df.to_dict(orient='records'),
            'ce_oi_chart_data': final_ce_data,
            'pe_oi_chart_data': final_pe_data,
            'raw_df_ce': df_ce,  # Include raw DFs for AI Bot
            'raw_df_pe': df_pe  # Include raw DFs for AI Bot
        }

    def _process_nse_data(self, sym: str):
        global ai_bot_trades
        global ai_bot_trade_history
        global last_ai_bot_run_time

        try:
            processed_data = self._get_nse_option_chain_data(sym, is_equity=False)
            if processed_data is None:
                return  # Data fetching failed or outside market hours, already handled

            summary = processed_data['summary']
            strikes_data = processed_data['strikes']
            pulse_summary = processed_data['pulse_summary']
            pcr = summary['pcr']
            ml_sentiment = summary['ml_sentiment']
            rule_based_sentiment = summary['sentiment']
            sp = summary['sp']

            with data_lock:
                if sym not in shared_data: shared_data[sym] = {}
                shared_data[sym].update({
                    'summary': summary,
                    'strikes': strikes_data,
                    'pulse_summary': pulse_summary,
                    'max_pain_chart_data': processed_data['max_pain_chart_data'],
                    'ce_oi_chart_data': processed_data['ce_oi_chart_data'],
                    'pe_oi_chart_data': processed_data['pe_oi_chart_data'],
                })
                # Update pcr_chart_data separately as it's cumulative over time
                if sym not in self.pcr_graph_data: self.pcr_graph_data[sym] = []
                self.pcr_graph_data[sym].append(processed_data['pcr_chart_data'][0])  # Append the single new point
                if len(self.pcr_graph_data[sym]) > 180: self.pcr_graph_data[sym].pop(0)  # Keep history limited
                shared_data[sym]['pcr_chart_data'] = self.pcr_graph_data[sym]

            print(
                f"{sym} LIVE DATA UPDATED | SP: {sp} | PCR: {pcr} | Sentiment: {rule_based_sentiment} | ML Sentiment: {ml_sentiment}")
            broadcast_live_update()

            now_ts_float = time.time()
            if now_ts_float - last_history_update.get(sym, 0) >= UPDATE_INTERVAL:
                self.previous_pcr[sym] = pcr
                with data_lock:
                    if sym not in todays_history: todays_history[sym] = []
                    todays_history[sym].insert(0, summary)
                broadcast_history_append(sym, summary)
                last_history_update[sym] = now_ts_float
                self._save_db(sym, summary)

            # NEW: Run DeepSeekBot analysis for NSE symbols
            if (now_ts_float - last_ai_bot_run_time.get(sym, 0) >= AI_BOT_UPDATE_INTERVAL):
                current_vix_value = shared_data.get("INDIAVIX", {}).get("live_feed_summary", {}).get(
                    "current_value", 15.0)

                bot_recommendation = self.deepseek_bot.analyze_and_recommend(sym, todays_history.get(sym, []),
                                                                             current_vix_value,
                                                                             processed_data['raw_df_ce'],
                                                                             processed_data['raw_df_pe'])
                with data_lock:
                    ai_bot_trades[sym] = bot_recommendation
                    # Clean up old history entries
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
                self.send_alert(sym, summary)  # Use self.send_alert
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
                # If data fetching failed or outside market hours, emit error/no data to client
                if symbol in equity_data_cache:  # If we have old data, send that
                    socketio.emit('equity_data_update',
                                  {'symbol': symbol, 'data': equity_data_cache[symbol]['current']}, to=sid)
                else:  # Otherwise, explicitly state no data
                    socketio.emit('equity_data_update',
                                  {'symbol': symbol, 'error': 'No current data available or outside market hours.'},
                                  to=sid)
                return

            # Update equity_data_cache globally
            if processed_data:
                previous_data = equity_data_cache.get(symbol, {}).get('current')
                equity_data_cache[symbol] = {'current': processed_data, 'previous': previous_data}

            # Emit updated data to the requesting client
            socketio.emit('equity_data_update', {'symbol': symbol, 'data': processed_data}, to=sid)

            # Update shared_data for equity symbols (for top movers calculation)
            with data_lock:
                if symbol not in shared_data: shared_data[symbol] = {}
                shared_data[symbol].update({
                    'summary': processed_data['summary'],
                    'pulse_summary': processed_data['pulse_summary'],
                })

            now_ts_float = time.time()
            # Run AI Bot analysis for F&O Equity symbols
            if (now_ts_float - last_ai_bot_run_time.get(symbol, 0) >= AI_BOT_UPDATE_INTERVAL):
                current_vix_value = shared_data.get("INDIAVIX", {}).get("live_feed_summary", {}).get(
                    "current_value", 15.0)

                bot_recommendation = self.deepseek_bot.analyze_and_recommend(symbol, todays_history.get(symbol, []),
                                                                             current_vix_value,
                                                                             processed_data['raw_df_ce'],
                                                                             processed_data['raw_df_pe'])
                with data_lock:
                    ai_bot_trades[symbol] = bot_recommendation
                    # Clean up old history entries
                    now_dt = self._get_ist_time()
                    two_days_ago_dt = now_dt - datetime.timedelta(days=AI_BOT_HISTORY_DAYS)
                    ai_bot_trade_history[:] = [
                        entry for entry in ai_bot_trade_history
                        if datetime.datetime.fromisoformat(entry["timestamp"]).date() >= two_days_ago_dt.date()
                    ]

                print(
                    f"DeepSeekBot recommendation for {symbol}: {bot_recommendation['recommendation']} - {bot_recommendation['trade']}")
                last_ai_bot_run_time[symbol] = now_ts_float
                broadcast_live_update()

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
            return None  # Data fetching failed or outside market hours

        # Add to todays_history and save to DB
        summary = processed_data['summary']
        now_ts_float = time.time()
        if now_ts_float - last_history_update.get(sym, 0) >= UPDATE_INTERVAL:
            self.previous_pcr[sym] = summary['pcr']
            with data_lock:
                if sym not in todays_history: todays_history[sym] = []
                todays_history[sym].insert(0, summary)
                if sym not in self.YFINANCE_SYMBOLS:
                    if sym not in self.pcr_graph_data: self.pcr_graph_data[sym] = []
                    self.pcr_graph_data[sym].append(processed_data['pcr_chart_data'][0])
                    if len(self.pcr_graph_data[sym]) > 180: self.pcr_graph_data[sym].pop(0)
                    # For equity, the pcr_chart_data emitted to client is just the latest point.
                    # The full chart history is built from todays_history on the frontend.
                    # So, we don't need to update shared_data[sym]['pcr_chart_data'] here like for indices.
            broadcast_history_append(sym, summary)
            last_history_update[sym] = now_ts_float
            self._save_db(sym, summary)

        if now_ts_float - last_alert.get(sym, 0) >= UPDATE_INTERVAL:
            self.send_alert(sym, summary)
            last_alert[sym] = now_ts_float

        return processed_data

    def get_atm_strike(self, df: pd.DataFrame, underlying: float) -> Optional[int]:
        try:
            strikes = df['strikePrice'].astype(int).unique()
            if len(strikes) > 0:
                closest_strike = min(strikes, key=lambda x: abs(x - underlying))
                # Consider a wider range for equities or if underlying can fluctuate significantly
                # For NIFTY/BANKNIFTY, 20% might be too wide, but for individual stocks it could be reasonable.
                # Adjust this threshold if you find it's too permissive or restrictive.
                if abs((closest_strike - underlying) / underlying) < 0.20:
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

    def _load_ml_models(self):
        try:
            # Ensure these files exist after running generate_ml_data.py and train_ml_model.py
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
                # Get the most recent history point for current features
                latest_data = history[0] if history else {}
                prev_data = history[1] if len(history) > 1 else {}

                # Construct features for the current snapshot, matching training script
                feature_values = {
                    'pcr': pcr,
                    'intraday_pcr': intraday_pcr,
                    'value': latest_data.get('value', 0),
                    'call_oi': latest_data.get('call_oi', 0),
                    'put_oi': latest_data.get('put_oi', 0),

                    'pcr_lag1': prev_data.get('pcr', pcr),  # Use current if no lag
                    'intraday_pcr_lag1': prev_data.get('intraday_pcr', intraday_pcr),
                    'value_lag1': prev_data.get('value', latest_data.get('value', 0)),
                    'call_oi_lag1': prev_data.get('call_oi', latest_data.get('call_oi', 0)),
                    'put_oi_lag1': prev_data.get('put_oi', latest_data.get('put_oi', 0)),
                }

                # Calculate ROC features
                feature_values['pcr_roc'] = pcr - feature_values['pcr_lag1']
                feature_values['intraday_pcr_roc'] = intraday_pcr - feature_values['intraday_pcr_lag1']
                feature_values['value_roc'] = latest_data.get('value', 0) - feature_values['value_lag1']
                feature_values['call_oi_roc'] = latest_data.get('call_oi', 0) - feature_values['call_oi_lag1']
                feature_values['put_oi_roc'] = latest_data.get('put_oi', 0) - feature_values['put_oi_lag1']

                # Calculate OI spread features
                feature_values['oi_spread'] = feature_values['put_oi'] - feature_values['call_oi']
                feature_values['oi_spread_roc'] = feature_values['oi_spread'] - (
                        prev_data.get('put_oi', 0) - prev_data.get('call_oi', 0))

                # Create a DataFrame for prediction
                input_df = pd.DataFrame([feature_values])

                # Ensure columns are in the exact order the model expects
                # Fill any missing features (e.g., if you add new ones to training later) with 0
                for feature in self.sentiment_features:
                    if feature not in input_df.columns:
                        input_df[feature] = 0.0  # Default to 0.0 for numerical features
                input_df = input_df[self.sentiment_features]  # Reorder columns

                # Predict sentiment (numerical label)
                predicted_label = self.sentiment_model.predict(input_df)[0]
                # Convert numerical label back to string sentiment
                predicted_sentiment_str = self.sentiment_label_encoder.inverse_transform([predicted_label])[0]

                return predicted_sentiment_str

            except Exception as e:
                print(f"Error during ML sentiment prediction for {sym}: {e}. Returning 'N/A'.")
                return 'N/A'
        return 'N/A'  # Return N/A if model is not loaded or history is empty

    def get_sentiment(self, pcr: float, intraday_pcr: float, total_call_coi: int, total_put_coi: int, sym: str,
                      history: List[Dict[str, Any]]) -> str:
        """
        Calculates rule-based sentiment.
        """
        PCR_BULLISH_THRESHOLD = 1.2
        PCR_BEARISH_THRESHOLD = 0.8
        PCR_STABLE_THRESHOLD = 0.05
        OI_CHANGE_THRESHOLD_FOR_TREND = 50000

        pe_oi_delta_positive = total_put_coi > 0
        pe_oi_delta_negative = total_put_coi < 0

        ce_oi_delta_positive = total_call_coi > 0
        ce_oi_delta_negative = total_call_coi < 0

        if pcr > PCR_BULLISH_THRESHOLD:
            if pe_oi_delta_positive:
                return "Strong Bullish"
            elif pe_oi_delta_negative:
                return "Strong Bearish"
        elif pcr < PCR_BEARISH_THRESHOLD:
            if pe_oi_delta_positive:
                return "Strong Bearish"
            elif pe_oi_delta_negative:
                return "Strong Bullish"

        TREND_LOOKBACK = 5
        if len(history) >= TREND_LOOKBACK:
            recent_history = history[:TREND_LOOKBACK]
            # Ensure history is not empty and has relevant data
            if recent_history and 'pcr' in recent_history[-1] and 'pcr' in recent_history[0]:
                first_pcr = recent_history[-1]['pcr']
                last_pcr = recent_history[0]['pcr']
                pcr_trend_change = last_pcr - first_pcr

                pcr_is_stable = abs(pcr_trend_change) < PCR_STABLE_THRESHOLD
                pcr_is_rising = pcr_trend_change > PCR_STABLE_THRESHOLD
                pcr_is_falling = pcr_trend_change < -PCR_STABLE_THRESHOLD

                cumulative_call_coi_history = sum(h.get('total_call_coi', 0) for h in recent_history)
                cumulative_put_coi_history = sum(h.get('total_put_coi', 0) for h in recent_history)

                ce_oi_adding_trend = cumulative_call_coi_history > OI_CHANGE_THRESHOLD_FOR_TREND
                ce_oi_exiting_trend = cumulative_call_coi_history < -OI_CHANGE_THRESHOLD_FOR_TREND
                pe_oi_adding_trend = cumulative_put_coi_history > OI_CHANGE_THRESHOLD_FOR_TREND
                pe_oi_exiting_trend = cumulative_put_coi_history < -OI_CHANGE_THRESHOLD_FOR_TREND

                if (pcr_is_stable or pcr_is_falling) and ce_oi_adding_trend:
                    return "Weakening"
                if (pcr_is_stable or pcr_is_rising) and pe_oi_adding_trend:
                    return "Strengthening"
                if pcr_is_falling and pe_oi_exiting_trend:
                    return "Bullish Reversal"
                if pcr_is_rising and ce_oi_exiting_trend:
                    return "Bearish Reversal"

        if pcr >= 1.0:
            if pe_oi_delta_positive:
                return "Mild Bullish"
            elif ce_oi_delta_positive:
                return "Mild Bearish"
        elif pcr < 1.0:
            if ce_oi_delta_negative:
                return "Mild Bullish"
            elif pe_oi_delta_negative:
                return "Mild Bearish"

        if pcr >= 1.1:
            return "Mild Bullish"
        elif pcr < 0.9:
            return "Mild Bearish"

        return "Neutral"

    def send_alert(self, sym: str, row: Dict[str, Any]):
        """
        Sends a Telegram alert with market data.
        """
        if not SEND_TEXT_UPDATES or TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN" or TELEGRAM_CHAT_ID == "YOUR_TELEGRAM_CHAT_ID":
            return

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
            message += f"• Sentiment (Rule-Based): {row.get('sentiment', 'N/A')}\n"
            message += f"• Sentiment (ML-Based): {row.get('ml_sentiment', 'N/A')}\n"  # ADDED ML sentiment to alert

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
                "INSERT INTO history (timestamp, symbol, sp, value, call_oi, put_oi, pcr, sentiment, add_exit, intraday_pcr, ml_sentiment) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                # ADDED ml_sentiment column
                (ts, sym, row.get('sp', 0), row.get('value', 0), row.get('call_oi', 0), row.get('put_oi', 0),
                 row.get('pcr', 0), row.get('sentiment', ''), row.get('add_exit', ''),
                 row.get('intraday_pcr', 0.0), row.get('ml_sentiment', 'N/A')))  # Save ml_sentiment
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
                # If min_pain_strike is not exactly in the list, find the closest one
                center_idx = (pd.Series(strikes_sorted) - min_pain_strike).abs().idxmin()

            start_idx = max(0, center_idx - 8)
            end_idx = min(len(strikes_sorted), center_idx + 8)

            # Ensure start_idx is not greater than end_idx if the list is very small
            if start_idx >= end_idx:
                return pd.DataFrame()  # Return empty if window is invalid

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
