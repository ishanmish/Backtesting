
# =============================================================================
# START: urlopen / requests SSL Verification Disable Patches (Keep if needed)
# =============================================================================
import ssl
import urllib.request
import urllib.error
import requests
import warnings
import traceback

# --- urlopen Patch ---
# print("--- Applying urlopen SSL Verification Disable Patch ---") # Keep logs minimal
_original_urlopen = urllib.request.urlopen
_unverified_context = None
try: _unverified_context = ssl._create_unverified_context(); # print("   - Created unverified SSL context.")
except AttributeError: # print("   - Warning: ssl._create_unverified_context not available.");
    pass
def _patched_urlopen(*args, **kwargs):
    if _unverified_context and 'context' not in kwargs: kwargs['context'] = _unverified_context
    try: return _original_urlopen(*args, **kwargs)
    except urllib.error.URLError as e: print(f"   - Patched urlopen caught URLError: {e}"); raise e
    except Exception as e: print(f"   - Patched urlopen caught unexpected Exception: {type(e).__name__} - {e}"); raise e
urllib.request.urlopen = _patched_urlopen
# print("--- urlopen patch applied (Potentially Insecure) ---")

# --- requests Patch ---
try: from requests.packages.urllib3.exceptions import InsecureRequestWarning
except ImportError:
    try: from urllib3.exceptions import InsecureRequestWarning
    except ImportError: InsecureRequestWarning = None
try:
    if InsecureRequestWarning: warnings.simplefilter('ignore', InsecureRequestWarning)
    original_request = requests.Session.request
    def patched_request(*args, **kwargs): kwargs['verify'] = False; return original_request(*args, **kwargs)
    requests.Session.request = patched_request
    def patched_top_level_request(method, url, **kwargs): kwargs['verify'] = False; session = requests.Session(); return session.request(method=method, url=url, **kwargs)
    requests.request = patched_top_level_request
    requests.get = lambda url, params=None, **kwargs: requests.request("get", url, params=params, **kwargs)
    requests.post = lambda url, data=None, json=None, **kwargs: requests.request("post", url, data=data, json=json, **kwargs)
    # print("--- Requests SSL Verification Disabled via Patch (Insecure) ---")
except Exception as patch_exc: print(f"!!! Failed to apply requests SSL patch: {patch_exc} !!!"); traceback.print_exc()
# =============================================================================
# END: SSL Verification Disable Patches
# =============================================================================


# --- Standard Libraries & Third-Party Imports ---
import os
import datetime
import calendar
import sys
import re
import time
import math
import warnings
import traceback
from io import BytesIO
import pickle
from pathlib import Path

import streamlit as st
import pandas as pd
import numpy as np
try: import plotly.graph_objects as go; PLOTLY_AVAILABLE = True
except ImportError: PLOTLY_AVAILABLE = False
from dotenv import load_dotenv, find_dotenv
from termcolor import colored
try: import openpyxl; from openpyxl.styles import Font, Alignment, PatternFill, numbers; from openpyxl.utils import get_column_letter; OPENPYXL_AVAILABLE = True
except ImportError: OPENPYXL_AVAILABLE = False
try: from breeze_connect import BreezeConnect
except ImportError: st.error("Fatal Error: breeze-connect library not found."); st.stop()

# --- Suppress Warnings ---
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)


# --- Constants ---
NIFTY_LOTS = 30
NIFTY_LOT_SIZE = 75 # Adjusted Lot Size (as per original script comment) # <<< USER CONFIRMED THIS IS FINE
NIFTY_STOCK_CODE = "NIFTY"
NIFTY_EXCHANGE_CODE = "NSE"
NFO_EXCHANGE_CODE = "NFO"
NIFTY_ROUNDING_BASE = 50
TRADE_START_TIME = datetime.time(9, 15)
TRADE_END_TIME = datetime.time(15, 29) # Strict end time before market close
EOD_SQUARE_OFF_TIME = datetime.time(15, 29, 59) # Time for EOD square off check
SAVED_RESULTS_DIR = Path("./saved_results")
COLOR_PROFIT_BG = '#C8E6C9'; COLOR_LOSS_BG = '#8B0000' # Lighter BG colors
COLOR_PROFIT_TEXT = '#1B5E20'; COLOR_LOSS_TEXT = '#9C0006' # Darker Text colors
COLOR_SMA = '#FFA500'

# --- Session State Initialization ---
default_states = {
    'breeze': None,
    'trade_log_df': pd.DataFrame(),
    'overall_metrics': None,
    'yearly_metrics_dict': {},
    'monthly_metrics_dict': {},
    'overall_equity': None,
    'daily_pnl_df': pd.DataFrame(),
    'run_completed': False,
    'current_params': {},
    'graph_granularity': 'Daily',
    'connection_status_shown': False,
    # Add keys for sidebar widgets if not already present
    'param_strategy': 1, # Default value
    'param_days': 30,     # Default value
    'param_orb': 30,      # Default value
    'param_max_trades': 1, # <<< ADDED: Default max trades per day
    'param_tp': 50.0,     # Default value
    'param_sl': 25.0,     # Default value
    'load_select': "",    # Default value
    # Button states are managed internally by Streamlit via their keys
}
for key, default_value in default_states.items():
    if key not in st.session_state:
        st.session_state[key] = default_value
# print("--- Session State Initialized ---")


# --- Ensure Saved Results Directory Exists ---
SAVED_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# ==============================================================
# Initialize Breeze Connection - WITH IMMEDIATE VALIDATION (NO CACHING)
# ==============================================================
# @st.cache_resource # Cache the Breeze connection object itself <<<<------ REMOVED/COMMENTED OUT
def initialize_breeze():
    """
    Initializes and returns the Breeze connection object. (NO LONGER CACHED)
    Includes immediate validation using get_customer_details.
    """
    # Needed imports inside the function if not globally available where called
    from dotenv import load_dotenv, find_dotenv
    import os
    from breeze_connect import BreezeConnect
    from termcolor import colored
    import traceback
    import streamlit as st

    # print("Attempting to initialize Breeze connection (NO CACHE)...") # Keep logs minimal
    breeze_conn_obj = None
    try:
        env_path = find_dotenv(filename='kitecred.env', raise_error_if_not_found=True, usecwd=True)
        load_dotenv(dotenv_path=env_path)
        API_KEY = os.getenv("BREEZE_API_KEY")
        API_SECRET = os.getenv("BREEZE_API_SECRET")
        SESSION_TOKEN = os.getenv("BREEZE_SESSION_TOKEN")

        if not API_KEY or not API_SECRET or not SESSION_TOKEN:
            st.error("Credentials missing in kitecred.env.")
            print(colored("   - Error: Credentials missing in kitecred.env.", "red"))
            return None

        # print("   - Credentials loaded. Creating BreezeConnect object...")
        breeze_conn_obj = BreezeConnect(api_key=API_KEY)

        # print(f"   - Generating session with token: ...{SESSION_TOKEN[-6:]}")
        breeze_conn_obj.generate_session(api_secret=API_SECRET, session_token=SESSION_TOKEN)
        # print("   - Session generated potentially successfully. Attempting immediate validation...")

        try:
            details = breeze_conn_obj.get_customer_details(api_session=SESSION_TOKEN)
            # print(f"   - Validation API call response: {details}") # Keep logs minimal

            if details and isinstance(details, dict) and details.get('Success') is not None:
                user_name = details['Success'].get('idirect_user_name', 'N/A')
                # print(colored(f"   - Breeze connection VALIDATED successfully for {user_name}.", "green"))
            elif details and isinstance(details, dict) and details.get('Error') is not None:
                error_message = details.get('Error')
                st.error(f"Breeze Connect Validation Error: {error_message}. Check Session Token.")
                print(colored(f"   - Breeze connection VALIDATION FAILED: {error_message}", "red"))
                return None
            else:
                st.error("Breeze connection validation returned an unexpected response format.")
                print(colored(f"   - Breeze connection VALIDATION FAILED: Unexpected response format: {details}", "red"))
                return None
        except Exception as validation_exc:
             st.error(f"Error during Breeze connection validation API call: {validation_exc}")
             print(colored(f"   - Exception during validation API call: {validation_exc}", "red"))
             traceback.print_exc()
             return None

        # print("   - Breeze connection initialization and validation complete.")
        return breeze_conn_obj

    except FileNotFoundError:
        st.error("'kitecred.env' not found. Place it in the correct directory.")
        print(colored("   - Error: 'kitecred.env' not found.", "red"))
        return None
    except IOError:
        st.error("Error reading 'kitecred.env'. Check file permissions.")
        print(colored("   - Error: IOError reading 'kitecred.env'.", "red"))
        return None
    except Exception as e:
        st.error(f"Error during Breeze connection initialization: {e}")
        print(colored(f"   - Exception during initialization (before validation): {e}", "red"))
        traceback.print_exc()
        return None
# --- End of the initialize_breeze function ---


# ==============================================================
# Helper Functions
# ==============================================================
def get_api_datetime_string(dt_obj):
    """Formats datetime object for Breeze API."""
    return dt_obj.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def parse_api_datetime_string(dt_str):
    """Parses datetime string from Breeze API response."""
    if not dt_str or not isinstance(dt_str, str): return None
    try:
        # Handle potential 'T' and milliseconds
        dt_str_clean = dt_str.replace('T', ' ').split('.')[0]
        return datetime.datetime.strptime(dt_str_clean, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        # print(f"Warn: Parse API datetime failed: '{dt_str}'") # Reduce noise
        return None

def get_nearest_monthly_expiry_details(target_date):
    """Finds the nearest upcoming monthly expiry date (last Thursday)."""
    year = target_date.year
    month = target_date.month
    input_date_part = target_date.date()

    while True:
        # Find the last day of the month
        last_day = calendar.monthrange(year, month)[1]
        date_last_day = datetime.date(year, month, last_day)

        # Find the last Thursday (weekday 3)
        days_to_subtract = (date_last_day.weekday() - 3 + 7) % 7
        expiry_day = last_day - days_to_subtract
        potential_expiry_date = datetime.date(year, month, expiry_day)

        # If this expiry is on or after the target date, use it
        if potential_expiry_date >= input_date_part:
            api_expiry_str = potential_expiry_date.strftime("%Y-%m-%d") + "T06:00:00.000Z"
            return potential_expiry_date, api_expiry_str

        # Otherwise, check the next month
        month += 1
        if month > 12:
            month = 1
            year += 1

def get_atm_strike(spot_price, rounding_base):
    """Calculates the At-The-Money strike price."""
    if spot_price is None or pd.isna(spot_price) or rounding_base <= 0:
        return None
    return round(spot_price / rounding_base) * rounding_base

# --- MODIFIED create_log_entry ---
def create_log_entry(date_obj, status, trade_num_day=None, orb_h=None, orb_l=None, pnl=0.0, entry_time=None, entry_price_opt=None, entry_spot=None, exit_time=None, exit_price_opt=None, exit_spot=None, option_symbol=None, strike=None, option_type=None, **other_kwargs):
    """Creates a dictionary for the trade log."""
    if isinstance(date_obj, datetime.datetime):
        date_str = date_obj.date().strftime('%Y-%m-%d')
    elif isinstance(date_obj, datetime.date):
        date_str = date_obj.strftime('%Y-%m-%d')
    else:
        date_str = str(date_obj)

    log_dict = {
        "Date": date_str,
        "Trade#": trade_num_day, # <<< ADDED
        "ORB High": orb_h, "ORB Low": orb_l, "Status": status,
        "Entry Time": entry_time.strftime('%H:%M') if isinstance(entry_time, datetime.datetime) else str(entry_time),
        "Entry Price (Opt)": entry_price_opt, "Entry Spot": entry_spot,
        "Exit Time": exit_time.strftime('%H:%M') if isinstance(exit_time, datetime.datetime) else str(exit_time),
        "Exit Price (Opt)": exit_price_opt, "Exit Spot": exit_spot,
        "PnL (Rupees)": pnl, "Option Symbol": option_symbol, "Strike": strike, "Option Type": option_type
    }
    log_dict.update(other_kwargs)
    # Replace NaN with None for better handling later (e.g., in Excel)
    for k, v in log_dict.items():
        if isinstance(v, (float, np.number)) and pd.isna(v):
            log_dict[k] = None
        # Ensure Trade# is None if not provided or NaN
        if k == "Trade#" and pd.isna(v):
            log_dict[k] = None
    return log_dict
# --- End of MODIFIED create_log_entry ---

# ==============================================================
# OPTIMIZED API Interaction Function with Caching
# ==============================================================
@st.cache_data(ttl=3600, show_spinner=False) # Cache for 1 hour, manage spinner outside
def get_historical_data_cached(_breeze_obj, interval, from_date_str, to_date_str, stock_code, exchange_code, product_type=None, expiry_date=None, right=None, strike_price=None):
    """
    Fetches historical data using the Breeze API with retries and caching.
    Uses st.cache_data for efficient caching based on ALL relevant arguments.
    Note: _breeze_obj is passed but not used in the cache key implicitly by st.cache_data.
    We rely on the other arguments to define uniqueness.
    """
    retries = 3
    delay = 2
    last_error = None

    # Construct kwargs carefully for the API call
    api_kwargs = {}
    if product_type: api_kwargs['product_type'] = product_type
    if expiry_date: api_kwargs['expiry_date'] = expiry_date
    if right: api_kwargs['right'] = right
    if strike_price: api_kwargs['strike_price'] = str(strike_price) # Ensure string

    # Create a unique identifier string for logging/debugging cache misses
    cache_key_desc = f"{stock_code}:{exchange_code}:{interval}:{from_date_str}-{to_date_str}"
    if product_type: cache_key_desc += f":{product_type}"
    if expiry_date: cache_key_desc += f":{expiry_date[:10]}" # Just date part for brevity
    if right: cache_key_desc += f":{right}"
    if strike_price: cache_key_desc += f":{strike_price}"
    # print(f"--- Cache Miss / API Call --- : {cache_key_desc}") # Reduce noise

    # Use the actual breeze object passed in the current session state for the call
    breeze_obj = st.session_state.breeze
    if not breeze_obj:
        print(colored("ERROR: Breeze object not found in session state during cached call.", "red"))
        return None, "Breeze object missing"

    for attempt in range(retries):
        try:
            data = breeze_obj.get_historical_data_v2(
                interval=interval,
                from_date=from_date_str,
                to_date=to_date_str,
                stock_code=stock_code,
                exchange_code=exchange_code,
                **api_kwargs # Pass specific args like product_type here
            )
            # print(f"DEBUG: API RAW Response (Try {attempt+1}): {str(data)[:200]}...") # Debug: Show API response start
        except Exception as e:
            last_error = f"Network/Request Exception: {e}"
            print(colored(f"DEBUG: API Call Exception (Try {attempt+1}/{retries}): {last_error}", "red"))
            if attempt < retries - 1: time.sleep(delay); delay *= 2
            continue # Retry

        # Process the response
        if data and isinstance(data, dict):
            if 'Success' in data and data['Success'] is not None:
                # Success: Return the data list and a success status
                return data['Success'], "Success"
            elif data.get('Status') == 500 and 'busy' in str(data.get('Error', '')).lower():
                last_error = "API Busy"
                print(colored(f"DEBUG: API Busy (Try {attempt+1}/{retries}). Retrying...", "yellow"))
                if attempt < retries - 1: time.sleep(delay); delay *= 2
                continue # Retry
            elif 'Error' in data:
                last_error = data['Error']
                print(colored(f"DEBUG: API Error Response: {last_error}", "red"))
                return None, f"API Error: {last_error}" # Return None on specific API error
            else:
                last_error = "Unexpected API dictionary response"
                print(colored(f"DEBUG: {last_error}: {data}", "red"))
                return None, last_error # Return None on other dict errors
        elif data is None: # Explicit None from API
             last_error = "API returned None"
             print(colored(f"DEBUG: {last_error} (Try {attempt+1}/{retries})", "red"))
             # Don't retry if API explicitly returns None, might indicate no data exists
             return None, last_error
        else: # Unexpected type
            last_error = f"Unexpected API response type: {type(data)}"
            print(colored(f"DEBUG: {last_error}", "red"))
            return None, last_error # Return None

    # If all retries fail
    print(colored(f"DEBUG: API call failed after {retries} retries. Last error: {last_error}", "red"))
    return None, f"Failed after retries: {last_error}"


def process_historical_data(raw_data):
    """Converts raw API data list into a DataFrame with proper types and index."""
    if not raw_data or not isinstance(raw_data, list):
        return pd.DataFrame() # Return empty DataFrame if no data

    try:
        df = pd.DataFrame(raw_data)
        if df.empty: return df

        # Basic Cleaning & Type Conversion
        df['datetime_obj'] = df['datetime'].apply(parse_api_datetime_string)
        df = df.dropna(subset=['datetime_obj']) # Drop rows where datetime couldn't be parsed
        if df.empty: return df # Check again after dropping bad dates
        df = df.set_index('datetime_obj')

        cols_to_numeric = ['open', 'high', 'low', 'close', 'volume']
        for col in cols_to_numeric:
            if col in df.columns:
                # Ensure it's string, replace comma, then convert
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '', regex=False), errors='coerce')

        # Select and rename columns for consistency (optional but good practice)
        df = df[['open', 'high', 'low', 'close', 'volume']].astype(float) # Ensure float type after conversion

        return df.sort_index() # Ensure data is sorted by time

    except Exception as e:
        print(colored(f"Error processing historical data into DataFrame: {e}", "red"))
        traceback.print_exc()
        return pd.DataFrame() # Return empty DataFrame on error

# ==============================================================
# REVISED Function: run_backtest (Adding max_trades_per_day)
# ==============================================================
# --- MODIFIED run_backtest Signature ---
def run_backtest(breeze_obj, days, orb_duration, strategy_type, max_trades_per_day, tp_points=None, sl_points=None):
    """
    Runs the ORB backtest using bulk data fetching and caching.
    Allows up to `max_trades_per_day` trades per day.
    """
    print("\n" + "="*40); print(f" Starting Backtest Calculation (Optimized) ".center(40, "=")); print(f"Params: Days={days}, ORB={orb_duration}m, Strat={strategy_type}, MaxTrades={max_trades_per_day}, TP={tp_points}, SL={sl_points}, Lots={NIFTY_LOTS}, LotSize={NIFTY_LOT_SIZE}"); print("="*40 + "\n") # <<< ADDED MaxTrades param display

    trade_log = []
    end_date = datetime.datetime.now().date()
    # Fetch slightly more days initially to ensure enough valid trading days
    start_date_approx = end_date - datetime.timedelta(days=max(days * 2, days + 90))
    all_potential_dates = list(pd.date_range(start=start_date_approx, end=end_date, freq='B').date)

    trading_days_processed = 0
    days_to_process = days
    processed_dates_count = 0
    progress_bar = st.progress(0)
    status_text = st.empty()

    # Ensure breeze_obj is valid before starting the loop
    if not breeze_obj:
        status_text.error("Breeze connection object is invalid. Cannot start backtest.")
        return pd.DataFrame()

    # Loop through potential dates in reverse (most recent first)
    for i, current_date in enumerate(reversed(all_potential_dates)):
        if trading_days_processed >= days_to_process:
            status_text.info(f"Target {days_to_process} trading days processed.")
            break

        processed_dates_count += 1
        # Safety break: If we scan too many calendar days without finding enough trading days
        if processed_dates_count > days_to_process * 5 and trading_days_processed < days_to_process:
            status_text.warning(f"Stopping early: processed {processed_dates_count} calendar days, found {trading_days_processed} valid trading days with data.")
            break

        # Update progress
        progress = min(1.0, (trading_days_processed + 1) / days_to_process)
        progress_bar.progress(progress)
        status_text.info(f"Processing Date: {current_date.strftime('%d-%b-%Y')} ({trading_days_processed+1}/{days_to_process})...")
        print(f"\n--- Processing Date: {current_date.strftime('%d-%b-%Y')} ({trading_days_processed+1}/{days_to_process}) ---")

        # --- Define Time Ranges for the Day ---
        day_start_dt = datetime.datetime.combine(current_date, TRADE_START_TIME)
        day_end_dt = datetime.datetime.combine(current_date, TRADE_END_TIME) # Represents the *end* of the last valid candle's period
        eod_square_off_dt = datetime.datetime.combine(current_date, EOD_SQUARE_OFF_TIME)
        orb_end_dt = day_start_dt + datetime.timedelta(minutes=orb_duration)

        # --- Fetch FULL DAY Nifty Spot Data ---
        fetch_start_api = get_api_datetime_string(day_start_dt - datetime.timedelta(minutes=1)) # Fetch from just before start
        fetch_end_api = get_api_datetime_string(day_end_dt + datetime.timedelta(minutes=2)) # Fetch until just after end

        print(f"Fetching DAILY NIFTY SPOT data for {current_date:%Y-%m-%d}...")
        spot_data_raw, fetch_status = get_historical_data_cached(
            breeze_obj, # Pass the object, though cache key ignores it
            interval="1minute",
            from_date_str=fetch_start_api,
            to_date_str=fetch_end_api,
            stock_code=NIFTY_STOCK_CODE,
            exchange_code=NIFTY_EXCHANGE_CODE
        )

        if spot_data_raw is None or not isinstance(spot_data_raw, list):
            log_msg = f"API Error (Daily Spot Fetch): {fetch_status}"
            print(colored(f"Skip {current_date}: {log_msg}", "red"))
            trade_log.append(create_log_entry(date_obj=current_date, status=log_msg))
            continue # Skip to next potential date

        spot_df = process_historical_data(spot_data_raw)

        if spot_df.empty:
            log_msg = "No Spot Data Received/Processed"
            print(colored(f"Skip {current_date}: {log_msg}", "yellow"))
            trade_log.append(create_log_entry(date_obj=current_date, status=log_msg))
            continue # Skip to next potential date

        # --- ORB Calculation from DataFrame ---
        print(f"Calculating ORB from {day_start_dt:%H:%M} to {orb_end_dt:%H:%M}...")
        try:
            # Select data within the ORB range (exclusive of orb_end_dt)
            orb_candles = spot_df[(spot_df.index >= day_start_dt) & (spot_df.index < orb_end_dt)]
            if orb_candles.empty:
                log_msg = "ORB Calc Error (No Candles in Range)"
                print(colored(f"Skip {current_date}: {log_msg}", "yellow"))
                trade_log.append(create_log_entry(date_obj=current_date, status=log_msg))
                continue

            orb_high = orb_candles['high'].max()
            orb_low = orb_candles['low'].min()

            if pd.isna(orb_high) or pd.isna(orb_low) or orb_low >= orb_high:
                log_msg = f"ORB Calc Error (Invalid Range: H={orb_high}, L={orb_low})"
                print(colored(f"Skip {current_date}: {log_msg}", "red"))
                trade_log.append(create_log_entry(date_obj=current_date, status=log_msg, orb_h=orb_high, orb_l=orb_low))
                continue

            print(f"ORB Calculated: High = {orb_high:.2f}, Low = {orb_low:.2f}")

        except Exception as e_orb_calc:
            log_msg = f"ORB Calc Error (Exception: {e_orb_calc})"
            print(colored(f"Critical ORB Calc Error {current_date}: {e_orb_calc}", "red"))
            traceback.print_exc()
            trade_log.append(create_log_entry(date_obj=current_date, status=log_msg))
            continue

        # --- If ORB is valid, this counts as a processed trading day ---
        trading_days_processed += 1

        # --- Intraday Loop & Trading Logic (Using Spot DataFrame) ---
        position_open = False
        # trade_executed_today = False # <<< REMOVED - Replaced by counter
        trades_taken_today = 0       # <<< ADDED - Initialize trade counter for the day
        day_skipped_due_to_option_data = False
        entry_details = {}
        option_df = pd.DataFrame() # Initialize empty DataFrame for option data

        # Iterate through candles AFTER the ORB period ends, up to EOD
        intraday_candles = spot_df[(spot_df.index >= orb_end_dt) & (spot_df.index < eod_square_off_dt)]

        for candle_time, candle_data in intraday_candles.iterrows():
            current_dt = candle_time # Already a datetime object (index)
            current_spot = candle_data['close']
            current_high = candle_data['high']
            current_low = candle_data['low']

            # --- Entry Logic ---
            # --- MODIFIED Entry Condition ---
            if not position_open and trades_taken_today < max_trades_per_day and not day_skipped_due_to_option_data:
                entry_spot_price = None
                option_type = None
                trigger_level = None
                potential_trade_num = trades_taken_today + 1 # For logging

                # Check for breakout
                if current_high > orb_high:
                    option_type = "Put" # Sell Put on High Breakout
                    trigger_level = orb_high
                    entry_spot_price = current_spot # Use close of trigger candle
                    print(f"[{current_dt:%H:%M}] Trade {potential_trade_num}/{max_trades_per_day} Signal: High BO (Spot H={current_high:.2f} > ORB_H={orb_high:.2f}). Prep Sell {option_type}...")
                elif current_low < orb_low:
                    option_type = "Call" # Sell Call on Low Breakout
                    trigger_level = orb_low
                    entry_spot_price = current_spot
                    print(f"[{current_dt:%H:%M}] Trade {potential_trade_num}/{max_trades_per_day} Signal: Low BO (Spot L={current_low:.2f} < ORB_L={orb_low:.2f}). Prep Sell {option_type}...")

                if option_type:
                    # trade_executed_today = True # <<< REMOVED

                    # --- Determine Option Details ---
                    try:
                        strike = get_atm_strike(entry_spot_price, NIFTY_ROUNDING_BASE)
                        if strike is None: raise ValueError(f"Could not determine ATM strike for spot {entry_spot_price}")

                        expiry_obj, expiry_api = get_nearest_monthly_expiry_details(current_dt)
                        expiry_disp = expiry_obj.strftime('%d%b%y').upper()
                        option_symbol = f"{NIFTY_STOCK_CODE}{expiry_disp}{int(strike)}{'CE' if option_type == 'Call' else 'PE'}"

                        print(f"    Target Option: {option_symbol} (Expiry: {expiry_obj:%Y-%m-%d}, Strike: {strike})")

                        # --- Fetch FULL DAY Option Data (Cached) ---
                        print(f"    Fetching DAILY OPTION data for {option_symbol}...")
                        option_data_raw, opt_fetch_status = get_historical_data_cached(
                            breeze_obj,
                            interval="1minute",
                            from_date_str=fetch_start_api, # Use same day range as spot
                            to_date_str=fetch_end_api,
                            stock_code=NIFTY_STOCK_CODE,
                            exchange_code=NFO_EXCHANGE_CODE,
                            product_type="options",
                            expiry_date=expiry_api,
                            right=option_type,
                            strike_price=strike
                        )

                        if option_data_raw is None or not isinstance(option_data_raw, list):
                            log_msg = f"Data Issue (Option Data Fetch Failed: {opt_fetch_status} for Trade {potential_trade_num})"
                            print(colored(f"    {log_msg}. Skipping day (further entries blocked).", "red"))
                            # Log this failure, but don't log it as a numbered trade yet
                            trade_log.append(create_log_entry(date_obj=current_date, status=log_msg, orb_h=orb_high, orb_l=orb_low))
                            day_skipped_due_to_option_data = True # Block further attempts today
                            break # Exit intraday loop for this day

                        option_df = process_historical_data(option_data_raw)

                        if option_df.empty:
                            log_msg = f"Data Issue (No Option Data Received/Processed for Trade {potential_trade_num})"
                            print(colored(f"    {log_msg}. Skipping day (further entries blocked).", "red"))
                            # Log this failure
                            trade_log.append(create_log_entry(date_obj=current_date, status=log_msg, orb_h=orb_high, orb_l=orb_low))
                            day_skipped_due_to_option_data = True # Block further attempts today
                            break # Exit intraday loop for this day

                        # --- Find Entry Price from Option DataFrame ---
                        entry_price_opt = None
                        price_source_entry = "Not Found in Data"
                        try:
                            entry_lookup_time = current_dt
                            entry_price_opt = option_df['close'].asof(entry_lookup_time)
                            if pd.notna(entry_price_opt):
                                price_source_entry = "Data Lookup (asof)"
                                # print(colored(f"    OK: Entry Option Price Found: {entry_price_opt:.2f} at {entry_lookup_time:%H:%M}", "green")) # Reduce noise
                            else:
                                if entry_lookup_time in option_df.index:
                                     entry_price_opt = option_df.loc[entry_lookup_time, 'close']
                                     if pd.notna(entry_price_opt):
                                         price_source_entry = "Data Lookup (exact)"
                                         # print(colored(f"    OK: Entry Option Price Found (exact): {entry_price_opt:.2f} at {entry_lookup_time:%H:%M}", "green")) # Reduce noise
                        except KeyError:
                            print(colored(f"    Warn: Exact entry time {current_dt:%H:%M} not found in option data index.", "yellow"))
                        except Exception as e_lookup:
                             print(colored(f"    Error looking up entry price in option data: {e_lookup}", "red"))

                        if pd.isna(entry_price_opt):
                            log_msg = f"Data Issue (Entry Option Price Not Found for Trade {potential_trade_num})"
                            print(colored(f"    {log_msg}. Skipping day (further entries blocked).", "red"))
                            # Log this failure
                            trade_log.append(create_log_entry(date_obj=current_date, status=log_msg, orb_h=orb_high, orb_l=orb_low))
                            day_skipped_due_to_option_data = True # Block further attempts today
                            break # Exit intraday loop for this day

                        # *** SUCCESSFUL ENTRY ***
                        trades_taken_today += 1 # <<< INCREMENT COUNTER HERE
                        entry_details = {
                            "trade_num_day": trades_taken_today, # <<< ADDED trade number
                            "date": current_date, "orb_high": orb_high, "orb_low": orb_low,
                            "entry_time": current_dt, "entry_spot": entry_spot_price,
                            "entry_option_price": entry_price_opt, "option_symbol": option_symbol,
                            "strike": strike, "expiry_api_str": expiry_api, "expiry_obj": expiry_obj,
                            "option_type": option_type, "trigger_level": trigger_level,
                            "price_source_entry": price_source_entry
                        }
                        position_open = True
                        print(colored(f"--> ENTRY #{trades_taken_today}: Sold {NIFTY_LOTS} lots {entry_details['option_symbol']} @ {entry_details['entry_option_price']:.2f} (Spot: {entry_details['entry_spot']:.2f})", "cyan"))

                    except Exception as e_entry_setup:
                        log_msg = f"Error During Entry Setup (Trade {potential_trade_num}): {e_entry_setup}"
                        print(colored(f"    {log_msg}. Skipping day (further entries blocked).", "red"))
                        traceback.print_exc()
                        # Log this failure
                        trade_log.append(create_log_entry(date_obj=current_date, status=log_msg, orb_h=orb_high, orb_l=orb_low))
                        day_skipped_due_to_option_data = True # Block further attempts today
                        break # Exit intraday loop for this day

            # --- Exit Logic ---
            # This part largely remains the same, but uses the entry_details['trade_num_day']
            elif position_open and entry_details.get('entry_time') and current_dt > entry_details['entry_time']:
                exit_reason = None
                exit_time = current_dt
                entry_opt_price = entry_details.get('entry_option_price')
                entry_spot = entry_details.get('entry_spot')
                trade_num_day = entry_details.get('trade_num_day', '?') # Get trade number for logging
                final_exit_spot = current_spot
                exit_price_opt_ref = None

                # --- Get Current Option Price from DataFrame ---
                current_option_price = None
                if not option_df.empty:
                     try:
                         current_option_price = option_df['close'].asof(exit_time)
                     except Exception as e_lookup:
                          print(colored(f"[{exit_time:%H:%M}] Error looking up current option price for Trade #{trade_num_day}: {e_lookup}", "yellow"))


                # --- Check Strategy 3 FIRST ---
                if strategy_type == 3 and tp_points is not None and sl_points is not None and entry_opt_price is not None:
                    if pd.notna(current_option_price):
                        exit_price_opt_ref = current_option_price
                        tp_target_price = entry_opt_price - tp_points
                        if current_option_price <= tp_target_price:
                            exit_reason = f"TP Hit (Option Price <= {tp_target_price:.2f})"
                        elif not exit_reason:
                            sl_target_price = entry_opt_price + sl_points
                            if current_option_price >= sl_target_price:
                                exit_reason = f"SL Hit (Option Price >= {sl_target_price:.2f})"

                # --- Check Strategy 1 or 2 (Spot Price TP/SL) ---
                if exit_reason is None and (strategy_type == 1 or strategy_type == 2) and entry_spot is not None:
                    opt_type = entry_details['option_type']
                    current_candle_high = current_high
                    current_candle_low = current_low

                    if opt_type == 'Put': # Selling Put
                        if tp_points is not None and current_candle_high >= entry_spot + tp_points:
                            exit_reason = f"TP Hit (Spot H >= {entry_spot + tp_points:.2f})"
                            final_exit_spot = current_candle_high
                        elif strategy_type == 1 and sl_points is not None and current_candle_low <= entry_spot - sl_points:
                            exit_reason = f"SL Hit (Spot L <= {entry_spot - sl_points:.2f})"
                            final_exit_spot = current_candle_low
                        elif strategy_type == 2 and current_candle_low <= orb_low:
                            exit_reason = f"Exit: Spot L <= ORB L ({orb_low:.2f})"
                            final_exit_spot = current_candle_low
                    elif opt_type == 'Call': # Selling Call
                        if tp_points is not None and current_candle_low <= entry_spot - tp_points:
                            exit_reason = f"TP Hit (Spot L <= {entry_spot - tp_points:.2f})"
                            final_exit_spot = current_candle_low
                        elif strategy_type == 1 and sl_points is not None and current_candle_high >= entry_spot + sl_points:
                            exit_reason = f"SL Hit (Spot H >= {entry_spot + sl_points:.2f})"
                            final_exit_spot = current_candle_high
                        elif strategy_type == 2 and current_candle_high >= orb_high:
                            exit_reason = f"Exit: Spot H >= ORB H ({orb_high:.2f})"
                            final_exit_spot = current_candle_high

                # --- Check EOD Exit ---
                if exit_reason is None and current_dt.time() >= TRADE_END_TIME:
                    exit_reason = "EOD Exit"
                    final_exit_spot = current_spot

                # --- Process Exit IF any reason was found ---
                if exit_reason:
                    print(f"[{exit_time:%H:%M}] Exit Trigger (Trade #{trade_num_day}): {exit_reason} (Ref Spot: {final_exit_spot:.2f})")

                    # Get Final Exit Option Price
                    exit_price_opt, exit_price_source = None, "Lookup Failed"
                    if pd.notna(exit_price_opt_ref):
                         exit_price_opt = exit_price_opt_ref
                         exit_price_source = "Trigger Price (Strat 3)"
                    elif pd.notna(current_option_price):
                        exit_price_opt = current_option_price
                        exit_price_source = "Data Lookup (asof)"
                    else:
                        exit_price_source = "Data Lookup Failed"

                    # Calculate PnL
                    pnl_per_share = np.nan
                    total_pnl_rupees = 0.0
                    final_status = exit_reason

                    if pd.notna(exit_price_opt) and pd.notna(entry_opt_price):
                        pnl_per_share = entry_opt_price - exit_price_opt # Short position
                        total_pnl_rupees = pnl_per_share * NIFTY_LOT_SIZE * NIFTY_LOTS
                    elif pd.isna(entry_opt_price):
                         final_status = f"{exit_reason} (Entry Option Price Invalid)"
                    else:
                         final_status = f"{exit_reason} (Exit Option Price Unavailable: {exit_price_source})"

                    # Log the trade
                    log_entry_args = {
                        "trade_num_day": trade_num_day, # <<< ADDED
                        "orb_h": entry_details['orb_high'], "orb_l": entry_details['orb_low'],
                        "pnl": total_pnl_rupees, "entry_time": entry_details['entry_time'],
                        "entry_price_opt": entry_details['entry_option_price'], "entry_spot": entry_details['entry_spot'],
                        "exit_time": exit_time, "exit_price_opt": exit_price_opt,
                        "exit_spot": final_exit_spot, "option_symbol": entry_details['option_symbol'],
                        "strike": entry_details['strike'], "option_type": entry_details['option_type'],
                        "price_source_entry": entry_details.get('price_source_entry', '?'),
                        "price_source_exit": exit_price_source
                    }
                    trade_log.append(create_log_entry(date_obj=entry_details['date'], status=final_status, **log_entry_args))
                    color = "green" if total_pnl_rupees > 0 else "red" if total_pnl_rupees < 0 else "white"
                    print(colored(f"<-- EXIT #{trade_num_day}: PnL: Rs {total_pnl_rupees:,.2f}", color))

                    # Reset state for the *next potential trade* within the day
                    position_open = False
                    entry_details = {}
                    option_df = pd.DataFrame() # Clear option df, will be refetched if needed for next trade
                    # DO NOT break here if max_trades_per_day > 1, allow loop to continue

        # --- End of Intraday Loop ---

        # --- EOD Check (If position somehow still open after loop - should be handled by loop end condition) ---
        if position_open:
            trade_num_day_eod = entry_details.get('trade_num_day', '?')
            print(colored(f"Warn: Position for Trade #{trade_num_day_eod} still open after intraday loop {current_date}. Forcing EOD close.", "yellow"))
            eod_exit_time = intraday_candles.index[-1] if not intraday_candles.empty else datetime.datetime.combine(current_date, TRADE_END_TIME)
            eod_exit_spot_ref = spot_df['close'].asof(eod_exit_time) if not spot_df.empty else entry_details.get('entry_spot')

            eod_exit_price_opt, eod_price_source = None, "Lookup Failed"
            if not option_df.empty: # Reuse df if available from last check
                 try: eod_exit_price_opt = option_df['close'].asof(eod_exit_time); eod_price_source = "Data Lookup (asof EOD)"
                 except: pass
            if pd.isna(eod_exit_price_opt): eod_price_source = "EOD Lookup Failed"

            # Calculate PnL for forced EOD exit
            exit_status = "Forced EOD Exit"; total_pnl_rupees = 0.0
            entry_opt_price_eod = entry_details.get('entry_option_price')
            if pd.notna(eod_exit_price_opt) and pd.notna(entry_opt_price_eod):
                 pnl_per_share = entry_opt_price_eod - eod_exit_price_opt
                 total_pnl_rupees = pnl_per_share * NIFTY_LOT_SIZE * NIFTY_LOTS
            elif pd.isna(entry_opt_price_eod): exit_status = f"Forced EOD Exit (Entry Price Invalid: {entry_opt_price_eod})"
            else: exit_status = f"Forced EOD Exit (Exit Price Unavailable: {eod_price_source})"

            # Log EOD exit
            log_entry_args = { "trade_num_day": trade_num_day_eod, "orb_h": entry_details['orb_high'], "orb_l": entry_details['orb_low'], "pnl": total_pnl_rupees, "entry_time": entry_details['entry_time'], "entry_price_opt": entry_opt_price_eod, "entry_spot": entry_details['entry_spot'], "exit_time": eod_exit_time, "exit_price_opt": eod_exit_price_opt, "exit_spot": eod_exit_spot_ref, "option_symbol": entry_details['option_symbol'], "strike": entry_details['strike'], "option_type": entry_details['option_type'], "price_source_entry": entry_details.get('price_source_entry', '?'), "price_source_exit": eod_price_source }
            trade_log.append(create_log_entry(date_obj=entry_details['date'], status=exit_status, **log_entry_args))
            color = "green" if total_pnl_rupees > 0 else "red" if total_pnl_rupees < 0 else "white"
            print(colored(f"<-- EXIT #{trade_num_day_eod} (FORCED EOD): PnL: Rs {total_pnl_rupees:,.2f}", color))
            position_open = False # Reset state

        # --- MODIFIED Log Condition: Log "No Tradeable Breakout" if no trades were taken AND day wasn't skipped early ---
        elif trades_taken_today == 0 and not day_skipped_due_to_option_data:
             print("No breakout signal generated or successfully traded today.")
             trade_log.append(create_log_entry(date_obj=current_date, status="No Tradeable Breakout", orb_h=orb_high, orb_l=orb_low))

    # --- End of Main Date Loop & Final Processing ---
    progress_bar.progress(1.0)
    status_text.success("✅ Backtest calculation finished!")
    print("\n" + "="*40); print(f" Backtest Calculation Completed ".center(40, "=")); print("="*40 + "\n")

    if not trade_log:
        return pd.DataFrame() # Return empty if no logs

    # Create DataFrame and Convert Data Types
    trade_log_df = pd.DataFrame(trade_log)
    # Reverse log to show earliest date first in the UI display
    trade_log_df = trade_log_df.iloc[::-1].reset_index(drop=True)

    # --- ADDED Trade# conversion ---
    numeric_cols = ['Trade#','ORB High', 'ORB Low', 'Entry Price (Opt)', 'Exit Price (Opt)', 'PnL (Rupees)', 'Entry Spot', 'Exit Spot', 'Strike']
    for col in numeric_cols:
        if col in trade_log_df.columns:
            trade_log_df[col] = pd.to_numeric(trade_log_df[col], errors='coerce')
            # Special handling for Trade# to make it integer or NA
            if col == 'Trade#':
                trade_log_df[col] = trade_log_df[col].astype('Int64') # Use nullable integer type

    if 'Date' in trade_log_df.columns:
        trade_log_df['Date'] = pd.to_datetime(trade_log_df['Date'], errors='coerce')

    # Replace None/NaT strings in time columns for display consistency
    for col in ['Entry Time', 'Exit Time']:
         if col in trade_log_df.columns:
             trade_log_df[col] = trade_log_df[col].fillna('-').astype(str).replace('NaT','-').replace('None','-')

    return trade_log_df


# ==============================================================
# Metrics Functions (calculate_metrics) - Largely unchanged
# ==============================================================
def calculate_metrics(trade_df, initial_capital=100000, period_label="Overall"):
    # --- [Keep the existing calculate_metrics function content here] ---
    # ... (No major changes needed for this feature)
    metrics = {'Period': period_label}
    # Updated non_trade_statuses regex
    non_trade_statuses = "No Breakout|API Error|No .* Data|ORB Calc Error|Invalid Range|No Candles|Data Issue|No Tradeable Breakout|Entry Price Invalid|Exit Price Unavailable|Forced EOD Exit"
    trades = trade_df[
        ~trade_df['Status'].astype(str).str.contains(non_trade_statuses, na=False, case=False, regex=True) &
        trade_df['Entry Time'].notna() & (trade_df['Entry Time'] != '-') &
        trade_df['Exit Time'].notna() & (trade_df['Exit Time'] != '-') &
        trade_df['PnL (Rupees)'].notna()
    ].copy()

    if 'Date' in trades.columns and not pd.api.types.is_datetime64_any_dtype(trades['Date']):
        trades['Date'] = pd.to_datetime(trades['Date'], errors='coerce')
    if 'PnL (Rupees)' in trades.columns: trades['PnL (Rupees)'] = pd.to_numeric(trades['PnL (Rupees)'], errors='coerce')
    if 'Entry Price (Opt)' in trades.columns: trades['Entry Price (Opt)'] = pd.to_numeric(trades['Entry Price (Opt)'], errors='coerce')
    trades = trades.dropna(subset=['Date', 'PnL (Rupees)', 'Entry Price (Opt)']).copy() # Drop if core values missing

    # Trading Days Analyzed calculation logic is refined slightly
    if not trade_df.empty and 'Date' in trade_df.columns:
         try:
             analyzed_days_df = trade_df.dropna(subset=['Date'])
             # Ensure Date is datetime
             analyzed_days_df['Date'] = pd.to_datetime(analyzed_days_df['Date'], errors='coerce')
             analyzed_days_df = analyzed_days_df.dropna(subset=['Date'])
             # Exclude days where spot fetch failed entirely (no ORB calc attempted)
             analyzed_days_df = analyzed_days_df[~analyzed_days_df['Status'].astype(str).str.contains("API Error.*Spot Fetch", na=False, case=False, regex=True)]
             if not analyzed_days_df.empty:
                 # Count unique dates where ORB was calculated or a data issue occurred after ORB calc
                 metrics['Trading Days Analyzed'] = analyzed_days_df['Date'].dt.date.nunique()
             else:
                 metrics['Trading Days Analyzed'] = 0
         except Exception as e: metrics['Trading Days Analyzed'] = f'Error ({e})'
    else: metrics['Trading Days Analyzed'] = 0


    metrics['Target Instrument'] = f"{NIFTY_STOCK_CODE} Options"; metrics['Total Trades Executed'] = len(trades)
    # Default values if no trades executed
    default_metrics = { 'Total PnL (Rupees)': 0.0, 'Profit Factor': np.nan, 'Win Rate': 0.0, 'Average Trade PnL (Rupees)': 0.0, 'Average Winning Trade (Rupees)': 0.0, 'Average Losing Trade (Rupees)': 0.0, 'Avg Win/Loss Ratio (Rupees)': np.nan, 'Max Drawdown (Rupees)': 0.0, 'Recovery Factor': np.nan, 'Sharpe Ratio (Annualized)': np.nan, 'Sortino Ratio (Annualized)': np.nan, 'Max Consecutive Wins': 0, 'Max Consecutive Losses': 0, 'Expectancy Per Trade %': np.nan, 'Average Trade PnL %': np.nan, 'Average Winning Trade %': np.nan, 'Average Losing Trade %': np.nan, 'Avg Win/Loss Ratio (%)': np.nan }
    if metrics['Total Trades Executed'] == 0:
        metrics.update(default_metrics)
        equity_curve_data = pd.DataFrame(columns=['Cumulative PnL (Rupees)'])
        equity_curve_data.index.name = 'Date'
        return metrics, equity_curve_data

    # --- Start Calculation for Trades ---
    # Sort trades chronologically (important for multiple trades per day)
    if 'Entry Time' in trades.columns and trades['Entry Time'].notna().any() and trades['Entry Time'].ne('-').any():
        try:
             # Handle potential time format issues if needed
             def safe_combine_datetime(row):
                 try: return datetime.datetime.combine(row['Date'].date(), datetime.datetime.strptime(row['Entry Time'], '%H:%M').time())
                 except: return pd.NaT # Handle errors like invalid time format or NaT date
             trades['Entry Datetime'] = trades.apply(safe_combine_datetime, axis=1)
             trades = trades.sort_values(by=['Entry Datetime'], na_position='first').drop(columns=['Entry Datetime'])
        except Exception as sort_e:
             print(f"Warning: Could not sort by precise entry time: {sort_e}")
             trades = trades.sort_values(by=['Date', 'Trade#'], na_position='first') # Fallback sort by Date and Trade#
    else: trades = trades.sort_values(by=['Date', 'Trade#'], na_position='first') # Fallback sort

    # --- The rest of the metric calculations remain the same ---
    # Calculate PnL related metrics
    trades['PnL Points Per Share'] = (trades['PnL (Rupees)'] / NIFTY_LOTS) / NIFTY_LOT_SIZE
    trades['Entry Price (Abs)'] = trades['Entry Price (Opt)'].abs().replace(0, np.nan) # Avoid division by zero
    trades['PnL %'] = np.where(trades['Entry Price (Abs)'].notna() & (trades['Entry Price (Abs)'] != 0), (trades['PnL Points Per Share'] / trades['Entry Price (Abs)']) * 100, 0.0)

    metrics['Total PnL (Rupees)'] = trades['PnL (Rupees)'].sum()
    wins = trades[trades['PnL (Rupees)'] > 0]
    losses = trades[trades['PnL (Rupees)'] < 0]
    total_profit_rupees = wins['PnL (Rupees)'].sum()
    total_loss_rupees = abs(losses['PnL (Rupees)'].sum())

    if total_loss_rupees > 0: metrics['Profit Factor'] = round(total_profit_rupees / total_loss_rupees, 2)
    elif total_profit_rupees > 0: metrics['Profit Factor'] = np.inf # Only wins
    else: metrics['Profit Factor'] = 0.0 # Should be 0 if no profit and no loss, or only losses (0/loss)

    metrics['Win Rate'] = round((len(wins) / len(trades)) * 100, 2) if len(trades) > 0 else 0.0
    metrics['Average Trade PnL (Rupees)'] = round(trades['PnL (Rupees)'].mean(), 2) if len(trades) > 0 else 0.0
    metrics['Average Winning Trade (Rupees)'] = round(wins['PnL (Rupees)'].mean(), 2) if len(wins) > 0 else 0.0
    metrics['Average Losing Trade (Rupees)'] = round(losses['PnL (Rupees)'].mean(), 2) if len(losses) > 0 else 0.0

    avg_losing_trade_rs = metrics['Average Losing Trade (Rupees)']
    if avg_losing_trade_rs != 0 and not pd.isna(avg_losing_trade_rs): metrics['Avg Win/Loss Ratio (Rupees)'] = abs(round(metrics['Average Winning Trade (Rupees)'] / avg_losing_trade_rs, 2))
    elif metrics['Average Winning Trade (Rupees)'] > 0: metrics['Avg Win/Loss Ratio (Rupees)'] = np.inf
    else: metrics['Avg Win/Loss Ratio (Rupees)'] = np.nan

    # PnL % Metrics
    metrics['Expectancy Per Trade %'] = round(trades['PnL %'].mean(), 2) if trades['PnL %'].notna().any() else np.nan
    metrics['Average Trade PnL %'] = metrics['Expectancy Per Trade %'] # They are the same calculation
    metrics['Average Winning Trade %'] = round(wins['PnL %'].mean(), 2) if len(wins) > 0 and wins['PnL %'].notna().any() else np.nan
    metrics['Average Losing Trade %'] = round(losses['PnL %'].mean(), 2) if len(losses) > 0 and losses['PnL %'].notna().any() else np.nan

    avg_winning_trade_perc = metrics['Average Winning Trade %']
    avg_losing_trade_perc = metrics['Average Losing Trade %']
    if pd.notna(avg_losing_trade_perc) and avg_losing_trade_perc != 0 and pd.notna(avg_winning_trade_perc):
        metrics['Avg Win/Loss Ratio (%)'] = abs(round(avg_winning_trade_perc / avg_losing_trade_perc, 2))
    elif pd.notna(avg_winning_trade_perc) and avg_winning_trade_perc != 0 and (pd.isna(avg_losing_trade_perc) or avg_losing_trade_perc == 0):
        metrics['Avg Win/Loss Ratio (%)'] = np.inf
    else:
        metrics['Avg Win/Loss Ratio (%)'] = np.nan

    # Drawdown Calculation (Crucially uses sorted trades)
    trades['Cumulative PnL (Rupees)'] = trades['PnL (Rupees)'].cumsum()
    trades['Peak PnL (Rupees)'] = trades['Cumulative PnL (Rupees)'].cummax()
    trades['Drawdown (Rupees)'] = trades['Peak PnL (Rupees)'] - trades['Cumulative PnL (Rupees)']
    metrics['Max Drawdown (Rupees)'] = round(trades['Drawdown (Rupees)'].max(), 2) if not trades['Drawdown (Rupees)'].empty else 0.0

    # Recovery Factor
    max_dd_rupees = metrics['Max Drawdown (Rupees)']
    total_pnl_rupees_calc = metrics['Total PnL (Rupees)']
    if max_dd_rupees > 0: metrics['Recovery Factor'] = round(total_pnl_rupees_calc / max_dd_rupees, 2)
    elif total_pnl_rupees_calc > 0: metrics['Recovery Factor'] = np.inf # Profit with no drawdown
    else: metrics['Recovery Factor'] = np.nan # No profit or zero drawdown with no profit

    # Sharpe & Sortino Ratios (using daily PnL)
    daily_pnl_rupees = trades.groupby(trades['Date'].dt.date)['PnL (Rupees)'].sum()
    sharpe_ratio = np.nan
    sortino_ratio = np.nan
    if not daily_pnl_rupees.empty and len(daily_pnl_rupees) > 1:
        daily_returns_proxy = daily_pnl_rupees # Using PnL directly as proxy for returns
        rf_daily = 0.0 # Assume risk-free rate is 0 for daily calculation

        mean_ret = daily_returns_proxy.mean()
        std_ret = daily_returns_proxy.std()

        # Sharpe Ratio
        if std_ret is not None and std_ret != 0 and not pd.isna(std_ret):
            daily_sharpe = (mean_ret - rf_daily) / std_ret
            sharpe_ratio = round(daily_sharpe * np.sqrt(252), 2) # Annualized
        elif mean_ret > rf_daily: sharpe_ratio = np.inf
        elif mean_ret < rf_daily: sharpe_ratio = -np.inf
        else: sharpe_ratio = 0.0 # mean == rf_daily

        # Sortino Ratio
        negative_returns = daily_returns_proxy[daily_returns_proxy < rf_daily] - rf_daily # Deviations below target (0)
        if not negative_returns.empty:
            downside_std_dev = np.sqrt((negative_returns**2).mean()) # Std Dev of negative returns
            if downside_std_dev is not None and downside_std_dev != 0 and not pd.isna(downside_std_dev):
                daily_sortino = (mean_ret - rf_daily) / downside_std_dev
                sortino_ratio = round(daily_sortino * np.sqrt(252), 2) # Annualized
            elif mean_ret > rf_daily: sortino_ratio = np.inf # Positive returns, zero downside deviation
            elif mean_ret < rf_daily: sortino_ratio = -np.inf # Negative returns, but zero downside deviation (error?) -> should be negative
            else: sortino_ratio = 0.0 # Mean == rf_daily
        elif mean_ret > rf_daily: sortino_ratio = np.inf # Only non-negative returns
        else: sortino_ratio = 0.0 # Only zero returns

    metrics['Sharpe Ratio (Annualized)'] = sharpe_ratio
    metrics['Sortino Ratio (Annualized)'] = sortino_ratio

    # Consecutive Wins/Losses
    trades['Win'] = trades['PnL (Rupees)'] > 0
    trades['Loss'] = trades['PnL (Rupees)'] < 0
    valid_pnl_trades = trades[trades['PnL (Rupees)'] != 0].copy() # Exclude zero PnL trades for streak calculation

    if not valid_pnl_trades.empty:
        valid_pnl_trades['Win_Group'] = valid_pnl_trades['Win'].ne(valid_pnl_trades['Win'].shift()).cumsum()
        valid_pnl_trades['Loss_Group'] = valid_pnl_trades['Loss'].ne(valid_pnl_trades['Loss'].shift()).cumsum()
        consecutive_wins = valid_pnl_trades[valid_pnl_trades['Win']].groupby('Win_Group').size()
        consecutive_losses = valid_pnl_trades[valid_pnl_trades['Loss']].groupby('Loss_Group').size()
        metrics['Max Consecutive Wins'] = int(consecutive_wins.max()) if not consecutive_wins.empty else 0
        metrics['Max Consecutive Losses'] = int(consecutive_losses.max()) if not consecutive_losses.empty else 0
    else:
        metrics['Max Consecutive Wins'] = 0
        metrics['Max Consecutive Losses'] = 0

    # Prepare Equity Curve Data (using trade dates - correct now due to sorting)
    equity_curve_data = trades[['Date', 'Cumulative PnL (Rupees)']].copy()
    equity_curve_data = equity_curve_data.set_index('Date')
    # Group by date and take the *last* cumulative PnL for that day for the daily equity curve
    daily_equity_curve = equity_curve_data.groupby(equity_curve_data.index.date)['Cumulative PnL (Rupees)'].last()
    daily_equity_curve.index = pd.to_datetime(daily_equity_curve.index) # Convert index back to datetime
    daily_equity_curve.index.name = 'Date'

    # Resample to daily, forward fill to show curve on non-trading days within the period
    if not daily_equity_curve.empty:
        equity_curve_final = pd.DataFrame(daily_equity_curve).resample('D').ffill()
    else:
        equity_curve_final = pd.DataFrame(columns=['Cumulative PnL (Rupees)'])
        equity_curve_final.index.name = 'Date'


    return metrics, equity_curve_final

# ==============================================================
# Plotting Functions (plot_equity_curve_plotly, plot_periodic_pnl_plotly)
# ==============================================================
# --- [Keep the existing plotting functions here] ---
# No changes needed for this feature
def plot_equity_curve_plotly(equity_df, granularity="Daily"):
    """Plots the equity curve with optional SMA, adapting title to granularity."""
    if not PLOTLY_AVAILABLE: st.warning("Plotly not installed."); return None
    if equity_df is None or equity_df.empty: return None
    try:
        fig = go.Figure()
        cum_pnl_col = 'Cumulative PnL (Rupees)'
        if cum_pnl_col not in equity_df.columns:
             st.warning(f"Equity plot error: Column '{cum_pnl_col}' not found.")
             return None

        # Ensure index is datetime
        if not pd.api.types.is_datetime64_any_dtype(equity_df.index):
            equity_df.index = pd.to_datetime(equity_df.index, errors='coerce')
            equity_df = equity_df.dropna(axis=0, subset=[cum_pnl_col]) # Drop rows if date conversion failed
        if equity_df.empty: return None


        fig.add_trace(go.Scatter(x=equity_df.index, y=equity_df[cum_pnl_col],
                                 mode='lines', name='Equity', line=dict(color='royalblue', width=2),
                                 hovertemplate='Date: %{x|%Y-%m-%d}<br>Cum PnL: ₹%{y:,.0f}<extra></extra>'))
        # Add SMA only if enough data points exist
        if len(equity_df) >= 20:
            sma20 = equity_df[cum_pnl_col].rolling(window=20, min_periods=1).mean() # Use min_periods=1 to show SMA earlier
            fig.add_trace(go.Scatter(x=equity_df.index, y=sma20, mode='lines', name='SMA(20)',
                                     line=dict(color=COLOR_SMA, width=1, dash='dot'),
                                     hovertemplate='Date: %{x|%Y-%m-%d}<br>SMA(20): ₹%{y:,.0f}<extra></extra>'))

        fig.add_hline(y=0, line_width=1, line_dash="dash", line_color="grey")
        fig.update_layout(
            title=f'{granularity} Equity Curve (Cumulative PnL)',
            xaxis_title='Date', yaxis_title='PnL (₹)', yaxis_tickprefix='₹', yaxis_tickformat=',.0f',
            hovermode='x unified', height=400, margin=dict(l=10, r=10, t=50, b=10),
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
        )
        return fig
    except Exception as e: st.error(f"Equity plot error: {e}"); traceback.print_exc(); return None

def plot_periodic_pnl_plotly(pnl_df, granularity="Daily"):
    """Plots PnL bars based on chosen granularity (Daily, Monthly, Yearly)."""
    if not PLOTLY_AVAILABLE: st.warning("Plotly not installed."); return None
    if pnl_df is None or pnl_df.empty:
        # st.caption(f"No {granularity.lower()} PnL data to plot.") # Already handled in main UI
        return None
    try:
        # Identify the PnL column (should be the first/only one after resampling)
        if pnl_df.columns.empty:
             st.warning("Periodic PNL plot error: No columns found in PnL DataFrame.")
             return None
        pnl_col = pnl_df.columns[0]
        pnl_values = pd.to_numeric(pnl_df[pnl_col], errors='coerce').fillna(0)

        # Ensure index is datetime
        if not pd.api.types.is_datetime64_any_dtype(pnl_df.index):
            pnl_df.index = pd.to_datetime(pnl_df.index, errors='coerce')
            # Use pnl_col here for dropping rows if date conversion failed
            pnl_df = pnl_df.dropna(axis=0, subset=[pnl_col])
        if pnl_df.empty:
            # st.caption(f"No valid {granularity.lower()} PnL data after cleaning.") # Handled in main UI
            return None


        colors = [COLOR_PROFIT_TEXT if pnl > 0 else COLOR_LOSS_TEXT if pnl < 0 else 'grey' for pnl in pnl_values]
        fig = go.Figure()

        # Set hover format based on granularity
        if granularity == "Daily": date_format_hover = '%Y-%m-%d'; date_format_axis = '%Y-%m-%d'
        elif granularity == "Monthly": date_format_hover = '%b %Y'; date_format_axis = '%b-%Y' # Abbreviated for axis ticks
        elif granularity == "Yearly": date_format_hover = '%Y'; date_format_axis = '%Y'
        else: date_format_hover = '%Y-%m-%d'; date_format_axis = '%Y-%m-%d' # Default to daily

        fig.add_trace(go.Bar(x=pnl_df.index, y=pnl_values, marker_color=colors, name=f'{granularity} PnL',
                             hovertemplate=f'Period: %{{x|{date_format_hover}}}<br>{granularity} PnL: ₹%{{y:,.0f}}<extra></extra>'))

        fig.add_hline(y=0, line_width=1, line_dash="dash", line_color="black")
        fig.update_layout(
            title=f'{granularity} Profit & Loss',
            xaxis_title='Period', yaxis_title='PnL (₹)', yaxis_tickprefix='₹', yaxis_tickformat=',.0f',
            hovermode='x unified', height=400, margin=dict(l=10, r=10, t=50, b=10), bargap=0.2
        )
        # Format x-axis ticks for better readability
        fig.update_xaxes(tickformat=date_format_axis) # Apply axis formatting

        return fig
    except Exception as e:
        st.error(f"{granularity} PNL plot error: {e}")
        traceback.print_exc()
        return None


# ==============================================================
# Excel Saving Function (save_to_excel_streamlit)
# ==============================================================
# --- MODIFIED save_to_excel_streamlit to include Trade# ---
def save_to_excel_streamlit(trade_df, overall_metrics, yearly_metrics_dict, monthly_metrics_dict):
    if not OPENPYXL_AVAILABLE: st.warning("Openpyxl not installed. Cannot create Excel file."); return None
    if trade_df is None or trade_df.empty: st.info("No trade data to save to Excel."); return None

    try:
        # --- Styling Definitions (Fonts, Fills, Alignments, Formats) ---
        header_font = Font(bold=True, color="FFFFFF")
        bold_font = Font(bold=True)
        header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid") # Blue header
        # Use consistent (lighter) BG colors from Constants
        green_fill = PatternFill(start_color=COLOR_PROFIT_BG.replace('#',''), end_color=COLOR_PROFIT_BG.replace('#',''), fill_type="solid")
        red_fill = PatternFill(start_color=COLOR_LOSS_BG.replace('#',''), end_color=COLOR_LOSS_BG.replace('#',''), fill_type="solid")
        light_grey_fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid") # Alternate row fill
        center_align = Alignment(horizontal='center', vertical='center', wrap_text=False)
        right_align = Alignment(horizontal='right', vertical='center', wrap_text=False)
        left_align = Alignment(horizontal='left', vertical='center', wrap_text=True) # Allow wrap for text like status/symbol
        # Number Formats
        currency_format_inr = '"₹"#,##0.00;[Red]-"₹"#,##0.00'
        number_format_2dp = '0.00'
        percentage_format = '0.00%'
        integer_format = '0'
        date_format_excel = 'yyyy-mm-dd'
        time_format_excel = 'hh:mm'

        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl',
                            datetime_format=date_format_excel
                            ) as writer:

            # --- Trade Log Sheet ---
            trade_df_excel = trade_df.copy()

            # --- MODIFIED column order ---
            expected_cols_log = [
                'Date', 'Trade#', 'Status', 'ORB High', 'ORB Low', # Added Trade#
                'Entry Time', 'Entry Spot', 'Entry Price (Opt)',
                'Exit Time', 'Exit Spot', 'Exit Price (Opt)',
                'PnL (Rupees)', 'Option Symbol', 'Strike', 'Option Type',
                'price_source_entry', 'price_source_exit'
            ]
            cols_to_write = [col for col in expected_cols_log if col in trade_df_excel.columns]
            trade_df_excel = trade_df_excel[cols_to_write]

            # Prepare data: Ensure correct types, handle None/NaN
            numeric_cols_log = ['Trade#', 'ORB High', 'ORB Low', 'Entry Price (Opt)', 'Exit Price (Opt)', 'PnL (Rupees)', 'Entry Spot', 'Exit Spot', 'Strike'] # Added Trade#
            for col in numeric_cols_log:
                 if col in trade_df_excel.columns:
                     trade_df_excel[col] = pd.to_numeric(trade_df_excel[col], errors='coerce')
                     # Convert NaN to None for Excel to handle as blank (handle Int64 NA)
                     trade_df_excel[col] = trade_df_excel[col].apply(lambda x: None if pd.isna(x) else x)

            if 'Date' in trade_df_excel.columns:
                trade_df_excel['Date'] = pd.to_datetime(trade_df_excel['Date'], errors='coerce').dt.date

            for col in ['Entry Time', 'Exit Time']:
                 if col in trade_df_excel.columns:
                     trade_df_excel[col] = trade_df_excel[col].astype(str).replace('NaT', '-').replace('None','-')

            for col in trade_df_excel.select_dtypes(include=['object']).columns:
                 if col not in ['Entry Time', 'Exit Time']:
                     trade_df_excel[col] = trade_df_excel[col].fillna('-').astype(str)

            trade_df_excel.to_excel(writer, sheet_name='TradeLog', index=False, freeze_panes=(1, 0))
            worksheet_log = writer.sheets['TradeLog']

            # Apply Formatting to Trade Log
            log_col_indices = {cn: i + 1 for i, cn in enumerate(cols_to_write)}
            pnl_col_idx = log_col_indices.get('PnL (Rupees)')

            for cell in worksheet_log[1]:
                cell.font = header_font; cell.fill = header_fill; cell.alignment = center_align

            for r_idx in range(2, worksheet_log.max_row + 1):
                 row_fill = light_grey_fill if r_idx % 2 != 0 else None
                 pnl_val = None
                 pnl_cell = worksheet_log.cell(r_idx, pnl_col_idx) if pnl_col_idx else None
                 if pnl_cell and pnl_cell.value is not None and isinstance(pnl_cell.value, (int, float, np.number)):
                     pnl_val = float(pnl_cell.value)
                 pnl_color_fill = green_fill if pnl_val is not None and pnl_val > 0 else red_fill if pnl_val is not None and pnl_val < 0 else None

                 for c_title, c_idx in log_col_indices.items():
                     cell = worksheet_log.cell(r_idx, c_idx)
                     final_fill = pnl_color_fill if pnl_color_fill else row_fill
                     if final_fill: cell.fill = final_fill

                     fmt, align = None, left_align # Default alignment
                     is_num_or_date = isinstance(cell.value, (int, float, np.number, datetime.date, datetime.time))

                     if c_title == 'Date' and isinstance(cell.value, datetime.date):
                         fmt, align = date_format_excel, center_align
                     elif c_title == 'Trade#': # <<< ADDED Trade# formatting
                         fmt, align = integer_format, center_align
                     elif c_title in ['Entry Time', 'Exit Time']:
                         align = center_align
                     elif c_title == 'PnL (Rupees)':
                         fmt, align = currency_format_inr, right_align
                     elif c_title in ['ORB High', 'ORB Low', 'Entry Price (Opt)', 'Exit Price (Opt)', 'Entry Spot', 'Exit Spot']:
                         fmt, align = number_format_2dp, right_align
                     elif c_title == 'Strike':
                         fmt, align = integer_format, right_align
                     elif c_title in ['Option Symbol', 'Status']:
                         align = left_align
                     elif c_title in ['Option Type', 'price_source_entry', 'price_source_exit']:
                         align = center_align

                     if fmt and is_num_or_date and cell.value is not None:
                         cell.number_format = fmt
                     if align:
                         cell.alignment = align

            # Auto-adjust column widths for Trade Log
            for c_idx, c_title in enumerate(cols_to_write, 1):
                col_letter = get_column_letter(c_idx)
                max_len = len(str(c_title))
                for i in range(2, worksheet_log.max_row + 1):
                    cell_value = worksheet_log.cell(i, c_idx).value
                    if cell_value is not None:
                         cell_format = worksheet_log.cell(i, c_idx).number_format
                         if isinstance(cell_value, datetime.date): cell_len = len(cell_value.strftime('%Y-%m-%d'))
                         elif cell_format == currency_format_inr and isinstance(cell_value, (int, float, np.number)): cell_len = len(f"₹{cell_value:,.2f}") + 1
                         elif cell_format == number_format_2dp and isinstance(cell_value, (int, float, np.number)): cell_len = len(f"{cell_value:.2f}") + 1
                         elif cell_format == integer_format and isinstance(cell_value, (int, float, np.number)): cell_len = len(f"{cell_value:,.0f}") + 1
                         else: cell_len = len(str(cell_value))
                         max_len = max(max_len, cell_len)
                # Adjusted width logic (small width for Trade#)
                if c_title == 'Trade#': adjusted_width = 8
                else: adjusted_width = min(max(max_len + 3, len(c_title)+2), 40) # Existing logic
                worksheet_log.column_dimensions[col_letter].width = adjusted_width


            # --- Summary Sheets Formatting Function (No changes needed here) ---
            def format_summary_sheet(worksheet, metrics_df):
                if metrics_df.empty: return # Skip formatting if df is empty
                col_indices = {cn: i + 1 for i, cn in enumerate(metrics_df.columns)}
                period_col_idx = col_indices.get('Period')

                # Header row formatting
                for cell in worksheet[1]:
                    cell.font = header_font; cell.fill = header_fill; cell.alignment = center_align

                # Data row formatting
                for r_idx in range(2, worksheet.max_row + 1):
                    row_fill = light_grey_fill if r_idx % 2 != 0 else None
                    for c_idx_iter in range(1, worksheet.max_column + 1):
                        cell = worksheet.cell(r_idx, c_idx_iter)
                        if row_fill: cell.fill = row_fill

                    # Format Period column
                    if period_col_idx:
                        period_cell = worksheet.cell(r_idx, period_col_idx)
                        period_cell.font = bold_font; period_cell.alignment = left_align

                    # Format metric value columns
                    for m_name, c_idx in col_indices.items():
                        if c_idx == period_col_idx: continue
                        cell = worksheet.cell(r_idx, c_idx); c_val = cell.value
                        align = right_align; fmt = None
                        is_num = isinstance(c_val, (int, float, np.number))

                        if is_num and pd.notna(c_val):
                            if np.isinf(c_val): cell.value = "+Inf" if c_val > 0 else "-Inf"
                            elif 'Rupees' in m_name: fmt = currency_format_inr
                            elif '%' in m_name or 'Win Rate' in m_name: fmt = percentage_format
                            elif 'Factor' in m_name or 'Ratio' in m_name or 'Expectancy' in m_name: fmt = number_format_2dp
                            elif m_name in ['Trading Days Analyzed','Total Trades Executed','Max Consecutive Wins','Max Consecutive Losses']: fmt = integer_format
                            else: fmt = number_format_2dp
                        elif c_val is None or pd.isna(c_val): cell.value = "N/A"
                        elif isinstance(c_val, str) and c_val.startswith("Error"): cell.fill = red_fill; align = left_align

                        if fmt and is_num and pd.notna(c_val) and not np.isinf(c_val): cell.number_format = fmt
                        if align: cell.alignment = align

                # Auto-adjust column widths for Summary sheet
                for c_idx, m_name in enumerate(metrics_df.columns, 1):
                    col_letter = get_column_letter(c_idx)
                    max_len = len(str(m_name))
                    for r_idx in range(1, worksheet.max_row + 1):
                         cell_val_str = str(worksheet.cell(r_idx, c_idx).value)
                         max_len = max(max_len, len(cell_val_str))
                    adjusted_width = min(max(max_len + 2, 18), 35)
                    worksheet.column_dimensions[col_letter].width = adjusted_width

                freeze_cell = 'B2' if period_col_idx == 1 else 'A2'
                worksheet.freeze_panes = freeze_cell


            # --- Write Summary Sheets (No changes needed here) ---
            if overall_metrics:
                 df_overall = pd.DataFrame([overall_metrics])
                 cols = ['Period'] + [c for c in df_overall.columns if c != 'Period']
                 df_overall = df_overall[cols]
                 df_overall.to_excel(writer, sheet_name='Summary_Overall', index=False)
                 format_summary_sheet(writer.sheets['Summary_Overall'], df_overall)
            if yearly_metrics_dict:
                 df_yearly = pd.DataFrame(list(yearly_metrics_dict.values()))
                 if not df_yearly.empty:
                     df_yearly['Period'] = pd.to_numeric(df_yearly['Period'], errors='coerce')
                     df_yearly = df_yearly.sort_values(by='Period').dropna(subset=['Period'])
                     df_yearly['Period'] = df_yearly['Period'].astype(int)
                     cols = ['Period'] + [c for c in df_yearly.columns if c != 'Period']
                     df_yearly = df_yearly[cols]
                     df_yearly.to_excel(writer, sheet_name='Summary_Yearly', index=False)
                     format_summary_sheet(writer.sheets['Summary_Yearly'], df_yearly)
            if monthly_metrics_dict:
                 df_monthly = pd.DataFrame(list(monthly_metrics_dict.values()))
                 if not df_monthly.empty:
                     try:
                         df_monthly['SortKey'] = pd.to_datetime(df_monthly['Period'], format='%b %Y', errors='coerce')
                         df_monthly['SortKey'] = df_monthly['SortKey'].fillna(pd.Timestamp.max)
                         df_monthly = df_monthly.sort_values(by='SortKey').drop(columns=['SortKey'])
                     except Exception as sort_e:
                         print(f"Warn: Could not sort monthly summary sheet by date: {sort_e}")
                         df_monthly = df_monthly.sort_values(by='Period')
                     cols = ['Period'] + [c for c in df_monthly.columns if c != 'Period']
                     df_monthly = df_monthly[cols]
                     df_monthly.to_excel(writer, sheet_name='Summary_Monthly', index=False)
                     format_summary_sheet(writer.sheets['Summary_Monthly'], df_monthly)

        excel_data = output.getvalue()
        return excel_data

    except Exception as e:
        st.error(f"Error generating Excel file: {e}")
        traceback.print_exc()
        return None
# --- End of MODIFIED save_to_excel_streamlit ---

# ==============================================================
# Functions for Saving/Loading State (Pickle)
# ==============================================================
# --- [Keep the existing save/load pickle functions here] ---
# No changes needed for this feature itself, but will use the new parameter
def get_saved_results_list():
    """Returns a list of saved .pkl result files."""
    try:
        return sorted([f.name for f in SAVED_RESULTS_DIR.glob("*.pkl") if f.is_file()], reverse=True)
    except Exception as e:
        st.error(f"Error listing saved results: {e}")
        return []

def save_results_to_pickle(filename, data_to_save):
    """Saves the results dictionary to a pickle file."""
    try:
        if not filename.endswith(".pkl"):
            filename += ".pkl"
        filepath = SAVED_RESULTS_DIR / filename
        with open(filepath, 'wb') as f:
            pickle.dump(data_to_save, f)
        print(f"Results saved successfully to {filepath}")
        return True
    except Exception as e:
        st.error(f"Error saving results to {filename}: {e}")
        traceback.print_exc()
        return False

def load_results_from_pickle(filename):
    """Loads results dictionary from a pickle file."""
    try:
        filepath = SAVED_RESULTS_DIR / filename
        if not filepath.is_file():
            st.error(f"Saved file not found: {filename}")
            return None
        with open(filepath, 'rb') as f:
            loaded_data = pickle.load(f)
        print(f"Results loaded successfully from {filepath}")
        # Basic validation
        if isinstance(loaded_data, dict) and 'trade_log_df' in loaded_data:
            return loaded_data
        else:
            st.error(f"Loaded data from {filename} is not in the expected format (missing 'trade_log_df').")
            return None
    except Exception as e:
        st.error(f"Error loading results from {filename}: {e}")
        traceback.print_exc()
        return None

# ==============================================================
# UI Helper Functions (display_styled_trade_log, display_period_metrics)
# ==============================================================
# --- MODIFIED display_styled_trade_log to include Trade# ---
def style_trade_log_row(row):
    """Applies background color style based on PnL and Status."""
    color_bg = '' # Default background
    try:
        pnl = row.get('PnL (Rupees)')
        status = str(row.get('Status', '')).lower()

        error_keywords = ['api error', 'data issue', 'no data', 'orb calc error', 'failed', 'invalid', 'unavailable', 'lookup failed']
        if any(keyword in status for keyword in error_keywords):
            color_bg = 'background-color: #E0E0E0' # Light gray for errors/issues
        elif pd.notna(pnl) and isinstance(pnl, (int, float, np.number)):
            if pnl < 0:
                color_bg = f'background-color: {COLOR_LOSS_BG}' # Use lighter defined color
            elif pnl > 0:
                color_bg = f'background-color: {COLOR_PROFIT_BG}' # Use lighter defined color
    except Exception as e:
        print(f"Error applying style to row: {e}")

    return [color_bg] * len(row)

def display_styled_trade_log(df):
    """Displays the trade log dataframe with Profit/Loss/Issue row coloring and Trade#."""
    if df is None or df.empty:
        st.info("Trade log is empty.")
        return

    df_display = df.copy()

    if 'PnL (Rupees)' in df_display.columns:
        df_display['PnL (Rupees)'] = pd.to_numeric(df_display['PnL (Rupees)'], errors='coerce')
    else:
        st.warning("Trade log missing 'PnL (Rupees)' column for styling.")
        df_display['PnL (Rupees)'] = np.nan

    # --- ADDED Trade# to column config ---
    column_config_dict = {
        "Date": st.column_config.DateColumn("Date", format="YYYY-MM-DD", width="small"),
        "Trade#": st.column_config.NumberColumn("Trade #", format="%d", width="small", help="Trade number within the day"), # <<< ADDED
        "Status": st.column_config.TextColumn("Status", width="large", help="Reason for trade exit or status."),
        "ORB High": st.column_config.NumberColumn("ORB High", format="%.2f", width="small"),
        "ORB Low": st.column_config.NumberColumn("ORB Low", format="%.2f", width="small"),
        "Entry Time": st.column_config.TextColumn("Entry Time", width="small"),
        "Entry Spot": st.column_config.NumberColumn("Entry Spot", format="%.2f", width="medium"),
        "Entry Price (Opt)": st.column_config.NumberColumn("Entry Price", format="%.2f", width="small"),
        "Exit Time": st.column_config.TextColumn("Exit Time", width="small"),
        "Exit Spot": st.column_config.NumberColumn("Exit Spot", format="%.2f", width="medium"),
        "Exit Price (Opt)": st.column_config.NumberColumn("Exit Price", format="%.2f", width="small"),
        "PnL (Rupees)": st.column_config.NumberColumn("PnL (₹)", format="₹%(#,##0.00;)₹(%(#,##0.00);₹0.00", width="medium"), # Custom format for neg parens
        "Option Symbol": st.column_config.TextColumn("Symbol", width="large"),
        "Strike": st.column_config.NumberColumn("Strike", format="%.0f", width="small"),
        "Option Type": st.column_config.TextColumn("Type", width="small"),
        "price_source_entry": st.column_config.TextColumn("Entry Src", width="small", help="Source status of entry price fetch"),
        "price_source_exit": st.column_config.TextColumn("Exit Src", width="small", help="Source status of exit price fetch"),
        # Hide intermediate columns if they exist
        "Year": None, "Month": None,
    }
    final_column_config = {k: v for k, v in column_config_dict.items() if k in df_display.columns}

    styled_df = df_display.style.apply(style_trade_log_row, axis=1)

    # --- ADDED Trade# to styler format dict ---
    format_dict_styler = {
        'Trade#': "{:.0f}", # Display as integer (using float format for compatibility)
        'PnL (Rupees)': "₹{:,.2f}",
        'Entry Price (Opt)': "{:.2f}", 'Exit Price (Opt)': "{:.2f}",
        'Entry Spot': "{:.2f}", 'Exit Spot': "{:.2f}",
        'ORB High': "{:.2f}", 'ORB Low': "{:.2f}",
        'Strike': "{:,.0f}"
    }
    valid_formats_styler = {k: v for k, v in format_dict_styler.items() if k in df_display.columns}
    try:
        # Handle potential Int64 NA which styler might not like with .0f format
        if 'Trade#' in valid_formats_styler:
            styled_df = styled_df.format(formatter=valid_formats_styler, na_rep="-", subset=pd.IndexSlice[:, [col for col in valid_formats_styler if col != 'Trade#']])
            styled_df = styled_df.format(formatter="{:.0f}", na_rep="-", subset=pd.IndexSlice[:, ['Trade#']])
        else:
             styled_df = styled_df.format(valid_formats_styler, na_rep="-")
    except Exception as e:
        st.warning(f"Could not apply Styler number formats: {e}")

    st.dataframe(
        styled_df,
        column_config=final_column_config,
        use_container_width=True,
        height=400,
        hide_index=True
    )
# --- End of MODIFIED display_styled_trade_log ---

# --- display_period_metrics (No changes needed) ---
def format_detail_value(metric_name, v):
                             if isinstance(v, (int, float, np.number)):
                                 if pd.isna(v): return "N/A"
                                 if np.isinf(v): return "+Inf" if v > 0 else "-Inf"
                                 # Apply specific formats based on metric name
                                 if 'Rupees' in metric_name: return f"₹{v:,.2f}"
                                 if '%' in metric_name or 'Win Rate' in metric_name: return f"{v:.2f}%"
                                 # Format integers nicely
                                 int_metrics = ['Total Trades Executed', 'Max Consecutive Wins', 'Max Consecutive Losses', 'Trading Days Analyzed']
                                 if metric_name in int_metrics or (isinstance(v, float) and v == int(v) and v < 1e6): return f"{int(v):,}" # Comma separated int
                                 # Default numeric format (usually 2dp)
                                 if abs(v) >= 1000: return f"{v:,.2f}" # Comma sep float
                                 if '.' in str(v): return f"{v:.2f}"
                                 return f"{v:,.0f}" # Fallback for large whole numbers
                             if v is None or pd.isna(v): return "N/A"
                             return str(v) # Return other types as string
def display_period_metrics(metrics_dict, period_type="Yearly"):
    """Displays Yearly or Monthly metrics with corrected colors and layout."""
    if not metrics_dict:
        st.info(f"No {period_type.lower()} metrics available.")
        return

    # --- Sorting Logic (Robust for Yearly and Monthly) ---
    sorted_keys = []
    if period_type == "Yearly":
        try:
            def year_sort_key(k):
                label = metrics_dict[k].get('Period', str(k))
                try: return int(label)
                except: return float('inf')
            sorted_keys = sorted(metrics_dict.keys(), key=year_sort_key)
        except Exception as e:
            print(f"Warn: Sort yearly keys failed: {e}")
            sorted_keys = sorted(metrics_dict.keys())

    elif period_type == "Monthly":
        try:
            def month_sort_key(k):
                label = metrics_dict[k].get('Period', str(k))
                try: return pd.to_datetime(label, format='%b %Y', errors='coerce')
                except: return pd.Timestamp.max
            sortable_keys = {k: month_sort_key(k) for k in metrics_dict.keys()}
            sorted_keys = sorted(metrics_dict.keys(), key=lambda k: sortable_keys.get(k, pd.Timestamp.max))
        except Exception as e:
            print(f"Error sorting monthly keys: {e}")
            sorted_keys = sorted(metrics_dict.keys())
    else:
        sorted_keys = sorted(metrics_dict.keys())

    if not sorted_keys:
        st.info(f"No valid {period_type.lower()} periods found after sorting.")
        return

    # --- Display Metrics in Expanders ---
    for key in sorted_keys:
        metrics = metrics_dict.get(key)
        if not metrics or not isinstance(metrics, dict): continue

        period_label = metrics.get('Period', str(key))
        pnl = metrics.get('Total PnL (Rupees)')
        win_rate = metrics.get('Win Rate')
        profit_factor = metrics.get('Profit Factor')
        trades = metrics.get('Total Trades Executed')

        pnl_display = "N/A"; pnl_delta_color = "off"; delta_val = None
        if pd.notna(pnl) and isinstance(pnl, (int, float)):
             pnl_display = f"₹{pnl:,.2f}"
             pnl_delta_color = "normal" if pnl >= 0 else "inverse"
             delta_val = f"{pnl:,.2f}"

        trades_display = f"{int(trades):,}" if pd.notna(trades) and isinstance(trades, (int, float)) else "N/A"
        win_rate_display = f"{win_rate:.2f}%" if pd.notna(win_rate) and isinstance(win_rate, (int, float)) else "N/A"

        pf_display = "N/A"
        if pd.notna(profit_factor):
            if np.isinf(profit_factor): pf_display = "∞"
            elif isinstance(profit_factor, (int, float)): pf_display = f"{profit_factor:.2f}"
            else: pf_display = "0.00"

        expander_title = f"{period_label}"
        with st.expander(expander_title):
             col1, col2, col3, col4 = st.columns(4)
             col1.metric("💰 Total PnL", pnl_display, delta=delta_val, delta_color=pnl_delta_color)
             col2.metric("📊 Trades", trades_display)
             col3.metric("🎯 Win Rate", win_rate_display)
             col4.metric("📈 Profit Factor", pf_display)

             details_key = f"details_checkbox_{period_type}_{period_label.replace(' ', '_').replace('.', '_')}"
             show_details = st.checkbox("🔍 Show All Metrics", key=details_key, value=False)

             if show_details:
                 try:
                     if isinstance(metrics, dict):
                          metrics_series = pd.Series(metrics)
                          metrics_df_t = metrics_series.reset_index()
                          metrics_df_t.columns = ['Metric', 'Value']
                          metrics_df_t['Formatted Value'] = metrics_df_t.apply(lambda row: format_detail_value(row['Metric'], row['Value']), axis=1)
                          st.dataframe(metrics_df_t[['Metric', 'Formatted Value']], use_container_width=True, hide_index=True)
                     else:
                          st.warning("Metrics data for this period is not in the expected dictionary format.")
                          st.write(metrics)
                 except Exception as detail_e:
                     st.error(f"Failed to display detailed metrics: {detail_e}")
                     st.write(metrics)


# ==============================================================
# Streamlit UI Code (Main Application Flow)
# ==============================================================
# --- Define Callback Function for Running Backtest ---
# --- MODIFIED handle_run_backtest_click ---
def handle_run_backtest_click():
    # print("--- handle_run_backtest_click CALLED ---") # Keep logs minimal

    breeze_local = st.session_state.get('breeze', None)
    if not breeze_local:
        st.error("Run Error: Breeze connection invalid state in callback.")
        print(colored("!!! Callback error: 'breeze' not found in session state!", "red"))
        return

    # print(f"--- Callback using breeze object: {type(breeze_local)} ---")
    # print("   - Callback: Getting current run parameters...")
    current_run_params = {
        "days": st.session_state.param_days,
        "orb": st.session_state.param_orb,
        "strategy": st.session_state.param_strategy,
        "max_trades": st.session_state.param_max_trades, # <<< ADDED
        "tp": st.session_state.param_tp,
        "sl": st.session_state.param_sl
    }
    st.session_state.current_params = current_run_params
    # print(f"   - Callback: Parameters set: {current_run_params}")

    # print("   - Callback: Resetting previous results...")
    st.session_state.trade_log_df = pd.DataFrame()
    st.session_state.overall_metrics = None
    st.session_state.yearly_metrics_dict = {}
    st.session_state.monthly_metrics_dict = {}
    st.session_state.overall_equity = None
    st.session_state.daily_pnl_df = pd.DataFrame()
    st.session_state.run_completed = False
    # print("   - Callback: State reset complete.")

    st.session_state.trigger_backtest_run = True
    # print("   - Callback: Set trigger_backtest_run = True")
# --- End of MODIFIED handle_run_backtest_click ---


st.set_page_config(layout="wide", page_title="ORB Backtester (Multi-Trade)", initial_sidebar_state="expanded")
st.title("📈 Nifty ORB Strategy Backtester (Multi-Trade)")

# Initialize Breeze Connection
# --- Simplified Connection Check (No Cache Assumed) ---
# print("--- Running Simplified Connection Check ---")
breeze_connection_object = initialize_breeze()

if breeze_connection_object:
    # print("   - [Simple Check] Connection object IS valid. Storing in session state.")
    st.session_state.breeze = breeze_connection_object
    breeze = st.session_state.breeze

    if not st.session_state.get('connection_status_shown', False):
        st.success("✅ Breeze connection active.")
        st.session_state.connection_status_shown = True
    # print("   - [Simple Check] Script proceeds.")

else:
    # print(colored("   - [Simple Check] Connection object IS None. Connection FAILED.", "red"))
    if 'breeze' in st.session_state: del st.session_state.breeze
    if 'connection_status_shown' in st.session_state: del st.session_state.connection_status_shown
    st.error("⚠️ Breeze connection failed or not initialized. Check terminal logs, credentials/token in `kitecred.env`, and restart.")
    print(colored("!!! Script stopped due to failed connection.", "red"))
    st.stop()

# print(f"--- Script proceeding past connection check with local 'breeze' variable type: {type(breeze)} ---")
# --- End of Connection Check Block ---

# --- Sidebar ---
with st.sidebar:
    st.header("⚙️ Parameters")

    def format_strategy(x):
        if x == 1: return "Strat 1: TP/SL Spot Pts"
        elif x == 2: return "Strat 2: TP Spot Pts / Opp ORB SL"
        elif x == 3: return "Strat 3: TP/SL Option Pts"
        else: return f"Strategy {x}"

    strategy_choice = st.selectbox(
        "Strategy", options=[1, 2, 3], index=1, key="param_strategy", format_func=format_strategy,
        help="Strategy Logic:\n- Strat 1: TP/SL based on SPOT price move.\n- Strat 2: TP=SPOT move; SL=SPOT hits opposite ORB.\n- Strat 3: TP/SL based on OPTION price move."
    )
    backtest_days = st.number_input("Trading Days Back", min_value=1, max_value=1000, value=st.session_state.param_days, step=1, key="param_days", help="Number of past trading days with data.") # Use session state for default
    orb_duration = st.selectbox("ORB Duration (Mins)", options=[15, 30, 60], index=1, key="param_orb", help="Opening Range duration.")
    # --- ADDED Max Trades Input ---
    max_trades = st.number_input("Max Trades Per Day", min_value=1, max_value=10, value=st.session_state.param_max_trades, step=1, key="param_max_trades", help="Maximum number of entries allowed per day.")

    tp_points = st.number_input(
        "Take Profit Pts", min_value=1.0, value=st.session_state.param_tp, step=1.0, key="param_tp", # Use session state for default
        help="Points for TP (SPOT for Strat 1/2, OPTION for Strat 3)."
    )
    sl_points = st.number_input(
        "Stop Loss Pts", min_value=1.0, value=st.session_state.param_sl, step=1.0, key="param_sl", # Use session state for default
        help="Points for SL (SPOT for Strat 1, OPTION for Strat 3. Not used in Strat 2)."
    )

    run_button = st.button("🚀 Run Backtest",
          type="primary",
          key="run_button_main_key",
          use_container_width=True,
          on_click=handle_run_backtest_click
          )
    st.divider()

    # --- Load / Save Results ---
    st.header("💾 Results")
    saved_files = get_saved_results_list()
    selected_file_to_load = st.selectbox("Load Saved Result:", options=[""] + saved_files, index=0, key="load_select", label_visibility="collapsed")

    if st.button("🔄 Load Selected", use_container_width=True, disabled=(selected_file_to_load == "")):
        if selected_file_to_load:
            loaded_data = load_results_from_pickle(selected_file_to_load)
            if loaded_data and isinstance(loaded_data, dict):
                # Load data into session state
                st.session_state.trade_log_df = loaded_data.get('trade_log_df', pd.DataFrame())
                st.session_state.overall_metrics = loaded_data.get('overall_metrics', None)
                st.session_state.yearly_metrics_dict = loaded_data.get('yearly_metrics_dict', {})
                st.session_state.monthly_metrics_dict = loaded_data.get('monthly_metrics_dict', {})
                st.session_state.overall_equity = loaded_data.get('overall_equity', None)
                st.session_state.daily_pnl_df = loaded_data.get('daily_pnl_df', pd.DataFrame())
                st.session_state.current_params = loaded_data.get('parameters', {})
                st.session_state.run_completed = True
                st.session_state.graph_granularity = loaded_data.get('graph_granularity', 'Daily')
                st.success(f"Loaded: {selected_file_to_load}")

                # Update sidebar widgets to reflect loaded parameters
                loaded_params = st.session_state.current_params
                if loaded_params:
                    st.session_state['param_strategy'] = loaded_params.get('strategy', strategy_choice)
                    st.session_state['param_days'] = loaded_params.get('days', backtest_days)
                    st.session_state['param_orb'] = loaded_params.get('orb', orb_duration)
                    st.session_state['param_max_trades'] = loaded_params.get('max_trades', max_trades) # <<< ADDED
                    st.session_state['param_tp'] = loaded_params.get('tp', tp_points)
                    st.session_state['param_sl'] = loaded_params.get('sl', sl_points)
                st.rerun()

    st.divider()
    st.subheader("ℹ️ Caching")
    st.caption("API calls for historical data are cached using `st.cache_data` to speed up repeated runs with the same date ranges and instruments.")
    if st.button("Clear Data Cache", use_container_width=True):
        st.cache_data.clear()
        st.success("Data cache cleared.")
        st.rerun()


# --- Execute Backtest Run IF Triggered by Callback ---
if st.session_state.get("trigger_backtest_run", False):
    # print("--- trigger_backtest_run is True: Executing backtest sequence ---")
    st.session_state.trigger_backtest_run = False
    # print("   - Reset trigger_backtest_run to False")

    st.header("⏳ Backtest Execution")
    if 'current_params' in st.session_state:
         st.json(st.session_state.current_params)
    else:
         st.warning("Parameters not found in session state for display.")

    with st.spinner(f"🏃 Running backtest for Strategy {st.session_state.param_strategy}..."):
        run_successful = False
        try:
            run_start = time.time(); # print("   - Calling run_backtest function...")
            breeze_local_run = st.session_state.get('breeze')
            params_local_run = st.session_state.get('current_params')
            if not breeze_local_run or not params_local_run:
                 st.error("Cannot run backtest: Connection or parameters missing.")
                 print(colored("!!! ERROR: Breeze or Params missing before run_backtest call!", "red"))
            else:
                # --- MODIFIED call to run_backtest ---
                trade_log_df_result = run_backtest(
                    breeze_local_run,
                    params_local_run["days"], params_local_run["orb"], params_local_run["strategy"],
                    params_local_run["max_trades"], # <<< ADDED
                    params_local_run["tp"], params_local_run["sl"]
                )
                # --- End of MODIFIED call ---

                st.session_state.trade_log_df = trade_log_df_result
                run_end = time.time(); # print(f"   - run_backtest finished in {run_end - run_start:.2f} sec.")
                st.caption(f"Backtest calculation took {run_end - run_start:.2f} sec.")

                # --- Metrics Calculation ---
                if st.session_state.trade_log_df.empty:
                    st.warning("Backtest completed, but no trades generated or logged.")
                    st.session_state.run_completed = True
                else:
                    st.info("⚙️ Calculating metrics...")
                    analysis_start = time.time()
                    trade_log_df_run = st.session_state.trade_log_df.copy()
                    if 'Date' in trade_log_df_run.columns and not pd.api.types.is_datetime64_any_dtype(trade_log_df_run['Date']):
                        trade_log_df_run['Date'] = pd.to_datetime(trade_log_df_run['Date'], errors='coerce')

                    # Call metrics calculation (ensure it uses the latest trade_log_df)
                    overall_metrics_run, overall_equity_run = calculate_metrics(st.session_state.trade_log_df, period_label="Overall")
                    st.session_state.overall_metrics = overall_metrics_run; st.session_state.overall_equity = overall_equity_run

                    yearly_metrics_dict_run = {}; monthly_metrics_dict_run = {}; daily_pnl_df_run = pd.DataFrame()
                    analysis_df = st.session_state.trade_log_df.dropna(subset=['Date']).copy() # Use current session state df
                    if 'Date' in analysis_df.columns and not pd.api.types.is_datetime64_any_dtype(analysis_df['Date']):
                         analysis_df['Date'] = pd.to_datetime(analysis_df['Date'], errors='coerce') # Ensure datetime for grouping

                    if not analysis_df.empty and 'Date' in analysis_df.columns:
                        # Filter for trades with PnL for daily sum
                        non_trade_statuses_metrics = "No Breakout|API Error|No .* Data|ORB Calc Error|Invalid Range|No Candles|Data Issue|No Tradeable Breakout|Entry Price Invalid|Exit Price Unavailable|Forced EOD Exit"
                        trades_only_df = analysis_df[~analysis_df['Status'].astype(str).str.contains(non_trade_statuses_metrics, na=False, case=False, regex=True) & analysis_df['PnL (Rupees)'].notna()].copy()
                        if not trades_only_df.empty and 'Date' in trades_only_df.columns:
                            daily_pnl_sum = trades_only_df.groupby(trades_only_df['Date'].dt.date)['PnL (Rupees)'].sum()
                            if not daily_pnl_sum.empty: daily_pnl_df_run = pd.DataFrame({'Daily PnL': daily_pnl_sum}); daily_pnl_df_run.index = pd.to_datetime(daily_pnl_df_run.index); daily_pnl_df_run.index.name = 'Date'; st.session_state.daily_pnl_df = daily_pnl_df_run

                        # Periodic Metrics (using all rows with valid dates)
                        analysis_df = analysis_df.dropna(subset=['Date']) # Ensure no NaT dates before grouping
                        if not analysis_df.empty:
                            analysis_df['Year'] = analysis_df['Date'].dt.year; analysis_df['Month'] = analysis_df['Date'].dt.month
                            all_years = sorted(analysis_df['Year'].dropna().unique().astype(int))
                            for year in all_years: yearly_metrics_dict_run[year], _ = calculate_metrics(analysis_df[analysis_df['Year'] == year].copy(), period_label=f"{year}")
                            monthly_groups = analysis_df.groupby(['Year', 'Month']); sorted_months = sorted([key for key in monthly_groups.groups.keys() if not any(pd.isna(k) for k in key)], key=lambda k: (k[0], k[1]))
                            for year, month in sorted_months: month_name = datetime.date(int(year), int(month), 1).strftime('%b'); month_year_label = f"{month_name} {int(year)}"; monthly_metrics_dict_run[(year, month)], _ = calculate_metrics(monthly_groups.get_group((year, month)).copy(), period_label=month_year_label)
                            st.session_state.yearly_metrics_dict = yearly_metrics_dict_run; st.session_state.monthly_metrics_dict = monthly_metrics_dict_run;
                            run_successful = True
                        else: st.warning("No valid date data for periodic metrics after filtering.")
                    else: st.warning("No valid date data for periodic metrics.")
                    analysis_end = time.time(); st.caption(f"Metrics calculation took {analysis_end - analysis_start:.2f} sec.")
                    if run_successful: st.success("✅ Analysis Complete!")
                    else: st.warning("Analysis issues encountered.")
                    st.session_state.run_completed = True

        except Exception as e:
            st.error(f"Backtest run failed: {e}"); st.exception(e)
            st.session_state.run_completed = False


# --- Display Section ---
if st.session_state.run_completed:
    st.header("📊 Backtest Results")

    # --- MODIFIED Parameter Display ---
    params = st.session_state.current_params
    if params and isinstance(params, dict):
        strategy_num = params.get('strategy', 'N/A')
        strat_desc = format_strategy(strategy_num)

        param_items = [
            strat_desc,
            f"ORB: {params.get('orb','N/A')}m",
            f"Days: {params.get('days','N/A')}",
            f"Max Trades: {params.get('max_trades','N/A')}", # <<< ADDED
            f"TP: {params.get('tp','N/A')} pts"
        ]
        if strategy_num in [1, 3] and 'sl' in params and params.get('sl') is not None:
             param_items.append(f"SL: {params.get('sl','N/A')} pts")
        elif strategy_num == 2:
            param_items.append("SL: Opp ORB")

        param_str = " | ".join(param_items)
        st.caption(f"Showing results for: {param_str}")
    else:
        st.caption("Showing previously loaded results (parameters might be incomplete).")
    # --- End of MODIFIED Parameter Display ---

    # --- Display Overall Metrics (No change needed here) ---
    st.subheader("💲 Performance Metrics")
    if st.session_state.overall_metrics:
        om = st.session_state.overall_metrics
        if isinstance(om, dict):
            st.markdown("###### Overall Performance")
            col1, col2, col3, col4 = st.columns(4)

            pnl_value = om.get('Total PnL (Rupees)')
            pf_value = om.get('Profit Factor')
            win_rate_value = om.get('Win Rate')
            trades_value = om.get('Total Trades Executed')

            pnl_display = f"₹{pnl_value:,.2f}" if pd.notna(pnl_value) and isinstance(pnl_value, (int,float)) else "N/A"
            pnl_delta_color = "normal" if pd.notna(pnl_value) and pnl_value >= 0 else "inverse"
            delta_val = f"{pnl_value:,.2f}" if pd.notna(pnl_value) and isinstance(pnl_value, (int,float)) else None

            trades_display = f"{int(trades_value):,}" if pd.notna(trades_value) and isinstance(trades_value, (int,float)) else "N/A"
            win_rate_display = f"{win_rate_value:.2f}%" if pd.notna(win_rate_value) and isinstance(win_rate_value, (int,float)) else "N/A"

            pf_display = "N/A"
            if pd.notna(pf_value):
                if np.isinf(pf_value): pf_display = "∞"
                elif isinstance(pf_value, (int, float)): pf_display = f"{pf_value:.2f}"
                else: pf_display = "0.00"

            col1.metric("💰 Total PnL", pnl_display, delta=delta_val, delta_color=pnl_delta_color)
            col2.metric("📊 Total Trades", trades_display)
            col3.metric("🎯 Win Rate", win_rate_display)
            col4.metric("📈 Profit Factor", pf_display)

            with st.expander("🔍 Details: All Overall Metrics"):
               try:
                   if isinstance(om, dict):
                       metrics_series = pd.Series(om)
                       metrics_df_t = metrics_series.reset_index(); metrics_df_t.columns = ['Metric', 'Value']
                       metrics_df_t['Formatted Value'] = metrics_df_t.apply(lambda row: format_detail_value(row['Metric'], row['Value']), axis=1)
                       st.dataframe(metrics_df_t[['Metric', 'Formatted Value']], use_container_width=True, hide_index=True)
                   else: st.warning("Overall metrics data is not in the expected dictionary format."); st.write(om)
               except Exception as e: st.error(f"Could not display overall metrics table: {e}"); st.write(om)
        else:
            st.warning("Overall Metrics data is not in the expected format (should be a dictionary).")
    else:
        st.warning("Overall Metrics not available or calculation failed.")


    # --- Display Periodic Metrics (No change needed here) ---
    st.divider()
    st.markdown("##### Periodic Performance")
    tab_yearly, tab_monthly = st.tabs(["📅 Yearly Summary", "🗓️ Monthly Summary"])
    with tab_yearly:
        display_period_metrics(st.session_state.yearly_metrics_dict, "Yearly")
    with tab_monthly:
        display_period_metrics(st.session_state.monthly_metrics_dict, "Monthly")


    # --- Display Visualizations (No change needed here) ---
    st.divider()
    st.subheader("📉 Visualizations")

    granularity = st.radio(
        "Select Graph Granularity:", options=["Daily", "Monthly", "Yearly"],
        key="graph_granularity", horizontal=True
    )

    min_date_overall, max_date_overall = None, None
    all_dates = []
    if st.session_state.overall_equity is not None and not st.session_state.overall_equity.empty:
        equity_dates = pd.to_datetime(st.session_state.overall_equity.index, errors='coerce').dropna()
        if not equity_dates.empty: all_dates.extend(equity_dates)
    if st.session_state.daily_pnl_df is not None and not st.session_state.daily_pnl_df.empty:
        pnl_dates = pd.to_datetime(st.session_state.daily_pnl_df.index, errors='coerce').dropna()
        if not pnl_dates.empty: all_dates.extend(pnl_dates)

    if all_dates:
        min_date_overall = min(all_dates).date(); max_date_overall = max(all_dates).date()
    else:
        param_days_disp = 30; params_disp = st.session_state.get('current_params', {})
        if params_disp and isinstance(params_disp, dict): param_days_disp = params_disp.get('days', 30)
        fallback_end_date = datetime.date.today()
        fallback_start_date = fallback_end_date - datetime.timedelta(days=param_days_disp * 1.5)
        min_date_overall = fallback_start_date; max_date_overall = fallback_end_date
    if min_date_overall > max_date_overall: min_date_overall = max_date_overall

    st.markdown("Filter Graph Data Range:")
    col_f1, col_f2 = st.columns([1, 1])
    with col_f1:
        start_date_filter = st.date_input("Start Date", value=min_date_overall, min_value=min_date_overall, max_value=max_date_overall, key="start_date")
    with col_f2:
        end_date_filter = st.date_input("End Date", value=max_date_overall, min_value=min_date_overall, max_value=max_date_overall, key="end_date")

    filtered_equity = pd.DataFrame()
    filtered_periodic_pnl = pd.DataFrame()

    if start_date_filter > end_date_filter:
        st.warning("Start Date cannot be after End Date.")
    else:
        resample_map = {'Daily': 'D', 'Monthly': 'MS', 'Yearly': 'AS'}
        resample_freq = resample_map.get(granularity, 'D')

        # Resample Equity Curve
        if st.session_state.overall_equity is not None and not st.session_state.overall_equity.empty:
            try:
                 equity_to_resample = st.session_state.overall_equity.copy()
                 if not pd.api.types.is_datetime64_any_dtype(equity_to_resample.index):
                     equity_to_resample.index = pd.to_datetime(equity_to_resample.index, errors='coerce')
                 equity_to_resample = equity_to_resample.dropna(axis=0, how='all')

                 if not equity_to_resample.empty:
                     resampled_equity = equity_to_resample.resample(resample_freq).last().ffill()
                     resampled_equity.index.name = 'Date'
                     resampled_equity.index = pd.to_datetime(resampled_equity.index)
                     mask = (resampled_equity.index.date >= start_date_filter) & (resampled_equity.index.date <= end_date_filter)
                     filtered_equity = resampled_equity[mask]
            except Exception as e: st.error(f"Equity resampling/filtering error ({granularity}): {e}")

        # Resample Periodic PnL
        pnl_col_name = f"{granularity} PnL"
        if st.session_state.daily_pnl_df is not None and not st.session_state.daily_pnl_df.empty:
            try:
                 pnl_to_resample = st.session_state.daily_pnl_df.copy()
                 if not pd.api.types.is_datetime64_any_dtype(pnl_to_resample.index):
                     pnl_to_resample.index = pd.to_datetime(pnl_to_resample.index, errors='coerce')
                 pnl_to_resample = pnl_to_resample.dropna(axis=0, how='all')

                 if not pnl_to_resample.empty:
                     resampled_periodic_pnl = pnl_to_resample.resample(resample_freq).sum()
                     resampled_periodic_pnl.rename(columns={'Daily PnL': pnl_col_name}, inplace=True)
                     resampled_periodic_pnl.index.name = 'Date'
                     resampled_periodic_pnl.index = pd.to_datetime(resampled_periodic_pnl.index)
                     mask = (resampled_periodic_pnl.index.date >= start_date_filter) & (resampled_periodic_pnl.index.date <= end_date_filter)
                     filtered_periodic_pnl = resampled_periodic_pnl[mask]
            except Exception as e: st.error(f"PnL resampling/filtering error ({granularity}): {e}")

        # --- Display Plots ---
        col_g1, col_g2 = st.columns(2)
        with col_g1: # Equity Curve Plot
            if PLOTLY_AVAILABLE:
                equity_fig = plot_equity_curve_plotly(filtered_equity, granularity)
                if equity_fig: st.plotly_chart(equity_fig, use_container_width=True)
                elif not filtered_equity.empty: st.warning("Could not generate equity plot for the selected range/granularity.")
                else: st.caption(f"No equity data available for the selected range/granularity ({granularity}).")
            else: st.warning("Plotly library not installed. Cannot display equity curve.")

        with col_g2: # Periodic PnL Plot
            if PLOTLY_AVAILABLE:
                periodic_pnl_fig = plot_periodic_pnl_plotly(filtered_periodic_pnl, granularity)
                if periodic_pnl_fig: st.plotly_chart(periodic_pnl_fig, use_container_width=True)
                elif not filtered_periodic_pnl.empty: st.warning(f"Could not generate {granularity.lower()} PNL plot for the selected range/granularity.")
                else: st.caption(f"No {granularity.lower()} PNL data available for the selected range/granularity.")
            else: st.warning("Plotly library not installed. Cannot display PNL plot.")


    # --- Display Trade Log (Uses updated function) ---
    st.divider()
    st.subheader("📜 Trade Log")
    display_styled_trade_log(st.session_state.trade_log_df)


    # --- Save / Download Section ---
    st.divider()
    col_s1, col_s2 = st.columns([2,1])

    with col_s1:
        st.subheader("💾 Save State / Download Report")
        # --- MODIFIED Filename Generation ---
        timestamp_save = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        params_save = st.session_state.current_params
        strat_num = params_save.get('strategy','X')
        orb_m = params_save.get('orb','X')
        days_num = params_save.get('days','X')
        max_trd = params_save.get('max_trades','X') # <<< ADDED
        save_filename_default = f"orb_Strat{strat_num}_{orb_m}m_{days_num}d_Max{max_trd}t_{timestamp_save}" # <<< MODIFIED NAME

        save_filename_pkl = st.text_input("Filename to Save State As (.pkl):", f"{save_filename_default}.pkl")
        if st.button("💾 Save Current State", use_container_width=True, disabled=(not save_filename_pkl), help="Save results & parameters to a .pkl file to reload later."):
             # --- MODIFIED Save Data ---
             data_to_save = {
                 'parameters': st.session_state.current_params, # Includes max_trades now
                 'trade_log_df': st.session_state.trade_log_df,
                 'overall_metrics': st.session_state.overall_metrics,
                 'yearly_metrics_dict': st.session_state.yearly_metrics_dict,
                 'monthly_metrics_dict': st.session_state.monthly_metrics_dict,
                 'overall_equity': st.session_state.overall_equity,
                 'daily_pnl_df': st.session_state.daily_pnl_df,
                 'graph_granularity': st.session_state.graph_granularity
             }
             if save_results_to_pickle(save_filename_pkl, data_to_save):
                 st.success(f"State saved successfully: {save_filename_pkl}")
                 st.rerun()

    with col_s2:
        st.write(" ")
        excel_filename_dl = st.text_input("Filename for Excel Report (.xlsx):", f"{save_filename_default}.xlsx", label_visibility="collapsed")

        if OPENPYXL_AVAILABLE:
            excel_gen_placeholder = st.empty(); excel_gen_placeholder.info("Generate Excel report...")
            excel_start = time.time()
            # --- Call MODIFIED Excel Function ---
            excel_bytes = save_to_excel_streamlit(
                st.session_state.trade_log_df, # Includes Trade# column
                st.session_state.overall_metrics,
                st.session_state.yearly_metrics_dict,
                st.session_state.monthly_metrics_dict
            )
            excel_end = time.time(); st.caption(f"Excel generation took: {excel_end - excel_start:.2f}s")

            if excel_bytes:
                excel_gen_placeholder.download_button(
                    label="📥 Download Excel Report", data=excel_bytes,
                    file_name=excel_filename_dl, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True, disabled=(not excel_filename_dl)
                )
            else: excel_gen_placeholder.warning("Excel generation failed. Check logs.")
        else: excel_gen_placeholder.warning("Install `openpyxl` library to enable Excel download.")


# --- Show initial message ---
if not st.session_state.run_completed:
    st.info("👈 Enter parameters and run backtest, or load previous results from the sidebar.")
