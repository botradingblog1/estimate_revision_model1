from config import *
import pandas as pd
from datetime import datetime, timedelta
from data_loaders.fmp_data_loader import FmpDataLoader
from utils.log_utils import *
import os


class EarningsSurpriseAnalyzer:
    def __init__(self, fmp_api_key):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)

    def find_earnings_surprises(self, symbol_list: list):
        logi("Finding earnings surprises...")
        earnings_surprise_results = []
        for symbol in symbol_list:
            try:
                # Fetch earnings surprise data
                earnings_surprise_df = self.fmp_data_loader.fetch_earnings_surprises(symbol)
                if earnings_surprise_df is None or earnings_surprise_df.empty:
                    continue

                # Filter records of the last quarter (3 months)
                start_date = datetime.now() - timedelta(days=90)
                earnings_surprise_df['date'] = pd.to_datetime(earnings_surprise_df['date'])
                earnings_surprise_df = earnings_surprise_df[earnings_surprise_df['date'] >= start_date]

                if not earnings_surprise_df.empty:
                    # Get the last record
                    last_record = earnings_surprise_df.iloc[-1]

                    # Check for missing data
                    if pd.isna(last_record['estimatedEarning']) or pd.isna(last_record['actualEarningResult']):
                        continue

                    # Calculate earnings surprise change
                    earnings_surprise_change = ((last_record['actualEarningResult'] - last_record['estimatedEarning']) /
                                                last_record['estimatedEarning'])

                    # Append the result
                    result_record = {
                        'symbol': symbol,
                        'earnings_surprise_change': earnings_surprise_change
                    }
                    earnings_surprise_results.append(result_record)
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
                continue

        # Convert results to DataFrame and save to CSV
        if earnings_surprise_results:
            earnings_surprise_results_df = pd.DataFrame(earnings_surprise_results)
            earnings_surprise_results_df.to_csv(os.path.join(CACHE_DIR, "earnings_surprise_results_df.csv"), index=False)
            return earnings_surprise_results_df
        else:
            print("No earnings surprise data found for the given symbols.")
            return None


