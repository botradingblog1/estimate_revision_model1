from config import *
import pandas as pd
from datetime import datetime, timedelta
from data_loaders.fmp_data_loader import FmpDataLoader
from utils.log_utils import *


class EarningsSurpriseLoader:
    def __init__(self, fmp_api_key):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)

    def find_earnings_surprises(self, symbol: str):
        #logd("Loading earnings surprises...")
        try:
            # Fetch earnings surprise data
            earnings_surprise_df = self.fmp_data_loader.fetch_earnings_surprises(symbol)
            if earnings_surprise_df is None or earnings_surprise_df.empty:
                return {
                'symbol': symbol,
                'avg_earnings_surprise': 0.0
            }

            # Filter records of the last quarter (3 months)
            start_date = datetime.now() - timedelta(days=90)
            earnings_surprise_df = earnings_surprise_df[earnings_surprise_df['date'] >= start_date]
            if earnings_surprise_df is None or earnings_surprise_df.empty:
                return {
                'symbol': symbol,
                'avg_earnings_surprise': 0.0
            }

            # Calculate the difference between actual and estimated earnings
            earnings_surprise_df['earningsDifferencePercent'] = ((earnings_surprise_df['actualEarningResult'] -
                                                                  earnings_surprise_df['estimatedEarning']) /
                                                                  earnings_surprise_df['estimatedEarning']) * 100

            # Calculate the average over the past three months
            avg_earnings_surprise = earnings_surprise_df['earningsDifferencePercent'].mean()

            # Append the result
            result = {
                'symbol': symbol,
                'avg_earnings_surprise': avg_earnings_surprise
            }
            return result
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
            return {
                'symbol': symbol,
                'avg_earnings_surprise': 0.0
            }



