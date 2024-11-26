from config import *
from data_loaders.fmp_data_loader import FmpDataLoader, Period
from data_loaders.market_symbol_loader import MarketSymbolLoader
from utils.log_utils import *
from utils.file_utils import *
from datetime import datetime
import pandas as pd
import os


class EstimateTracker:
    def __init__(self, fmp_api_key):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.market_symbol_loader = MarketSymbolLoader()

    def load_tracking_file(self, file_name):
        # Load quarterly or annual tracking file
        path = os.path.join(CACHE_DIR, file_name)
        if os.path.exists(path):
            tracking_df = pd.read_csv(path)
        else:
            tracking_df = pd.DataFrame(columns=[
                'date', 'tracking_date', 'estimatedEpsAvg', 'estimatedEpsHigh',
                'estimatedEpsLow', 'numberAnalystsEstimatedEps', 'symbol'
            ])
        return tracking_df

    def track_estimates(self):
        logi(f"Tracking estimates...")
        # Get list of symbols
        symbols_df = self.market_symbol_loader.fetch_sp500_symbols(cache_file=True)
        symbol_list = symbols_df['symbol'].unique()
        #symbol_list = symbol_list[:5]

        # Load existing tracking file
        estimate_tracking_df = load_csv(CACHE_DIR, ESTIMATE_TRACKING_FILE_NAME)
        if estimate_tracking_df is None:
            estimate_tracking_df = pd.DataFrame(columns=[
                'date', 'tracking_date', 'estimatedEpsAvg', 'estimatedEpsHigh',
                'estimatedEpsLow', 'numberAnalystsEstimatedEps', 'symbol'
            ])
        # Convert dates
        estimate_tracking_df['date'] = pd.to_datetime(estimate_tracking_df['date'], errors="coerce")
        estimate_tracking_df['tracking_date'] = pd.to_datetime(estimate_tracking_df['tracking_date'], errors="coerce")

        # Fetch new estimates
        for symbol in symbol_list:
            # Fetch new estimates
            new_estimates_df = self.fmp_data_loader.fetch_analyst_estimates(symbol, Period.ANNUAL, limit=100)
            if new_estimates_df is None or len(new_estimates_df) == 0:
                continue
            new_estimates_df = new_estimates_df[['symbol', 'date', 'estimatedEpsAvg', 'estimatedEpsHigh', 'estimatedEpsLow',
                                                 'numberAnalystsEstimatedEps']]

            # Filter out records from past years
            new_estimates_df['date'] = pd.to_datetime(new_estimates_df['date'], errors="coerce")

            # Filter out records from past years
            new_estimates_df = new_estimates_df[new_estimates_df['date'].dt.year >= datetime.today().year]

            # Add tracking date
            new_estimates_df['tracking_date'] = datetime.today()

            # Add to existing estimates
            estimate_tracking_df = pd.concat([estimate_tracking_df, new_estimates_df], axis=0, ignore_index=True)

        # Store records
        path = os.path.join(CACHE_DIR, ESTIMATE_TRACKING_FILE_NAME)
        estimate_tracking_df.to_csv(path, index=False)
