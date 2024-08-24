from config import *
from data_loaders.fmp_data_loader import FmpDataLoader, Period
from datetime import datetime
import pandas as pd
import os


class EstimateTracker:
    def __init__(self, fmp_api_key):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)

    def load_tracking_file(self, file_name):
        # Load quarterly or annual tracking file
        path = os.path.join(RESULTS_DIR, file_name)
        if os.path.exists(path):
            tracking_df = pd.read_csv(path, parse_dates=['target_date', 'tracking_date'])
        else:
            tracking_df = pd.DataFrame(columns=[
                'target_date', 'estimatedEpsAvg', 'estimatedEpsHigh',
                'estimatedEpsLow', 'numberAnalystsEstimatedEps',
                'tracking_date', 'symbol'
            ])
        return tracking_df

    def fetch_and_add_new_estimates(self, symbol_list, tracking_df, period: Period):
        for symbol in symbol_list:
            # Fetch new quarterly or annual estimates
            new_estimates_df = self.fmp_data_loader.fetch_analyst_estimates(symbol, period, limit=100)
            new_estimates_df = new_estimates_df[
                ['date', 'estimatedEpsAvg', 'estimatedEpsHigh', 'estimatedEpsLow', 'numberAnalystsEstimatedEps']]
            new_estimates_df.rename(columns={'date': 'target_date'}, inplace=True)
            new_estimates_df['tracking_date'] = pd.to_datetime(datetime.now().date())
            new_estimates_df['target_date'] = pd.to_datetime(new_estimates_df['target_date'])
            new_estimates_df['symbol'] = symbol

            # Compare and find revisions by checking the first existing entry
            for _, row in new_estimates_df.iterrows():
                # Check if the target_date and symbol exist in the tracking dataframe
                existing_rows = tracking_df[
                    (tracking_df['symbol'] == row['symbol']) &
                    (tracking_df['target_date'] == row['target_date'])
                    ]

                if existing_rows.empty:
                    # If no record exists, it's a new entry
                    tracking_df = pd.concat([tracking_df, pd.DataFrame([row])], ignore_index=True)
                else:
                    # If a record exists, check the first tracking entry for comparison
                    existing_row = existing_rows.iloc[0]
                    if (existing_row['estimatedEpsAvg'] != row['estimatedEpsAvg']) or \
                        (existing_row['estimatedEpsHigh'] != row['estimatedEpsHigh']) or \
                        (existing_row['estimatedEpsLow'] != row['estimatedEpsLow']):
                        # If any data has changed, append the new row
                        tracking_df = pd.concat([tracking_df, pd.DataFrame([row])], ignore_index=True)
        return tracking_df

    def track_estimates(self, symbol_list):
        # Load existing tracking files
        quarterly_tracking_df = self.load_tracking_file(QUARTERY_EPS_ESTIMATES_FILE_NAME)
        annual_tracking_df = self.load_tracking_file(ANNUAL_EPS_ESTIMATES_FILE_NAME)

        # Fetch and add new estimates
        updated_quarterly_tracking_df = self.fetch_and_add_new_estimates(symbol_list, quarterly_tracking_df,
                                                                         Period.QUARTERLY)
        updated_annual_tracking_df = self.fetch_and_add_new_estimates(symbol_list, annual_tracking_df, Period.ANNUAL)

        # Save the updated tracking data
        quarterly_path = os.path.join(RESULTS_DIR, QUARTERY_EPS_ESTIMATES_FILE_NAME)
        updated_quarterly_tracking_df.to_csv(quarterly_path, index=False)

        annual_path = os.path.join(RESULTS_DIR, ANNUAL_EPS_ESTIMATES_FILE_NAME)
        updated_annual_tracking_df.to_csv(annual_path, index=False)
