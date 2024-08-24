import pandas as pd
import numpy as np
from config import *
from datetime import datetime
import os


class EstimateRevisionAnalyzer:
    def load_tracking_file(self, file_name):
        # Load quarterly or annual tracking file
        path = os.path.join(RESULTS_DIR, file_name)
        if os.path.exists(path):
            tracking_df = pd.read_csv(path, parse_dates=['target_date', 'tracking_date'])
            return tracking_df
        return None

    def calculate_estimate_percentage_change(self, group):
        # Calculate percentage change between consecutive tracking dates within the group
        group = group.sort_values(by='tracking_date', ascending=True)
        if len(group) < 2:
            return None  # Return None if there are not enough data points to calculate change

        percentage_changes = group['estimatedEpsAvg'].pct_change().dropna() * 100
        return percentage_changes.mean() if not percentage_changes.empty else None

    def calculate_agreement_score(self, group):
        # Calculate the agreement score based on the direction of changes
        group = group.sort_values(by='tracking_date', ascending=True)
        score = 0
        for i in range(1, len(group)):
            if group.iloc[i]['estimatedEpsAvg'] > group.iloc[i - 1]['estimatedEpsAvg']:
                score += 1  # Increase
            elif group.iloc[i]['estimatedEpsAvg'] < group.iloc[i - 1]['estimatedEpsAvg']:
                score -= 1  # Decrease
            # No change contributes 0 to the score
        return score

    def normalize_series(self, series):
        """Normalize a pandas series to a 0-1 range"""
        if series.empty or series.isna().all():
            return series  # Return the series as is if it's empty or all values are NaN

        min_val = series.min()
        max_val = series.max()

        if min_val == max_val:
            # If all values are the same, normalization would result in division by zero,
            # So return a series of 0.5s (or any constant value, since they are all the same)
            return pd.Series(np.ones(len(series)) * 0.5, index=series.index)

        return (series - min_val) / (max_val - min_val)

    def check_revisions(self, symbol_list):
        # Load tracking files
        quarterly_tracking_df = self.load_tracking_file(QUARTERY_EPS_ESTIMATES_FILE_NAME)
        annual_tracking_df = self.load_tracking_file(ANNUAL_EPS_ESTIMATES_FILE_NAME)

        # Filter out records with past tracking dates
        current_date = pd.to_datetime(datetime.now().date())
        quarterly_tracking_df = quarterly_tracking_df[quarterly_tracking_df['target_date'] >= current_date]
        annual_tracking_df = annual_tracking_df[annual_tracking_df['target_date'] >= current_date]

        results = []
        for symbol in symbol_list:
            # Filter records by symbol
            symbol_quarterly_tracking_df = quarterly_tracking_df[quarterly_tracking_df['symbol'] == symbol]
            symbol_annual_tracking_df = annual_tracking_df[annual_tracking_df['symbol'] == symbol]

            # Calculate percentage changes by target_date
            quarterly_percentage_changes = symbol_quarterly_tracking_df.groupby('target_date').apply(
                self.calculate_estimate_percentage_change).dropna()
            annual_percentage_changes = symbol_annual_tracking_df.groupby('target_date').apply(
                self.calculate_estimate_percentage_change).dropna()

            # Calculate the average of all future percentage changes
            avg_quarterly_percent_change = quarterly_percentage_changes.mean()
            avg_annual_percent_change = annual_percentage_changes.mean()

            # Calculate agreement score
            quarterly_agreement_results = symbol_quarterly_tracking_df.groupby('target_date').apply(
                self.calculate_agreement_score).dropna()
            annual_agreement_results = symbol_annual_tracking_df.groupby('target_date').apply(
                self.calculate_agreement_score).dropna()

            # Calculate the average of all agreement scores
            avg_quarterly_agreement_score = quarterly_agreement_results.mean()
            avg_annual_agreement_score = annual_agreement_results.mean()

            # Create results row
            results_record = {
                'symbol': symbol,
                'avg_quarterly_percent_change': avg_quarterly_percent_change,
                'avg_annual_percent_change': avg_annual_percent_change,
                'avg_quarterly_agreement_score': avg_quarterly_agreement_score,
                'avg_annual_agreement_score': avg_annual_agreement_score
            }
            results.append(results_record)

        # Convert results to dataframe
        results_df = pd.DataFrame(results)

        # Normalize scores
        results_df['normalized_quarterly_percent_change'] = self.normalize_series(results_df['avg_quarterly_percent_change'])
        results_df['normalized_annual_percent_change'] = self.normalize_series(results_df['avg_annual_percent_change'])
        results_df['normalized_quarterly_agreement_score'] = self.normalize_series(results_df['avg_quarterly_agreement_score'])
        results_df['normalized_annual_agreement_score'] = self.normalize_series(results_df['avg_annual_agreement_score'])

        # Calculate weighted score with quarterly/annual 60/40 percent and percent change/agreement 70/30
        results_df['final_score'] = (
            0.6 * (
            0.7 * results_df['normalized_quarterly_percent_change'] +
            0.3 * results_df['normalized_quarterly_agreement_score']
        ) +
            0.4 * (
                0.7 * results_df['normalized_annual_percent_change'] +
                0.3 * results_df['normalized_annual_agreement_score']
            )
        )

        return results_df


