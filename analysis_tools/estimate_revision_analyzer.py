import pandas as pd
import numpy as np
from config import *
from datetime import datetime
import os
from utils.log_utils import *


class EstimateRevisionAnalyzer:
    def load_tracking_file(self, file_name):
        # Load quarterly or annual tracking file
        path = os.path.join(CACHE_DIR, file_name)
        if os.path.exists(path):
            tracking_df = pd.read_csv(path, parse_dates=['target_date', 'tracking_date'])
            return tracking_df
        return None

    def calculate_estimate_percentage_change(self, tracking_df):
        # Calculate percentage change between the first and last tracking dates within each group
        tracking_df = tracking_df.sort_values(by='tracking_date', ascending=True)
        results = []
        groups = tracking_df.groupby("target_date")

        for target_date, group in groups:
            if len(group) < 2:
                row = {
                    'target_date': target_date,
                    'eps_percent_change': 0.0
                }
                results.append(row)
                continue

            # Calculate percentage change between the two most recent records
            symbol = group['symbol']
            previous_value = group['estimatedEpsAvg'].iloc[-2]
            last_value = group['estimatedEpsAvg'].iloc[-1]

            # Calculate the percentage change
            eps_percent_change = ((last_value - previous_value) / previous_value) * 100

            row = {
                'symbol': symbol,
                'target_date': target_date,
                'eps_percent_change': eps_percent_change
            }
            results.append(row)

        result_df = pd.DataFrame(results)
        return result_df

    def calculate_agreement_score(self, tracking_df):
        # Calculate the agreement score based on the direction of changes
        tracking_df = tracking_df.sort_values(by='tracking_date', ascending=True)
        groups = tracking_df.groupby("target_date")
        results = []

        for target_date, group in groups:
            if len(group) < 2:
                row = {
                    'symbol': group['symbol'].iloc[0],  # Extracting symbol from the group
                    'target_date': target_date,
                    'agreement_score': 0.0
                }
                results.append(row)
                continue

            # Initialize the agreement score
            score = 0

            # Iterate over the group rows to calculate the agreement score
            for i in range(1, len(group)):
                if group['estimatedEpsAvg'].iloc[i] > group['estimatedEpsAvg'].iloc[i - 1]:
                    score += 1  # Increase in EPS estimate
                elif group['estimatedEpsAvg'].iloc[i] < group['estimatedEpsAvg'].iloc[i - 1]:
                    score -= 1  # Decrease in EPS estimate
                # No change contributes 0 to the score

            row = {
                'symbol': group['symbol'].iloc[0],  # Extracting symbol from the group
                'target_date': target_date,
                'agreement_score': score
            }
            results.append(row)

        result_df = pd.DataFrame(results)
        return result_df

    def check_revisions(self, symbol_list):
        logi("Analyzing estimate revisions")
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
            quarterly_eps_change_df = self.calculate_estimate_percentage_change(symbol_quarterly_tracking_df)
            annual_eps_change_df = self.calculate_estimate_percentage_change(symbol_annual_tracking_df)

            # Calculate the average of all future percentage changes
            avg_quarterly_percent_change = quarterly_eps_change_df['eps_percent_change'].mean()
            avg_annual_percent_change = annual_eps_change_df['eps_percent_change'].mean()

            # Calculate agreement score
            quarterly_agreement_results_df = self.calculate_agreement_score(symbol_quarterly_tracking_df)
            annual_agreement_results_df = self.calculate_agreement_score(symbol_annual_tracking_df)

            # Calculate the average of all agreement scores
            avg_quarterly_agreement_score = quarterly_agreement_results_df['agreement_score'].mean()
            avg_annual_agreement_score = annual_agreement_results_df['agreement_score'].mean()

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

        return results_df


