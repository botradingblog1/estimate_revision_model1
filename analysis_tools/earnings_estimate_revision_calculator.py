import pandas as pd
from data_loaders.market_symbol_loader import MarketSymbolLoader
from data_loaders.earnings_surprise_loader import EarningsSurpriseLoader
from utils.log_utils import *
from utils.file_utils import *
from datetime import timedelta
from utils.df_utils import normalize_dataframe
import time


class EarningsEstimateRevisionCalculator:
    def __init__(self, fmp_api_key):
        self.earnings_surprise_loader = EarningsSurpriseLoader(fmp_api_key)

    def calculate_earnings_surprise(self, symbol: str):
        try:
            earnings_surprise_dict = self.earnings_surprise_loader.find_earnings_surprises(symbol)
            avg_earnings_surprise = earnings_surprise_dict.get('avg_earnings_surprise')
            if avg_earnings_surprise is None:
                return 0.0

            return round(avg_earnings_surprise, 2)
        except Exception as ex:
            loge(ex)
        return 0.0

    def calculate_agreement(self, symbol, estimate_tracking_df, days=30):
        """
        Calculate the agreement ratio of upwards revisions versus total revisions
        for a given symbol over the past `days` days.
        """
        # Filter data for the specific symbol
        symbol_df = estimate_tracking_df[estimate_tracking_df['symbol'] == symbol]
        if symbol_df.empty:
            return 0.0

        # Filter for data within the last `days` period
        cutoff_date = datetime.now() - timedelta(days=days)
        symbol_df = symbol_df[symbol_df['tracking_date'] >= cutoff_date]

        if symbol_df.empty:
            return 0.0

        # Calculate percentage change in estimates
        symbol_df['estimatedEpsChange'] = symbol_df.groupby('date')['estimatedEpsAvg'].transform(pd.Series.pct_change)
        symbol_df.dropna(subset=['estimatedEpsChange'], inplace=True)

        # Count upward and downward revisions
        upward_revisions = (symbol_df['estimatedEpsChange'] > 0).sum()
        downward_revisions = (symbol_df['estimatedEpsChange'] < 0).sum()

        # Calculate total revisions
        total_revisions = upward_revisions + downward_revisions
        if total_revisions == 0:
            return 0.0

        # Calculate agreement score
        agreement_score = upward_revisions / total_revisions
        return round(agreement_score, 2)

    def calculate_magnitude(self, symbol, estimate_tracking_df):
        """
        The magnitude component targets the size of the recent changes for the current and next fiscal years.
        """
        symbol_df = estimate_tracking_df[estimate_tracking_df['symbol'] == symbol]
        if symbol_df.empty:
            return 0.0

        # Get recent estimates (last month)
        one_month_ago = datetime.now() - timedelta(days=30)
        symbol_df = symbol_df[symbol_df['tracking_date'] >= one_month_ago]

        if symbol_df.empty:
            return 0.0

        # Calculate difference between one month ago and most recent estimates
        # for current and next fiscal year estimates
        current_fiscal_year_df = symbol_df[symbol_df['date'].dt.year == datetime.now().year]
        next_fiscal_year_df = symbol_df[symbol_df['date'].dt.year == datetime.now().year + 1]
        if len(current_fiscal_year_df) == 0 or len(next_fiscal_year_df) == 0:
            return 0.0

        current_fiscal_change = ((current_fiscal_year_df['estimatedEpsAvg'].iloc[-1] -
                                  current_fiscal_year_df['estimatedEpsAvg'].iloc[0]) /
                                 current_fiscal_year_df['estimatedEpsAvg'].iloc[
                                     0]) if not current_fiscal_year_df.empty else 0

        next_fiscal_change = ((next_fiscal_year_df['estimatedEpsAvg'].iloc[-1] -
                               next_fiscal_year_df['estimatedEpsAvg'].iloc[0]) /
                              next_fiscal_year_df['estimatedEpsAvg'].iloc[0]) * 100 if not next_fiscal_year_df.empty else 0

        magnitude_score = (current_fiscal_change + next_fiscal_change) / 2
        return round(magnitude_score, 2)

    def calculate_upside(self, symbol, estimate_tracking_df):
        """
        Calculates the upside as the percentage change between the most recent consensus
        and the average consensus over a specified time period.
        """
        # Filter data for the given symbol
        symbol_df = estimate_tracking_df[estimate_tracking_df['symbol'] == symbol]
        if symbol_df.empty:
            return 0.0

        # Filter data for the last 90 days
        cutoff_date = datetime.now() - timedelta(days=90)
        recent_estimates_df = symbol_df[symbol_df['tracking_date'] >= cutoff_date]

        if recent_estimates_df.empty:
            return 0.0

        # Calculate average consensus for the last 90 days
        avg_recent_consensus = recent_estimates_df['estimatedEpsAvg'].mean()

        # Extract the most recent tracking date and its corresponding consensus
        most_recent_tracking_date = recent_estimates_df['tracking_date'].max()
        last_estimates_df = recent_estimates_df[recent_estimates_df['tracking_date'] == most_recent_tracking_date]
        last_consensus = last_estimates_df['estimatedEpsAvg'].mean()

        # Handle edge cases
        if pd.isna(avg_recent_consensus) or pd.isna(last_consensus) or avg_recent_consensus == 0:
            return 0.0

        # Calculate upside as a percentage change
        upside_score = ((last_consensus - avg_recent_consensus) / avg_recent_consensus) * 100
        return round(upside_score, 2)

    def calculate_avg_number_analysts(self, symbol, estimate_tracking_df):
        """
        Calculates the upside as the percentage change between the most recent consensus
        and the average consensus over a specified time period.
        """
        # Filter data for the given symbol
        symbol_df = estimate_tracking_df[estimate_tracking_df['symbol'] == symbol]
        if symbol_df.empty:
            return 0.0

        # Filter data for the last 90 days
        cutoff_date = datetime.now() - timedelta(days=90)
        recent_estimates_df = symbol_df[symbol_df['tracking_date'] >= cutoff_date]

        if recent_estimates_df.empty:
            return 0.0

        # Calculate average number of analysis for the last 90 days
        avg_num_analysts = recent_estimates_df['numberAnalystsEstimatedEps'].mean()
        return avg_num_analysts

    def calculate_earnings_estimate_revisions(self):
        logi("Calculating earnings estimate revisions...")
        symbol_loader = MarketSymbolLoader()
        symbols_df = symbol_loader.fetch_sp500_symbols(cache_file=True, cache_dir=CACHE_DIR)
        symbol_list = symbols_df['symbol'].unique()

        estimate_tracking_df = load_csv(CACHE_DIR, ESTIMATE_TRACKING_FILE_NAME)
        if estimate_tracking_df is None or estimate_tracking_df.empty:
            logi(f"estimate_tracking_df is empty")
            return
        estimate_tracking_df['date'] = pd.to_datetime(estimate_tracking_df['date'], errors='coerce')
        estimate_tracking_df['tracking_date'] = pd.to_datetime(estimate_tracking_df['tracking_date'], errors='coerce')

        results = []
        for symbol in symbol_list:
            logd(f"Now processing {symbol}...")
            agreement_score = self.calculate_agreement(symbol, estimate_tracking_df)
            magnitude_score = self.calculate_magnitude(symbol, estimate_tracking_df)
            upside_score = self.calculate_upside(symbol, estimate_tracking_df)
            avg_earnings_surprise = self.calculate_earnings_surprise(symbol)
            avg_num_analysts = self.calculate_avg_number_analysts(symbol, estimate_tracking_df)

            result = {
                'symbol': symbol,
                'agreement_score': agreement_score,
                'magnitude_score': magnitude_score,
                'upside_score': upside_score,
                'avg_earnings_surprise': avg_earnings_surprise,
                'avg_num_analysts': avg_num_analysts
            }
            results.append(result)
            # Throttle FMP API calls for earnings surprises
            #time.sleep(0.1)

        results_df = pd.DataFrame(results)

        results_norm_df = normalize_dataframe(
            results_df.copy(),
            column_list=['agreement_score', 'magnitude_score', 'upside_score', 'avg_earnings_surprise', 'avg_num_analysts']
        )

        results_norm_df['weighted_score'] = (
            results_norm_df['agreement_score'] * 0.10 +
            results_norm_df['magnitude_score'] * 0.35 +
            results_norm_df['upside_score'] * 0.35 +
            results_norm_df['avg_earnings_surprise'] * 0.10 +
            results_norm_df['avg_num_analysts'] * 0.10
        )

        # Merge normalized weighted score back to the original DataFrame
        final_results_df = results_df.copy()
        final_results_df['weighted_score'] = results_norm_df['weighted_score']

        # Sort by weighted score
        final_results_df.sort_values(by=['weighted_score'], ascending=False, inplace=True)

        file_name = f"earnings_revision_results.csv"
        store_csv(RESULTS_DIR, file_name, final_results_df)
        path = os.path.join(RESULTS_DIR, file_name)
        logd(f"Results file stored to: {path}")
