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

            return avg_earnings_surprise
        except Exception as ex:
            loge(ex)
        return 0.0

    def calculate_agreement(self, symbol, estimate_tracking_df):
        """
        Calculate changes in estimates over a time period,
        and then count the ratio of upwards revisions versus downward revisions.
        """
        symbol_df = estimate_tracking_df[estimate_tracking_df['symbol'] == symbol]
        if symbol_df.empty:
            return 0.0

        # Get recent data - last month
        one_month_ago = datetime.now() - timedelta(days=30)
        symbol_df = symbol_df[symbol_df['tracking_date'] >= one_month_ago]

        if symbol_df.empty:
            return 0.0

        # Calculate upward/downward changes
        symbol_df['estimatedEpsChange'] = symbol_df['estimatedEpsAvg'].pct_change()
        upward_revisions = symbol_df[symbol_df['estimatedEpsChange'] > 0].shape[0]
        downward_revisions = symbol_df[symbol_df['estimatedEpsChange'] < 0].shape[0]
        total_revisions = upward_revisions + downward_revisions

        if total_revisions == 0:
            return 0.0

        agreement_score = upward_revisions / total_revisions
        return agreement_score

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
        return magnitude_score

    def calculate_upside(self, symbol, estimate_tracking_df):
        """
        Calculates the average over a period of time as a ‘general consensus’.
        The 'Upside' is the difference between the average and the most recent estimate
        """
        symbol_df = estimate_tracking_df[estimate_tracking_df['symbol'] == symbol]
        if symbol_df.empty:
            return 0.0
        recent_consensus = symbol_df['estimatedEpsAvg'].iloc[-1]

        # Get the estimates for the last three months
        three_month_avg_consensus = symbol_df[symbol_df['tracking_date'] >= datetime.now() - timedelta(days=90)][
            'estimatedEpsAvg'].mean()

        # Avoid division by zero
        if pd.isna(three_month_avg_consensus) or three_month_avg_consensus == 0:
            return 0.0

        # Calculate change between most recent and average consensus estimates
        upside_score = ((recent_consensus - three_month_avg_consensus) / three_month_avg_consensus) * 100
        return upside_score

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

            result = {
                'symbol': symbol,
                'agreement_score': agreement_score,
                'magnitude_score': magnitude_score,
                'upside_score': upside_score,
                'avg_earnings_surprise': avg_earnings_surprise
            }
            results.append(result)
            # Throttle FMP API calls for earnings surprises
            #time.sleep(0.1)

        results_df = pd.DataFrame(results)

        results_norm_df = normalize_dataframe(
            results_df.copy(),
            column_list=['agreement_score', 'magnitude_score', 'upside_score', 'avg_earnings_surprise']
        )

        results_norm_df['weighted_score'] = (
            results_norm_df['agreement_score'] * 0.20 +
            results_norm_df['magnitude_score'] * 0.40 +
            results_norm_df['upside_score'] * 0.2 +
            results_norm_df['avg_earnings_surprise'] * 0.20
        )

        # Merge normalized weighted score back to the original DataFrame
        final_results_df = results_df.copy()
        final_results_df['weighted_score'] = results_norm_df['weighted_score']

        # Sort by weighted score
        final_results_df.sort_values(by=['weighted_score'], ascending=False, inplace=True)

        file_name = f"earnings_revision_results_{datetime.today().strftime('%Y-%m-%d')}.csv"
        store_csv(RESULTS_DIR, file_name, results_norm_df)



