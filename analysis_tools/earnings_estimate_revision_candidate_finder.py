import pandas as pd

from data_loaders.market_symbol_loader import MarketSymbolLoader
from analysis_tools.estimate_tracker import EstimateTracker
from analysis_tools.estimate_revision_analyzer import EstimateRevisionAnalyzer
from analysis_tools.earnings_surprise_analyzer import EarningsSurpriseAnalyzer
from config import *
from utils.log_utils import *
from utils.file_utils import store_csv


class EarningsEstimateRevisionCandidateFinder:
    def __init__(self, fmp_api_key):
        self.estimate_tracker = EstimateTracker(fmp_api_key)
        self.estimate_revision_analyzer = EstimateRevisionAnalyzer()
        self.estimate_surprise_analyzer = EarningsSurpriseAnalyzer(fmp_api_key)

    def normalize_series(self, series):
        """Normalize a pandas series to a 0-1 range."""
        return (series - series.min()) / (series.max() - series.min())

    def find_candidates(self):
        logi(f"Finding estimate revision candidates...")
        # Load symbols
        symbol_loader = MarketSymbolLoader()
        symbols_df = symbol_loader.fetch_nasdaq100_symbols(cache_file=True, cache_dir=CACHE_DIR)
        symbol_list = symbols_df['symbol'].unique()

        # Track estimates (uncomment if needed)
        self.estimate_tracker.track_estimates(symbol_list)

        # Calculate estimate revision stats
        estimate_revisions_df = self.estimate_revision_analyzer.check_revisions(symbol_list)

        # Fetch earnings surprises
        earnings_surprise_df = self.estimate_surprise_analyzer.find_earnings_surprises(symbol_list)

        # Merge dataframes on symbol
        merged_df = pd.DataFrame()
        if estimate_revisions_df is not None and earnings_surprise_df is not None:
            merged_df = pd.merge(estimate_revisions_df, earnings_surprise_df, on='symbol', how='inner')

        if merged_df.empty:
            logi(f"merged_df is empty -> no candidates found")

        # Normalize metric columns
        metric_columns = [
            'avg_quarterly_percent_change',
            'avg_annual_percent_change',
            'avg_quarterly_agreement_score',
            'avg_annual_agreement_score',
            'earnings_surprise_change'
        ]

        for column in metric_columns:
            merged_df[f'normalized_{column}'] = self.normalize_series(merged_df[column])

        # Assign sample weights for each metric
        weights = {
            'normalized_avg_quarterly_percent_change': 0.6,
            'normalized_avg_annual_percent_change': 0.0,
            'normalized_avg_quarterly_agreement_score': 0.2,
            'normalized_avg_annual_agreement_score': 0.0,
            'normalized_earnings_surprise_change': 0.2
        }

        # Calculate weighted score
        merged_df['weighted_score'] = sum(
            merged_df[col] * weight for col, weight in weights.items()
        )

        # Drop all columns that start with 'norm_'
        columns_to_drop = [col for col in merged_df.columns if col.startswith('norm_')]
        merged_df.drop(columns=columns_to_drop, inplace=True)

        # Sort by weighted score in descending order
        candidates_df = merged_df.sort_values(by='weighted_score', ascending=False)

        # Store file
        store_csv(RESULTS_DIR, EARNINGS_ESTIMATE_REVISION_CANDIDATE_FILE_NAME, candidates_df)





