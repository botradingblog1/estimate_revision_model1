from utils.file_utils import *
from utils.file_utils import get_os_variable
from utils.log_utils import *
from data_loaders.market_symbol_loader import MarketSymbolLoader
from analysis_tools.estimate_tracker import EstimateTracker
from analysis_tools.estimate_revision_analyzer import EstimateRevisionAnalyzer
from analysis_tools.earnings_surprise_analyzer import EarningsSurpriseAnalyzer
import schedule
import time


# Get API key from environment variables
FMP_API_KEY = get_os_variable('FMP_API_KEY')



def track_estimate_data():
    pass

def schedule_events():
    schedule.every().day.at('01:30').do(track_estimate_data)


if __name__ == "__main__":
    create_output_directories()
    setup_logger("estimate_revision_model_log.txt")

    # Load symbols
    symbol_loader = MarketSymbolLoader()
    symbols_df = symbol_loader.fetch_nasdaq100_symbols(cache_file=True, cache_dir=CACHE_DIR)
    symbol_list = symbols_df['symbol'].unique()
    symbol_list = symbol_list[0:2]

    estimate_tracker = EstimateTracker(FMP_API_KEY)
    #estimate_tracker.track_estimates()

    est_rev_analyzer = EstimateRevisionAnalyzer()
    results_df = est_rev_analyzer.check_revisions(symbol_list)

    est_surprise_analyzer = EarningsSurpriseAnalyzer(FMP_API_KEY)
    #est_surprise_analyzer.find_earnings_surprises(symbol_list)

    """

    #  Schedule events - to run the script at regular intervals
    schedule_events()

    #  Check schedule
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check time every x seconds
    """


