from utils.file_utils import *
from utils.file_utils import get_os_variable
from utils.log_utils import *
from trackers.estimate_tracker import EstimateTracker
from analysis_tools.earnings_estimate_revision_calculator import EarningsEstimateRevisionCalculator
import schedule
import time


# Get API key from environment variables
FMP_API_KEY = get_os_variable('FMP_API_KEY')


def perform_tasks():
    tracker = EstimateTracker(FMP_API_KEY)
    tracker.track_estimates()

    revision_calculator = EarningsEstimateRevisionCalculator(FMP_API_KEY)
    revision_calculator.calculate_earnings_estimate_revisions()


def schedule_events():
    schedule.every().monday.at('01:30').do(perform_tasks)
    schedule.every().tuesday.at('01:30').do(perform_tasks)
    schedule.every().wednesday.at('01:30').do(perform_tasks)
    schedule.every().thursday.at('01:30').do(perform_tasks)
    schedule.every().friday.at('01:30').do(perform_tasks)


if __name__ == "__main__":
    create_output_directories()
    setup_logger("estimate_revision_model_log.txt")

    #perform_tasks()

    #  Schedule events - to run the script at regular intervals
    schedule_events()

    #  Check schedule
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check time every x seconds



