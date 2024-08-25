from utils.file_utils import *
from utils.file_utils import get_os_variable
from utils.log_utils import *
from analysis_tools.earnings_estimate_revision_candidate_finder import EarningsEstimateRevisionCandidateFinder
import schedule


# Get API key from environment variables
FMP_API_KEY = get_os_variable('FMP_API_KEY')


def find_candidates():
    candidate_finder = EarningsEstimateRevisionCandidateFinder(FMP_API_KEY)
    candidate_finder.find_candidates()


def schedule_events():
    schedule.every().day.at('01:30').do(find_candidates())


if __name__ == "__main__":
    create_output_directories()
    setup_logger("estimate_revision_model_log.txt")

    find_candidates()
    """

    #  Schedule events - to run the script at regular intervals
    schedule_events()

    #  Check schedule
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check time every x seconds
    """


