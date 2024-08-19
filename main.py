from utils.file_utils import *
from utils.file_utils import get_os_variable
from utils.log_utils import *
import schedule
import time


# Get API key from environment variables
FMP_API_KEY = get_os_variable('FMP_API_KEY')



def track_estimate_data():
    pass

def schedule_events():
    schedule.every().day.at('01:01').do(track_estimate_data)


if __name__ == "__main__":
    create_output_directories()
    setup_logger("estimate_revision_model_log.txt")



    #  Schedule events - to run the script at regular intervals
    schedule_events()

    #  Check schedule
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check time every x seconds


