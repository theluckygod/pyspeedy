import datetime
import sys
import time

import schedule
from loguru import logger
from lyrics_updater import VALID_DEPLOY_TYPE, LyricsUpdater
from termcolor import colored

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        deploy_type = sys.argv[1]
        if deploy_type not in VALID_DEPLOY_TYPE:
            logger.info(
                colored("Unknown deploy type. Deploying with local settings", "yellow")
            )
            deploy_type = "local"
        else:
            logger.info(colored("Deploying with type: " + deploy_type, "yellow"))
    else:
        logger.info(
            colored("Unknown deploy type. Deploying with local settings", "yellow")
        )
        deploy_type = "local"

    updater = LyricsUpdater(deploy_type)
    schedule.every().day.at("02:00").do(updater.sync_media)

    f = open("data/start_log/lyrics_updater.txt", "w")
    f.write(str(datetime.datetime.now()))
    f.close()

    while True:
        schedule.run_pending()
        time.sleep(1)
