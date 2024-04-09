import argparse
import datetime

import common_package.constants as kiki_logs_constants
import common_package.file_utils as file_utils
import common_package.process_logs_utils as process_logs_utils

# at "kiki_music_data/tracking/lyrics_nearlymatch"
import crawl_daily_logs.crawler as crawler
import pandas as pd
from crawl_daily_logs.whitelist_common_id import WHITELIST_COMMON_ID
from loguru import logger
from termcolor import colored

WAIT_TIME = 60


def update_database_from_date(date: str, is_forced=False, wait_time=WAIT_TIME):
    """update database from date

    Args:
        date (str): date string with format YYYY-mm-dd
        is_forced (boolean): ignore OK status
    """
    logger.info(colored(f"UPDATING DATEBASE FROM {date}", "green"))
    today = datetime.datetime.today().date()  # today
    start = datetime.datetime.strptime(date, kiki_logs_constants.DATE_FORMAT)
    while start.date() < today:
        start_str = start.strftime(kiki_logs_constants.DATE_FORMAT)
        crawl_daily_logs(start_str, is_forced, wait_time)

        today = datetime.datetime.today().date()
        start = start + datetime.timedelta(1)
    logger.info(colored(f"UPDATED DATEBASE FROM {date}", "blue"))
    return True


def is_failure_crawling(df):
    return len(df[df["duration"].notnull()]) / len(df) < 0.2


def crawl_daily_logs(date: str, is_forced=False, wait_time=WAIT_TIME):
    # check file is existing
    if not is_forced and file_utils.is_logs_data_existing(date):
        data = file_utils.load_logs_data(date)
        if not is_failure_crawling(data):
            return file_utils.get_logs_data_path(date)
        else:
            logger.info(colored(f"RECRAWLING DATE {date}", "green"))

    # crawl data
    start_ts = (
        datetime.datetime.strptime(date, kiki_logs_constants.DATE_FORMAT).timestamp()
        * 1000
    )
    end_ts = start_ts + 1000 * 60 * 60 * 24
    instance = crawler.KiKiLogsCrawler()
    instance.wait_duration_logs_available(start_ts, end_ts, wait_time=wait_time)
    data = instance.crawl_log(start_ts, end_ts)

    # except whitelist common id
    data = process_logs_utils.filt_by_common_id(
        data, WHITELIST_COMMON_ID, is_not_contain=True
    )

    path = file_utils.write_logs_data(data, date)

    if is_failure_crawling(data):
        raise ValueError("Crawling failure!!!")

    return path


def crawl_daily_suggestion_mp3_logs(date: str, wait_time=WAIT_TIME) -> pd.DataFrame:
    start_ts = (
        datetime.datetime.strptime(date, kiki_logs_constants.DATE_FORMAT).timestamp()
        * 1000
    )
    end_ts = start_ts + 1000 * 60 * 60 * 24
    instance = crawler.KiKiLogsCrawler()
    instance.wait_suggestion_mp3_logs_available(start_ts, end_ts, wait_time=wait_time)
    data = instance.crawl_suggestion_mp3_logs(start_ts, end_ts)
    return data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="crawl date on date", type=str)
    parser.add_argument(
        "--is_from_date",
        help="is crawl date from date or on date",
        default=False,
        type=bool,
    )
    parser.add_argument(
        "--is_forced", help="is forced crawling", default=False, type=bool
    )
    args = parser.parse_args()

    if args.is_from_date:
        update_database_from_date(args.date, args.is_forced)
    else:
        crawl_daily_logs(args.date, args.is_forced)
