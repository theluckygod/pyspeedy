import datetime
import gc

import common_package.constants as constants
import crawl_daily_logs.crawl_tools as crawl_tools
import pandas as pd
from loguru import logger
from tqdm import tqdm


def read_df_from_file(path):
    df = pd.read_csv(path, sep=",", encoding="utf-8")
    return df


def is_failure_crawling(path):
    df = read_df_from_file(path)
    return len(df[df["duration"].notnull()]) / len(df) < 0.2


def crawl_logs(date, is_from_date=False, is_forced=False):
    if is_from_date:
        is_successful = crawl_tools.update_database_from_date(
            date, is_forced, wait_time=0
        )
        return is_successful
    else:
        path = crawl_tools.crawl_daily_logs(date, is_forced, wait_time=0)
        if is_failure_crawling(path):
            raise ValueError("Crawling failure!!!")
        return path


def main(from_date, to_date, is_forced):
    start = datetime.datetime.strptime(from_date, constants.DATE_FORMAT)
    end = datetime.datetime.strptime(to_date, constants.DATE_FORMAT)
    info_lst = {}
    for _ in tqdm(range((end - start).days + 1)):
        date = start.strftime(constants.DATE_FORMAT)
        print(f"extract date: {date}")

        if date == to_date:
            break

        gc.collect()
        crawl_logs(date, is_from_date=False, is_forced=is_forced)

        today = datetime.datetime.today().date()
        start = start + datetime.timedelta(1)


if __name__ == "__main__":
    from_date = "2023-05-29"
    to_date = "2023-05-30"
    is_forced = True
    main(from_date, to_date, is_forced)
