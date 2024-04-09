import datetime
import glob
import time
import traceback

import common_package.constants as constants
import common_package.kiki_db_constants as config
import mysql.connector
import pandas as pd
import psycopg2
from loguru import logger
from termcolor import colored

DUR_THRESHOLD = 30000
SONG_LENGTH_THRESHOLD = 30000
NEW_UAR_FORMULATION_MILESTONE = 1664816400000  # 2022-10-04
NEW_FASTER_DATABASE_MILESTONE = 1650585600000  # 2022-04-22
MOVE_TO_DATA_PLATFORM_MILESTONE = 1686070800000  # 2023-06-07
NEW_FASTER_DATABASE_AUTO_DURATION_MILESTONE = 1685552400000  # 2023-06-01

KIKI_MP3_DURATION_STATS_PATH = (
    "/data/data_upload/data_platform/kiki_mp3_duration_stats_v2/{}/*.csv"
)
KIKI_MP3_DURATION_STATS_DATE_FORMAT = "%Y/%m/%d"
KIKI_MP3_DURATION_STATS_HEADERS = ["id", "duration", "song_length", "type"]

KIKI_AUTO_MUSIC_DURATION_STATS_PATH = (
    "/data/data_upload/data_platform/kiki_auto_music_duration_production/{}/*.csv"
)


class KiKiLogsCrawler:
    def __init__(self):
        self.fast_db_params = config.FAST_MYSQL_CRAWLER_PROD
        self.db_params = config.MYSQL_CRAWLER_PROD
        self.fast_postgres_db_params = config.FAST_POSTGRES_CRAWLER_PROD

    def connect_kiki_db(self, params):
        """connect to datebase"""
        port = int(params["port"])
        if port >= 3300 and port < 3400:
            db_conn = mysql.connector.connect(**params, connect_timeout=5)
            db_type = "mysql"
        elif port >= 5400 and port < 5500:
            db_conn = psycopg2.connect(**params, connect_timeout=5)
            db_type = "postgres"
        else:
            raise Exception("Unknown database type")

        logger.info(colored("CONNECTED KIKI DATABASE SUCCESSFULLY", "blue"))
        return db_conn, db_type

    def add_timestamp_to_query(
        self, query, start_ts, end_ts, field_name="created_time"
    ):
        query = query.strip()
        if query[-1] == ";":
            query = query[:-1]
        query = query + f" WHERE {field_name} >= {start_ts} and {field_name} < {end_ts}"
        return query

    def add_limit_to_query(self, query, pos, limit, db_type):
        query = query.strip()
        if query[-1] == ";":
            query = query[:-1]
        if db_type == "mysql":
            query = query + f" LIMIT {pos}, {limit}"
        elif db_type == "postgres":
            query = query + f" OFFSET {pos} LIMIT {limit}"
        else:
            raise ValueError(f"Cannot found db_type {db_type}!!!")
        return query

    def get_action_logs_query(self):
        query = """
            select SQL_NO_CACHE *
            from kiki_production_action_logs
        """
        return query

    def get_backend_logs_query(self):
        query = """
            select SQL_NO_CACHE id, backend
            from kiki_action_logs_backend_info
        """
        return query

    def get_duration_logs_query(self, _type, timestamp=None, count_only=None):
        if _type == "zingmp3":
            info = (
                "count(*) as count" if count_only else "id, duration, song_length, type"
            )
            query = f"""
                select SQL_NO_CACHE {info}
                from kiki_mp3_song_duration_stats
            """
        elif _type == "car":
            if count_only == True:
                info = "count(*) as count"
            elif (
                timestamp is not None
                and timestamp >= NEW_FASTER_DATABASE_AUTO_DURATION_MILESTONE
            ):
                info = (
                    "id, duration, media_length as song_length, media_title, media_id"
                )
            else:
                info = "id, duration, media_length as song_length"

            query = f"""
                select {info}
                from kiki_auto_music_duration_production
            """
        else:
            raise ValueError(f"Cannot found type {_type}!!!")
        return query

    def get_suggestion_mp3_logs_query(self, count_only=None):
        info = (
            "count(*) as count"
            if count_only
            else "id, action_type, item_index, item_type"
        )
        query = f"""
            select SQL_NO_CACHE {info}
            from kiki_suggestion_mp3_stats
        """
        return query

    def crawl_log_helper(
        self,
        query,
        start_ts,
        end_ts,
        params,
        limit=1000,
        timestamp_field="created_time",
    ):
        db_conn, db_type = self.connect_kiki_db(params)

        step = int((end_ts - start_ts) / 24)
        result_dataFrame = None
        try:
            for ts in range(int(start_ts), int(end_ts), step):
                _query = self.add_timestamp_to_query(
                    query, ts, ts + step, timestamp_field
                )
                logger.debug(colored(f"QUERY: {_query}", "white"))

                pos = 0
                while True:
                    result = pd.read_sql(
                        self.add_limit_to_query(_query, pos, limit, db_type), db_conn
                    )

                    if result is None:
                        break
                    if result_dataFrame is None:
                        result_dataFrame = result
                    else:
                        result_dataFrame = pd.concat(
                            [result_dataFrame, result], ignore_index=True
                        )

                    if len(result) != limit:
                        break

                    pos += limit

            logger.info(colored("EXECUTED CRAWL QUERY SUCCESSFULLY", "blue"))
        except:
            result_dataFrame = None
            logger.error(colored("EXECUTED CRAWL QUERY FAILURE", "red"))
            logger.error(traceback.format_exc())

        db_conn.close()
        return result_dataFrame

    def crawl_slow_log_helper(
        self, query, start_ts, end_ts, params, timestamp_field="created_time"
    ):
        return self.crawl_log_helper(
            query, start_ts, end_ts, params, limit=100, timestamp_field=timestamp_field
        )

    def load_logs_from_rsync_folder(
        self,
        action_df: pd.DataFrame,
        start_ts: int,
        end_ts: int,
        _type: str,
        timestamp_field: str = "created_time",
        count_only=False,
    ):
        """load logs from rsync folder

        Parameters
        ----------
        action_df: pd.DataFrame
            action logs. Use to map id
        start_ts, end_ts : int
            timestamp as miliseconds
        _type : str
            type of logs, can be "zingmp3" or "car"
        timestamp_field : str
            name of timestamp field in logs
            default is "created_time"
        """

        def map_logs_id(df: pd.DataFrame, action_df: pd.DataFrame) -> pd.DataFrame:
            df.set_index("id", inplace=True)
            action_df["sub_id"] = action_df.index.map(
                lambda x: x if x.startswith("1conn") else x[-12:]
            )
            df = action_df.merge(df, left_on="sub_id", right_index=True, how="right")
            df = df.reset_index().rename(columns={"index": "id"})
            return df[df["id"].notnull()]

        if _type == "zingmp3":
            file_path = KIKI_MP3_DURATION_STATS_PATH
            date_format = KIKI_MP3_DURATION_STATS_DATE_FORMAT
            headers = KIKI_MP3_DURATION_STATS_HEADERS
        elif _type == "car":
            file_path = KIKI_AUTO_MUSIC_DURATION_STATS_PATH
            date_format = KIKI_MP3_DURATION_STATS_DATE_FORMAT
            headers = KIKI_MP3_DURATION_STATS_HEADERS
        else:
            raise ValueError(f"Cannot found type {_type}!!!")

        df_lst = []

        start_date = datetime.datetime.fromtimestamp(
            start_ts / 1000, tz=constants.LOCAL_TIMEZONE
        ).strftime("%Y-%m-%d")
        end_date = (
            datetime.datetime.fromtimestamp(end_ts / 1000, tz=constants.LOCAL_TIMEZONE)
            - datetime.timedelta(days=1)
        ).strftime("%Y-%m-%d")
        for date in pd.date_range(start=start_date, end=end_date, freq="D"):
            str_date = date.strftime(date_format)
            file_paths = glob.glob(file_path.format(str_date))
            assert (
                len(file_paths) == 1
            ), f"Found {len(file_paths)} files with date {str_date}!!!"
            file_path = file_paths[0]
            df = pd.read_csv(file_path, sep=",")
            df = df.rename(
                columns={
                    "source_id": "id",
                    "duration_max": "duration",
                    "song_length_max": "song_length",
                }
            )
            df_lst.append(df)

        mp3_song_duration_df = pd.concat(df_lst, ignore_index=True)

        if count_only:
            return pd.DataFrame({"count": [len(mp3_song_duration_df)]})
        else:
            mp3_song_duration_df = map_logs_id(mp3_song_duration_df, action_df)
            return mp3_song_duration_df[headers]

    def process_duration_data(self, df, _type, start_ts):
        if _type == "zingmp3":
            if start_ts >= NEW_UAR_FORMULATION_MILESTONE:
                album_filter = df["type"] == "album"
                df.loc[album_filter, "is_accepted"] = (
                    df[album_filter]["duration"] >= DUR_THRESHOLD
                )

                # process song_length is not None
                has_song_length_filter = (
                    (df["type"] == "song")
                    & (df["song_length"].notnull())
                    & (df["song_length"] > 0)
                )
                long_song_filter = has_song_length_filter & (
                    df["song_length"] > SONG_LENGTH_THRESHOLD
                )
                short_song_filter = has_song_length_filter & (
                    df["song_length"] <= SONG_LENGTH_THRESHOLD
                )
                df.loc[long_song_filter, "is_accepted"] = (
                    df[long_song_filter]["duration"] >= DUR_THRESHOLD
                )
                df.loc[short_song_filter, "is_accepted"] = (
                    df[short_song_filter]["duration"]
                    >= 0.9 * df[short_song_filter]["song_length"]
                )
                # process song_length is None
                df.loc[~has_song_length_filter, "is_accepted"] = (
                    df[~has_song_length_filter]["duration"] >= DUR_THRESHOLD
                )
            else:
                df["is_accepted"] = df["duration"] >= DUR_THRESHOLD

        elif _type == "car":
            has_song_length_filter = (df["song_length"].notnull()) & (
                df["song_length"] > 0
            )
            long_song_filter = has_song_length_filter & (
                df["song_length"] > SONG_LENGTH_THRESHOLD
            )
            short_song_filter = has_song_length_filter & (
                df["song_length"] <= SONG_LENGTH_THRESHOLD
            )
            df.loc[long_song_filter, "is_accepted"] = (
                df[long_song_filter]["duration"] >= DUR_THRESHOLD
            )
            df.loc[short_song_filter, "is_accepted"] = (
                df[short_song_filter]["duration"]
                >= 0.9 * df[short_song_filter]["song_length"]
            )
            # process song_length is None
            df.loc[~has_song_length_filter, "is_accepted"] = (
                df[~has_song_length_filter]["duration"] >= DUR_THRESHOLD
            )
        else:
            raise ValueError(f"Cannot found type {_type}!!!")

        if "media_id" not in df.columns:
            df["media_id"] = None
        if "media_title" not in df.columns:
            df["media_title"] = None

        return df[["id", "duration", "is_accepted", "media_id", "media_title"]]

    def crawl_log(self, start_ts, end_ts):
        action_query = self.get_action_logs_query()
        backend_query = self.get_backend_logs_query()
        zmp3_dur_query = self.get_duration_logs_query("zingmp3")
        car_dur_query = self.get_duration_logs_query("car", timestamp=start_ts)

        if start_ts >= NEW_FASTER_DATABASE_MILESTONE:
            action_df = self.crawl_log_helper(
                action_query, start_ts, end_ts, self.fast_db_params
            )
        else:
            action_df = self.crawl_slow_log_helper(
                action_query, start_ts, end_ts, self.db_params
            )
        action_df.set_index("id", inplace=True)

        if start_ts >= NEW_FASTER_DATABASE_MILESTONE:
            backend_df = self.crawl_log_helper(
                backend_query, start_ts / 1000, end_ts / 1000, self.fast_db_params
            )
        else:
            backend_df = self.crawl_slow_log_helper(
                backend_query, start_ts / 1000, end_ts / 1000, self.db_params
            )
        backend_df.set_index("id", inplace=True)

        if start_ts >= MOVE_TO_DATA_PLATFORM_MILESTONE:
            zmp3_dur_df = self.load_logs_from_rsync_folder(
                action_df, start_ts, end_ts, "zingmp3"
            )
        elif start_ts >= NEW_FASTER_DATABASE_MILESTONE:
            zmp3_dur_df = self.crawl_log_helper(
                zmp3_dur_query, start_ts, end_ts, self.fast_db_params
            )
        else:
            zmp3_dur_df = self.crawl_slow_log_helper(
                zmp3_dur_query, start_ts, end_ts, self.db_params
            )
        zmp3_dur_df = self.process_duration_data(
            zmp3_dur_df, _type="zingmp3", start_ts=start_ts
        )
        zmp3_dur_df.set_index("id", inplace=True)

        if start_ts >= NEW_FASTER_DATABASE_AUTO_DURATION_MILESTONE:
            car_dur_df = self.crawl_log_helper(
                car_dur_query, start_ts, end_ts, self.fast_postgres_db_params
            )
        else:
            car_dur_df = self.crawl_log_helper(
                car_dur_query, start_ts, end_ts, self.db_params
            )
        car_dur_df = self.process_duration_data(
            car_dur_df, _type="car", start_ts=start_ts
        )
        car_dur_df.set_index("id", inplace=True)

        car_dur_df.update(zmp3_dur_df)
        dur_df = pd.concat([zmp3_dur_df, car_dur_df], axis=0)
        dur_df = dur_df[~dur_df.index.duplicated(keep="first")]

        df = action_df.join(backend_df, how="left")
        df = df.join(dur_df, how="left")

        return df

    def wait_duration_logs_available(
        self, start_ts, end_ts, wait_time, timestamp_field="created_time"
    ):
        if start_ts >= NEW_FASTER_DATABASE_MILESTONE:
            zing_params = self.fast_db_params
        else:
            zing_params = self.db_params
        zmp3_dur_query = self.get_duration_logs_query("zingmp3", count_only=True)
        _query = self.add_timestamp_to_query(
            zmp3_dur_query, start_ts, end_ts, timestamp_field
        )

        zmp3_dur_df = None
        while True:
            if start_ts >= MOVE_TO_DATA_PLATFORM_MILESTONE:
                df = self.load_logs_from_rsync_folder(
                    None, start_ts, end_ts, "zingmp3", count_only=True
                )
            else:
                db_conn, db_type = self.connect_kiki_db(zing_params)
                df = pd.read_sql(_query, db_conn)

            if zmp3_dur_df is None or zmp3_dur_df["count"][0] != df["count"][0]:
                zmp3_dur_df = df
                logger.info(colored("Waiting...", "blue"))
                time.sleep(wait_time)
                continue
            else:
                break

        assert zmp3_dur_df["count"][0] > 0, "Zing duration Logs is not available!!!"

        if start_ts >= NEW_FASTER_DATABASE_AUTO_DURATION_MILESTONE:
            car_params = self.fast_postgres_db_params
        else:
            car_params = self.db_params
        car_dur_query = self.get_duration_logs_query("car", count_only=True)
        _query = self.add_timestamp_to_query(
            car_dur_query, start_ts, end_ts, timestamp_field
        )
        car_dur_df = None
        while True:
            db_conn, db_type = self.connect_kiki_db(car_params)
            df = pd.read_sql(_query, db_conn)

            if car_dur_df is None or car_dur_df["count"][0] != df["count"][0]:
                car_dur_df = df
                logger.info(colored("Waiting...", "blue"))
                time.sleep(wait_time)
                continue
            else:
                break

        assert car_dur_df["count"][0] > 0, "Car duration Logs is not available!!!"

    def wait_suggestion_mp3_logs_available(
        self, start_ts, end_ts, wait_time, timestamp_field="timestamp"
    ):
        params = self.db_params

        query = self.get_suggestion_mp3_logs_query(count_only=True)
        _query = self.add_timestamp_to_query(query, start_ts, end_ts, timestamp_field)

        df = None
        while True:
            db_conn, db_type = self.connect_kiki_db(params)
            df = pd.read_sql(_query, db_conn)

            if df is None or df["count"][0] == 0:
                logger.info(colored("Waiting...", "blue"))
                time.sleep(wait_time)
                continue
            else:
                break

        assert df["count"][0] > 0, "Suggestion mp3 Logs is not available!!!"

    def crawl_suggestion_mp3_logs(self, start_ts, end_ts):
        query = self.get_suggestion_mp3_logs_query()
        df = self.crawl_slow_log_helper(
            query, start_ts, end_ts, self.db_params, "timestamp"
        )
        df.set_index("id", inplace=True)
        return df
