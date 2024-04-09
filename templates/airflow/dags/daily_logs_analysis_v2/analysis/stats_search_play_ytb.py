import csv
import gc

import common_package.file_utils as file_utils
import common_package.process_logs_utils as process_logs_utils
import pandas as pd

if __name__ == "__main__":

    from_date = "2023-07-01"
    to_date = "2023-08-11"

    data = {
        "date": [],
        "search_num": [],
        "play_num": [],
        "request_num": [],
        "backend": [],
        "device_type": [],
    }
    for date in pd.date_range(from_date, to_date):
        gc.collect()

        date = str(date)[:10]
        print(date)

        df = file_utils.load_logs_data(date)

        for device_type in df["device_type"].dropna().unique().tolist():
            filt_device_df = process_logs_utils.filt_by_device_type(df, device_type)

            for backend in df["backend"].dropna().unique().tolist():
                filted_df = process_logs_utils.filt_by_backend(filt_device_df, backend)

                ytb_df = filted_df[
                    filted_df.actions.str.contains(
                        "PlayMusicApp|PlayVideoApp", na=False
                    )
                ]
                search_df = ytb_df[ytb_df.actions.str.contains("youtube_search")]

                # append to data
                data["date"].append(date)
                data["search_num"].append(len(search_df))
                data["play_num"].append(len(ytb_df) - len(search_df))
                data["request_num"].append(len(filted_df))
                data["backend"].append(backend)
                data["device_type"].append(device_type)

    pd.DataFrame(data).to_csv(
        "outputs/stats_search_play_ytb_by_date.csv",
        index=False,
        quoting=csv.QUOTE_ALL,
        encoding="utf-8",
        sep=",",
    )
