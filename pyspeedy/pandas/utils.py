import concurrent.futures
import time
from multiprocessing import cpu_count
from typing import List, Literal

import pandas as pd
from loguru import logger


def run_parallel(
    df: pd.DataFrame,
    map_dataframe_func: callable,
    n_jobs: int = 6,
    threads_or_processes: Literal["threads", "processes"] = "threads",
) -> pd.DataFrame:
    """Run a function in parallel on a pandas DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to be processed.
        map_dataframe_func (callable): The function to be run on the DataFrame.
        n_jobs (int, optional): Number of jobs to run in parallel. Defaults to 6.
        threads_or_processes (Literal["threads", "processes"], optional): Whether to use threads or processes. Defaults to "threads".

    Returns:
        pd.DataFrame: The processed DataFrame.

    Example:
        def map_dataframe_func(df: pd.DataFrame):
            for idx, row in tqdm(df.iterrows(), total=len(df)):
                if row["res"] != "":
                    continue
                df.at[idx, "res"] = row["num"] + 1
            return df

        df = pd.DataFrame({"num": range(1000)})
        df = run_parallel(df, map_dataframe_func)
        print
    """

    if n_jobs == -1:
        n_jobs = cpu_count()

    org_index: pd.Index = df.index
    batch_dfs: List[pd.DataFrame] = []  # break df into smaller chunks
    for i in range(n_jobs):
        batch_dfs.append(df[i::n_jobs])

    _executor_func = (
        concurrent.futures.ThreadPoolExecutor
        if threads_or_processes == "threads"
        else concurrent.futures.ProcessPoolExecutor
    )

    start = time.time()
    with _executor_func() as executor:
        results = executor.map(map_dataframe_func, batch_dfs)
    results: List[pd.DataFrame] = list(results)  # force execution
    logger.info(f"Parallel processing took {time.time() - start:.2f} seconds")
    return pd.concat(results, axis=0).reindex(index=org_index)


if __name__ == "__main__":
    import time

    from tqdm import tqdm

    def map_dataframe_func(df: pd.DataFrame):
        for idx, row in tqdm(df.iterrows(), total=len(df)):
            df.at[idx, "res"] = row["num"] + 1
            time.sleep(0.5)
        return df

    df = pd.DataFrame({"num": range(100)})
    df = run_parallel(df, map_dataframe_func)
    print(df)
