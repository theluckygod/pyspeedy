import csv

import pandas as pd
from beartype import beartype

from pyspeedy.common.logging import logger
from pyspeedy.files.file_concrete.file import File
from pyspeedy.functional.inspect_kwargs import ignore_unmatched_kwargs


class CSV(File):
    @beartype
    def read(self, path: str, **kwargs) -> pd.DataFrame:
        logger.info(f"Reading CSV file from {path}")
        df: pd.DataFrame = ignore_unmatched_kwargs(pd.read_csv)(path, **kwargs)
        logger.info(f"CSV file read from {path}")
        return df

    @beartype
    def write(
        self,
        data: pd.DataFrame,
        path: str,
        index=False,
        quoting=csv.QUOTE_ALL,
        encoding="utf-8",
        **kwargs,
    ) -> None:
        logger.info(f"Writing CSV file to {path}")
        ignore_unmatched_kwargs(data.to_csv)(
            path,
            index=index,
            encoding=encoding,
            quoting=quoting,
            **kwargs,
        )
        logger.info(f"CSV file written to {path}")
