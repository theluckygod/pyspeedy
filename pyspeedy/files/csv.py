import csv
import pandas as pd
from beartype import beartype
from loguru import logger

from pyspeedy.files.file import File


class CSV(File):
    @beartype
    def read(self, path: str, **kwargs) -> pd.DataFrame:
        logger.info(f"Reading CSV file from {path}")
        df: pd.DataFrame = pd.read_csv(path, **kwargs)
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
        data.to_csv(
            path,
            index=index,
            encoding=encoding,
            quoting=quoting,
            **kwargs,
        )
        logger.info(f"CSV file written to {path}")
