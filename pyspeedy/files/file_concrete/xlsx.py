import pandas as pd
from beartype import beartype

from pyspeedy.common.logging import logger
from pyspeedy.files.file_concrete.file import File
from pyspeedy.functional.inspect_kwargs import ignore_unmatched_kwargs


class XLSX(File):
    @beartype
    def read(self, path: str, **kwargs) -> pd.DataFrame:
        logger.info(f"Reading XLSX file from {path}")
        df: pd.DataFrame = ignore_unmatched_kwargs(pd.read_excel)(path, **kwargs)
        logger.info(f"XLSX file read from {path}")
        return df

    @beartype
    def write(
        self,
        data: pd.DataFrame,
        path: str,
        index=False,
        **kwargs,
    ) -> None:
        logger.info(f"Writing XLSX file to {path}")
        ignore_unmatched_kwargs(data.to_excel)(
            path,
            index=index,
            **kwargs,
        )
        logger.info(f"XLSX file written to {path}")
