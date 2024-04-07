import pandas as pd
from beartype import beartype
from beartype.typing import Literal, Union
from loguru import logger

from pyspeedy.files import File


class TXT(File):
    @beartype
    def read(
        self, path: str, format: Literal["list, dataframe"] = "list", **kwargs
    ) -> Union[pd.DataFrame, list]:
        logger.info(f"Reading TXT file from {path}")
        df: pd.DataFrame = pd.read_fwf(path, names=["value"])
        logger.info(f"TXT file read from {path}")

        if format == "list":
            return df.value.tolist()

        return df

    @beartype
    def write(
        self,
        data: Union[pd.DataFrame, list[str]],
        path: str,
        encoding: str = "utf-8",
        newline: str = "\n",
        **kwargs,
    ) -> None:
        logger.info(f"Writing TXT file to {path}")
        if isinstance(data, pd.DataFrame):
            data = data.value.tolist()

        with open(path, "w", encoding=encoding) as f:
            for idx, line in enumerate(data):
                f.write(line + newline if idx < len(data) - 1 else line)

        logger.info(f"TXT file written to {path}")
