import json
import pandas as pd
from beartype import beartype
from beartype.typing import Union, Literal
from loguru import logger

from pyspeedy.files.file import File


class JSON(File):
    @beartype
    def read(
        self, path: str, format: Literal["dataframe", "dict"] = "dict", **kwargs
    ) -> Union[pd.DataFrame, dict, list[dict]]:
        logger.info(f"Reading JSON file from {path}")
        df: pd.DataFrame = pd.read_json(path, **kwargs)
        logger.info(f"JSON file read from {path}")

        if format == "dict":
            return df.to_dict("records")

        return df

    @beartype
    def write(
        self,
        data: Union[pd.DataFrame, dict, list[dict]],
        path: str,
        encoding: str = "utf-8",
        ensure_ascii: bool = False,
        indent: int = 4,
        **kwargs,
    ) -> None:
        logger.info(f"Writing JSON file to {path}")
        if isinstance(data, pd.DataFrame):
            data = data.to_dict("records")

        with open(path, "w", encoding=encoding) as f:
            json.dump(data, f, ensure_ascii=ensure_ascii, indent=indent, **kwargs)

        logger.info(f"JSON file written to {path}")
