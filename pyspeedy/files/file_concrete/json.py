import json

import pandas as pd
from beartype import beartype
from beartype.typing import Dict, List, Literal, Union
from loguru import logger

from pyspeedy.files.file_concrete.file import File
from pyspeedy.functional.inspect_kwargs import ignore_unmatched_kwargs


class JSON(File):
    @beartype
    def read(
        self, path: str, format: Literal["dataframe", "dict"] = "dict", **kwargs
    ) -> Union[pd.DataFrame, Dict, List[Dict]]:
        logger.info(f"Reading JSON file from {path}")
        df: pd.DataFrame = ignore_unmatched_kwargs(pd.read_json)(path, **kwargs)
        logger.info(f"JSON file read from {path}")

        if format == "dict":
            return df.to_dict("records")

        return df

    @beartype
    def write(
        self,
        data: Union[pd.DataFrame, Dict, List[Dict]],
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


class JSONL(JSON):
    @beartype
    def read(
        self, path: str, format: Literal["dataframe", "list"] = "list", **kwargs
    ) -> Union[pd.DataFrame, List[Dict]]:
        def load_jsonl(filename):
            with open(filename) as fd:
                for line in fd:
                    yield json.loads(line)

        df: pd.DataFrame = pd.DataFrame(load_jsonl(path))

        if format == "list":
            return df.to_dict("records")

        return df

    @beartype
    def write(
        self,
        data: Union[pd.DataFrame, Dict, List[Dict]],
        path: str,
        encoding: str = "utf-8",
        ensure_ascii: bool = False,
        **kwargs,
    ) -> None:
        logger.info(f"Writing JSONL file to {path}")
        if isinstance(data, pd.DataFrame):
            data = data.to_dict("records")

        with open(path, "w", encoding=encoding) as f:
            for entry in data:
                json.dump(entry, f, ensure_ascii=ensure_ascii, indent=None, **kwargs)
                f.write("\n")

        logger.info(f"JSONL file written to {path}")
