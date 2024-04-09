from __future__ import annotations

import common_package.constants as constants
import common_package.db_utils as db_utils
import mysql.connector
import pandas as pd
from beartype import beartype
from beartype.typing import Callable, Dict, List
from loguru import logger

TIMEOUT = 5


class DBUpdater:
    def __init__(self, deploy_type=None, table_name=None) -> None:
        self.params: Dict = db_utils.get_kiki_db_params(deploy_type)
        self.table_name: str = table_name
        self.buffer: pd.DataFrame = pd.DataFrame()

    def set_table_name(self, table_name: str) -> DBUpdater:
        self.table_name = table_name
        return self

    def add_values(self, values: Dict) -> DBUpdater:
        """
        add_values will add data to buffer
        """
        if len(self.buffer) == 0:
            self.buffer = pd.DataFrame([values])
        else:
            self.buffer.loc[len(self.buffer)] = values
        return self

    @staticmethod
    @beartype
    def _get_upsert_query(
        table_name: str, keys: List[str], on_duplicate_key_update: List[str] = None
    ):
        keys = [f"`{key}`" for key in keys]
        dummy_values = ["%s" for _ in range(len(keys))]

        if on_duplicate_key_update is None or len(on_duplicate_key_update) == 0:
            # insert ignore
            query = (
                f"""INSERT IGNORE INTO {table_name} ({','.join(keys)})"""
                f""" VALUES ({','.join(dummy_values)});"""
            )

        else:
            on_duplicate_key_update = [
                f"`{key}`=values(`{key}`)" for key in on_duplicate_key_update
            ]

            query = (
                f"""INSERT INTO {table_name} ({','.join(keys)})"""
                f""" VALUES ({','.join(dummy_values)})"""
                f""" ON DUPLICATE KEY UPDATE {','.join(on_duplicate_key_update)};"""
            )

        return query

    @beartype
    def do_upsert(self):
        """
        do_upsert will run insert or update query from data in buffer
        """
        assert (
            self.table_name is not None
        ), "Table name must be set before doing upsert!!!"
        assert len(self.buffer) > 0, "Buffer must not be empty!!!"

        keys = self.buffer.keys().tolist()
        values = self.buffer.values.tolist()

        query = __class__._get_upsert_query(self.table_name, keys, keys)

        logger.debug(f"Upsert query: {query}")
        with mysql.connector.connect(**self.params, connection_timeout=TIMEOUT) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, values)
                conn.commit()

                logger.info(
                    f"Upsert query {query} with {len(values)} rows successfully!"
                )

    @beartype
    def clean_rows(self, filt_func: Callable[[pd.Series], bool]):
        current_rows = self.buffer.shape[0]
        self.buffer = self.buffer[~self.buffer.apply(filt_func, axis=1)]
        self.buffer = self.buffer.reset_index(drop=True)
        logger.info("Cleaned rows: %s", current_rows - self.buffer.shape[0])


if __name__ == "__main__":
    DBUpdater(deploy_type="prod", table_name="test_table").add_values(
        {"a": 1, "b": 2}
    ).add_values({"a": 3, "b": 4}).add_values({"a": 5, "b": 6}).do_upsert()
