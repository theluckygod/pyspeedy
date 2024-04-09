from pyspeedy.airflow import *


def test_dump_settings():
    dump_template("tests", to_overwrite=True, test_mode=True)
