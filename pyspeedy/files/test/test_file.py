import pandas as pd

from pyspeedy.files import *

CSV_DUMMY_PATH = "data/dummy.csv"
JSON_DUMMY_PATH = "data/dummy.json"
TXT_DUMMY_PATH = "data/dummy.txt"


def test_file_csv():
    # Test read
    csv = CSV()
    data = csv.read(CSV_DUMMY_PATH)
    assert isinstance(data, pd.DataFrame)

    # Test write
    csv.write(data, "outputs/test_dummy.csv")


def test_file_json():
    # Test read
    json = JSON()

    assert isinstance(json.read(JSON_DUMMY_PATH, "dataframe"), pd.DataFrame)

    data = json.read(JSON_DUMMY_PATH)
    assert isinstance(data, list) or isinstance(data, dict)

    # Test write
    json.write(data, "outputs/test_dummy.json")


def test_file_txt():
    # Test read
    txt = TXT()
    data = txt.read(TXT_DUMMY_PATH)
    assert isinstance(data, list)

    # Test write
    txt.write(data, "outputs/test_dummy.txt")


def test_load_file_by_ext():
    # Test CSV
    data = load_file_by_ext(CSV_DUMMY_PATH)
    assert isinstance(data, pd.DataFrame)

    # Test JSON
    data = load_file_by_ext(JSON_DUMMY_PATH)
    assert isinstance(data, list) or isinstance(data, dict)

    # Test TXT
    data = load_file_by_ext(TXT_DUMMY_PATH)
    assert isinstance(data, list)
