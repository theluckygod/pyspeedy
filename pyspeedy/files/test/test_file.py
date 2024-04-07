import pandas as pd

CSV_DUMMY_PATH = "data/dummy.csv"
JSON_DUMMY_PATH = "data/dummy.json"


def test_file_csv():
    from pyspeedy.files.csv import CSV

    # Test read
    csv = CSV()
    data = csv.read(CSV_DUMMY_PATH)
    assert isinstance(data, pd.DataFrame)

    # Test write
    csv.write(data, "outputs/test_dummy.csv")


def test_file_json():
    from pyspeedy.files.json import JSON

    # Test read
    json = JSON()

    assert isinstance(json.read(JSON_DUMMY_PATH, "dataframe"), pd.DataFrame)

    data = json.read(JSON_DUMMY_PATH)
    assert isinstance(data, list) or isinstance(data, dict)

    # Test write
    json.write(data, "outputs/test_dummy.json")


def test_load_file_by_ext():
    from pyspeedy.files.utils import load_file_by_ext

    # Test CSV
    data = load_file_by_ext(CSV_DUMMY_PATH)
    assert isinstance(data, pd.DataFrame)

    # Test JSON
    data = load_file_by_ext(JSON_DUMMY_PATH)
