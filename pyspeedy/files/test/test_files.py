import pandas as pd

from pyspeedy.files.file_concrete.csv import CSV
from pyspeedy.files.file_concrete.json import JSON, JSONL
from pyspeedy.files.file_concrete.txt import TXT
from pyspeedy.files.utils import *

DATA_FOLDER = "data/*"

CSV_DUMMY_PATH = "data/dummy.csv"
JSON_DUMMY_PATH = "data/dummy.json"
JSONL_DUMMY_PATH = "data/dummy.jsonl"
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

    assert isinstance(json.read(JSON_DUMMY_PATH, format="dataframe"), pd.DataFrame)

    data = json.read(JSON_DUMMY_PATH)
    assert isinstance(data, list) or isinstance(data, dict)

    # Test write
    json.write(data, "outputs/test_dummy.json")


def test_file_jsonl():
    # Test read
    json = JSONL()

    assert isinstance(json.read(JSONL_DUMMY_PATH, format="dataframe"), pd.DataFrame)

    data = json.read(JSONL_DUMMY_PATH)
    assert isinstance(data, list) or isinstance(data, dict)

    # Test write
    json.write(data, "outputs/test_dummy.jsonl")


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
    data = load_file_by_ext(JSONL_DUMMY_PATH, format="dataframe")
    assert isinstance(data, pd.DataFrame)

    # Test TXT
    data = load_file_by_ext(TXT_DUMMY_PATH)
    assert isinstance(data, list)


def test_load_by_ext():
    data = load_by_ext(DATA_FOLDER)
    assert (
        isinstance(data, list)
        or isinstance(data, dict)
        or isinstance(data, pd.DataFrame)
    )


def test_write_by_ext():
    data = load_file_by_ext(CSV_DUMMY_PATH)
    path = "outputs/test_dummy.csv"
    written_path = write_by_ext(data, path, to_overwrite=True)
    assert os.path.exists(written_path) and written_path == path

    data = load_file_by_ext(JSON_DUMMY_PATH)
    path = "outputs/test_dummy.json"
    written_path = write_by_ext(data, path, to_overwrite=True)
    assert os.path.exists(written_path) and written_path == path

    data = load_file_by_ext(JSONL_DUMMY_PATH)
    path = "outputs/test_dummy.jsonl"
    written_path = write_by_ext(data, path, to_overwrite=True)
    assert os.path.exists(written_path) and written_path == path

    data = load_file_by_ext(TXT_DUMMY_PATH)
    path = "outputs/test_dummy.txt"
    written_path = write_by_ext(data, path, to_overwrite=True)
    assert os.path.exists(written_path) and written_path == path
