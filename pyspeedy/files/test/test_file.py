CSV_DUMMY_PATH = "data/dummy.csv"


def test_file_csv():
    from pyspeedy.files.csv import CSV

    # Test read
    csv = CSV()
    data = csv.read(CSV_DUMMY_PATH)
    assert type(data).__name__ == "DataFrame"

    # Test write
    csv.write(data, "outputs/test_dummy.csv")


def test_load_file_by_ext():
    from pyspeedy.files.utils import load_file_by_ext

    # Test CSV
    data = load_file_by_ext(CSV_DUMMY_PATH)
    assert type(data).__name__ == "DataFrame"
