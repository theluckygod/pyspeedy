import common_package.constants as constants
from elasticsearch import Elasticsearch, NotFoundError, helpers
from tqdm import tqdm

MUSIC_INDEX = "music_1.2"
config = {
    "elasticsearch-stg": "10.30.78.101:9200",
    "elasticsearch-prod": "10.30.78.48:9200",
}
deploy = constants.DEPLOY_TYPE


def get(es, id):
    try:
        ret = es.get(MUSIC_INDEX, id)
        return ret["_source"]
    except NotFoundError:
        return None


def fill_listen_to_df(df, deploy_type, id_field="id"):
    es = Elasticsearch(config[f"elasticsearch-{deploy_type}"])
    df["listens"] = None

    for idx, row in tqdm(df.iterrows(), total=len(df)):
        id = row[id_field]

        doc = get(es, id)
        if doc is not None:
            df.loc[idx, "listens"] = doc["listens"]
    return df


def search(es, query, index=MUSIC_INDEX):
    return es.search(index=index, body=query)


def build_doc(row, KEY):
    doc = {}
    for key in row.keys():
        if key == KEY or (key == "item_id" and not isinstance(row[key], str)):
            continue
        doc[key] = row[key]
    return doc


def bulk_upsert(es, index, key, df: pd.DataFrame):
    def gendata(df):
        for _, row in df.iterrows():
            yield {
                "_op_type": "update",
                "_index": index,
                "_id": row[key],
                "doc": build_doc(row),
                "doc_as_upsert": True,
            }

    helpers.bulk(es, gendata(df))


if __name__ == "__main__":
    import pandas as pd

    df = pd.read_csv("/home/lap14351/Downloads/block_keyword_2023-03-08.csv")
    df = fill_listen_to_df(df, deploy_type="stg", id_field="Music ID")
    df.to_csv("/home/lap14351/Downloads/fill_listens_block_keyword_2023-03-08.csv")
