import pandas as pd
from beartype.typing import List, Optional
from tqdm import tqdm

from pyspeedy.models.embedders.embedder import Embedder
from pyspeedy.models.embedders.pretrain_embedder import PretrainEmbedder


def deduplicate_text(
    texts: List[str], embedder: Optional[Embedder] = None, threshold: float = 0.8
):
    """Deduplicate a vocabulary."""

    if embedder is None:
        embedder = PretrainEmbedder()

    df = pd.DataFrame(texts, columns=["text"])

    embeddings = embedder.embed(
        Embedder.Request(inputs=texts), return_format="np"
    ).embedding
    scores = embeddings @ embeddings.T

    dup_ids = []
    for idx, row in tqdm(df.iterrows(), total=len(df)):
        dup_df = df.loc[(df.index > idx) & (scores[idx, ...] >= threshold).tolist()]

        if len(dup_df) > 0:
            dup_ids += dup_df.index.tolist()

    df = df[~df.index.isin(dup_ids)]
    return df.text.tolist()


if __name__ == "__main__":
    embedder = None

    texts = ["1", "xin chào", "hello", "5", "1"]
    print(deduplicate_text(texts, embedder, threshold=0.8))
