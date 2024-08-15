import pandas as pd
from beartype.typing import Dict, List, Optional, Tuple, Union
from loguru import logger
from tqdm import tqdm

from pyspeedy.models.config import config
from pyspeedy.models.embedders.embedder import Embedder
from pyspeedy.models.embedders.pretrain_embedder import PretrainEmbedder
from pyspeedy.models.embedders.tei_embedder import TEIEmbedder


def deduplicate_text(
    texts: List[str],
    embedder: Optional[Embedder] = None,
    threshold: float = 0.8,
    batch_size: int = 16,
    return_duplication: bool = False,
    disable_tqdm: bool = False,
) -> Union[List[str], Tuple[List[str], Dict]]:
    """Deduplicate a vocabulary.

    Args:
        texts (List[str]): List of texts to deduplicate.
        embedder (Optional[Embedder], optional): Embedder to use. Defaults to None.
        threshold (float, optional): Threshold to consider two texts as duplicate. Defaults to 0.8.
        return_duplication (bool, optional): Return duplication dictionary. Defaults to False.

    Returns:
        Return List[str] if return_duplication is False else Tuple[List[str], Dict]
    """
    
    if embedder is None:
        if config.embedder_endpoint:
            embedder = TEIEmbedder()
        else:
            embedder = PretrainEmbedder()

    df = pd.DataFrame(texts, columns=["text"])
    org_len = len(df)
    df = df[df.text.str.strip() != ""]
    df.reset_index(drop=True, inplace=True)

    # embed and calculate similarity
    logger.info(f"Embedding {len(df)} texts")
    embeddings = embedder.embed(
        Embedder.Request(inputs=df.text.values.tolist()),
        return_format="np",
        batch_size=batch_size if len(df) > batch_size else 1,
        disable_tqdm=disable_tqdm,
    ).embedding
    scores = embeddings @ embeddings.T

    # deduplicate by comparing
    logger.info(f"Deduplicating texts with threshold {threshold}")
    dup_ids = []
    dup_dict = {}
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing", ncols=100, disable=disable_tqdm):
        dup_df = df.loc[(df.index > idx) & (scores[idx, ...] >= threshold).tolist()]

        if len(dup_df) > 0:
            dup_ids += dup_df.index.tolist()
            if return_duplication:
                dup_dict[row.text] = dup_dict.get(row.text, []) + dup_df.text.tolist()

    df = df[~df.index.isin(dup_ids)]
    logger.info(f"Removed {org_len - len(df)}/{org_len} texts")

    if return_duplication:
        return df.text.tolist(), dup_dict

    return df.text.tolist()


if __name__ == "__main__":
    embedder = None

    texts = ["1", "xin chào", "hello", "5", "1", ""]
    print(deduplicate_text(texts, embedder, threshold=0.8))
