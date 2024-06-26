import numpy as np
import torch
from beartype.typing import List, Literal
from FlagEmbedding import BGEM3FlagModel
from loguru import logger
from tqdm import tqdm

from pyspeedy.models.config import config
from pyspeedy.models.embedders.embedder import Embedder


class PretrainEmbedder(Embedder):
    def __init__(
        self,
        model_path: str = config.embedder_model_path,
        device: str = config.embedder_device,
        embedding_size: int = config.embedding_size,
    ) -> None:
        super().__init__()
        self.device = device
        self.embedding_size = embedding_size
        logger.info(f"Loading model from {model_path}")
        self.model = BGEM3FlagModel(model_path, use_fp16=True, device=device)

    def embed(
        self,
        request: Embedder.Request,
        return_format: Literal["list", "np", "pt"] = "list",
        batch_size=16,
        max_length=8192,
    ) -> Embedder.Response:

        texts = request.inputs
        emb: List[List[float]] = []
        with tqdm(
            total=len(texts),
            desc="Processing",
            unit="batch" if batch_size > 1 else "it",
            ncols=100,
        ) as pbar:
            for i in range(0, len(texts), batch_size):
                batch = texts[i : i + batch_size]
                emb += self.model.encode(
                    batch,
                    batch_size=batch_size,
                    max_length=max_length,
                )["dense_vecs"].tolist()

                pbar.update(len(batch))  # Update the progress bar

        if return_format == "np":
            emb = np.array(emb)
        elif return_format == "pt":
            emb = torch.tensor(emb, device="cpu")
        return Embedder.Response(embedding=emb)

    def get_embedding_size(self) -> int:
        return self.embedding_size


if __name__ == "__main__":

    request = Embedder.Request(inputs=["What is Deep Learning?"])
    response: Embedder.Response = PretrainEmbedder().embed(
        request, return_format="list"
    )
    print(response.embedding)
