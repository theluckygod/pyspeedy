import numpy as np
import torch
from beartype.typing import Literal
from FlagEmbedding import BGEM3FlagModel

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
        self.model = BGEM3FlagModel(model_path, use_fp16=True, device=device)

    def embed(
        self,
        request: Embedder.Request,
        return_format: Literal["list", "np", "pt"] = "list",
        batch_size=1,
        max_length=8192,
    ) -> Embedder.Response:
        emb: np.ndarray = self.model.encode(
            request.inputs,
            batch_size=batch_size,
            max_length=max_length,
        )["dense_vecs"]

        if return_format == "list":
            emb = emb.tolist()
        elif return_format == "pt":
            emb = torch.tensor(emb, device=self.device)
        return Embedder.Response(embedding=emb)

    def get_embedding_size(self) -> int:
        return self.embedding_size


if __name__ == "__main__":

    request = Embedder.Request(inputs=["What is Deep Learning?"])
    response: Embedder.Response = PretrainEmbedder().embed(
        request, return_format="list"
    )
    print(response.embedding)
