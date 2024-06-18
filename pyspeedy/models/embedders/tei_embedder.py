import numpy as np
import requests
import torch
from beartype.typing import List, Literal

from pyspeedy.models.config import config
from pyspeedy.models.embedders.embedder import Embedder


class TEIEmbedder(Embedder):
    def __init__(
        self,
        endpoint: str = config.embedder_endpoint,
        embedding_size: int = config.embedding_size,
    ) -> None:
        super().__init__()
        self.endpoint = endpoint
        self.embedding_size = embedding_size

    def embed(
        self,
        request: Embedder.Request,
        return_format: Literal["list", "np", "pt"] = "list",
    ) -> Embedder.Response:
        response = requests.post(self.endpoint, json=request.dict())
        response.raise_for_status()

        emb: List[List[float]] = response.json()
        if return_format == "np":
            emb = np.array(emb)
        elif return_format == "pt":
            emb = torch.tensor(emb, device="cpu")
        return Embedder.Response(embedding=emb)

    def get_embedding_size(self) -> int:
        return self.embedding_size


if __name__ == "__main__":

    request = Embedder.Request(inputs=["What is Deep Learning?"])
    response: Embedder.Response = TEIEmbedder().embed(request)
    print(response.embedding)
