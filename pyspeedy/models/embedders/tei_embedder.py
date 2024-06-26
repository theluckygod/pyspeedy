import numpy as np
import requests
import torch
from beartype.typing import List, Literal
from tqdm import tqdm

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
        batch_size: int = 16,
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
                response = requests.post(self.endpoint, json={"inputs": batch})
                response.raise_for_status()
                emb += response.json()

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
    response: Embedder.Response = TEIEmbedder().embed(request)
    print(response.embedding)
