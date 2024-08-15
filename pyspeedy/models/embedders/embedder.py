from abc import ABC, abstractmethod

import numpy as np
import pydantic
import torch
from beartype.typing import List, Literal, Union


class Embedder(ABC):
    class Request(pydantic.BaseModel):
        inputs: List[str]

    class Response(pydantic.BaseModel):
        model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)
        embedding: Union[List[List[float]], torch.Tensor, np.ndarray]

    @abstractmethod
    def embed(
        self, request: Request, return_format: Literal["list", "np", "pt"] = "list"
    ) -> Response:
        pass

    @abstractmethod
    def get_embedding_size(self) -> int:
        pass
