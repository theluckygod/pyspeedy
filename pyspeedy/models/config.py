from pydantic_settings import BaseSettings


class Config(BaseSettings):
    ## embedder
    embedder_endpoint: str = "http://localhost:8000/embed"
    embedding_size: int = 1024
    embedder_model_path: str = "BAAI/bge-m3"
    embedder_device: str = "cpu"


config = Config()
