import httpx
import pytest
from beartype.typing import Optional
from models.faq_model import (
    Response,
)  # Example pydantic.BaseModel of the response object

API_PATH = "https://services/faq/search?uid=pytest"

HAPPY_CASE_PROMPTS = [
    ("để tóc dài khi lái xe phạt bao nhiêu", "lỗi để tóc dài lái xe"),
    ("đeo tai nghe khi lái oto có bị phạt không", "lỗi đeo tai nghe khi lái ô tô"),
    ("Quên mang GPLX", "lỗi không mang theo giấy phép lái xe"),
    ("lái xe mà không có cà vẹt", "lỗi xe không có cà vẹt"),
    ("tôi lái ôtô mà chạy quá tốc độ 5km/h", None),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("question,document", HAPPY_CASE_PROMPTS)
async def test_classifier(
    async_client: httpx.AsyncClient, question: str, document: Optional[str]
):
    await async_client.post(url=API_PATH, json={"text": question}, timeout=2)
    assert response.status_code == 200

    response: Response = Response.parse_obj(response.json())
    predict = response.candidates[0].question if response.candidates else None
    assert predict == document
