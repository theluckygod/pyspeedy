import pytest
from httpx import AsyncClient, Response
from service import classifier  # Example module to test

HAPPY_CASE_PROMPTS = [
    ("Hello", False, False),
    ("Xin chào", False, False),
    ("Lỗi nồng độ cồn khi đi xe máy", True, True),
    ("nồng độ cồn xe đạp", True, True),
    ("Ok", False, False),
    ("thật không", False, False),
    ("17 tuổi có được thi bằng ô tô", False, True),
    ("mức phạt cho nồng độ cồn xe đạp cao", True, True),
    ("cảm ơn", False, False),
    ("mày chắc chắn không", False, False),
    ("làm thơ về mùng 8/3", False, False),
    ("🤑", False, False),
    ("🥲", False, False),
    ("Thủ tục lấy lại bằng lái sau khi nộp phạt online", True, True),
    ("Loại xe mà người đủ 18 tuổi có thể lái", False, True),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("question,is_in_whitelist,is_legal", HAPPY_CASE_PROMPTS)
async def test_classifier(
    async_client: AsyncClient,
    question: str,
    is_in_whitelist: bool | None,
    is_legal: bool,
):
    assert classifier.is_in_whitelist(question=question) == is_in_whitelist
    response: Response = await classifier.classify(
        client=async_client,
        question=question,
    )
    assert classifier.is_legal(response) == is_legal
