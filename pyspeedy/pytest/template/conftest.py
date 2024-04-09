import pytest_asyncio

from typing import Any, AsyncGenerator

from httpx import AsyncClient

from main import app


@pytest_asyncio.fixture
async def client() -> AsyncGenerator[AsyncClient, Any]:
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture
async def async_client() -> AsyncGenerator[AsyncClient, Any]:
    async with AsyncClient() as client:
        yield client
