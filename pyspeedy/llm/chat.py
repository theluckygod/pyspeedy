from __future__ import annotations

from beartype.typing import List, Literal
from pydantic import AliasChoices, BaseModel, Field


class Chat(BaseModel):
    messages: List[Message]


class Message(BaseModel):
    from_: Literal["user", "assistant", "system"] = Field(
        validation_alias=AliasChoices("from", "role")
    )
    value: str = Field(validation_alias=AliasChoices("value", "content"))
