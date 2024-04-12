from __future__ import annotations

from beartype.typing import Dict, List, Literal
from pydantic import AliasChoices, BaseModel, Field


class Chat(BaseModel):
    messages: List[Message]

    def to_huggingface_format(self) -> List[Dict]:
        return [
            {"role": message.role, "content": message.content}
            for message in self.messages
        ]

    def to_qwen_format(self) -> List[Dict]:
        return [
            {"from": message.role, "value": message.content}
            for message in self.messages
        ]


class Message(BaseModel):
    role: Literal["user", "assistant", "system"] = Field(
        validation_alias=AliasChoices("from", "role")
    )
    content: str = Field(validation_alias=AliasChoices("value", "content"))
