from __future__ import annotations

from beartype.typing import Dict, List, Literal
from pydantic import AliasChoices, BaseModel, Field


class Chat(BaseModel):
    messages: List[Message]

    def to_huggingface_format(self) -> List[Dict]:
        return list(map(lambda message: message.to_huggingface_format(), self.messages))

    def to_qwen_format(self) -> List[Dict]:
        return list(map(lambda message: message.to_qwen_format(), self.messages))


class Message(BaseModel):
    role: Literal["user", "assistant", "system"] = Field(
        validation_alias=AliasChoices("from", "role")
    )
    content: str = Field(validation_alias=AliasChoices("value", "content"))

    def to_huggingface_format(self) -> Dict:
        return {"role": self.role, "content": self.content}

    def to_qwen_format(self) -> Dict:
        return {"from": self.role, "value": self.content}
