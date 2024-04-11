from beartype import beartype
from beartype.typing import Union
from IPython.core.display import HTML

import pyspeedy.llm.constants as constants
from pyspeedy.llm.chat import Chat, Message


def _get_user_chat_html(
    content: str, title: str = "User", avatar: str = constants.user_avatar
) -> str:
    return constants.user_chat_html.format(title=title, avatar=avatar, message=content)


def _get_bot_chat_html(
    content: str, title: str = "Assistant", avatar: str = constants.bot_avatar
) -> str:
    return constants.bot_chat_html.format(title=title, avatar=avatar, message=content)


def _get_system_chat_html(
    content: str, title: str = "System", avatar: str = constants.system_avatar
) -> str:
    return constants.bot_chat_html.format(title=title, avatar=avatar, message=content)


@beartype
def _get_message_html(message: Message) -> str:
    content = message.value
    content = content.replace("\n", "<br>")

    role = message.from_

    if role == "user":
        return _get_user_chat_html(content)
    if role == "assistant":
        return _get_bot_chat_html(content)
    return _get_system_chat_html(content)


@beartype
def get_chat_html(chat_or_messages: Union[Chat, list[dict]]) -> HTML:
    if isinstance(chat_or_messages, Chat):
        chat = chat_or_messages
    else:
        chat = Chat(messages=chat_or_messages)

    messages: list[Message] = chat.messages
    messages_html: list[str] = [_get_message_html(message) for message in messages]
    html: str = constants.format_chat_html_template.substitute(
        chats="".join(messages_html)
    )
    return HTML(html)
