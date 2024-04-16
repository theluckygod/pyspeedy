import markdown
from beartype import beartype
from beartype.typing import Dict, List, Union
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
def _get_message_html(message: Message, use_markdown: bool) -> str:
    content = message.content
    if use_markdown:
        content = markdown.markdown(content, extensions=["fenced_code"])

    role = message.role
    if role == "user":
        return _get_user_chat_html(content)
    if role == "assistant":
        return _get_bot_chat_html(content)
    return _get_system_chat_html(content)


@beartype
def get_chat_html(
    chat_or_messages: Union[Chat, List[Dict]], use_markdown: bool = True
) -> HTML:
    """Get the HTML representation of a chat.

    Args:
        chat_or_messages (Union[Chat, List[Dict]]): A Chat object or a list of messages.
        use_markdown (bool, optional): Whether to use markdown for the messages. Defaults to True.

    Returns:
        HTML: The HTML representation of the chat.
    """

    if isinstance(chat_or_messages, Chat):
        chat = chat_or_messages
    else:
        chat = Chat(messages=chat_or_messages)

    messages: List[Message] = chat.messages
    messages_html: List[str] = [
        _get_message_html(message, use_markdown) for message in messages
    ]
    html: str = constants.format_chat_html_template.substitute(
        chats="".join(messages_html)
    )
    return HTML(html)
