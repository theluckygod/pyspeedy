from beartype.typing import Dict, List
from IPython.core.display import HTML

from pyspeedy.llm import *


def test_parse_message():
    # Huggingface format
    messages = [
        {"role": "user", "content": "Hello, how are you?"},
        {"role": "assistant", "content": "I'm doing great. How can I help you today?"},
        {"role": "user", "content": "I'd like to show off how chat templating works!"},
    ]
    Chat(messages=messages)

    # Qwen format
    messages = [
        {"from": "user", "value": "Hello, how are you?"},
        {"from": "assistant", "value": "I'm doing great. How can I help you today?"},
        {"from": "user", "value": "I'd like to show off how chat templating works!"},
    ]
    Chat(messages=messages)


def test_get_chat_html():
    # Test
    messages: List[Dict] = [
        {"from": "user", "value": "You are a bot."},
        {"from": "user", "value": "Hello!"},
        {"from": "assistant", "value": "Hi! How can I help you today?"},
        {"from": "user", "value": "I need help with something."},
        {"from": "assistant", "value": "Sure! What do you need help with?"},
    ]
    html = get_chat_html(messages)
    assert isinstance(html, HTML)
