from beartype.typing import Dict, List

from pyspeedy.text import json_loads_corrector

texts: List[str] = [
    "{'model': 'qwen2.5-zai', 'prompt': '<|im_start|>system\\\\nResponse {\\\"is_answer\\\": \\\"yes\\\"} hoặc {\\\"is_answer\\\": \\\"no\\\"}.\\\\n\\\\n<|im_end|>\\\\n<|im_start|>assistant\\\\n{\\\"is_answer\\\": \\\"', 'max_tokens': 1, 'top_k': 1, 'stream': False, 'repetition_penalty': 1.0, 'logprobs': True, 'top_logprobs': 2}",
    "[{'from': 'user', 'value': 'Expectation Format\\n## Mở bài: ... ## Nội dung: ... \"có thể\" \\n[doc-id], [doc-id] **Content 1**: ... \\n[doc-id] **Content 2**: ...\\n...\\n## Kết bài\\n...\\n\\n\\n# Other expectation format (if needed)\\\\n\\\\n```\\n\\n---\\nFollow these notes to answer:\\nAnswer by following provided format.'}, {'from': 'assistant', 'value': '## No related document found'}]",
]
results: List[Dict] = [
    '{"model": "qwen2.5-zai", "prompt": "<|im_start|>system\\nResponse {\\"is_answer\\": \\"yes\\"} hoặc {\\"is_answer\\": \\"no\\"}.\\n\\n<|im_end|>\\n<|im_start|>assistant\\n{\\"is_answer\\": \\"", "max_tokens": 1, "top_k": 1, "stream": false, "repetition_penalty": 1.0, "logprobs": true, "top_logprobs": 2}',
    '[{"from": "user", "value": "Expectation Format\\n## Mở bài: ... ## Nội dung: ... \\"có thể\\" \\n[doc-id], [doc-id] **Content 1**: ... \\n[doc-id] **Content 2**: ...\\n...\\n## Kết bài\\n...\\n\\n\\n# Other expectation format (if needed)\\n\\n```\\n\\n---\\nFollow these notes to answer:\\nAnswer by following provided format."}, {"from": "assistant", "value": "## No related document found"}]',
]


def test_json_loads_corrector():
    for text, result in zip(texts, results):
        assert json_loads_corrector(text, return_dict=False) == result
