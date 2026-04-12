import json
import logging
import os

from openai import OpenAI

_openai_client = None
log = logging.getLogger(__name__)


def get_openai_client():
    global _openai_client
    if _openai_client is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set")
        _openai_client = OpenAI(api_key=api_key)
    return _openai_client


def _require_message_content(message):
    content = message.content
    if content is None:
        raise RuntimeError("OpenAI returned an empty message")
    return content


def chat(system_prompt, user_prompt, model="gpt-5.2", temperature=0, max_tokens=4096):
    client = get_openai_client()
    log.debug("LLM chat request, model=%s", model)
    kwargs = {
        "model": model,
        "temperature": temperature,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    kwargs["max_completion_tokens"] = max_tokens
    resp = client.chat.completions.create(**kwargs)
    return _require_message_content(resp.choices[0].message)


def chat_json(system_prompt, user_prompt, model="gpt-5.2", temperature=0):
    client = get_openai_client()
    resp = client.chat.completions.create(
        model=model,
        temperature=temperature,
        max_completion_tokens=4096,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    return json.loads(_require_message_content(resp.choices[0].message))


def chat_with_tools(messages, tools, model="gpt-5.2", temperature=0):
    client = get_openai_client()
    log.debug("LLM tool-call request, model=%s, tools=%d", model, len(tools))
    resp = client.chat.completions.create(
        model=model,
        temperature=temperature,
        max_completion_tokens=4096,
        messages=messages,
        tools=tools,
    )
    return resp.choices[0].message
