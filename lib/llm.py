import json
import logging
import os

from openai import OpenAI

_openai_client = None
_groq_client = None
log = logging.getLogger(__name__)


def get_openai_client():
    global _openai_client
    if _openai_client is None:
        _openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return _openai_client


def get_groq_client():
    global _groq_client
    if _groq_client is None:
        _groq_client = OpenAI(
            api_key=os.getenv("GROQ_API_KEY"),
            base_url=os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1"),
        )
    return _groq_client


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
    return resp.choices[0].message.content


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
    return json.loads(resp.choices[0].message.content)


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


def groq_strict(system_prompt, user_prompt, schema, schema_name="response"):
    client = get_groq_client()
    model = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
    log.debug("Groq strict request, model=%s schema=%s", model, schema_name)
    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        max_tokens=16384,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": schema_name,
                "strict": True,
                "schema": schema,
            },
        },
    )
    return json.loads(resp.choices[0].message.content)
