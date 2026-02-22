import json
import os

from openai import OpenAI

_client = None


def get_client():
    global _client
    if _client is None:
        _client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return _client


def chat(system_prompt, user_prompt, model="gpt-4.1", temperature=0, max_tokens=4096):
    client = get_client()
    resp = client.chat.completions.create(
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    return resp.choices[0].message.content


def chat_json(system_prompt, user_prompt, model="gpt-4.1", temperature=0):
    client = get_client()
    resp = client.chat.completions.create(
        model=model,
        temperature=temperature,
        max_tokens=4096,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    return json.loads(resp.choices[0].message.content)
