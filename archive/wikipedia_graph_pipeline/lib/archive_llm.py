import json
import os

from openai import OpenAI


def chat_strict(system_prompt, user_prompt, schema, schema_name="response", model="gpt-4o-mini"):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")
    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        max_completion_tokens=16384,
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
    content = resp.choices[0].message.content
    if content is None:
        raise RuntimeError("OpenAI returned an empty message")
    return json.loads(content)
