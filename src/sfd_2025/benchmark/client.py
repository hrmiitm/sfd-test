"""Bounded concurrent OpenRouter-compatible calls with auditable usage."""
import asyncio
import json
import time
from datetime import datetime, timezone
import httpx


class ChatClient:
    def __init__(self, url, key, concurrency, audit):
        self.url, self.key, self.audit = url, key, audit
        self.client = httpx.AsyncClient(timeout=180, headers={'Authorization': f'Bearer {key}'})
        self.semaphore = asyncio.Semaphore(concurrency)
        self.catalog = {}

    async def preflight(self, model_ids):
        response = await self.client.get(self.url.rsplit('/chat/completions', 1)[0] + '/models')
        response.raise_for_status()
        self.catalog = {m['id']: m for m in response.json()['data']}
        missing = set(model_ids) - self.catalog.keys()
        if missing:
            raise ValueError(f'Models unavailable: {sorted(missing)}; no automatic substitutions')

    async def chat(self, model, messages, tag, max_tokens=3000, tools=None, tool_choice=None):
        payload = dict(model=model, messages=messages, max_tokens=max_tokens,
                       reasoning={'effort': 'low'}, usage={'include': True})
        if tools is not None:
            payload['tools'] = tools
            payload['tool_choice'] = tool_choice or 'auto'
        if 'temperature' in self.catalog[model].get('supported_parameters', []):
            payload['temperature'] = 0
        async with self.semaphore:
            started = time.perf_counter()
            for attempt in range(5):
                try:
                    response = await self.client.post(self.url, json=payload)
                    if response.status_code == 429 or response.status_code >= 500:
                        if attempt < 4:
                            await asyncio.sleep(min(30, 2 ** attempt)); continue
                    if not response.is_success:
                        raise ValueError(f'Chat HTTP {response.status_code}: {response.text[:500].replace(self.key, "[REDACTED]")}')
                    body = response.json()
                    usage = body.get('usage', {})
                    record = dict(timestamp_utc=datetime.now(timezone.utc).isoformat(),
                                  parameters={k:payload[k] for k in ('reasoning','temperature','max_tokens','tool_choice') if k in payload},
                                  tag=tag, requested_model=model, actual_model=body.get('model'),
                                  response_id=body.get('id'), usage=usage,
                                  seconds=time.perf_counter()-started, attempts=attempt+1)
                    with self.audit.open('a') as f:
                        f.write(json.dumps(record) + '\n')
                    choice = body.get('choices', [{}])[0]
                    message = choice.get('message', {})
                    content = message.get('content')
                    tool_calls = message.get('tool_calls', [])
                    if choice.get('finish_reason') == 'length' or (not tool_calls and (not isinstance(content, str) or not content.strip())):
                        raise ValueError(f'Empty or truncated response for {tag}; usage saved in requests.jsonl')
                    return dict(text=content or '', message=message, tool_calls=tool_calls, **record)
                except httpx.TransportError:
                    if attempt == 4:
                        raise
                    await asyncio.sleep(2 ** attempt)

    async def close(self):
        await self.client.aclose()
