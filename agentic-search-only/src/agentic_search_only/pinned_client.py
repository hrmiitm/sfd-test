"""Provider-pinned OpenRouter calls; durable raw payloads for EVERY attempt."""
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import time
import uuid

import httpx


def now():
    return datetime.now(timezone.utc).isoformat()


def save(path, value):
    path = Path(path)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str))
    tmp.replace(path)


def append(path, value):
    with Path(path).open('a') as stream:
        stream.write(json.dumps(value, ensure_ascii=False, default=str) + '\n')


class PinnedChatClient:
    def __init__(self, url, key, output, configs, concurrency=4, attempts=3):
        self.url, self.key, self.output = url, key, Path(output)
        self.configs = {c['model']: c for c in configs}
        self.semaphore = asyncio.Semaphore(concurrency)
        self.attempts = attempts
        self.client = httpx.AsyncClient(timeout=180, headers={'Authorization': f'Bearer {key}'})
        self.catalog, self.endpoints = {}, {}
        (self.output / 'raw').mkdir(parents=True, exist_ok=True)

    def redact(self, text):
        return text.replace(self.key, '[REDACTED]') if self.key else text

    def save_raw(self, path, value):
        # Preserve returned fields, except accidental echoes of our credential.
        save(path, json.loads(self.redact(json.dumps(value, default=str))))

    async def get_metadata(self, url):
        call_id = uuid.uuid4().hex
        record = dict(call_id=call_id, method='GET', url=url, started_utc=now(),
                      billing='not an inference request')
        self.save_raw(self.output / 'raw' / (call_id + '.request.json'), record)
        started = time.perf_counter()
        try:
            response = await self.client.get(url)
            try:
                body = response.json()
            except ValueError:
                body = {'non_json_body': response.text}
            self.save_raw(self.output / 'raw' / (call_id + '.response.json'),
                          dict(status_code=response.status_code, body=body))
            record.update(status_code=response.status_code, status='ok' if response.is_success else 'error')
            response.raise_for_status()
            return body
        except Exception as error:
            record.update(status='error', error=self.redact(str(error)))
            raise
        finally:
            record.update(finished_utc=now(), seconds=time.perf_counter() - started)
            append(self.output / 'metadata-requests.jsonl', record)

    async def preflight(self):
        base = self.url.rsplit('/chat/completions', 1)[0]
        catalog = await self.get_metadata(base + '/models')
        self.catalog = {m['id']: m for m in catalog['data']}
        for model, config in self.configs.items():
            if model not in self.catalog:
                raise ValueError(f'Model unavailable: {model}; no substitutions')
            body = await self.get_metadata(base + '/models/' + model + '/endpoints')
            save(self.output / ('endpoints-' + model.replace('/', '--') + '.json'), body)
            endpoints = body['data']['endpoints']
            selected = [e for e in endpoints if e.get('tag') == config['provider'] and e.get('status', 0) == 0]
            if not selected:
                raise ValueError(f"Pinned endpoint unavailable: {model} / {config['provider']}")
            endpoint = selected[0]
            self.endpoints[model] = endpoint
            supported = endpoint.get('supported_parameters', [])
            if not {'reasoning', 'max_tokens'} <= set(supported):
                raise ValueError(f'Pinned endpoint does not support reasoning/max_tokens: {model}')
            if config.get('role') != 'judge' and not {'tools', 'tool_choice'} <= set(supported):
                raise ValueError(f'Pinned answer endpoint has no tool support: {model}')
            efforts = self.catalog[model].get('reasoning', {}).get('supported_efforts')
            if efforts and config['reasoning_effort'] not in efforts:
                raise ValueError(f'Unsupported reasoning effort for {model}')
        save(self.output / 'models.json', {m: self.catalog[m] for m in self.configs})
        save(self.output / 'model-pins.json', [dict(**c, endpoint=self.endpoints[m],
            temperature_sent='temperature' in self.endpoints[m].get('supported_parameters', []))
            for m, c in self.configs.items()])

    async def chat(self, model, messages, tag, max_tokens=4000, tools=None, tool_choice=None):
        config = self.configs[model]
        payload = dict(model=model, messages=messages, max_tokens=max_tokens,
            reasoning={'effort': config['reasoning_effort']}, usage={'include': True},
            provider={'only': [config['provider']], 'order': [config['provider']],
                      'allow_fallbacks': False, 'require_parameters': True})
        supported = self.endpoints[model].get('supported_parameters', [])
        if 'temperature' in supported:
            payload['temperature'] = config['temperature']
        if tools is not None:
            payload.update(tools=tools, tool_choice=tool_choice or 'auto')
        queued = time.perf_counter()
        async with self.semaphore:
            queue_seconds = time.perf_counter() - queued
            for attempt in range(1, self.attempts + 1):
                call_id = uuid.uuid4().hex
                raw = self.output / 'raw' / call_id
                # Saved before sending; an interrupted in-flight request is visible.
                record = dict(call_id=call_id, tag=tag, attempt=attempt, started_utc=now(),
                    requested_model=model, requested_provider=config['provider'],
                    parameters={k: v for k, v in payload.items() if k not in ('messages', 'tools')},
                    temperature_requested=config['temperature'], temperature_sent=payload.get('temperature'),
                    temperature_omitted_reason=None if 'temperature' in payload else 'unsupported by pinned endpoint',
                    queue_seconds=queue_seconds, request_file=f'raw/{call_id}.request.json',
                    response_file=f'raw/{call_id}.response.json')
                self.save_raw(raw.with_suffix('.request.json'), dict(url=self.url, method='POST', **record, body=payload))
                started = time.perf_counter()
                body, response, failure, choice = None, None, None, {}
                try:
                    response = await self.client.post(self.url, json=payload)
                    try:
                        body = response.json()
                    except ValueError:
                        body = {'non_json_body': self.redact(response.text)}
                    self.save_raw(raw.with_suffix('.response.json'), dict(status_code=response.status_code,
                        headers={k: v for k, v in response.headers.items()
                                 if k.lower() in ('content-type', 'date', 'x-request-id', 'retry-after')
                                 or k.lower().startswith(('x-ratelimit', 'x-openrouter'))}, body=body))
                    if not isinstance(body, dict):
                        body = {'invalid_response_body': body}
                    if not response.is_success or body.get('error'):
                        failure = f'HTTP {response.status_code}: {self.redact(str(body))[:1000]}'
                    else:
                        choices = body.get('choices') or [{}]
                        choice = choices[0]
                        message = choice.get('message') or {}
                        content, calls = message.get('content'), message.get('tool_calls') or []
                        actual_model = body.get('model')
                        permitted_models = {model, self.catalog.get(model, {}).get('canonical_slug')}
                        if actual_model not in permitted_models:
                            failure = f'Unexpected actual model: {actual_model}'
                        actual_provider = body.get('provider')
                        expected_provider = self.endpoints[model].get('provider_name')
                        if actual_provider and expected_provider and actual_provider != expected_provider:
                            failure = f'Provider pin mismatch: {actual_provider} != {expected_provider}'
                        if choice.get('finish_reason') == 'length' or (not calls and (not isinstance(content, str) or not content.strip())):
                            failure = 'Empty or truncated model response'
                except httpx.TransportError as error:
                    failure = self.redact(f'{type(error).__name__}: {error}')
                    self.save_raw(raw.with_suffix('.response.json'), dict(transport_error=failure,
                        billing_status='unknown; request may have reached provider'))
                except (ValueError, KeyError, IndexError, TypeError, AttributeError) as error:
                    failure = self.redact(f'Malformed response: {type(error).__name__}: {error}')
                record.update(finished_utc=now(), seconds=time.perf_counter() - started,
                    status_code=response.status_code if response is not None else None,
                    actual_model=body.get('model') if body else None,
                    actual_provider=body.get('provider') if body else None,
                    provider_verification='reported_match' if body and body.get('provider') and not failure else
                        'reported_mismatch_or_failed' if failure else 'not_reported',
                    response_id=body.get('id') if body else None,
                    usage=body.get('usage', {}) if body else {},
                    finish_reason=choice.get('finish_reason') if isinstance(choice, dict) else None,
                    status='error' if failure else 'ok', error=failure)
                append(self.output / 'requests.jsonl', record)
                if not failure:
                    return dict(text=content or '', message=message, tool_calls=calls, **record)
                retryable = response is None or response.status_code in (408, 429) or response.status_code >= 500
                if not retryable or attempt == self.attempts:
                    raise ValueError(f'{tag}: {failure}; full attempt saved to {record["response_file"]}')
                await asyncio.sleep(min(8, 2 ** (attempt - 1)))

    async def close(self):
        await self.client.aclose()
