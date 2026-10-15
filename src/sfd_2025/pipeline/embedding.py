"""LLM Foundry OpenRouter-compatible embedding client (no chat calls)."""
import math
import base64
import struct
import random
import json
import logging
from datetime import datetime, timezone
import time
import httpx


class Embedder:
    def __init__(self, url, key, model, dimensions, client=None):
        if not key:
            raise ValueError('Set LLMFOUNDRY_API_KEY or LLMFOUNDRY_TOKEN in .env before embedding')
        self.metrics_path = None
        self.client = client or httpx.Client(timeout=120)
        self.url, self.key, self.model, self.dimensions = url, key, model, dimensions

    def embed(self, texts):
        # The gateway can estimate request tokens as characters / 4. Leave
        # headroom under its 300k request limit, even for whitespace-heavy text.
        if len(texts) > 1 and sum(len(t) for t in texts) > 1_000_000:
            middle = len(texts) // 2
            return self.embed(texts[:middle]) + self.embed(texts[middle:])
        started = time.perf_counter()
        for attempt in range(6):
            try:
                response = self.client.post(self.url,
                    headers={'Authorization': f'Bearer {self.key}'},
                    json={'model': self.model, 'input': texts,
                          'dimensions': self.dimensions, 'encoding_format': 'base64'})
                if response.status_code == 429 or response.status_code >= 500:
                    if attempt == 5: response.raise_for_status()
                    retry_after = response.headers.get('retry-after', '')
                    delay = float(retry_after) if retry_after.replace('.', '', 1).isdigit() else 2 ** attempt
                    time.sleep(min(120, delay) + random.uniform(0, 1)); continue
                if response.status_code == 400 and 'max_tokens_per_request' in response.text and len(texts) > 1:
                    logging.getLogger(__name__).warning('Provider request token limit; splitting batch of %s', len(texts))
                    middle = len(texts) // 2
                    return self.embed(texts[:middle]) + self.embed(texts[middle:])
                if not response.is_success:
                    message = response.text[:2000].replace(self.key, '[REDACTED]')
                    raise ValueError(f'Embedding API HTTP {response.status_code}: {message}')
                body = response.json()
                data = sorted(body['data'], key=lambda x: x['index'])
                if [x['index'] for x in data] != list(range(len(texts))):
                    raise ValueError('Embedding response has missing/duplicate indices')
                vectors = []
                for item in data:
                    value = item['embedding']
                    if isinstance(value, str):
                        raw = base64.b64decode(value, validate=True)
                        if len(raw) != self.dimensions * 4:
                            raise ValueError('Invalid base64 embedding dimensions')
                        value = list(struct.unpack(f'<{self.dimensions}f', raw))
                    vectors.append(value)
                if any(len(v) != self.dimensions or not all(math.isfinite(x) for x in v)
                       or not any(v) for v in vectors):
                    raise ValueError('Invalid embedding dimensions or vector values')
                if self.metrics_path:
                    record = dict(timestamp=datetime.now(timezone.utc).isoformat(),
                                  seconds=time.perf_counter()-started, attempts=attempt+1,
                                  inputs=len(texts), model=self.model, encoding_format='base64',
                                  usage=body.get('usage', {}), response_id=body.get('id'))
                    with self.metrics_path.open('a') as stream:
                        stream.write(json.dumps(record) + '\n')
                return vectors
            except httpx.TransportError:
                if attempt == 5: raise
                time.sleep(min(60, 2 ** attempt))

    def close(self):
        self.client.close()
