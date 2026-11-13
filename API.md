# Dispatch Room local API

Base URL: `http://127.0.0.1:8000`. Responses are JSON, except the three static interface assets. All responses use `Cache-Control: no-store`.

Get `token` from `GET /api/session` and send it as `X-Dispatch-Token` on every POST. The token rotates when the server restarts. POST requests require `Content-Type: application/json`, one Content-Length, no transfer encoding, and a JSON object. Unknown job fields, duplicate JSON keys, non-finite numbers, and invalid options are rejected.

| Method | Route | Behavior |
| --- | --- | --- |
| GET | `/api/session` | Obtain this server's local request token. |
| GET | `/api/jobs` | Newest jobs first, counts for all states, `next_before` cursor, and server time. Optional `status`, `before`, and `limit` (1–100, default 30). Counts are global, independent of the filter. |
| GET | `/api/jobs/{id}` | Job payload, result, timestamps, and ordered transition events. |
| POST | `/api/jobs` | Persist a validated job. Returns 201 when created, 200 for an identical duplicate, or 409 for a conflicting key. |
| POST | `/api/run-one` | Claim and execute one eligible job. Body `{}`; response `job` is null when idle. |
| POST | `/api/recover` | Recover expired leases. Body `{}`; response includes `recovered` count. |
| POST | `/api/drill-lease` | Claim one job for five seconds without executing or finishing it. Body `{}`; used only for the explicit fault drill. |
| POST | `/api/jobs/{id}/cancel` | Cancel a queued job. Body `{}`; other states return 409. |

Create request:

```json
{
  "key": "example-001",
  "kind": "word-count",
  "text": "Useful work deserves a durable record.",
  "max_attempts": 3,
  "priority": 0,
  "delay_seconds": 0,
  "fail_first": 0
}
```

`key`, `kind`, and `text` are required. The other fields default to the values shown. `fail_first` can be nonzero only for `retry-demo`. The idempotency fingerprint includes all normalized request fields, including the original delay duration. Replaying an identical request returns the existing job without moving its scheduled time.

Errors contain `{"error": "description"}`. Common statuses: 400 invalid input/Host, 403 request guard rejection, 404 missing resource, 409 conflicting identity/state/lease, 408 body timeout, and 503 unavailable database. A handler failure is a successfully processed worker action with a job in `queued` or `dead`, not an HTTP failure.

## Standard-library client

Start the server, then run this in Python. The example creates or reuses one job and processes the next eligible job, which may be another queued job if the database already contains work.

```python
import json
from urllib.request import Request, urlopen

base = 'http://127.0.0.1:8000'
with urlopen(base + '/api/session', timeout=5) as response:
    token = json.load(response)['token']

def post(path, payload):
    request = Request(base + path, data=json.dumps(payload).encode(),
        headers={'Content-Type': 'application/json', 'X-Dispatch-Token': token},
        method='POST')
    with urlopen(request, timeout=10) as response:
        return json.load(response)

created = post('/api/jobs', {
    'key': 'client-example-001', 'kind': 'sha256', 'text': 'hello'
})
print('Job ID:', created['job']['id'])
print(post('/api/run-one', {}))
```

For a complete snapshot of a job, request its detail route. The console's Export record button saves that object as JSON. The public API never returns lease tokens; lease operations for worker implementations live in the Python `Queue` library.
