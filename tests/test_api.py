import json
import tempfile
import threading
import unittest
from http.client import HTTPConnection
from pathlib import Path
from queue_store import Queue
from run import Server


class APITests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.now = 100.0
        self.queue = Queue(Path(self.temp.name) / 'api.sqlite3', clock=lambda: self.now)
        self.server = Server(('127.0.0.1', 0), self.queue)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.token = self.request('GET', '/api/session')[1]['token']

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.temp.cleanup()

    def request(self, method, path, data=None, headers=None, raw=None):
        connection = HTTPConnection(*self.server.server_address, timeout=3)
        merged = {'Content-Type': 'application/json'}
        if hasattr(self, 'token'):
            merged['X-Dispatch-Token'] = self.token
        merged.update(headers or {})
        connection.request(method, path, body=raw if raw is not None else json.dumps(data) if data is not None else None, headers=merged)
        response = connection.getresponse()
        body = response.read()
        result = (response.status, json.loads(body) if body else None, response.headers)
        connection.close()
        return result

    def test_end_to_end_idempotency_execution_and_exportable_detail(self):
        data = {'key': 'api-one', 'kind': 'sha256', 'text': 'hello'}
        status, response, _ = self.request('POST', '/api/jobs', data)
        self.assertEqual(status, 201)
        job_id = response['job']['id']
        self.assertEqual(self.request('POST', '/api/jobs', data)[0], 200)
        self.assertEqual(self.request('POST', '/api/jobs', {**data, 'text': 'changed'})[0], 409)
        status, result, _ = self.request('POST', '/api/run-one', {})
        self.assertEqual(status, 200)
        self.assertEqual(result['job']['status'], 'succeeded')
        self.assertEqual(result['job']['result']['utf8_bytes'], 5)
        self.assertNotIn('lease_token', result['job'])
        detail = self.request('GET', f'/api/jobs/{job_id}')[1]
        self.assertEqual(len(detail['events']), 3)
        self.assertEqual(self.request('GET', '/api/jobs?status=succeeded')[1]['counts']['succeeded'], 1)

    def test_host_origin_and_request_token_boundaries(self):
        self.assertEqual(self.request('GET', '/api/session', headers={'Host': 'evil.invalid'})[0], 400)
        self.assertEqual(self.request('GET', '/api/session', headers={'Origin': 'https://evil.invalid'})[0], 403)
        self.assertEqual(self.request('GET', '/api/session', headers={'Sec-Fetch-Site': 'cross-site'})[0], 403)
        for token in ['', 'wrong', 'é']:
            self.assertEqual(self.request('POST', '/api/run-one', {}, {'X-Dispatch-Token': token})[0], 403)
        self.assertEqual(self.request('GET', '/.git/config')[0], 404)
        self.assertEqual(self.request('GET', '/data/dispatch.sqlite3')[0], 404)
        status, body, headers = self.request('HEAD', '/api/session')
        self.assertEqual(status, 200)
        self.assertIsNone(body)
        self.assertIn("frame-ancestors 'none'", headers['Content-Security-Policy'])

    def test_invalid_body_queries_and_state_transitions(self):
        for raw in ['[]', '{"a":1,"a":2}', '{"a":NaN}', '{', '['*1500]:
            self.assertEqual(self.request('POST', '/api/jobs', raw=raw)[0], 400)
        self.assertEqual(self.request('POST', '/api/jobs', {}, {'Content-Type': 'text/plain'})[0], 400)
        for path in ['/api/jobs?limit=0', '/api/jobs?limit=1&limit=2', '/api/jobs?status=bogus', '/api/jobs?before=99999999999999999999']:
            self.assertEqual(self.request('GET', path)[0], 400)
        self.assertEqual(self.request('POST', '/api/jobs/99/cancel', {})[0], 404)
        self.assertEqual(self.request('POST', '/api/run-one', {'unknown': 1})[0], 400)

    def test_crash_drill_recovery_and_cancel(self):
        job = self.request('POST', '/api/jobs', {'key': 'drill', 'kind': 'word-count', 'text': 'test'})[1]['job']
        claimed = self.request('POST', '/api/drill-lease', {})[1]['job']
        self.assertEqual(claimed['status'], 'running')
        self.assertNotIn('lease_token', claimed)
        self.assertEqual(self.request('POST', f"/api/jobs/{job['id']}/cancel", {})[0], 409)
        self.now += 5
        self.assertEqual(self.request('POST', '/api/recover', {})[1]['recovered'], 1)
        self.assertEqual(self.request('POST', f"/api/jobs/{job['id']}/cancel", {})[0], 200)
        self.assertIsNone(self.request('POST', '/api/run-one', {})[1]['job'])


if __name__ == '__main__':
    unittest.main()
