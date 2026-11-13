import hashlib
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from queue_store import Queue, Conflict, execute, run_one, validate_job


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'queue.sqlite3'
        self.now = 1000.0
        self.queue = Queue(self.path, clock=lambda: self.now)

    def tearDown(self):
        self.temp.cleanup()

    def add(self, key='one', **options):
        return self.queue.enqueue({'key': key, 'kind': 'word-count', 'text': 'Hello hello world', **options})[0]

    def test_idempotency_is_persistent_and_rejects_changed_requests(self):
        job = self.add()
        same, created = self.queue.enqueue({'key': 'one', 'kind': 'word-count', 'text': 'Hello hello world'})
        self.assertFalse(created)
        self.assertEqual(job['id'], same['id'])
        with self.assertRaises(Conflict):
            self.add(text='different')
        reopened = Queue(self.path, clock=lambda: self.now)
        self.assertEqual(reopened.get(job['id'])['text'], 'Hello hello world')
        self.assertNotIn('lease_token', reopened.get(job['id']))

    def test_many_concurrent_enqueues_create_one_job(self):
        with ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(lambda _: self.add()['id'], range(30)))
        self.assertEqual(len(set(results)), 1)
        self.assertEqual(self.queue.list()['counts']['queued'], 1)

    def test_claims_are_exclusive_across_connections(self):
        for i in range(12):
            self.add(str(i))
        with ThreadPoolExecutor(max_workers=12) as pool:
            claims = list(pool.map(lambda _: self.queue.claim(), range(24)))
        ids = [job['id'] for job in claims if job]
        self.assertEqual(len(ids), 12)
        self.assertEqual(len(set(ids)), 12)

    def test_priority_delay_and_stable_order(self):
        low = self.add('low', priority=1)
        high = self.add('high', priority=9)
        delayed = self.add('delayed', priority=9, delay_seconds=10)
        self.assertEqual(self.queue.claim()['id'], high['id'])
        self.assertEqual(self.queue.claim()['id'], low['id'])
        self.assertIsNone(self.queue.claim())
        self.now += 10
        self.assertEqual(self.queue.claim()['id'], delayed['id'])

    def test_expired_lease_is_fenced_before_and_after_reclaim(self):
        self.add()
        old = self.queue.claim(lease_seconds=2)
        self.now += 2
        with self.assertRaises(Conflict):
            self.queue.complete(old['id'], old['lease_token'], {'late': True})
        self.assertEqual(self.queue.recover(), 1)
        self.assertIsNone(self.queue.claim())
        self.now += 1
        new = self.queue.claim()
        self.assertNotEqual(old['lease_token'], new['lease_token'])
        for action in [lambda: self.queue.complete(old['id'], old['lease_token'], {}),
                       lambda: self.queue.fail(old['id'], old['lease_token'], 'late'),
                       lambda: self.queue.heartbeat(old['id'], old['lease_token'])]:
            with self.assertRaises(Conflict):
                action()
        self.queue.complete(new['id'], new['lease_token'], {'ok': True})
        self.assertEqual(self.queue.get(new['id'])['result'], {'ok': True})

    def test_retry_backoff_then_dead_letter(self):
        self.add(kind='retry-demo', fail_first=8, max_attempts=3)
        first = run_one(self.queue)
        self.assertEqual((first['status'], first['available']), ('queued', 1001))
        self.assertIsNone(run_one(self.queue))
        self.now = 1001
        second = run_one(self.queue)
        self.assertEqual((second['status'], second['available']), ('queued', 1003))
        self.now = 1003
        third = run_one(self.queue)
        self.assertEqual((third['status'], third['attempts']), ('dead', 3))
        self.assertIsNone(self.queue.claim())
        self.assertEqual([e['event'] for e in third['events']], ['enqueued', 'claimed', 'failed', 'claimed', 'failed', 'claimed', 'failed'])

    def test_lease_expiration_consumes_retry_budget(self):
        self.add(max_attempts=1)
        lease = self.queue.claim(lease_seconds=1)
        self.now += 1
        self.assertIsNone(self.queue.claim())
        self.assertEqual(self.queue.get(lease['id'])['status'], 'dead')

    def test_heartbeat_extends_live_lease_but_cannot_revive_it(self):
        self.add()
        job = self.queue.claim(lease_seconds=2)
        self.now += 1
        self.queue.heartbeat(job['id'], job['lease_token'], 10)
        self.now += 2
        self.assertEqual(self.queue.recover(), 0)
        self.now += 8
        with self.assertRaises(Conflict):
            self.queue.heartbeat(job['id'], job['lease_token'])

    def test_success_result_and_cancel_state_transitions(self):
        completed = run_one(self.queue)  # Empty queues are idle, not errors.
        self.assertIsNone(completed)
        self.add()
        completed = run_one(self.queue)
        self.assertEqual(completed['result']['words'], 3)
        self.assertEqual(completed['result']['top_words'][0], ['hello', 2])
        self.assertEqual(completed['status'], 'succeeded')
        with self.assertRaises(Conflict):
            self.queue.cancel(completed['id'])
        queued = self.add('cancel')
        self.queue.cancel(queued['id'])
        self.assertEqual(self.queue.get(queued['id'])['status'], 'cancelled')
        self.assertIsNone(self.queue.claim())

    def test_cursor_pages_do_not_repeat_jobs(self):
        for i in range(8):
            self.add(str(i))
        page1 = self.queue.list(limit=3)
        page2 = self.queue.list(limit=3, before=page1['next_before'])
        self.assertEqual([j['id'] for j in page1['jobs']], [8, 7, 6])
        self.assertEqual([j['id'] for j in page2['jobs']], [5, 4, 3])
        self.assertNotIn('text', page1['jobs'][0])
        with self.assertRaises(ValueError):
            self.queue.list(status='nope')

    def test_validation_and_builtin_handlers(self):
        for patch in [{'max_attempts': True}, {'delay_seconds': -1}, {'priority': 10}, {'text': 3},
                      {'key': '../path'}, {'fail_first': 1}, {'extra': 'no'}, {'kind': 'shell'}]:
            with self.assertRaises(ValueError):
                validate_job({'key': 'ok', 'kind': 'word-count', 'text': '', **patch})
        result = execute({'kind': 'sha256', 'text': 'café'})
        self.assertEqual(result['sha256'], hashlib.sha256('café'.encode()).hexdigest())
        self.assertEqual(result['utf8_bytes'], 5)

    def test_maximum_unicode_payload_completes_without_escaping_overflow(self):
        self.add(text='界' * 20000)
        self.assertEqual(run_one(self.queue)['status'], 'succeeded')
        with self.assertRaises(ValueError):
            self.add('bad-unicode', text='\ud800')


if __name__ == '__main__':
    unittest.main()
