"""Durable, local SQLite queue with fenced leases and bounded retry budgets."""
import hashlib
import json
import re
import secrets
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

KINDS = ('word-count', 'sha256', 'retry-demo')
STATES = ('queued', 'running', 'succeeded', 'dead', 'cancelled')


class Conflict(ValueError):
    pass


def integer(value, low, high, name):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f'{name} must be an integer from {low} to {high}.')
    return value


def validate_job(data):
    fields = {'key', 'kind', 'text', 'max_attempts', 'fail_first', 'delay_seconds', 'priority'}
    if not isinstance(data, dict) or set(data) - fields:
        raise ValueError('Unknown job fields.')
    key = data.get('key')
    if not isinstance(key, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,80}', key):
        raise ValueError('Key must contain 1–80 letters, digits, dots, colons, underscores, or hyphens.')
    kind, text = data.get('kind'), data.get('text')
    if kind not in KINDS or not isinstance(text, str) or len(text) > 20000:
        raise ValueError('Choose a supported handler and text of at most 20,000 characters.')
    try:
        text.encode('utf-8')
    except UnicodeEncodeError:
        raise ValueError('Text must contain valid Unicode characters.') from None
    options = {name: integer(data.get(name, default), low, high, name) for name, default, low, high in [
        ('max_attempts', 3, 1, 8), ('fail_first', 0, 0, 8), ('delay_seconds', 0, 0, 3600), ('priority', 0, 0, 9)]}
    if kind != 'retry-demo' and options['fail_first']:
        raise ValueError('Failure injection is only supported by retry-demo.')
    return {'key': key, 'kind': kind, 'text': text, **options}


class Queue:
    def __init__(self, path, clock=time.time):
        self.path, self.clock = str(Path(path).resolve()), clock
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY, key TEXT NOT NULL UNIQUE, fingerprint TEXT NOT NULL,
                    kind TEXT NOT NULL, text TEXT NOT NULL, max_attempts INTEGER NOT NULL,
                    fail_first INTEGER NOT NULL, priority INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'queued', available REAL NOT NULL,
                    created REAL NOT NULL, updated REAL NOT NULL, lease_token TEXT, lease_until REAL,
                    result TEXT, error TEXT
                );
                CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(status, available, priority);
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY, job_id INTEGER NOT NULL REFERENCES jobs(id),
                    at REAL NOT NULL, event TEXT NOT NULL, detail TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS events_job ON events(job_id, id);
            ''')

    @contextmanager
    def connection(self, write=False):
        db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON')
        try:
            if write:
                db.execute('BEGIN IMMEDIATE')
            yield db
            if write:
                db.commit()
        except BaseException:
            if write:
                db.rollback()
            raise
        finally:
            db.close()

    def _event(self, db, job_id, now, event, detail):
        db.execute('INSERT INTO events(job_id,at,event,detail) VALUES(?,?,?,?)', (job_id, now, event, detail))

    @staticmethod
    def public(row):
        result = dict(row)
        result.pop('lease_token', None)
        result.pop('fingerprint', None)
        result['result'] = json.loads(result['result']) if result['result'] is not None else None
        return result

    def enqueue(self, data):
        data = validate_job(data)
        fingerprint = hashlib.sha256(json.dumps(data, sort_keys=True, ensure_ascii=True).encode()).hexdigest()
        now = self.clock()
        with self.connection(write=True) as db:
            old = db.execute('SELECT * FROM jobs WHERE key=?', (data['key'],)).fetchone()
            if old:
                if old['fingerprint'] != fingerprint:
                    raise Conflict('This idempotency key already belongs to a different request.')
                return self.public(old), False
            if db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0] >= 2000:
                raise Conflict('This local queue has reached its 2,000-job limit. Start a new database.')
            cursor = db.execute('''INSERT INTO jobs(key,fingerprint,kind,text,max_attempts,fail_first,priority,available,created,updated)
                VALUES(?,?,?,?,?,?,?,?,?,?)''', (data['key'], fingerprint, data['kind'], data['text'],
                data['max_attempts'], data['fail_first'], data['priority'], now + data['delay_seconds'], now, now))
            self._event(db, cursor.lastrowid, now, 'enqueued', 'Job persisted.')
            row = db.execute('SELECT * FROM jobs WHERE id=?', (cursor.lastrowid,)).fetchone()
            return self.public(row), True

    def _retry(self, db, row, now, error, event):
        exhausted = row['attempts'] >= row['max_attempts']
        delay = min(60, 2 ** (row['attempts'] - 1))
        status = 'dead' if exhausted else 'queued'
        db.execute('''UPDATE jobs SET status=?,available=?,updated=?,lease_token=NULL,lease_until=NULL,error=? WHERE id=?''',
                   (status, now + (0 if exhausted else delay), now, error[:500], row['id']))
        self._event(db, row['id'], now, event, f'{error[:300]} → {status}' + (f'; retry in {delay}s.' if not exhausted else '.'))

    def _recover(self, db, now):
        rows = db.execute("SELECT * FROM jobs WHERE status='running' AND lease_until<=?", (now,)).fetchall()
        for row in rows:
            self._retry(db, row, now, 'Lease expired before completion', 'lease_expired')
        return len(rows)

    def recover(self):
        with self.connection(write=True) as db:
            return self._recover(db, self.clock())

    def claim(self, lease_seconds=30):
        integer(lease_seconds, 1, 300, 'lease_seconds')
        with self.connection(write=True) as db:
            now = self.clock()
            self._recover(db, now)
            row = db.execute("SELECT * FROM jobs WHERE status='queued' AND available<=? ORDER BY priority DESC,available,id LIMIT 1", (now,)).fetchone()
            if not row:
                return None
            token = secrets.token_hex(24)
            db.execute("UPDATE jobs SET status='running',attempts=attempts+1,lease_token=?,lease_until=?,updated=? WHERE id=?", (token, now + lease_seconds, now, row['id']))
            self._event(db, row['id'], now, 'claimed', f"Attempt {row['attempts'] + 1}; lease {lease_seconds}s.")
            return dict(db.execute('SELECT * FROM jobs WHERE id=?', (row['id'],)).fetchone())

    def _leased(self, db, job_id, token, now):
        row = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
        if not row or row['status'] != 'running' or row['lease_token'] != token or row['lease_until'] <= now:
            raise Conflict('Lease is missing, expired, or belongs to another attempt.')
        return row

    def heartbeat(self, job_id, token, lease_seconds=30):
        integer(lease_seconds, 1, 300, 'lease_seconds')
        with self.connection(write=True) as db:
            now = self.clock()
            self._leased(db, job_id, token, now)
            db.execute('UPDATE jobs SET lease_until=?,updated=? WHERE id=?', (now + lease_seconds, now, job_id))

    def complete(self, job_id, token, result):
        encoded = json.dumps(result, allow_nan=False, ensure_ascii=False)
        if len(encoded) > 40000:
            raise ValueError('Result exceeds 40,000 characters.')
        with self.connection(write=True) as db:
            now = self.clock()
            self._leased(db, job_id, token, now)
            db.execute("UPDATE jobs SET status='succeeded',result=?,error=NULL,lease_token=NULL,lease_until=NULL,updated=? WHERE id=?", (encoded, now, job_id))
            self._event(db, job_id, now, 'succeeded', 'Result committed under the current lease.')

    def fail(self, job_id, token, error):
        with self.connection(write=True) as db:
            now = self.clock()
            row = self._leased(db, job_id, token, now)
            self._retry(db, row, now, str(error), 'failed')

    def cancel(self, job_id):
        with self.connection(write=True) as db:
            now = self.clock()
            row = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
            if not row:
                raise KeyError('Job not found.')
            if row['status'] != 'queued':
                raise Conflict('Only queued jobs can be cancelled.')
            db.execute("UPDATE jobs SET status='cancelled',updated=? WHERE id=?", (now, job_id))
            self._event(db, job_id, now, 'cancelled', 'Cancelled before the next claim.')

    def get(self, job_id):
        with self.connection() as db:
            db.execute('BEGIN')
            row = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
            if not row:
                raise KeyError('Job not found.')
            result = self.public(row)
            result['events'] = [dict(e) for e in db.execute('SELECT at,event,detail FROM events WHERE job_id=? ORDER BY id', (job_id,))]
            return result

    def list(self, status=None, before=None, limit=30):
        integer(limit, 1, 100, 'limit')
        if status is not None and status not in STATES:
            raise ValueError('Unknown status filter.')
        if before is not None:
            integer(before, 1, 2**63-1, 'before')
        clauses, args = [], []
        if status:
            clauses.append('status=?'); args.append(status)
        if before:
            clauses.append('id<?'); args.append(before)
        where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
        with self.connection() as db:
            db.execute('BEGIN')
            rows = db.execute('SELECT * FROM jobs'+where+' ORDER BY id DESC LIMIT ?', (*args, limit + 1)).fetchall()
            counts = {status: 0 for status in STATES}
            counts.update({r['status']: r['n'] for r in db.execute('SELECT status,COUNT(*) n FROM jobs GROUP BY status')})
            return {'jobs': [{k: v for k, v in self.public(r).items() if k not in ('text', 'result')} for r in rows[:limit]],
                    'next_before': rows[limit-1]['id'] if len(rows)>limit else None, 'counts': counts, 'now': self.clock()}


def execute(job):
    text = job['text']
    if job['kind'] == 'retry-demo':
        if job['attempts'] <= job['fail_first']:
            raise RuntimeError(f"Deliberate demo failure on attempt {job['attempts']}")
        return {'message': text, 'successful_attempt': job['attempts']}
    if job['kind'] == 'sha256':
        raw = text.encode('utf-8')
        return {'sha256': hashlib.sha256(raw).hexdigest(), 'utf8_bytes': len(raw)}
    if job['kind'] == 'word-count':
        words = re.findall(r"\w+(?:['’]\w+)*", text.lower(), re.UNICODE)
        frequencies = {}
        for word in words:
            frequencies[word] = frequencies.get(word, 0) + 1
        return {'characters': len(text), 'words': len(words), 'unique_words': len(frequencies),
                'top_words': sorted(frequencies.items(), key=lambda item: (-item[1], item[0]))[:10]}
    raise ValueError('Unknown handler.')


def run_one(queue):
    job = queue.claim()
    if not job:
        return None
    try:
        result = execute(job)
    except Exception as error:
        queue.fail(job['id'], job['lease_token'], str(error))
    else:
        queue.complete(job['id'], job['lease_token'], result)
    return queue.get(job['id'])
