"""Serve the local Dispatch Room console and its JSON API."""
import argparse
import json
import re
import secrets
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from queue_store import Queue, Conflict, run_one

ROOT = Path(__file__).resolve().parent
ASSETS = {'/': ('index.html', 'text/html'), '/style.css': ('style.css', 'text/css'), '/app.js': ('app.js', 'text/javascript')}


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, queue):
        self.queue, self.csrf = queue, secrets.token_urlsafe(32)
        super().__init__(address, Handler)


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(5)

    def log_message(self, *_):
        pass  # Do not log payloads or user-provided request paths.

    def reply(self, status, value, content_type='application/json'):
        body = value if isinstance(value, bytes) else json.dumps(value, ensure_ascii=True, allow_nan=False).encode()
        self.send_response(status)
        self.send_header('Content-Type', content_type + '; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
        self.send_header('Referrer-Policy', 'no-referrer')
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(body)

    def trusted(self, mutate=False):
        hosts = self.headers.get_all('Host', [])
        allowed = {f'127.0.0.1:{self.server.server_port}', f'localhost:{self.server.server_port}'}
        if len(hosts) != 1 or hosts[0].lower() not in allowed:
            self.reply(400, {'error': 'Use the local server address.'})
            return False
        if self.headers.get('Sec-Fetch-Site') == 'cross-site':
            self.reply(403, {'error': 'Cross-site requests are not allowed.'})
            return False
        origin = self.headers.get('Origin')
        if origin is not None and origin != 'http://' + hosts[0].lower():
            self.reply(403, {'error': 'Origin does not match this local console.'})
            return False
        if mutate and not secrets.compare_digest(self.headers.get('X-Dispatch-Token', '').encode('utf-8'), self.server.csrf.encode('ascii')):
            self.reply(403, {'error': 'Reload the console to obtain its request token.'})
            return False
        return True

    def do_GET(self):
        if not self.trusted():
            return
        try:
            url = urlsplit(self.path)
            if url.path in ASSETS:
                name, mime = ASSETS[url.path]
                return self.reply(200, (ROOT / name).read_bytes(), mime)
            if url.path == '/api/session':
                return self.reply(200, {'token': self.server.csrf})
            if url.path == '/api/jobs':
                params = parse_qs(url.query, keep_blank_values=True, max_num_fields=3)
                if set(params) - {'status', 'before', 'limit'} or any(len(v) != 1 for v in params.values()):
                    raise ValueError('Unknown or repeated query parameters.')
                data = self.server.queue.list(status=params.get('status', [None])[0],
                    before=int(params['before'][0]) if 'before' in params else None,
                    limit=int(params.get('limit', ['30'])[0]))
                return self.reply(200, data)
            match = re.fullmatch(r'/api/jobs/([1-9][0-9]{0,9})', url.path)
            if match:
                return self.reply(200, self.server.queue.get(int(match[1])))
            self.reply(404, {'error': 'Route not found.'})
        except KeyError:
            self.reply(404, {'error': 'Job not found.'})
        except (ValueError, OverflowError) as error:
            self.reply(400, {'error': str(error)})
        except sqlite3.OperationalError:
            self.reply(503, {'error': 'Database temporarily unavailable. Try again shortly.'})

    do_HEAD = do_GET

    def read_json(self):
        if self.headers.get_content_type() != 'application/json':
            raise ValueError('Use Content-Type: application/json.')
        sizes = self.headers.get_all('Content-Length', [])
        if self.headers.get('Transfer-Encoding') or len(sizes) != 1 or not re.fullmatch(r'[0-9]{1,6}', sizes[0]):
            raise ValueError('Send one bounded Content-Length, without transfer encoding.')
        length = int(sizes[0])
        if length > 160000:
            raise ValueError('Request body exceeds 160 KB.')
        raw = self.rfile.read(length)
        if len(raw) != length:
            raise ValueError('Incomplete request body.')
        def pairs(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise ValueError('Duplicate JSON field.')
                result[key] = value
            return result
        def reject_constant(_):
            raise ValueError('JSON numbers must be finite.')
        data = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_constant=reject_constant)
        if not isinstance(data, dict):
            raise ValueError('Expected a JSON object.')
        return data

    def do_POST(self):
        if not self.trusted(mutate=True):
            return
        try:
            path = urlsplit(self.path).path
            data = self.read_json()
            if path == '/api/jobs':
                job, created = self.server.queue.enqueue(data)
                return self.reply(201 if created else 200, {'job': job, 'created': created})
            if data:
                raise ValueError('This action expects an empty JSON object.')
            if path == '/api/run-one':
                return self.reply(200, {'job': run_one(self.server.queue)})
            if path == '/api/recover':
                return self.reply(200, {'recovered': self.server.queue.recover()})
            if path == '/api/drill-lease':
                job = self.server.queue.claim(lease_seconds=5)
                return self.reply(200, {'job': self.server.queue.get(job['id']) if job else None})
            match = re.fullmatch(r'/api/jobs/([1-9][0-9]{0,9})/cancel', path)
            if match:
                self.server.queue.cancel(int(match[1]))
                return self.reply(200, self.server.queue.get(int(match[1])))
            self.reply(404, {'error': 'Route not found.'})
        except Conflict as error:
            self.reply(409, {'error': str(error)})
        except KeyError:
            self.reply(404, {'error': 'Job not found.'})
        except (ValueError, UnicodeError, RecursionError, OverflowError) as error:
            self.reply(400, {'error': str(error) if not isinstance(error, RecursionError) else 'JSON nesting is too deep.'})
        except sqlite3.OperationalError:
            self.reply(503, {'error': 'Database temporarily unavailable. Try again shortly.'})
        except TimeoutError:
            self.reply(408, {'error': 'Request body timed out.'})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--db', default=str(ROOT / 'data' / 'dispatch.sqlite3'))
    args = parser.parse_args()
    server = Server(('127.0.0.1', args.port), Queue(args.db))
    print(f'Dispatch Room: http://127.0.0.1:{server.server_port}', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
