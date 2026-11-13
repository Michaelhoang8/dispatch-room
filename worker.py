"""Run bounded built-in jobs from a local Dispatch Room database."""
import argparse
import json
import time
from pathlib import Path
from queue_store import Queue, run_one


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', default=str(Path(__file__).resolve().parent / 'data' / 'dispatch.sqlite3'))
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    queue = Queue(args.db)
    try:
        while True:
            job = run_one(queue)
            print(json.dumps({'job': job['id'], 'status': job['status'], 'attempts': job['attempts']} if job else {'idle': True}), flush=True)
            if args.once:
                break
            if not job:
                time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
