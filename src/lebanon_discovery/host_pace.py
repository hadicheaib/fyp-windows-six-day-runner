"""One durable source grant ledger for collectors on the same Docker engine."""
from contextlib import contextmanager
from datetime import datetime
import os
from pathlib import Path
import sqlite3
import time
from uuid import uuid4
from zoneinfo import ZoneInfo

ROOT = Path('/pace')
LANES = ('west', 'east')
SIX_DAYS = 6 * 24 * 3600
HOURLY = 114
DAILY = 2736
ZONE = ZoneInfo('Asia/Beirut')


def bootstrap(root, states, mode, now=None):
    """Create a ledger only for a fresh pair. Do not recreate lost live authority."""
    root = Path(root)
    if mode not in ('fixture', 'live'):
        raise ValueError('Invalid pacing mode')
    path = root / 'pacing.sqlite3'
    root.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        if any((Path(s) / mode).exists() for s in states):
            raise RuntimeError('Shared pacing ledger is missing for an existing session')
        now = time.time() if now is None else now
        with sqlite3.connect(path) as db:
            db.execute('PRAGMA synchronous=FULL')
            db.executescript('''
                CREATE TABLE authority(id TEXT NOT NULL, mode TEXT NOT NULL, ends REAL NOT NULL, halted INTEGER NOT NULL);
                CREATE TABLE grants(id INTEGER PRIMARY KEY, lane TEXT NOT NULL, kind TEXT NOT NULL, at REAL NOT NULL, day TEXT NOT NULL);
                CREATE INDEX grants_at ON grants(at);
                CREATE INDEX grants_kind_at ON grants(kind,at);
                CREATE INDEX grants_kind_day ON grants(kind,day);
            ''')
            db.execute('INSERT INTO authority VALUES (?,?,?,0)',
                       (uuid4().hex, mode, now + (SIX_DAYS if mode == 'live' else 3600)))
        db.close()
    gate = HostPace(root, mode)
    result = gate.identity, gate.expires_epoch
    gate.close()
    return result


class HostPace:
    def __init__(self, root, mode):
        target = (Path(root) / 'pacing.sqlite3').resolve().as_uri() + '?mode=rw'
        self.db = sqlite3.connect(target, uri=True, timeout=2, isolation_level=None)
        self.db.execute('PRAGMA synchronous=FULL')
        try:
            [(identity, stored_mode, expires, _)] = self.db.execute('SELECT * FROM authority').fetchall()
            if stored_mode != mode or not identity:
                raise ValueError('Pacing ledger mode mismatch')
            self.identity = identity
            self.expires_epoch = expires
            self.mode = mode
        except BaseException:
            self.db.close()
            raise

    def close(self):
        self.db.close()

    @contextmanager
    def transaction(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def halted(self):
        return bool(self.db.execute('SELECT halted FROM authority').fetchone()[0])

    def halt(self):
        with self.transaction():
            self.db.execute('UPDATE authority SET halted=1')

    def reserve(self, lane, kind, now=None):
        """Commit a grant before the private journal/browser. A crash can only waste it."""
        if lane not in LANES or kind not in ('search', 'detail'):
            raise ValueError('Invalid pacing request')
        with self.transaction():
            now = time.time() if now is None else now
            if self.halted():
                return 'halted'
            if now >= self.expires_epoch:
                return 'expired'
            day = datetime.fromtimestamp(now, ZONE).date().isoformat()
            if self.mode == 'live':
                last = self.db.execute('SELECT max(at) FROM grants').fetchone()[0]
                last_map = self.db.execute("SELECT max(at) FROM grants WHERE kind='search'").fetchone()[0]
                if (last is not None and now < last + 5) or (
                    kind == 'search' and last_map is not None and now < last_map + 30
                ):
                    return 'wait'
                if self.db.execute('SELECT count(*) FROM grants WHERE kind=? AND at>?', (kind, now-3600)).fetchone()[0] >= HOURLY:
                    return 'wait'
                if self.db.execute('SELECT count(*) FROM grants WHERE kind=? AND day=?', (kind, day)).fetchone()[0] >= DAILY:
                    return 'wait'
            self.db.execute('INSERT INTO grants(lane,kind,at,day) VALUES (?,?,?,?)', (lane, kind, now, day))
            return 'ready'


def main():
    states = (Path('/state-west'), Path('/state-east'))
    for path in (ROOT, *states):
        path.mkdir(parents=True, exist_ok=True)
        os.chown(path, 10001, 10001)
    bootstrap(ROOT, states, os.environ['FYP_RUN_MODE'])
    os.chown(ROOT / 'pacing.sqlite3', 10001, 10001)


if __name__ == '__main__':
    main()
