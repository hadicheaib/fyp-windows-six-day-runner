"""Offline tests: competing grants, persistence, shared stop and expiry."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier
import unittest

from lebanon_discovery.host_pace import DAILY, HOURLY, SIX_DAYS, ZONE, HostPace, bootstrap

BASE = datetime(2026, 11, 23, 12, tzinfo=ZONE).timestamp()


class PacingTest(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name) / 'pace'
        self.states = [Path(temp.name) / lane for lane in ('west', 'east')]
        self.identity, self.expiry = bootstrap(self.root, self.states, 'live', BASE)

    def gate(self):
        gate = HostPace(self.root, 'live')
        self.addCleanup(gate.close)
        return gate

    def test_two_contenders_cannot_take_the_same_map_slot(self):
        barrier = Barrier(2)

        def attempt(lane):
            gate = HostPace(self.root, 'live')
            try:
                barrier.wait(timeout=3)
                return gate.reserve(lane, 'search', BASE)
            finally:
                gate.close()

        with ThreadPoolExecutor(2) as pool:
            self.assertEqual(sorted(pool.map(attempt, ('west', 'east'))), ['ready', 'wait'])
        gate = self.gate()
        self.assertEqual(gate.reserve('east', 'detail', BASE + 4), 'wait')
        self.assertEqual(gate.reserve('east', 'detail', BASE + 5), 'ready')
        self.assertEqual(gate.reserve('west', 'search', BASE + 29), 'wait')
        self.assertEqual(gate.reserve('west', 'search', BASE + 30), 'ready')

    def test_hour_and_day_caps_persist_across_openers(self):
        for i in range(HOURLY):
            gate = HostPace(self.root, 'live')
            try:
                self.assertEqual(gate.reserve(('west', 'east')[i % 2], 'search', BASE + i*30), 'ready')
            finally:
                gate.close()
        gate = self.gate()
        self.assertEqual(gate.reserve('east', 'search', BASE + HOURLY*30), 'wait')
        self.assertEqual(gate.reserve('west', 'search', BASE + 3600), 'ready')
        day = datetime.fromtimestamp(BASE, ZONE).date().isoformat()
        gate.db.executemany('INSERT INTO grants(lane,kind,at,day) VALUES (?,?,?,?)',
                            [('west', 'detail', BASE + i, day) for i in range(DAILY)])
        self.assertEqual(gate.reserve('east', 'detail', BASE + 7000), 'wait')

    def test_expiry_shared_halt_and_missing_ledger(self):
        self.assertEqual(self.expiry - BASE, SIX_DAYS)
        self.assertEqual(bootstrap(self.root, self.states, 'live', BASE+50), (self.identity, self.expiry))
        a, b = self.gate(), self.gate()
        self.assertEqual(a.reserve('west', 'search', self.expiry), 'expired')
        a.halt()
        self.assertEqual(b.reserve('east', 'detail', BASE+50), 'halted')
        (self.states[0] / 'live').mkdir(parents=True)
        with self.assertRaisesRegex(RuntimeError, 'ledger is missing'):
            bootstrap(self.root.parent/'replacement', self.states, 'live', BASE)


if __name__ == '__main__':
    unittest.main()
