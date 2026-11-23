"""Linux regional pool: bundled Chromium, local SQLite, synchronous snapshots.

The frozen Windows pool remains unchanged. Only the worker-local compatibility
classes are substituted; shared FIFO pacing and access stops are retained.
"""
from __future__ import annotations

import hashlib
import copy
import json
import math
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from contextlib import closing, contextmanager

from . import map_walk_pool as original
from .browser_trial import WorkerLock
from .browser_service import day_at
from .map_walk import Store, camera, key, validate_plan
from .hpc_memory_v1 import memory_snapshot
from .hpc_workbook_v1 import publish


def sync_directory(path):
    if os.name != 'nt':
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def snapshot(source, destination):
    """One temporary backup writer; no active SQLite database on shared storage."""
    source, destination = Path(source), Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + '.' + uuid.uuid4().hex + '.partial')
    with closing(sqlite3.connect(source.resolve().as_uri() + '?mode=ro', uri=True)) as src:
        with closing(sqlite3.connect(temporary)) as dst:
            src.backup(dst)
            if dst.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                raise ValueError('Snapshot integrity check failed')
    with temporary.open('r+b') as file:
        os.fsync(file.fileno())
    os.replace(temporary, destination)
    sync_directory(destination.parent)


def state_file(folder, region=None):
    return Path(folder) / ('source.sqlite3' if region is None else original.slug(region) + '.sqlite3')


@contextmanager
def snapshot_lock(path):
    """Serialize same-node source publishers, waiting briefly for the owner."""
    deadline = time.monotonic() + 10
    while True:
        lock = WorkerLock(Path(path))
        try:
            lock.__enter__()
            break
        except RuntimeError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(.05)
    try:
        yield
    finally:
        lock.__exit__(None, None, None)


class AuthorityLease:
    """Atomic shared-directory lease; crashed jobs require explicit inspection."""
    def __init__(self, authority):
        self.authority = Path(authority).resolve()
        self.path = self.authority / 'ACTIVE_LEASE'
        self.token = uuid.uuid4().hex

    def __enter__(self):
        self.authority.mkdir(parents=True, exist_ok=True)
        self.path.mkdir()  # Atomic exclusive create on shared storage, not local flock.
        original.write(self.path / 'owner.json', {'token': self.token,
                       'job_id': os.environ.get('SLURM_JOB_ID'), 'created_at': original.now_iso()})
        sync_directory(self.path)
        sync_directory(self.authority)
        return self

    def __exit__(self, *args):
        owner = self.path / 'owner.json'
        if original.read_json(owner)['token'] != self.token:
            raise RuntimeError('Authority lease ownership changed; retain for inspection')
        owner.unlink()
        self.path.rmdir()  # Only this owned, now-empty lease directory is removed.
        sync_directory(self.authority)


def source_rows(path):
    with closing(sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)) as db:
        return [list(row) for row in db.execute('SELECT * FROM events ORDER BY id')]


def fixture_plan(seed_plan):
    return {'kind': 'lebanon-map-walk-v1', 'schema': 1, 'mode': 'fixture',
            'regions': seed_plan['regions'], 'views': [{**view, 'camera': local_camera(view['camera'])} for region in seed_plan['regions']
            for view in [next(v for v in seed_plan['views'] if v['governorate'] == region)]]}


def local_camera(value):
    return camera(value['latitude'], value['longitude'], zoom=value['zoom'], width=value['width'],
                  height=value['height'], inset_x=value['roi'][0], inset_y=value['roi'][1])


def validate_portable_plan(plan):
    """Preserve the original plan/hash; tolerate only sub-millimetre libm drift.

    Windows/Linux libm can differ at the last binary digit of derived bounds.
    New synthetic cameras are recomputed locally; real frozen cameras are not.
    """
    checked = copy.deepcopy(plan)

    def equivalent(left, right):
        if isinstance(left, dict) and isinstance(right, dict):
            return left.keys() == right.keys() and all(equivalent(left[k], right[k]) for k in left)
        if isinstance(left, list) and isinstance(right, list):
            return len(left) == len(right) and all(equivalent(a, b) for a, b in zip(left, right))
        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            return math.isclose(left, right, rel_tol=0, abs_tol=1e-10)
        return left == right

    for original_view, checked_view in zip(plan['views'], checked['views']):
        expected = local_camera(original_view['camera'])
        if not equivalent(original_view['camera'], expected):
            raise ValueError('Saved camera geometry differs beyond floating-point tolerance')
        checked_view['camera'] = expected
    validate_plan(checked)
    return key(plan)


def quarantine_interrupted(store, region):
    """Keep uncertain work in quarantine; never turn started targets pending."""
    with closing(store.connect(write=True)) as db:
        owned = db.execute('SELECT id,owner FROM views WHERE governorate=? AND owner IS NOT NULL', (region,)).fetchall()
        for row in owned:
            db.execute("UPDATE views SET state='unresolved',owner=NULL WHERE id=?", (row['id'],))
            db.execute("INSERT INTO audit(at,view_id,event,payload) VALUES (?,?,?,?)",
                       (time.time(), row['id'], 'hpc_interrupted_view_quarantined',
                        json.dumps({'previous_owner': row['owner'], 'automatic_retry': False})))
        db.commit()
    return len(owned)


def prepare(work, durable, seed, *, mode, seconds=120, limit=60, regions=None, authority_state=None):
    work, durable, seed = Path(work), Path(durable), Path(seed)
    if mode not in ('live', 'fixture') or not 5 <= seconds <= 3600 or not 1 <= limit <= 120:
        raise ValueError('Explicit finite mode/time/shared request ceiling required')
    if (work / 'config.json').exists() or (durable / 'config.json').exists():
        raise FileExistsError('A campaign folder is immutable; create a new finite campaign')
    source_plan = json.loads((seed / 'plan.json').read_text())
    if mode == 'live' and source_plan['mode'] != 'live':
        raise ValueError('Synthetic state cannot seed live evidence')
    resume = source_plan['mode'] == mode and (seed / 'source.sqlite3').exists()
    plan = source_plan if resume else fixture_plan(source_plan)
    validate_portable_plan(plan)
    selected = list(regions or plan['regions'])
    if not selected or len(set(selected)) != len(selected) or not set(selected) <= set(plan['regions']):
        raise ValueError('Unknown/repeated assigned governorates')
    work.mkdir(parents=True, exist_ok=True)
    durable.mkdir(parents=True, exist_ok=True)
    durable_state = Path(authority_state) if authority_state else durable / 'state'
    durable_state.mkdir(parents=True, exist_ok=authority_state is not None)
    baseline_source = []
    if resume:
        snapshot(seed / 'source.sqlite3', work / 'source.sqlite3')
        baseline_source = source_rows(work / 'source.sqlite3')
    source_limits = {'mode': mode, 'max_searches': limit, 'max_details': limit}
    today = day_at(time.time())
    for kind, label in [('search', 'daily_searches'), ('detail', 'daily_details')]:
        source_limits[label] = sum(row[2] == kind and row[5] == today for row in baseline_source) + limit
    config = {'kind': 'hpc-mapwalk-v1', 'mode': mode, 'seconds': seconds, 'limit': limit,
              'regions': selected, 'source_path': str(work / 'source.sqlite3'), 'source_limits': source_limits,
              'durable': str(durable), 'durable_state': str(durable_state),
              'authority_stop': str(Path(authority_state).parent / 'STOP') if authority_state else None,
              'created_at': original.now_iso(), 'minimum_free_mb': 768,
              'fixture_max_views': 1, 'requested_scope': 'finite viewport pass; not exhaustive coverage'}
    config['campaign'] = 'hpc-mapwalk:' + key(config)
    source = original.PoolSource(work / 'source.sqlite3', work, source_limits, config['campaign'])
    source.initialize()  # No implicit import/reseed of other source budgets.
    source.setup_queue()
    if mode == 'live':
        with closing(source.connect()) as db:
            flags = dict(db.execute('SELECT key,value FROM meta'))
        if not all(flags.get(name) == 'true' for name in
                   ('legacy_seeded', 'browser_cache_seed_v1', 'browser_registry_history_v1')):
            raise ValueError('Live source must contain the existing initialized history')
    quarantine = {}
    # Carry all regional states forward even when only one region is selected.
    for region in plan['regions']:
        child = work / 'regions' / original.slug(region)
        child.mkdir(parents=True)
        if resume:
            snapshot(state_file(seed, region), child / 'walk.sqlite3')
        else:
            Store.create(child / 'walk.sqlite3', plan)
        store = original.PoolStore(child / 'walk.sqlite3')
        if store.plan() != plan:
            raise ValueError('Regional immutable route differs from saved plan')
        quarantine[region] = quarantine_interrupted(store, region)
        with closing(store.connect()) as db:
            baseline = [list(row) for row in db.execute('SELECT target_id,attempt FROM outcomes')]
        original.write(child / 'baseline.json', baseline)
        snapshot(store.path, state_file(durable_state, region))
    snapshot(source.path, state_file(durable_state))
    for folder in (work, durable_state):
        original.write(folder / 'plan.json', plan)
    original.write(work / 'source-baseline.json', baseline_source)
    original.write(work / 'config.json', config)
    (work / 'config.sha256').write_text(hashlib.sha256((work / 'config.json').read_bytes()).hexdigest() + '\n')
    original.write(durable / 'config.json', config)
    original.write(durable / 'preparation.json', {'seed_mode': source_plan['mode'], 'mode': mode,
                    'baseline_source_events': len(baseline_source), 'quarantined_interrupted_views': quarantine,
                    'new_supported_domains': 0, 'production_imports': 0})
    return config


class LinuxReader(original.PoolReader):
    def __init__(self, *args, **kwargs):
        kwargs['headed'] = False
        super().__init__(*args, **kwargs)
        self.channel = None


def worker(root, folder, region):
    config = original.configuration(folder)
    durable = Path(config['durable_state'])

    class DurableSource(original.PoolSource):
        def stopped(self):
            return bool(config.get('authority_stop') and Path(config['authority_stop']).exists()) or super().stopped()

        def persist(self):
            with snapshot_lock(self.path.with_suffix('.snapshot.lock')):
                snapshot(self.path, state_file(durable))

        def reserve(self, *args, **kwargs):
            result = super().reserve(*args, **kwargs)
            if result['state'] == 'reserved':
                self.persist()  # Must succeed before the browser uses the grant.
            return result

        def halt(self, reason):
            super().halt(reason)
            self.persist()

    class DurableStore(original.PoolStore):
        def persist(self):
            snapshot(self.path, state_file(durable, region))

        def claim_next(self, *args, **kwargs):
            value = super().claim_next(*args, **kwargs)
            if value:
                self.persist()
            return value

        def save_view(self, *args, **kwargs):
            value = super().save_view(*args, **kwargs)
            self.persist()
            return value

        def begin_target(self, *args, **kwargs):
            value = super().begin_target(*args, **kwargs)
            self.persist()  # An interrupted click remains started/quarantined on resume.
            return value

        def remember(self, *args, **kwargs):
            value = super().remember(*args, **kwargs)
            self.persist()
            return value

        def release(self, *args, **kwargs):
            value = super().release(*args, **kwargs)
            self.persist()
            return value

    # Each invocation is a separate process; the frozen module is not edited.
    before = original.PoolReader, original.PoolSource, original.PoolStore
    original.PoolReader, original.PoolSource, original.PoolStore = LinuxReader, DurableSource, DurableStore
    try:
        return original.worker(root, folder, region)
    finally:
        original.PoolReader, original.PoolSource, original.PoolStore = before


def collect(folder, *, status, reason, elapsed=0, memory=None):
    folder = Path(folder)
    config = original.configuration(folder)
    payload = original.collect(folder)
    all_links = []
    for region in config['regions']:
        child = folder / 'regions' / original.slug(region)
        baseline = {tuple(row) for row in json.loads((child / 'baseline.json').read_text())}
        with closing(original.PoolStore(child / 'walk.sqlite3').connect()) as db:
            rows = db.execute('SELECT o.* FROM outcomes o JOIN targets t ON t.id=o.target_id '
                              'JOIN views v ON v.id=t.view_id WHERE v.governorate=? ORDER BY o.at', (region,))
            for row in rows:
                value = json.loads(row['result'])
                if value.get('website'):
                    all_links.append({**value, 'region': region,
                                      'this_run': (row['target_id'], row['attempt']) not in baseline})
    payload.update(pool_status=status, reason=reason, elapsed_seconds=round(elapsed, 3),
                   requested_seconds=config['seconds'], all_links=all_links, memory=memory)
    return payload


def terminate_group(process):
    if os.name == 'nt':
        process.terminate()
    else:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass


def run(root, folder, *, enable_live=False, authority_lease=False):
    root, folder = Path(root), Path(folder)
    config = original.configuration(folder)
    live = config['mode'] == 'live'
    if live and (not enable_live or not os.environ.get('SLURM_JOB_ID')):
        raise ValueError('Live mode requires an explicit flag inside an assigned Slurm job')
    if live and not authority_lease:
        raise ValueError('Use the live campaign entry point with the shared authority lease')
    if (folder / 'runtime.json').exists():
        raise FileExistsError('Run already attempted; inspect durable state before a new campaign')
    durable = Path(config['durable'])
    processes, logs, memory_samples = {}, [], []
    start = time.time()
    reason = None
    status = 'finished'
    source = original.PoolSource(Path(config['source_path']), folder, config['source_limits'], config['campaign'])
    if source.stopped() or (durable / 'STOP').exists():
        raise PermissionError('Existing source/manual stop retained')
    initial_memory = memory_snapshot(allocation_mb=os.environ.get('FYP_JOB_MEMORY_MB'))
    if live and not initial_memory['finite_cgroup']:
        raise RuntimeError('Live execution requires a finite readable memory cgroup')
    runtime = {'start_unix': start, 'deadline_unix': start + config['seconds'], 'pid': os.getpid(), 'children': {}}
    # Import/probe the publisher before reserving a real browser action.
    initial = collect(folder, status='starting', reason=None, memory=initial_memory)
    publish(durable, initial)
    original.write(folder / 'runtime.json', runtime)
    with WorkerLock(durable / '.pool.lock'):
        try:
            for region in config['regions']:
                child = folder / 'regions' / original.slug(region)
                logfile = (child / 'worker.log').open('w', encoding='utf-8')
                logs.append(logfile)
                proc = subprocess.Popen([sys.executable, '-X', 'utf8', str(root / 'scripts/map_walk_pool_hpc.py'),
                         '--folder', str(folder), 'worker', '--region', region], cwd=root,
                         stdout=logfile, stderr=logfile, start_new_session=os.name != 'nt')
                processes[region] = proc
                runtime['children'][region] = proc.pid
            original.write(folder / 'runtime.json', runtime)
            original.write(durable / 'runtime.json', runtime)
            last_export, stopping_at = 0, None
            while any(process.poll() is None for process in processes.values()):
                memory = memory_snapshot(allocation_mb=os.environ.get('FYP_JOB_MEMORY_MB'))
                memory_samples.append(memory)
                if time.time() >= runtime['deadline_unix']:
                    reason = reason or 'deadline'
                elif memory['headroom_mb'] < config['minimum_free_mb']:
                    reason = reason or 'low_memory'
                elif (durable / 'STOP').exists() or source.stopped() or (config.get('authority_stop') and Path(config['authority_stop']).exists()):
                    reason = reason or 'source_or_manual_stop'
                for region, process in processes.items():
                    path = folder / 'regions' / original.slug(region) / 'result.json'
                    if process.poll() is not None and (process.returncode != 0 or not path.exists()):
                        reason = reason or 'worker_failed'
                    elif path.exists() and original.read_json(path)['status'] in ('needs_inspection', 'cleanup_needs_inspection', 'blocked'):
                        reason = reason or 'worker_needs_inspection'
                if reason:
                    stopping_at = stopping_at or time.time()
                    for region in processes:
                        (folder / 'regions' / original.slug(region) / 'STOP').touch(exist_ok=True)
                if time.time() - last_export >= 4:
                    payload = collect(folder, status='stopping' if reason else 'running', reason=reason,
                                      elapsed=time.time() - start, memory=memory)
                    original.write(durable / 'progress.json', payload)
                    publish(durable, payload)
                    last_export = time.time()
                if stopping_at and time.time() - stopping_at > 20:
                    for process in processes.values():
                        if process.poll() is None:
                            terminate_group(process)
                    break
                time.sleep(.5)
        except BaseException:
            status, reason = 'needs_inspection', 'supervisor_error'
            raise
        finally:
            for region, process in processes.items():
                if process.poll() is None:
                    (folder / 'regions' / original.slug(region) / 'STOP').touch(exist_ok=True)
            for process in processes.values():
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    terminate_group(process)
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        if os.name != 'nt':
                            os.killpg(process.pid, signal.SIGKILL)
                        else:
                            process.kill()
                        process.wait()
            for logfile in logs:
                logfile.close()
            for region in config['regions']:
                child = folder / 'regions' / original.slug(region)
                snapshot(child / 'walk.sqlite3', state_file(config['durable_state'], region))
                for name in ('worker.log', 'result.json', 'heartbeat.json'):
                    path = child / name
                    if path.exists():
                        target = durable / 'workers' / original.slug(region)
                        target.mkdir(parents=True, exist_ok=True)
                        (target / name).write_bytes(path.read_bytes())
            snapshot(source.path, state_file(config['durable_state']))
            if reason and status == 'finished':
                status = 'stopped'
            final = collect(folder, status=status, reason=reason or 'workers_finished',
                            elapsed=time.time() - start, memory=memory_samples[-1] if memory_samples else initial_memory)
            after = source_rows(source.path)
            before = json.loads((folder / 'source-baseline.json').read_text())
            after_by_id = {row[0]: row for row in after}
            preserved = all(after_by_id.get(row[0]) == row for row in before)
            final.update(worker_jobs_started=len(processes), exit_codes={region: p.returncode for region, p in processes.items()},
                         source_events_total=len(after), source_events_added=len(after) - len(before),
                         inherited_source_events_unchanged=preserved,
                         mode=config['mode'], google_navigations=0 if not live else None)
            original.write(durable / 'memory-samples.json', memory_samples)
            original.write(durable / 'result.json', final)
            publish(durable, final)
    return final


def live_campaign(root, work, durable, initial_seed, authority, *, seconds, limit, regions, enable_live=False):
    """One authority/lease across live jobs; later jobs always use latest snapshots.

    The durable source and per-region files are replaced before browser actions
    proceed, so a killed job does not cause the next job to start from old seeds.
    """
    if not enable_live or not os.environ.get('SLURM_JOB_ID'):
        raise ValueError('An explicitly enabled Slurm live campaign is required')
    authority = Path(authority)
    authority.mkdir(parents=True, exist_ok=True)
    with AuthorityLease(authority):
        if (authority / 'STOP').exists():
            raise PermissionError('Authority manual stop retained')
        initialized = authority / 'INITIALIZED.json'
        state = authority / 'state'
        seed = state if initialized.exists() else Path(initial_seed)
        config = prepare(work, durable, seed, mode='live', seconds=seconds, limit=limit,
                         regions=regions, authority_state=state)
        if not initialized.exists():
            original.write(initialized, {'kind': 'hpc-live-authority-v1',
                           'initialized_at': original.now_iso(), 'seed_events': len(source_rows(state_file(state))),
                           'policy': 'No concurrent PC/live collection; this is the migrated source authority.'})
        return run(root, work, enable_live=True, authority_lease=True)
