"""Bounded regional processes, shared FIFO request pacing and one workbook writer.

Regional ledgers are single-writer resumable snapshots. Source budgets remain in
one shared ledger; cloning a regional checkpoint never clones/reset budgets.
"""
from __future__ import annotations

import csv
import ctypes
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
import uuid
from contextlib import closing, nullcontext
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from .browser_service import Source, day_at
from .browser_trial import WorkerLock
from .map_walk import Store, canonical, key, prepare_plan
from .map_walk_browser import MapReader, match_target, place_identity


def write(path, value):
    path = Path(path)
    tmp = path.with_name(path.name + '.writing')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    # Windows readers/virus scanners can briefly hold the destination open.
    for attempt in range(30):
        try:
            os.replace(tmp, path)
            return
        except PermissionError:
            if attempt == 29:
                raise
            time.sleep(.05)


def now_iso():
    return datetime.now(ZoneInfo('Asia/Beirut')).isoformat()


def read_json(path):
    for attempt in range(30):
        try:
            return json.loads(Path(path).read_text(encoding='utf-8'))
        except (PermissionError, json.JSONDecodeError):
            if attempt == 29:
                raise
            time.sleep(.05)


def slug(region):
    return region.lower().replace(' ', '-').replace('_', '-')


def free_memory_mb():
    if os.name != 'nt':
        return os.sysconf('SC_AVPHYS_PAGES') * os.sysconf('SC_PAGE_SIZE') / 1048576
    class Memory(ctypes.Structure):
        _fields_ = [('length', ctypes.c_ulong), ('load', ctypes.c_ulong)] + [(n, ctypes.c_ulonglong) for n in
            ('totalPhysical', 'availablePhysical', 'totalPage', 'availablePage', 'totalVirtual', 'availableVirtual', 'extended')]
    m = Memory(); m.length = ctypes.sizeof(m)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m)):
        raise OSError('Memory census failed')
    return m.availablePhysical / 1048576


class PoolSource(Source):
    """Reserve only at the actual grant time; no future slots blocking details.

    One FIFO queue of ready requests across all regions. SQLite transactions
    cover only grant decisions, never browser operations or pacing waits.
    """
    def setup_queue(self):
        with closing(self.connect()) as db:
            db.execute('CREATE TABLE IF NOT EXISTS mapwalk_pool_waiters(campaign TEXT,identity TEXT,kind TEXT,requested REAL,region TEXT,PRIMARY KEY(campaign,identity))')
            db.commit()

    def reserve(self, identity, *, now=None, kind='detail', deadline=None, region='', **ignored):
        if kind not in ('search', 'detail'):
            raise ValueError('Unknown action')
        deadline = deadline if deadline is not None else time.time() + 30
        with closing(self.connect()) as db:
            db.execute('INSERT OR IGNORE INTO mapwalk_pool_waiters VALUES (?,?,?,?,?)',
                       (self.campaign, identity, kind, time.time(), region))
            db.commit()
        try:
            while time.time() < deadline - .2:
                if self.stopped():
                    return {'state': 'stopped'}
                moment = time.time()
                with closing(self.connect()) as db:
                    db.execute('BEGIN IMMEDIATE')
                    if db.execute("SELECT 1 FROM meta WHERE key='halt'").fetchone():
                        return {'state': 'stopped'}
                    cap = self.config['max_searches' if kind == 'search' else 'max_details']
                    count = db.execute('SELECT count(*) FROM events WHERE campaign=? AND kind=?', (self.campaign, kind)).fetchone()[0]
                    day = day_at(moment)
                    daily = db.execute('SELECT count(*) FROM events WHERE day=? AND kind=?', (day, kind)).fetchone()[0]
                    if count >= cap:
                        return {'state': 'budget_exhausted'}
                    if daily >= self.config['daily_searches' if kind == 'search' else 'daily_details']:
                        return {'state': 'daily_limit'}
                    last = db.execute('SELECT coalesce(max(due),0) FROM events').fetchone()[0]
                    last_map = db.execute("SELECT coalesce(max(due),0) FROM events WHERE kind='search'").fetchone()[0]
                    detail_ready = self.config['mode'] != 'live' or moment >= last + 5
                    map_ready = detail_ready and (self.config['mode'] != 'live' or moment >= last_map + 30)
                    if detail_ready:
                        selected = db.execute('SELECT identity FROM mapwalk_pool_waiters WHERE campaign=? AND (kind=\'detail\' OR ?=1) ORDER BY requested,identity LIMIT 1',
                                              (self.campaign, int(map_ready))).fetchone()
                        if selected and selected[0] == identity:
                            event_id = 'pool:' + self.campaign + ':' + identity
                            if db.execute('SELECT 1 FROM events WHERE id=?', (event_id,)).fetchone():
                                return {'state': 'already_reserved'}
                            db.execute('INSERT INTO events VALUES (?,?,?,?,?,?)', (event_id, self.campaign, kind, identity, moment, day))
                            db.execute('DELETE FROM mapwalk_pool_waiters WHERE campaign=? AND identity=?', (self.campaign, identity))
                            db.commit()
                            return {'state': 'reserved', 'due': moment}
                    db.rollback()
                time.sleep(.15)
            return {'state': 'deadline'}
        finally:
            with closing(self.connect()) as db:
                db.execute('DELETE FROM mapwalk_pool_waiters WHERE campaign=? AND identity=?', (self.campaign, identity))
                db.commit()


class PoolStore(Store):
    def claim_next(self, region, owner):
        """Quarantined views stay unresolved while later route views can proceed."""
        with closing(self.connect(write=True)) as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT * FROM views WHERE plan=(SELECT value FROM meta WHERE key='active_plan') AND governorate=? AND state IN ('pending','paused') AND owner IS NULL ORDER BY ordinal LIMIT 1", (region,)).fetchone()
            if not row:
                return None
            payload = json.loads(row['payload'])
            expected = next(v for v in self.plan()['views'] if v['governorate'] == region and v['ordinal'] == row['ordinal'])
            if payload != expected:
                raise ValueError('Immutable route mismatch')
            db.execute("UPDATE views SET state='working',owner=?,updated=? WHERE id=?", (owner, time.time(), row['id']))
            db.commit()
            return {**dict(row), 'owner': owner, 'payload': payload, 'camera': json.loads(row['camera']) if row['camera'] else None}


class AccessStop(RuntimeError):
    pass


class NetworkInterrupted(RuntimeError):
    pass


def check_network_error(error):
    codes = ("INTERNET_DISCONNECTED", "NETWORK_CHANGED", "NAME_NOT_RESOLVED",
             "CONNECTION_RESET", "CONNECTION_TIMED_OUT", "ADDRESS_UNREACHABLE")
    if any(f"net::ERR_{code}" in str(error) for code in codes):
        raise NetworkInterrupted("Browser connection interrupted") from error


class PoolReader(MapReader):
    def check_block(self):
        try:
            return super().check_block()
        except PermissionError as error:
            raise AccessStop('Observed browser access stop') from error

    def restore(self, *args, **kwargs):
        try:
            return super().restore(*args, **kwargs)
        except PermissionError as error:
            if self.stage == 'camera_document' and self.diagnostic.get('main_document_status') in (401, 403, 429):
                raise AccessStop('Observed document access status') from error
            raise
        except Exception as error:
            check_network_error(error)
            raise

    def inspect(self, saved, cache):
        current = match_target(saved, self.current_targets)
        self.emit('marker_click')
        self.left()
        self.page.mouse.click(current['x'], current['y'])
        if self.mode == 'live':
            import re
            try:
                self.page.wait_for_url(re.compile(r'https://(?:www\.)?google\.com/maps/place/'), timeout=self.left(7))
            except Exception as error:
                check_network_error(error)
                self.check_block()
                return {'status': 'not_place', 'reason': 'detected_color_did_not_open_a_place'}
            identity = place_identity(self.page.url)
        else:
            identity = self.page.locator('#detail').get_attribute('data-identity')
        if not identity:
            return {'status': 'unresolved', 'reason': 'selected_place_identity_missing'}
        # Wait for a stable single visible title, never choose an arbitrary title.
        end = min(time.monotonic() + 4, getattr(self, 'deadline', float('inf')))
        previous = None
        stable = 0
        heading = self.page.locator('h1:visible')
        while time.monotonic() < end:
            self.check_block()
            text = heading.inner_text().strip()[:200] if heading.count() == 1 else None
            still_same = self.mode != 'live' or place_identity(self.page.url) == identity
            stable = stable + 1 if text and text == previous and still_same else 0
            if stable >= 2:
                break
            previous = text
            self.page.wait_for_timeout(min(self.left(), 150))
        else:
            return {'status': 'unresolved', 'reason': 'ambiguous_or_unsettled_selected_place_heading', 'visible_h1_count': heading.count()}
        known = cache(identity)
        if known:
            return {'status': 'cached', 'identity': identity, 'label': text, 'website': known.get('website')}
        self.emit('selected_place_heading')
        actions = self.page.locator('a[data-item-id="authority"]:visible')
        try:
            actions.first.wait_for(state='visible', timeout=min(self.left(2), 100 if self.mode == 'fixture' else 2000))
        except Exception:
            pass
        self.check_block()
        urls = {actions.nth(i).get_attribute('href') for i in range(min(actions.count(), 5))}
        urls.discard(None)
        if len(urls) > 1:
            return {'status': 'unresolved', 'identity': identity, 'label': text, 'reason': 'ambiguous_website_actions'}
        return {'status': 'captured' if urls else 'no_website', 'identity': identity, 'label': text,
                'website': next(iter(urls), None), 'identity_note': 'Observed public Maps feature/fingerprint, not an API Place ID'}


def configuration(folder):
    raw = (folder / 'config.json').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == (folder / 'config.sha256').read_text().strip(), 'Configuration changed'
    return json.loads(raw)


def prepare(root, folder, *, mode, seconds=3600, limit=120, workbook=None, node=None, writer=None):
    if not 5 <= seconds <= 3600 or not 1 <= limit <= 120 or mode not in ('live', 'fixture'):
        raise ValueError('Finite pilot bounds required')
    folder.mkdir(parents=True, exist_ok=False)
    template = root / 'reports/map_walk_live_pilot_2026-10-16/walk.sqlite3'
    if mode == 'live':
        pins = json.loads((root / 'reviews/fyp_map_walk_ten_minute_checkpoint_2026-10-16.json').read_text(encoding='utf-8'))['artifacts']
        for name, pin in pins.items():
            p = Path(name); p = p if p.is_absolute() else root / p
            if hashlib.sha256(p.read_bytes()).hexdigest() != pin['sha256']:
                raise ValueError('Previous protected artifact changed: ' + name)
        plan = Store(template).plan()
    else:
        plan = prepare_plan(root, mode='fixture')
    source = root / 'reports/local_browser_service_v1/source.sqlite3' if mode == 'live' else folder / 'source.sqlite3'
    baseline_daily = {'search': 0, 'detail': 0}
    if mode == 'live':
        with closing(sqlite3.connect(source.resolve().as_uri() + '?mode=ro', uri=True)) as db:
            for kind in baseline_daily:
                baseline_daily[kind] = db.execute('SELECT count(*) FROM events WHERE day=? AND kind=?', (day_at(time.time()), kind)).fetchone()[0]
        with closing(sqlite3.connect(source.resolve().as_uri() + '?mode=ro', uri=True)) as src, closing(sqlite3.connect(folder / 'source-before.sqlite3')) as dst:
            src.backup(dst)
    config = {'kind': 'mapwalk-pool-v1', 'mode': mode, 'seconds': seconds, 'regions': plan['regions'], 'limit': limit,
              'source_path': str(source), 'source_limits': {'mode': mode, 'max_searches': limit, 'max_details': limit,
              'daily_searches': baseline_daily['search'] + limit, 'daily_details': baseline_daily['detail'] + limit},
              'created_at': now_iso(), 'workbook': str(workbook) if workbook else None, 'node': str(node) if node else None,
              'writer': str(writer) if writer else None, 'minimum_free_mb': 768, 'fixture_max_views': 1}
    config['campaign'] = 'mapwalk-pool:' + key(config)
    for region in plan['regions']:
        child = folder / 'regions' / slug(region); child.mkdir(parents=True)
        if mode == 'live':
            with closing(sqlite3.connect(template.resolve().as_uri() + '?mode=ro', uri=True)) as src, closing(sqlite3.connect(child / 'walk.sqlite3')) as dst:
                src.backup(dst)
        else:
            Store.create(child / 'walk.sqlite3', plan)
        store = PoolStore(child / 'walk.sqlite3')
        with closing(store.connect()) as db:
            baseline = [[r[0], r[1]] for r in db.execute('SELECT target_id,attempt FROM outcomes')]
            owned = [dict(r) for r in db.execute('SELECT * FROM views WHERE governorate=? AND owner IS NOT NULL', (region,))]
        for row in owned:
            targets = store.targets(row['id'])
            if any(t['state'] == 'started' for t in targets):
                raise ValueError('Started historical target requires separate review')
            store.recover(row['id'], 'New user-authorized pool: prior deadline stopped camera restoration; no started target exists. Preserve previous source events and outcomes.')
        write(child / 'baseline.json', baseline)
    write(folder / 'plan.json', plan)
    write(folder / 'config.json', config)
    (folder / 'config.sha256').write_text(hashlib.sha256((folder / 'config.json').read_bytes()).hexdigest() + '\n')
    return config


def worker(root, folder, region):
    c = configuration(folder); child = folder / 'regions' / slug(region)
    if region not in c['regions']:
        raise ValueError('Unassigned region')
    runtime = json.loads((folder / 'runtime.json').read_text())
    deadline_epoch = runtime['deadline_unix']
    deadline = time.monotonic() + max(0, deadline_epoch - time.time())
    store = PoolStore(child / 'walk.sqlite3')
    source = PoolSource(Path(c['source_path']), child, c['source_limits'], c['campaign'])
    owner = uuid.uuid4().hex
    reader = None; view = None; started_target = None
    counts = {'maps': 0, 'clicks': 0, 'views_inspected': 0, 'quarantined_views': 0}
    status = 'running'; reason = None
    def heartbeat(stage):
        write(child / 'heartbeat.json', {'region': region, 'pid': os.getpid(), 'status': status,
             'stage': stage, 'at': time.time(), 'view_id': view['id'] if view else None, **counts})
    def reserve(kind):
        nonlocal reason
        heartbeat('pacing_' + kind)
        result = source.reserve(owner + ':' + kind + ':' + str(sum(counts.values())), kind=kind, deadline=deadline_epoch, region=region)
        if result['state'] != 'reserved':
            reason = result['state']; return False
        counts['maps' if kind == 'search' else 'clicks'] += 1
        heartbeat('reserved_' + kind)
        return True
    try:
        while time.monotonic() < deadline and not source.stopped():
            if c['mode'] == 'fixture' and counts['views_inspected'] >= c['fixture_max_views']:
                status = 'fixture_finished'; break
            view = store.claim_next(region, owner)
            if not view:
                status = 'eligible_route_finished'; break
            if view['camera'] is not None and not [t for t in store.targets(view['id']) if t['state'] == 'pending']:
                store.release(view['id'], owner, complete=True); counts['views_inspected'] += 1; view = None; continue
            if not reserve('search'):
                store.release(view['id'], owner); view = None; break
            if reader is None:
                reader = PoolReader(c['mode'], child, headed=c['mode'] == 'live'); reader.__enter__()
            reader.deadline = deadline
            heartbeat('restore')
            observed, targets = reader.restore(view, navigation_number=counts['maps'])
            if view['camera'] is None:
                store.save_view(view['id'], owner, observed, targets)
            pending = [t for t in store.targets(view['id']) if t['state'] == 'pending']
            clean = True
            for i, target in enumerate(pending):
                if i:
                    if not reserve('search'):
                        clean = False; break
                    reader.restore(view, navigation_number=counts['maps'])
                if not reserve('detail'):
                    clean = False; break
                if time.monotonic() >= deadline:
                    clean = False; break
                store.begin_target(view['id'], owner, target['id']); started_target = target
                heartbeat('inspect')
                result = reader.inspect(target['payload'], store.cached)
                store.remember(view['id'], owner, target['id'], result); started_target = None
                if result['status'] == 'unresolved':
                    clean = False; counts['quarantined_views'] += 1; reason = result.get('reason'); break
            store.release(view['id'], owner, complete=clean, reason=reason or 'pool_pause')
            if clean:
                counts['views_inspected'] += 1
            view = None
            if not clean and reason in ('deadline', 'stopped', 'daily_limit', 'budget_exhausted'):
                break
        if status == 'running':
            status = 'paused'; reason = reason or ('deadline' if time.monotonic() >= deadline else 'stop_requested')
    except Exception as error:
        # Do not persist exception bodies/URLs/account text.
        status, reason = ('blocked', 'access_stop') if isinstance(error, AccessStop) else ('paused_at_deadline', 'deadline') if time.monotonic() >= deadline else ('needs_inspection', type(error).__name__)
        if status == 'blocked':
            source.halt('mapwalk_pool_access_stop')
        if view:
            if started_target:
                store.remember(view['id'], owner, started_target['id'], {'status': 'unresolved', 'reason': 'action_unfinished:' + reason})
                counts['quarantined_views'] += 1
            store.release(view['id'], owner, reason=reason)
            view = None
    finally:
        if reader:
            try:
                reader.__exit__(None, None, None)
            except Exception:
                status = 'cleanup_needs_inspection'
        result = {'region': region, 'pid': os.getpid(), 'status': status, 'reason': reason, 'ended_at': now_iso(), **counts}
        heartbeat('ended'); write(child / 'result.json', result)
        (child / 'result.sha256').write_text(hashlib.sha256((child / 'result.json').read_bytes()).hexdigest() + '\n')
    return result


def collect(folder):
    c = configuration(folder); rows = []; regions = []
    for region in c['regions']:
        child = folder / 'regions' / slug(region); store = PoolStore(child / 'walk.sqlite3')
        baseline = {tuple(x) for x in json.loads((child / 'baseline.json').read_text())}
        observations = []
        with closing(store.connect()) as db:
            for r in db.execute('SELECT * FROM outcomes ORDER BY at'):
                if (r['target_id'], r['attempt']) not in baseline:
                    o = json.loads(r['result']); o.update(region=region, target_id=r['target_id'], attempt=r['attempt']); observations.append(o)
            states = dict(db.execute('SELECT state,count(*) FROM views WHERE governorate=? GROUP BY state', (region,)))
        rows.extend(observations)
        hp = child / 'heartbeat.json'
        heartbeat = read_json(hp) if hp.exists() else {'status': 'not_started'}
        regions.append({'region': region, 'links': sum(bool(o.get('website')) for o in observations),
             'no_website': sum(o['status'] == 'no_website' for o in observations), 'unresolved_attempts': sum(o['status'] == 'unresolved' for o in observations),
             'view_states': states, **heartbeat})
    return {'kind': 'mapwalk-pool-progress', 'updated_at': now_iso(), 'mode': c['mode'], 'regions': regions,
            'rows': sorted(rows, key=lambda r: r['observed_unix']), 'links': sum(bool(r.get('website')) for r in rows),
            'new_supported_domains': 0, 'reviews': 0, 'imports': 0, 'source_notices': 'Google Maps public UI candidate observations. Unreviewed. Colored-icon detection is incomplete.'}


def run(root, folder, *, enable_live=False, enable_workbook=True):
    c = configuration(folder); live = c['mode'] == 'live'
    if live and not enable_live:
        raise ValueError('Live browser flag required')
    if (folder / 'runtime.json').exists():
        raise ValueError('Existing pool run is read-only; inspect before any resume')
    lock = root / 'reports/local_browser_source_v1/source.lock'
    with WorkerLock(folder / '.pool.lock'), (WorkerLock(lock) if live else nullcontext()):
        source = PoolSource(Path(c['source_path']), folder, c['source_limits'], c['campaign'])
        source.initialize(root if live else None); source.setup_queue()
        if source.stopped():
            raise PermissionError('Existing source halt retained; inspected acknowledgement required')
        start = time.time(); runtime = {'start_unix': start, 'deadline_unix': start + c['seconds'], 'started_at': now_iso(), 'pid': os.getpid(), 'children': {}}
        write(folder / 'runtime.json', runtime)
        processes = {}; logs = []; excel = None; status = 'running'; reason = None
        payload_path = folder / 'workbook-data.json'
        initial = collect(folder); initial.update(revision=0, pool_status='starting', deadline_unix=runtime['deadline_unix'])
        write(payload_path, initial)
        if enable_workbook and c.get('workbook'):
            logfile = (folder / 'workbook.log').open('w', encoding='utf-8'); logs.append(logfile)
            excel = subprocess.Popen([c['node'], c['writer'], str(payload_path), c['workbook'], '--watch'], cwd=root, stdout=logfile, stderr=logfile)
        try:
            for region in c['regions']:
                log = (folder / 'regions' / slug(region) / 'worker.log').open('w', encoding='utf-8'); logs.append(log)
                proc = subprocess.Popen([sys.executable, '-X', 'utf8', str(root / 'scripts/map_walk_pool.py'), '--folder', str(folder), 'worker', '--region', region], cwd=root, stdout=log, stderr=log)
                processes[region] = proc; runtime['children'][region] = proc.pid
            write(folder / 'runtime.json', runtime)
            peak = len(processes); revision = 0; last_export = 0; stopping_at = None
            while any(p.poll() is None for p in processes.values()):
                free = free_memory_mb()
                if time.time() >= runtime['deadline_unix']:
                    status, reason = 'stopping', 'deadline'
                elif free < c['minimum_free_mb']:
                    status, reason = 'stopping', 'low_memory'
                elif (folder / 'STOP').exists() or source.stopped():
                    status, reason = 'stopping', 'source_or_manual_stop'
                elif excel and excel.poll() is not None:
                    status, reason = 'stopping', 'workbook_writer_stopped'
                if status == 'stopping':
                    stopping_at = stopping_at or time.time()
                    for region in processes:
                        (folder / 'regions' / slug(region) / 'STOP').touch(exist_ok=True)
                if time.time() - last_export >= 2:
                    progress = collect(folder); revision += 1
                    progress.update(revision=revision, pool_status=status, reason=reason, deadline_unix=runtime['deadline_unix'], free_memory_mb=round(free),
                                    active_processes=sum(p.poll() is None for p in processes.values()), peak_processes=peak)
                    write(folder / 'progress.json', progress); write(payload_path, progress); last_export = time.time()
                if stopping_at and time.time() > stopping_at + 20:
                    for p in processes.values():
                        if p.poll() is None:
                            if os.name == 'nt':
                                subprocess.run(['taskkill', '/PID', str(p.pid), '/T', '/F'], capture_output=True)
                            else:
                                p.terminate()
                    break
                time.sleep(.5)
            final = collect(folder); revision += 1
            final.update(revision=revision, pool_status='finished', reason=reason or 'workers_finished', deadline_unix=runtime['deadline_unix'],
                         elapsed_seconds=round(time.time() - start, 3), active_processes=sum(p.poll() is None for p in processes.values()), peak_processes=peak,
                         exit_codes={r: p.poll() for r, p in processes.items()})
            write(folder / 'progress.json', final); write(payload_path, final)
            with (folder / 'collected-links.csv').open('w', encoding='utf-8-sig', newline='') as f:
                fields = ['region','label','website','site_key','domain','identity','observed_in_view','observed_unix','status','review','http_availability']
                w = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore'); w.writeheader(); w.writerows(r for r in final['rows'] if r.get('website'))
            if excel:
                until = time.time() + 35
                while time.time() < until and excel.poll() is None:
                    receipt = Path(c['workbook']).with_suffix('.receipt.json')
                    if receipt.exists() and read_json(receipt).get('revision') == revision:
                        break
                    time.sleep(.5)
            write(folder / 'result.json', {**final, 'workbook': c.get('workbook')})
            return final
        finally:
            for region, p in processes.items():
                if p.poll() is None:
                    (folder / 'regions' / slug(region) / 'STOP').touch(exist_ok=True)
            for p in processes.values():
                try:
                    p.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    if os.name == 'nt':
                        subprocess.run(['taskkill', '/PID', str(p.pid), '/T', '/F'], capture_output=True)
                    else:
                        p.terminate()
            if excel and excel.poll() is None:
                excel.terminate(); excel.wait(timeout=10)
            for log in logs:
                log.close()
