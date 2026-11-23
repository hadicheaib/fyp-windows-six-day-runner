"""Immutable geographic work catalog; no network or browser operations."""
import hashlib
import json
from pathlib import Path
import sqlite3


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def camera_key(camera):
    value = [round(camera['latitude'], 8), round(camera['longitude'], 8), camera['zoom'],
             camera['width'], camera['height'], camera['roi'], 'colored-marker-v1', 'en-US']
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


def allocate(counts, total=50):
    """One worker per region, then largest-remainder allocation by work size."""
    if total < len(counts) or any(n < 1 for n in counts.values()):
        raise ValueError('Each region needs work and at least one worker')
    left = total - len(counts)
    size = sum(counts.values())
    quota = {r: left * n / size for r, n in counts.items()}
    answer = {r: 1 + int(q) for r, q in quota.items()}
    for r in sorted(counts, key=lambda r: (-(quota[r] - int(quota[r])), r))[:total-sum(answer.values())]:
        answer[r] += 1
    if any(answer[r] > counts[r] for r in counts):
        raise ValueError('Too many shards for available geographic views')
    return answer


def build(planning, destination, workers=50):
    planning, destination = Path(planning), Path(destination)
    if destination.exists():
        raise FileExistsError(destination)
    manifest_raw = (planning / 'manifest.json').read_bytes()
    manifest = json.loads(manifest_raw)
    if manifest.get('kind') != 'national-chunks-planning-v1' or manifest['live_collection_enabled']:
        raise ValueError('Expected immutable offline planning chunks')
    destination.mkdir(parents=True)
    db = sqlite3.connect(destination / 'catalog.sqlite3')
    try:
        db.executescript('''
          PRAGMA application_id=1279413809;
          CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE cameras(id TEXT PRIMARY KEY,camera_key TEXT UNIQUE,region TEXT,ordinal INTEGER,
            camera TEXT,worker_id TEXT);
          CREATE TABLE memberships(id TEXT,region TEXT,source_id TEXT,ordinal INTEGER,
            PRIMARY KEY(id,region),UNIQUE(source_id));
          CREATE TABLE workers(id TEXT PRIMARY KEY,region TEXT,views INTEGER,bbox TEXT);
        ''')
        original = 0
        with db:
            for chunk in manifest['chunks']:
                raw = (planning / chunk['file']).read_bytes()
                if hashlib.sha256(raw).hexdigest() != chunk['sha256']:
                    raise ValueError('National planning chunk changed')
                plan = json.loads(raw)
                for view in plan['views']:
                    sig = camera_key(view['camera']); vid = 'camera:' + sig
                    db.execute('INSERT OR IGNORE INTO cameras VALUES (?,?,?,?,?,NULL)',
                               (vid, sig, view['governorate'], view['ordinal'], canonical(view['camera'])))
                    db.execute('INSERT INTO memberships VALUES (?,?,?,?)',
                               (vid, view['governorate'], view['view_id'], view['ordinal']))
                    original += 1
            if original != manifest['views']:
                raise ValueError('National view count mismatch')
            db.execute('UPDATE cameras SET region=(SELECT min(region) FROM memberships m WHERE m.id=cameras.id)')
            db.execute('UPDATE cameras SET ordinal=(SELECT ordinal FROM memberships m WHERE m.id=cameras.id AND m.region=cameras.region)')
            counts = dict(db.execute('SELECT region,count(*) FROM cameras GROUP BY region'))
            assignment = allocate(counts, workers)
            worker_rows = []
            for region in sorted(counts):
                views = db.execute('SELECT id,camera FROM cameras WHERE region=? ORDER BY ordinal,id', (region,)).fetchall()
                groups = [[] for _ in range(assignment[region])]
                for i, row in enumerate(views):
                    groups[min(len(groups)-1, i*len(groups)//len(views))].append(row)
                for group in groups:
                    name = 'w%03d' % (len(worker_rows)+1)
                    boxes = [json.loads(v[1])['planned_roi_bbox'] for v in group]
                    bounds = [min(v[0] for v in boxes), min(v[1] for v in boxes),
                              max(v[2] for v in boxes), max(v[3] for v in boxes)]
                    db.executemany('UPDATE cameras SET worker_id=? WHERE id=?', [(name,v[0]) for v in group])
                    db.execute('INSERT INTO workers VALUES (?,?,?,?)', (name,region,len(group),canonical(bounds)))
                    worker_rows.append({'worker_id':name,'region':region,'views':len(group),'planned_bbox':bounds})
            db.executescript('CREATE INDEX shard_order ON cameras(worker_id,ordinal); CREATE INDEX regional_order ON cameras(region,ordinal);')
            meta = {'kind':'national-camera-catalog-v1','workers':workers,'original_regional_views':original,
                    'unique_camera_views':sum(counts.values()),'primary_region_counts':counts,
                    'workers_per_region':assignment,'worker_shards':worker_rows,
                    'grid_sha256':manifest['grid_sha256'],'boundary_status':manifest['boundary_status'],
                    'planning_manifest_sha256':hashlib.sha256(manifest_raw).hexdigest(),
                    'meaning':'All queued camera windows, not exhaustive business/website recall. Regional assignment is scheduling metadata.'}
            db.executemany('INSERT INTO metadata VALUES (?,?)', [(k,canonical(v)) for k,v in meta.items()])
        assert db.execute('SELECT count(*) FROM cameras WHERE worker_id IS NULL').fetchone()[0] == 0
    finally:
        db.close()
    path = destination / 'catalog.sqlite3'
    meta.update(catalog_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),catalog_bytes=path.stat().st_size)
    (destination / 'catalog.json').write_text(json.dumps(meta,indent=2)+'\n',encoding='utf-8')
    return meta
