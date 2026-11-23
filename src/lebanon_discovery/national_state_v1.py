"""Single-writer national state and fsynced action journal; node-local SQLite.

The durable journal is acknowledged before any browser action. Rolling SQLite
snapshots are caches of that journal, not a replacement for historical evidence.
"""
import csv
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
from contextlib import closing
from datetime import datetime
from zoneinfo import ZoneInfo

from .national_catalog_v1 import canonical, camera_key


def write_json(path, value):
    path = Path(path);temp = path.with_name(path.name + '.writing')
    with temp.open('w',encoding='utf-8') as f:
        json.dump(value,f,ensure_ascii=True,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
    os.replace(temp,path)
    if os.name!='nt':
        fd=os.open(str(path.parent),os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(fd)
        finally:os.close(fd)


def normalized(value):
    from .websites import normalize_website
    site = normalize_website(value)
    result = {k:getattr(site,k) for k in ('canonical_url','registrable_domain','site_key','classification')}
    if site.hostname == 'sites.google.com':
        result['classification'] = 'candidate'
    if site.hostname.endswith('.wixsite.com'):
        from urllib.parse import urlsplit
        parts = [s for s in urlsplit(site.canonical_url).path.split('/') if s]
        if parts:result['site_key'] = site.hostname + '/' + parts[0]
    return result


def snapshot_db(source, destination):
    destination = Path(destination)
    with closing(sqlite3.connect(destination)) as out:
        source.backup(out)
    with destination.open('r+b') as f:os.fsync(f.fileno())


class State:
    def __init__(self,path,catalog):
        self.path=Path(path)
        self.db=sqlite3.connect(path,uri=True)
        self.db.row_factory=sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=NORMAL')
        self.db.execute('ATTACH DATABASE ? AS geography',(Path(catalog).resolve().as_uri()+'?mode=ro',))

    def close(self):self.db.close()

    @classmethod
    def initialize(cls,path,catalog,source=None,legacy=None,regions=None):
        path=Path(path)
        if path.exists():raise FileExistsError(path)
        if source:
            with closing(sqlite3.connect(Path(source).resolve().as_uri()+'?mode=ro',uri=True)) as src:
                with closing(sqlite3.connect(path)) as out:src.backup(out)
        self=cls(path,catalog)
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY,campaign TEXT,kind TEXT,identity TEXT,due REAL,day TEXT);
          CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS n_event_time ON events(kind,due);
          CREATE INDEX IF NOT EXISTS n_event_day ON events(day,kind);
          CREATE INDEX IF NOT EXISTS n_event_campaign ON events(campaign,kind);
          CREATE TABLE n_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE n_views(id TEXT PRIMARY KEY,state TEXT,owner TEXT,observed TEXT,
            active_kind TEXT,active_target TEXT,reason TEXT,updated REAL);
          CREATE INDEX n_owners ON n_views(owner);
          CREATE TABLE n_targets(id TEXT PRIMARY KEY,view_id TEXT,ordinal INTEGER,payload TEXT,
            state TEXT,result TEXT);
          CREATE INDEX n_target_view ON n_targets(view_id,ordinal);
          CREATE TABLE n_cache(identity TEXT PRIMARY KEY,result TEXT,at REAL);
          CREATE TABLE n_sites(site_key TEXT PRIMARY KEY,domain TEXT,url TEXT,classification TEXT,first_seen REAL);
          CREATE TABLE n_observations(id TEXT PRIMARY KEY,region TEXT,view_id TEXT,target_id TEXT,
            observed REAL,payload TEXT,legacy INTEGER);
          CREATE INDEX n_observation_time ON n_observations(observed);
        ''')
        with self.db:
            for key,value in [('journal_offset','0'),('journal_hash',''),('journal_seq','0')]:
                self.db.execute('INSERT INTO n_meta VALUES (?,?)',(key,value))
            for i,row in enumerate(legacy or []):
                self.db.execute('INSERT INTO n_observations VALUES (?,?,?,?,?,?,1)',
                    ('legacy:%08d'%i,row.get('region',''),row.get('observed_in_view','legacy'),'',
                     row.get('observed_unix',0),canonical(row)))
                self._site(row)
            tables={x[0] for x in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if 'browser_sites' in tables:
                for row in self.db.execute('SELECT website FROM browser_sites').fetchall():
                    try:self._site({'website':json.loads(row[0])['canonical_url']})
                    except (KeyError,ValueError):pass
            previous_quarantine=[]
            for region,path2 in (regions or {}).items():
                with closing(sqlite3.connect(Path(path2).resolve().as_uri()+'?mode=ro',uri=True)) as old:
                    for identity,raw in old.execute('SELECT identity,result FROM place_cache'):
                        value=json.loads(raw)
                        if value.get('status') in ('captured','no_website'):
                            self.db.execute('INSERT OR IGNORE INTO n_cache VALUES (?,?,?)',
                                            (identity,raw,value.get('observed_unix',0)))
                    for state2,raw in old.execute('SELECT state,payload FROM views WHERE governorate=?', (region,)):
                        if state2 not in ('inspected','inspected_no_detected_targets','unresolved'):continue
                        sig=camera_key(json.loads(raw)['camera'])
                        current=self.db.execute('SELECT id FROM geography.cameras WHERE camera_key=?',(sig,)).fetchone()
                        if not current:continue
                        status='incomplete' if state2=='unresolved' else 'inspected' if state2=='inspected' else 'no_targets'
                        self.db.execute('INSERT OR IGNORE INTO n_views VALUES (?,?,NULL,NULL,NULL,NULL,?,?)',
                                        (current[0],status,'previous_same_camera_'+state2,time.time()))
                        if status=='incomplete':previous_quarantine.append(current[0])
            self.db.execute('INSERT INTO n_meta VALUES (?,?)',('legacy_quarantined_views',canonical(previous_quarantine)))
        return self

    def _site(self,result):
        if not result.get('website'):return
        try:site=normalized(result['website'])
        except (ValueError,UnicodeError):return
        self.db.execute('INSERT OR IGNORE INTO n_sites VALUES (?,?,?,?,?)',
            (site['site_key'],site['registrable_domain'],site['canonical_url'],site['classification'],result.get('observed_unix',time.time())))

    def meta(self,key):
        return self.db.execute('SELECT value FROM n_meta WHERE key=?',(key,)).fetchone()[0]

    def pending_view(self,worker):
        sql='''SELECT c.* FROM geography.cameras c LEFT JOIN n_views v ON v.id=c.id
          WHERE (v.id IS NULL OR (v.state IN ('paused','partial') AND v.owner IS NULL))'''
        row=self.db.execute(sql+' AND c.worker_id=? ORDER BY c.ordinal,c.id LIMIT 1',(worker,)).fetchone()
        if not row:
            row=self.db.execute(sql+' ORDER BY c.region,c.ordinal,c.id LIMIT 1').fetchone()
        return dict(row) if row else None

    def task(self,vid):
        c=dict(self.db.execute('SELECT * FROM geography.cameras WHERE id=?',(vid,)).fetchone())
        v=self.db.execute('SELECT * FROM n_views WHERE id=?',(vid,)).fetchone()
        return {'id':vid,'payload':{'governorate':c['region'],'camera':json.loads(c['camera'])},
                'observed':json.loads(v['observed']) if v and v['observed'] else None,
                'targets':[{'id':r['id'],'payload':json.loads(r['payload'])} for r in
                    self.db.execute("SELECT * FROM n_targets WHERE view_id=? AND state='pending' ORDER BY ordinal",(vid,))]}

    def owned(self,owner,vid):
        row=self.db.execute('SELECT * FROM n_views WHERE id=? AND owner=?',(vid,owner)).fetchone()
        if not row:raise ValueError('View ownership changed')
        return row

    def validate(self,kind,d):
        if kind=='claim':
            if not self.db.execute('SELECT 1 FROM geography.cameras WHERE id=?',(d['view'],)).fetchone():
                raise ValueError('Unknown camera')
            old=self.db.execute('SELECT * FROM n_views WHERE id=?',(d['view'],)).fetchone()
            if old and (old['owner'] or old['state'] not in ('paused','partial')):
                raise ValueError('Unclaimable view')
        elif kind in ('grant','observed','outcome','release','abandon'):
            v=self.owned(d['owner'],d['view'])
            if kind=='grant':
                if v['active_kind'] is not None:raise ValueError('Action already in flight')
                if d['kind']=='detail' and not self.db.execute(
                    "SELECT 1 FROM n_targets WHERE id=? AND view_id=? AND state='pending'",(d['target'],d['view'])).fetchone():
                    raise ValueError('Target not pending')
                if self.db.execute('SELECT 1 FROM events WHERE id=?',(d['event'][0],)).fetchone():
                    raise ValueError('Duplicate source event')
            elif kind=='observed' and (v['active_kind']!='search' or len(d['targets'])>100):
                raise ValueError('Invalid camera response')
            elif kind=='outcome':
                if v['active_kind']!='detail' or v['active_target']!=d['target']:
                    raise ValueError('Outcome target changed')
                if d['result']['status'] not in ('captured','cached','no_website','not_place','unresolved'):
                    raise ValueError('Invalid outcome status')
                if d['result'].get('website'):normalized(d['result']['website'])
            elif kind=='release' and v['active_kind'] is not None:
                raise ValueError('Cannot release an unfinished action')
        elif kind not in ('halt','batch_begin','batch_end'):
            raise ValueError('Unknown journal event')

    def allowance(self,kind,campaign,limits,now=None):
        now=time.time() if now is None else now
        if kind not in ('search','detail'):raise ValueError('Unknown source action')
        if self.db.execute("SELECT 1 FROM meta WHERE key='halt'").fetchone():return 'halted',None
        day=datetime.fromtimestamp(now,ZoneInfo('Asia/Beirut')).date().isoformat()
        daily=self.db.execute('SELECT count(*) FROM events WHERE day=? AND kind=?',(day,kind)).fetchone()[0]
        batch=self.db.execute('SELECT count(*) FROM events WHERE campaign=? AND kind=?',(campaign,kind)).fetchone()[0]
        rolling=self.db.execute('SELECT count(*) FROM events WHERE kind=? AND due>?',(kind,now-3600)).fetchone()[0]
        if daily>=limits['daily']:return 'daily_limit',None
        if batch>=limits['batch']:return 'batch_limit',None
        if rolling>=limits['hourly']:return 'hourly_limit',None
        last=self.db.execute('SELECT coalesce(max(due),0) FROM events').fetchone()[0]
        last_map=self.db.execute("SELECT coalesce(max(due),0) FROM events WHERE kind='search'").fetchone()[0]
        due=max(now,last+limits['action_spacing'],last_map+limits['map_spacing'] if kind=='search' else 0)
        return ('ready' if due<=now+.00001 else 'pacing'),(due,day)

    def apply(self,record,offset):
        seq=int(self.meta('journal_seq'))
        if record['seq']!=seq+1 or record['previous']!=self.meta('journal_hash'):
            raise ValueError('Journal sequence mismatch')
        kind,d=record['kind'],record['data']
        with self.db:
            if kind=='claim':
                old=self.db.execute('SELECT * FROM n_views WHERE id=?',(d['view'],)).fetchone()
                if old and (old['owner'] or old['state'] not in ('paused','partial')):
                    raise ValueError('Unclaimable view')
                self.db.execute('''INSERT INTO n_views VALUES (?, 'working', ?, NULL,NULL,NULL,NULL,?)
                  ON CONFLICT(id) DO UPDATE SET state='working',owner=excluded.owner,updated=excluded.updated''',
                  (d['view'],d['owner'],record['at']))
            elif kind=='grant':
                self.owned(d['owner'],d['view'])
                self.db.execute('INSERT INTO events VALUES (?,?,?,?,?,?)',tuple(d['event']))
                if d['kind']=='detail':
                    changed=self.db.execute("UPDATE n_targets SET state='started' WHERE id=? AND view_id=? AND state='pending'",
                                            (d['target'],d['view'])).rowcount
                    if changed!=1:raise ValueError('Target not pending')
                self.db.execute('UPDATE n_views SET active_kind=?,active_target=?,updated=? WHERE id=?',
                                (d['kind'],d.get('target'),record['at'],d['view']))
            elif kind=='observed':
                row=self.owned(d['owner'],d['view'])
                if row['active_kind']!='search':raise ValueError('Unreserved camera observation')
                if row['observed'] is None:
                    for i,target in enumerate(d['targets']):
                        tid=hashlib.sha256(canonical([d['view'],i,target]).encode('utf-8')).hexdigest()
                        self.db.execute('INSERT INTO n_targets VALUES (?,?,?,?,?,NULL)',(tid,d['view'],i,canonical(target),'pending'))
                self.db.execute('UPDATE n_views SET observed=?,active_kind=NULL,active_target=NULL WHERE id=?',
                                (canonical(d['observed']),d['view']))
            elif kind=='outcome':
                view=self.owned(d['owner'],d['view'])
                if view['active_kind']!='detail' or view['active_target']!=d['target']:
                    raise ValueError('Outcome has no active target')
                value=d['result'];status=value['status']
                if status not in ('captured','cached','no_website','not_place','unresolved'):
                    raise ValueError('Unknown outcome')
                self.db.execute('UPDATE n_targets SET state=?,result=? WHERE id=?',(status,canonical(value),d['target']))
                if value.get('identity') and status in ('captured','no_website'):
                    self.db.execute('INSERT OR REPLACE INTO n_cache VALUES (?,?,?)',(value['identity'],canonical(value),record['at']))
                if value.get('website'):
                    self._site(value)
                    region=self.db.execute('SELECT region FROM geography.cameras WHERE id=?',(d['view'],)).fetchone()[0]
                    self.db.execute('INSERT INTO n_observations VALUES (?,?,?,?,?,?,0)',
                        ('target:'+d['target'],region,d['view'],d['target'],record['at'],canonical(value)))
                self.db.execute('UPDATE n_views SET active_kind=NULL,active_target=NULL WHERE id=?',(d['view'],))
            elif kind in ('release','abandon'):
                view=self.owned(d['owner'],d['view'])
                if kind=='abandon' and view['active_kind']=='detail':
                    result={'status':'unresolved','reason':d['reason'],'automatic_replay':False}
                    self.db.execute("UPDATE n_targets SET state='unresolved',result=? WHERE id=? AND state='started'",
                                    (canonical(result),view['active_target']))
                pending=self.db.execute("SELECT count(*) FROM n_targets WHERE view_id=? AND state='pending'",(d['view'],)).fetchone()[0]
                unresolved=self.db.execute("SELECT count(*) FROM n_targets WHERE view_id=? AND state='unresolved'",(d['view'],)).fetchone()[0]
                count=self.db.execute('SELECT count(*) FROM n_targets WHERE view_id=?',(d['view'],)).fetchone()[0]
                if view['active_kind']=='search':
                    state='incomplete'
                elif pending:state='partial' if unresolved else 'paused'
                elif unresolved:state='incomplete'
                elif view['observed'] is None:state='paused'
                else:state='inspected' if count else 'no_targets'
                if kind=='release' and view['active_kind'] is not None:
                    raise ValueError('Cannot release an unfinished action')
                self.db.execute('UPDATE n_views SET state=?,owner=NULL,active_kind=NULL,active_target=NULL,reason=?,updated=? WHERE id=?',
                                (state,d.get('reason'),record['at'],d['view']))
            elif kind=='halt':
                self.db.execute("INSERT OR REPLACE INTO meta VALUES ('halt',?)",(d['reason'],))
            elif kind not in ('batch_begin','batch_end'):
                raise ValueError('Unknown journal event')
            for key,value in [('journal_seq',str(record['seq'])),('journal_hash',record['hash']),('journal_offset',str(offset))]:
                self.db.execute('UPDATE n_meta SET value=? WHERE key=?',(value,key))

    def stats(self):
        total=self.db.execute('SELECT count(*) FROM geography.cameras').fetchone()[0]
        counts=dict(self.db.execute('SELECT state,count(*) FROM n_views GROUP BY state'))
        rows=self.db.execute('SELECT count(*) FROM n_observations').fetchone()[0]
        return {'total_views':total,'states':counts,'unvisited_views':total-sum(counts.values()),
                'remaining_views':total-sum(counts.get(k,0) for k in ('inspected','no_targets','incomplete')),
                'candidate_observations':rows,'candidate_site_keys':self.db.execute("SELECT count(*) FROM n_sites WHERE classification='candidate'").fetchone()[0],
                'source_events':self.db.execute('SELECT count(*) FROM events').fetchone()[0],
                'halted':bool(self.db.execute("SELECT 1 FROM meta WHERE key='halt'").fetchone()),
                'owned_views':self.db.execute('SELECT count(*) FROM n_views WHERE owner IS NOT NULL').fetchone()[0],
                'new_supported_domains':0,'reviews':0,'imports':0}

    def regional_stats(self):
        return [dict(r) for r in self.db.execute('''SELECT m.region,count(*) AS queued_views,
          sum(CASE WHEN v.state IN ('inspected','no_targets') THEN 1 ELSE 0 END) AS inspected_views,
          sum(CASE WHEN v.state='incomplete' THEN 1 ELSE 0 END) AS incomplete_views
          FROM geography.memberships m LEFT JOIN n_views v ON v.id=m.id GROUP BY m.region''')]


class Journal:
    def __init__(self,path,state):
        self.path=Path(path);self.state=state;self.failed=False
        self.path.touch(exist_ok=True)
        offset=int(state.meta('journal_offset'))
        if self.path.stat().st_size<offset:raise ValueError('Durable journal shrank')
        with self.path.open('rb') as f:
            f.seek(offset)
            while True:
                raw=f.readline()
                if not raw:break
                if not raw.endswith(b'\n'):raise ValueError('Partial durable journal tail; preserve and inspect')
                record=json.loads(raw)
                check=dict(record);digest=check.pop('hash')
                if hashlib.sha256(canonical(check).encode('utf-8')).hexdigest()!=digest:
                    raise ValueError('Durable journal checksum mismatch')
                state.apply(record,f.tell())
        self.file=self.path.open('ab',buffering=0)

    def event(self,kind,data):
        if self.failed:raise RuntimeError('Journal failed; no further writes or grants allowed')
        self.state.validate(kind,data)
        record={'seq':int(self.state.meta('journal_seq'))+1,'previous':self.state.meta('journal_hash'),
                'at':time.time(),'kind':kind,'data':data}
        record['hash']=hashlib.sha256(canonical(record).encode('utf-8')).hexdigest()
        raw=(canonical(record)+'\n').encode('utf-8')
        try:
            if self.file.write(raw)!=len(raw):raise OSError('Short durable journal write')
            os.fsync(self.file.fileno())
            self.state.apply(record,self.file.tell())
        except BaseException:
            self.failed=True
            raise
        return record

    def close(self):self.file.close()


def checkpoint(state,session):
    session=Path(session);folder=session/'snapshots';folder.mkdir(exist_ok=True)
    tmp=folder/('next-%d-%d.sqlite3'%(os.getpid(),time.time_ns()));snapshot_db(state.db,tmp)
    digest=hashlib.sha256(tmp.read_bytes()).hexdigest()
    current,previous=folder/'current.sqlite3',folder/'previous.sqlite3'
    if current.exists():os.replace(current,previous)
    os.replace(tmp,current)
    write_json(folder/'snapshot.json',{'sha256':digest,'journal_offset':int(state.meta('journal_offset')),
              'journal_hash':state.meta('journal_hash'),'source_events':state.stats()['source_events']})


def export_candidates(state,folder):
    folder=Path(folder);folder.mkdir(exist_ok=True)
    for filename,sql,columns in [
        ('candidate-observations.csv','SELECT id,region,view_id,payload FROM n_observations ORDER BY observed,id',
         ('observation_id','governorate','view_id','entity','website','site_key','domain','classification','review','http_availability')),
        ('unique-site-candidates.csv','SELECT site_key,domain,url,classification FROM n_sites ORDER BY site_key',
         ('site_key','domain','url','classification','support'))]:
        temporary=folder/(filename+'.writing')
        with temporary.open('w',encoding='utf-8-sig',newline='') as f:
            writer=csv.writer(f);writer.writerow(columns)
            for row in state.db.execute(sql):
                if filename.startswith('candidate-observations'):
                    value=json.loads(row[3]);site=normalized(value['website'])
                    values=[row[0],row[1],row[2],value.get('label',''),value['website'],site['site_key'],
                            site['registrable_domain'],site['classification'],'pending','not_checked']
                else:values=list(row)+['candidate_not_reviewed']
                writer.writerow([("'"+x if isinstance(x,str) and x.startswith(('=','+','-','@')) else x) for x in values])
            f.flush();os.fsync(f.fileno())
        os.replace(temporary,folder/filename)
