"""Versioned file-scan race correction; frozen v1 is preserved.

Browser workers, budgets, journal, snapshots and renewal policy are unchanged.
"""
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import queue
import shutil
import signal
import sqlite3
import subprocess
import sys
import time

from .hpc_memory_v1 import memory_snapshot
from .national_file_health_v1 import folder_bytes, record_failure
from .national_state_v1 import State, Journal, checkpoint, export_candidates, write_json, normalized
from .national_worker_v1 import worker


def read(path):return json.loads(Path(path).read_text(encoding='utf-8'))


def restore(session,work,catalog,config):
    local_catalog=work/'catalog.sqlite3'
    shutil.copyfile(catalog,local_catalog)
    if hashlib.sha256(local_catalog.read_bytes()).hexdigest()!=config['catalog_sha256']:
        raise ValueError('National catalog changed')
    pointer=session/'snapshots/snapshot.json'
    if pointer.exists():
        meta=read(pointer);chosen=None
        for name in ('current.sqlite3','previous.sqlite3'):
            p=session/'snapshots'/name
            if p.exists() and hashlib.sha256(p.read_bytes()).hexdigest()==meta['sha256']:
                chosen=p;break
        if chosen is None:raise ValueError('No verified snapshot matches pointer')
        shutil.copyfile(chosen,work/'state.sqlite3')
        state=State(work/'state.sqlite3',local_catalog)
    else:
        if (session/'events.jsonl').exists() and (session/'events.jsonl').stat().st_size:
            raise ValueError('Journal exists without a verified bootstrap snapshot')
        if config['mode']=='live':
            source=Path('/predecessor/state/source.sqlite3')
            if hashlib.sha256(source.read_bytes()).hexdigest()!=config['seed_source_sha256']:
                raise ValueError('Predecessor source changed before migration')
            for name,digest in config['seed_state_hashes'].items():
                if hashlib.sha256((Path('/predecessor/state')/name).read_bytes()).hexdigest()!=digest:
                    raise ValueError('Predecessor state changed before migration')
            if hashlib.sha256(Path('/predecessor-run/latest/result.json').read_bytes()).hexdigest()!=config['seed_result_sha256']:
                raise ValueError('Predecessor export changed before migration')
            legacy=read('/predecessor-run/latest/result.json')['all_links']
            old={r:Path('/predecessor/state')/(r.lower().replace(' ','-')+'.sqlite3') for r in config['regions']}
            state=State.initialize(work/'state.sqlite3',local_catalog,source,legacy,old)
        else:state=State.initialize(work/'state.sqlite3',local_catalog)
        checkpoint(state,session)
        write_json(session/'BOOTSTRAP.json',{'mode':config['mode'],'stats':state.stats(),
                   'catalog_sha256':config['catalog_sha256'],'seed_source_sha256':config.get('seed_source_sha256')})
    journal=Journal(session/'events.jsonl',state)
    return state,journal


def manage(session,work,catalog,*,job_id=None,reader_factory=None,stop_requested=None):
    session,work,catalog=map(Path,(session,work,catalog))
    config=read(session/'config.json');job=job_id or os.environ.get('SLURM_JOB_ID','fixture')
    if job_id is not None and (config.get('kind')!='cloud-session-v1'
            or not (session/'snapshots/snapshot.json').exists()):
        raise ValueError('Cloud execution requires its own initialized session')
    if config['mode']=='live' and (job=='fixture' or os.environ.get('FYP_ENABLE_LIVE')!='1'):
        raise ValueError('Explicit live execution required')
    gate=None
    if config.get('host_pace_id'):
        from .host_pace import HostPace, ROOT
        gate=HostPace(ROOT,config['mode'])
        if gate.identity!=config['host_pace_id'] or gate.expires_epoch!=config['expires_epoch']:
            raise ValueError('Shared pacing ledger identity or expiry changed')
    work.mkdir(parents=True,exist_ok=False)
    run=session/'runs'/job;run.mkdir(parents=True,exist_ok=True)
    started=time.monotonic();seconds=config['batch_seconds']
    if job_id is not None and config['expires_epoch'] is not None:
        seconds=min(seconds,max(0,config['expires_epoch']-time.time()))
    deadline=started+seconds
    state,journal=restore(session,work,catalog,config)
    # This allocation is exclusive and the controller has verified its predecessor terminal.
    for row in state.db.execute('SELECT id,owner FROM n_views WHERE owner IS NOT NULL').fetchall():
        journal.event('abandon',{'view':row['id'],'owner':row['owner'],'reason':'previous_allocation_interrupted'})
    campaign=config['session_id']+':'+job
    baseline_count=state.stats()['source_events']
    baseline_rows={r[0]:tuple(r) for r in state.db.execute('SELECT * FROM events')}
    journal.event('batch_begin',{'job_id':job,'campaign':campaign})
    context=multiprocessing.get_context('spawn')
    events=context.Queue();inboxes={};processes={};logs={};heartbeats={};pending={};exited=set();retries={};generations={}
    stop=False;stop_reason=None;quarantine_streak=0
    last_progress=last_snapshot=last_region=0
    regional=[];fixture_barrier={};fixture_released=False;fixture_hold_until=None
    limits={'batch':config['batch_limit'],'hourly':config['hourly_limit'],'daily':config['daily_limit'],
            'map_spacing':config['map_spacing'],'action_spacing':config['action_spacing']}
    peak=0;max_browsers=0;last_memory={};errors=[]

    def owner(number):return job+':'+number+':'+str(generations[number])
    def reply(number,request,value=None,stopping=False):
        inboxes[number].put({'request':request,'value':value,'stop':stopping})
    def launch(number):
        generations[number]=generations.get(number,0)+1
        inboxes[number]=context.Queue()
        p=context.Process(target=worker,args=(number,inboxes[number],events,config['mode'],
              str(work/('browser-'+number+'-'+str(generations[number]))),max(5,deadline-time.monotonic()),generations[number]),
              kwargs={'reader_factory':reader_factory},name='national-'+number)
        p.start();processes[number]=p
        heartbeats[number]={'at':time.time(),'stage':'starting','pid':p.pid,'browser_open':False}
    def release_owned(number,reason):
        pending.pop(number,None)
        for row in state.db.execute('SELECT id FROM n_views WHERE owner=?',(owner(number),)).fetchall():
            journal.event('abandon',{'view':row[0],'owner':owner(number),'reason':reason})
    def status_file(final=False):
        nonlocal regional,last_region
        if time.monotonic()-last_region>30 or final:
            regional=state.regional_stats();last_region=time.monotonic()
        used=dict(state.db.execute('SELECT kind,count(*) FROM events WHERE campaign=? GROUP BY kind',(campaign,)))
        value={'kind':'national-progress-v1','mode':config['mode'],'job_id':job,
               'heartbeat_epoch':time.time(),'elapsed_seconds':round(time.monotonic()-started,3),
               'phase':'stopping' if stop else 'running','reason':stop_reason,'workers_configured':config['workers'],
               'worker_processes_alive':sum(p.is_alive() for p in processes.values()),
               'browsers_open':sum(bool(x.get('browser_open')) for n,x in heartbeats.items() if n not in exited),
               'source_used_this_batch':{'search':used.get('search',0),'detail':used.get('detail',0)},
               'stats':state.stats(),'regional_progress':regional,'worker_states':heartbeats,
               'sampled_peak_cgroup_mib':round(peak,3),'memory':last_memory,
               'coverage_meaning':'Queued geographic camera pass; incomplete items retained; no exhaustive entity/website recall.'}
        write_json(run/'progress.json',value)
        return value

    failure_stage='worker_start'
    failure_recorded=False
    try:
        for number, in state.db.execute('SELECT id FROM geography.workers ORDER BY id'):
            launch(number)
        if len(processes)!=config['workers']:raise ValueError('Worker catalog/config mismatch')
        while True:
            now=time.monotonic()
            if now>=deadline or (job_id is not None and config['expires_epoch'] is not None
                                 and time.time()>=config['expires_epoch']):
                stop=True;stop_reason=stop_reason or 'deadline'
            if stop_requested is not None and stop_requested():
                stop=True;stop_reason=stop_reason or 'shutdown_requested'
            if (session/'STOP').exists() or (config['mode']=='live' and Path('/predecessor/STOP').exists()):
                if not state.stats()['halted']:journal.event('halt',{'reason':'manual_stop'})
                stop=True;stop_reason='manual_stop'
            if state.stats()['halted']:stop=True;stop_reason=stop_reason or 'source_stop'
            if gate is not None and gate.halted():
                stop=True;stop_reason=stop_reason or 'shared_source_stop'
            failure_stage='worker_messages'
            # Drain ready IPC without pacing sleeps in worker transactions.
            for _ in range(500):
                try:m=events.get(timeout=.05 if _==0 else .0001)
                except queue.Empty:break
                number=m['worker'];kind=m['type'];request=m.get('request')
                if m.get('generation')!=generations.get(number):continue
                if kind=='heartbeat':
                    heartbeats[number]={k:v for k,v in m.items() if k not in ('type','worker')}
                    continue
                if kind=='worker_exit':exited.add(number);continue
                if kind in ('worker_error','cleanup_error'):
                    errors.append({'worker':number,'error_type':m.get('error_type'),'view':m.get('view')})
                    if m.get('access_stop'):
                        if gate is not None:gate.halt()
                        journal.event('halt',{'reason':'source_access_stop'});stop=True;stop_reason='source_access_stop'
                    elif m.get('error_type')=='NetworkInterrupted' and not stop:
                        stop=True;stop_reason='network_unavailable'
                    release_owned(number,'worker_'+m.get('error_type','error'))
                    quarantine_streak+=1
                    if quarantine_streak>=5 and not stop:
                        stop=True;stop_reason='repeated_reader_failures_requires_inspection'
                    continue
                if kind=='claim':
                    view=None if stop else state.pending_view(number)
                    if view is None:reply(number,request,None,True)
                    else:
                        journal.event('claim',{'view':view['id'],'owner':owner(number)})
                        reply(number,request,state.task(view['id']))
                elif kind=='reserve':
                    if stop or deadline-now<15:reply(number,request,False)
                    else:pending[number]=m
                elif kind=='cache':
                    row=state.db.execute('SELECT result,at FROM n_cache WHERE identity=?',(m['identity'],)).fetchone()
                    cached=json.loads(row[0]) if row else None
                    if cached and not cached.get('website') and row[1]<time.time()-86400:cached=None
                    reply(number,request,cached)
                elif kind=='observed':
                    targets=m['targets'][:config.get('fixture_target_limit',100)] if config['mode']=='fixture' else m['targets']
                    journal.event('observed',{'view':m['view'],'owner':owner(number),'observed':m['observed'],'targets':targets})
                    value=state.task(m['view'])['targets'];quarantine_streak=0
                    if config.get('fixture_barrier') and not fixture_released:
                        fixture_barrier[number]=(request,value)
                    else:reply(number,request,value)
                elif kind=='outcome':
                    value={**m['result'],'observed_unix':time.time(),'review':'pending','http_availability':'not_checked'}
                    if value.get('website'):
                        try:value.update(normalized(value['website']))
                        except (ValueError,UnicodeError):value={'status':'unresolved','reason':'invalid_website_action'}
                    journal.event('outcome',{'view':m['view'],'owner':owner(number),'target':m['target'],'result':value})
                    reply(number,request,True)
                elif kind=='release':
                    journal.event('release',{'view':m['view'],'owner':owner(number),'reason':m['reason']})
                    reply(number,request,True)
                else:raise ValueError('Unknown worker message')
            if config.get('fixture_barrier') and len(fixture_barrier)==config['workers'] and not fixture_released:
                fixture_hold_until=fixture_hold_until or time.monotonic()+config.get('fixture_hold_seconds',15)
                if time.monotonic()>=fixture_hold_until:
                    for number,(request,value) in fixture_barrier.items():reply(number,request,value)
                    fixture_released=True
            failure_stage='budget_grants'
            for number,m in list(pending.items()):
                if stop or deadline-time.monotonic()<15:
                    reply(number,m['request'],False);del pending[number];continue
                allowance,due=state.allowance(m['kind'],campaign,limits)
                if allowance in ('batch_limit','daily_limit','hourly_limit','halted'):
                    reply(number,m['request'],False);del pending[number]
                    stop=True;stop_reason=stop_reason or ('shared_budget' if allowance=='batch_limit' else allowance)
                    continue
                if allowance!='ready':continue
                if gate is not None:
                    shared=gate.reserve(config['host_pace_lane'],m['kind'])
                    if shared=='wait':continue
                    if shared!='ready':
                        reply(number,m['request'],False);del pending[number]
                        stop=True;stop_reason=stop_reason or ('deadline' if shared=='expired' else 'shared_source_stop')
                        continue
                event_id='national:'+campaign+':'+str(int(state.meta('journal_seq'))+1)
                journal.event('grant',{'view':m['view'],'owner':owner(number),'kind':m['kind'],'target':m.get('target'),
                    'event':[event_id,campaign,m['kind'],m['view']+':'+str(m.get('target') or ''),due[0],due[1]]})
                reply(number,m['request'],True);del pending[number]
            failure_stage='worker_health'
            for number,p in list(processes.items()):
                if not p.is_alive() and p.exitcode is not None and number not in exited:
                    exited.add(number);release_owned(number,'worker_process_lost')
                    if p.exitcode!=0:
                        errors.append({'worker':number,'error_type':'worker_exit_nonzero'})
                        quarantine_streak+=1
                        if quarantine_streak>=5:stop=True;stop_reason='repeated_worker_startup_failures'
                if number in exited and not p.is_alive():
                    pending.pop(number,None)
                    if not stop and state.pending_view(number) is not None:
                        retries[number]=retries.get(number,0)+1
                        if retries[number]<=2:
                            exited.discard(number);launch(number)
                    continue
                hb=heartbeats.get(number,{})
                if p.is_alive() and time.time()-hb.get('at',time.time())>120:
                    errors.append({'worker':number,'error_type':'heartbeat_stale'})
                    p.terminate();p.join(timeout=5);release_owned(number,'worker_heartbeat_stale')
                    exited.add(number)
            if now-last_progress>=2:
                failure_stage='memory_snapshot'
                last_memory=memory_snapshot(allocation_mb=os.environ.get('FYP_JOB_MEMORY_MB'))
                peak=max(peak,last_memory.get('cgroup_usage_mb',0))
                max_browsers=max(max_browsers,sum(bool(v.get('browser_open')) for n,v in heartbeats.items() if n not in exited))
                if config['mode']=='live' and not last_memory['finite_cgroup']:
                    stop=True;stop_reason='finite_memory_cgroup_required'
                if last_memory['headroom_mb']<4096 or last_memory.get('cgroup_usage_mb',0)>.88*config['memory_mib']:
                    stop=True;stop_reason='memory_guard'
                failure_stage='storage_scan'
                if shutil.disk_usage(session).free<config['minimum_free_bytes'] or folder_bytes(session)>config['maximum_session_bytes']:
                    stop=True;stop_reason='storage_guard'
                failure_stage='write_progress'
                status_file();last_progress=now
            if now-last_snapshot>=config.get('checkpoint_seconds',300):
                failure_stage='checkpoint'
                checkpoint(state,session)
                failure_stage='export_candidates'
                export_candidates(state,session/'exports');last_snapshot=now
            if all(not p.is_alive() for p in processes.values()):break
            if stop:
                for number,m in list(pending.items()):reply(number,m['request'],False);del pending[number]
                if fixture_barrier and not fixture_released:
                    for number,(request,_) in fixture_barrier.items():reply(number,request,None,True)
                    fixture_released=True
            if now>deadline+90:raise TimeoutError('Worker shutdown deadline')
        for p in processes.values():p.join(timeout=2)
    except BaseException as error:
        stop=True;stop_reason='coordinator_error:'+type(error).__name__
        errors.append({'error_type':type(error).__name__})
        failure_recorded=record_failure(run,error,failure_stage,write_json)
    finally:
        for number in inboxes:
            try:inboxes[number].put({'stop':True})
            except Exception:pass
        until=time.monotonic()+30
        for p in processes.values():
            if p.is_alive():p.join(timeout=max(0,until-time.monotonic()))
            if p.is_alive():p.terminate();p.join(timeout=5)
            if p.is_alive():p.kill();p.join(timeout=5)
        if not journal.failed:
            for row in state.db.execute('SELECT id,owner FROM n_views WHERE owner IS NOT NULL').fetchall():
                journal.event('abandon',{'view':row['id'],'owner':row['owner'],'reason':'allocation_shutdown_incomplete'})
            journal.event('batch_end',{'job_id':job,'reason':stop_reason or 'workers_finished'})
            checkpoint(state,session);export_candidates(state,session/'exports')
        else:stop_reason='journal_durability_failure'
        progress=status_file(True)
        after={r[0]:tuple(r) for r in state.db.execute('SELECT * FROM events')}
        preserved=all(after.get(k)==v for k,v in baseline_rows.items())
        if len(after)==baseline_count and state.stats()['remaining_views'] and stop_reason not in ('daily_limit','hourly_limit'):
            stop_reason=stop_reason or 'no_progress_with_remaining_work'
        good_reasons=(None,'deadline','shared_budget','hourly_limit','daily_limit','shutdown_requested','network_unavailable')
        final={**progress,'phase':'completed' if stop_reason in good_reasons and preserved else 'needs_attention',
               'reason':stop_reason or 'eligible_work_exhausted','inherited_source_events_unchanged':preserved,
               'baseline_source_events':baseline_count,'source_events_added':len(after)-baseline_count,
               'max_simultaneous_browsers':max_browsers,'worker_errors':errors,
               'worker_exit_codes':{n:p.exitcode for n,p in processes.items()},
               'fixture_barrier_released':fixture_released,'journal_seq':int(state.meta('journal_seq')),
               'failure_details_recorded':failure_recorded}
        final['google_navigations']=0 if config['mode']=='fixture' else None
        if gate is not None:gate.close()
        final['source_events_scope']='synthetic_fixture_only' if config['mode']=='fixture' else 'inherited_live_history_plus_global_grants'
        write_json(run/'final.json',final)
        journal.close();state.close()
    return 0 if final['phase']=='completed' else 2


def supervise(session,work,catalog,script):
    session,work=Path(session),Path(work);config=read(session/'config.json')
    job=os.environ['SLURM_JOB_ID'];run=session/'runs'/job;run.mkdir(parents=True,exist_ok=True)
    lease=session/'ACTIVE_LEASE';lease.mkdir()
    write_json(lease/'owner.json',{'job_id':job,'pid':os.getpid()})
    started=time.time();forced=None
    with (run/'coordinator.log').open('w',encoding='utf-8') as log:
        p=subprocess.Popen([sys.executable,'-B',str(script),'manage','--session',str(session),
                            '--work',str(work),'--catalog',str(catalog)],stdout=log,stderr=log,start_new_session=True)
        while p.poll() is None:
            progress=read(run/'progress.json') if (run/'progress.json').exists() else None
            age=time.time()-(progress['heartbeat_epoch'] if progress else started)
            if age>(90 if progress else 180):forced='coordinator_heartbeat_stale'
            if time.time()-started>config['batch_seconds']+120:forced='allocation_deadline'
            if forced:
                os.killpg(p.pid,signal.SIGTERM)
                try:p.wait(timeout=30)
                except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=10)
                break
            write_json(run/'watchdog.json',{'state':'running','at':time.time(),'job_id':job,
                       'elapsed_seconds':round(time.time()-started,2),'coordinator_pid':p.pid})
            time.sleep(2)
    final=read(run/'final.json') if (run/'final.json').exists() else None
    state='completed' if p.returncode==0 and final and final['phase']=='completed' and not forced else 'needs_attention'
    write_json(run/'watchdog-final.json',{'state':state,'reason':forced or (final or {}).get('reason'),
               'job_id':job,'exit_code':p.returncode,'elapsed_seconds':round(time.time()-started,2)})
    if state=='completed' and read(lease/'owner.json')['job_id']==job:
        (lease/'owner.json').unlink();lease.rmdir()
    return 0 if state=='completed' else 2
