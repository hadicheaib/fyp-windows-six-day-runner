"""One browser worker. Only the coordinator may grant a source action."""
import os
import queue
import time


def worker(number,inbox,outbox,mode,folder,seconds,generation=1,reader_factory=None):
    from pathlib import Path
    import cv2
    cv2.setNumThreads(1)
    if reader_factory is None:
        from .hpc_pool_v1 import LinuxReader
        reader_factory = LinuxReader
    reader=None;task=None;counter=0;reply_counter=0
    deadline=time.monotonic()+seconds

    def send(value):outbox.put({'worker':number,'generation':generation,**value})

    def beat(stage):
        send({'type':'heartbeat','stage':stage,'at':time.time(),
                    'pid':os.getpid(),'browser_open':reader is not None})

    def ask(command,**data):
        nonlocal reply_counter
        reply_counter+=1;request=reply_counter
        send({'type':command,'request':request,**data})
        while True:
            beat('waiting_'+command)
            try:message=inbox.get(timeout=1)
            except queue.Empty:
                if time.monotonic()>deadline+90:raise TimeoutError('Coordinator response deadline')
                continue
            if message.get('stop'):return None
            if message.get('request')!=request:raise RuntimeError('Coordinator response mismatch')
            return message.get('value')

    def grant(kind,target=None):
        return bool(ask('reserve',view=task['id'],kind=kind,target=target))

    try:
        while time.monotonic()<deadline:
            task=ask('claim')
            if task is None:break
            beat('claimed')
            if not grant('search'):
                ask('release',view=task['id'],reason='batch_or_time_limit');task=None;break
            if reader is None:
                folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
                reader=reader_factory(mode,folder);reader.__enter__()
            reader.deadline=deadline
            beat('restore')
            counter+=1;observed,targets=reader.restore(task,navigation_number=counter)
            observed_reply=ask('observed',view=task['id'],observed=observed,targets=targets)
            if observed_reply is None:break
            pending=observed_reply
            for i,target in enumerate(pending):
                if i:
                    if not grant('search'):break
                    beat('restore')
                    counter+=1;observed,targets=reader.restore(task,navigation_number=counter)
                    if ask('observed',view=task['id'],observed=observed,targets=targets) is None:break
                if not grant('detail',target['id']):break
                beat('inspect')
                result=reader.inspect(target['payload'],lambda identity:ask('cache',identity=identity))
                if ask('outcome',view=task['id'],target=target['id'],result=result) is None:break
            if ask('release',view=task['id'],reason='pending_targets_or_view_finished') is None:break
            task=None
        beat('ended')
    except BaseException as error:
        # No exception bodies, browser body text or account identifiers in records.
        access=type(error).__name__ in ('AccessStop','PermissionError')
        send({'type':'worker_error','error_type':type(error).__name__,
                    'access_stop':access,'view':task['id'] if task else None})
    finally:
        if reader:
            try:reader.__exit__(None,None,None)
            except Exception:
                send({'type':'cleanup_error','error_type':'browser_cleanup'})
        send({'type':'worker_exit'})
