"""Race-safe size accounting and sanitized failure records for the national runner."""
import json
import os
from pathlib import Path
import re
import stat
import traceback


def checker_temporary(root, path):
    """Only the checker's documented atomic-write temporary may disappear."""
    try:
        parts = path.relative_to(root).parts
    except ValueError:
        return False
    return len(parts) == 3 and parts[0] == 'runs' and parts[2] == 'watchdog.json.writing'


def folder_bytes(path):
    root = Path(path)
    if not stat.S_ISDIR(root.stat().st_mode):
        raise NotADirectoryError(str(root))
    total = 0
    pending = [root]
    while pending:
        directory = pending.pop()
        # scandir and unexpected stat errors propagate. A missing root, unreadable
        # directory or durable file must not turn into a healthy zero-byte result.
        with os.scandir(directory) as entries:
            for entry in entries:
                item = directory / entry.name
                try:
                    info = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    if checker_temporary(root, item):
                        continue
                    raise
                if stat.S_ISDIR(info.st_mode):
                    pending.append(item)
                elif stat.S_ISREG(info.st_mode):
                    total += info.st_size
                # Symlinks and other special files are not traversed or counted.
    root.stat()  # Still require the session root at the end of the scan.
    return total


def safe_location(value):
    """Keep recognizable runtime paths; never save arbitrary exception strings."""
    if value is None:
        return None
    text = str(value).replace('\\', '/')
    if text.startswith('/session/'):
        relative = text[len('/session/'):]
        known = (r'(?:events\.jsonl|config\.json|STATE\.json|'
                 r'snapshots/(?:current\.sqlite3|previous\.sqlite3|snapshot\.json|next-\d+-\d+\.sqlite3)|'
                 r'runs/\d+/(?:watchdog\.json|progress\.json|final\.json|watchdog-final\.json|'
                 r'failure-details\.json|coordinator\.log))(?:\.writing)?')
        return 'session/' + relative if re.fullmatch(known, relative) else 'session/<other path redacted>'
    if text in ('/proc/meminfo', '/proc/self/cgroup'):
        return text
    if text.startswith('/proc/'):
        return '/proc/<process path>'
    if text.startswith('/sys/fs/cgroup/'):
        return '/sys/fs/cgroup/<accounting path>'
    if text.startswith('/state/'):
        return '/state/<node-local work path>'
    return '<outside runtime roots; redacted>'


def failure_details(error, stage):
    return {
        'kind': 'national-coordinator-failure-v1',
        'stage': stage if re.fullmatch(r'[a-z_]{1,48}', stage or '') else 'unknown',
        'error_type': type(error).__name__,
        'errno': getattr(error, 'errno', None),
        'filename': safe_location(getattr(error, 'filename', None)),
        'filename2': safe_location(getattr(error, 'filename2', None)),
        'frames': [{'file': Path(frame.filename).name, 'line': frame.lineno, 'function': frame.name}
                   for frame in traceback.extract_tb(error.__traceback__)[-12:]],
        # No exception message, source-code lines, locals, URLs or environment.
    }


def record_failure(run, error, stage, writer):
    details = failure_details(error, stage)
    try:
        writer(Path(run) / 'failure-details.json', details)
        return True
    except Exception as logging_error:
        # A diagnostic write failure must not replace the original failure reason.
        details['diagnostic_write_error_type'] = type(logging_error).__name__
        print(json.dumps(details, ensure_ascii=True), flush=True)
        return False
