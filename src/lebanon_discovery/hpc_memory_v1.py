"""Read-only job memory guard; cgroup limits take precedence over host RAM."""
from __future__ import annotations

import os
from pathlib import Path

MIB = 1024 * 1024


def process_tree_rss(proc: Path, parent: int) -> int:
    processes = {}
    for directory in proc.iterdir():
        if not directory.name.isdigit():
            continue
        try:
            # Split after the final ')' because process names may contain spaces.
            stat = (directory / 'stat').read_text().rsplit(')', 1)[1].split()
            processes[int(directory.name)] = (int(stat[1]), int(stat[21]) * os.sysconf('SC_PAGE_SIZE'))
        except (OSError, ValueError, IndexError):
            continue
    owned = {parent}
    while True:
        enlarged = owned | {pid for pid, (ppid, _) in processes.items() if ppid in owned}
        if enlarged == owned:
            return sum(rss for pid, (_, rss) in processes.items() if pid in owned)
        owned = enlarged


def memory_snapshot(*, proc=Path('/proc'), cgroups=Path('/sys/fs/cgroup'), allocation_mb=None, parent=None):
    """Fallback RSS is conservative for shared libraries, incomplete for cache/SHM.

    A 25% allocation reserve applies to the fallback. Live execution requires
    finite cgroup accounting; fixture execution also exercises the fallback.
    """
    proc, cgroups = Path(proc), Path(cgroups)
    values = {}
    for line in (proc / 'meminfo').read_text().splitlines():
        parts = line.split()
        if len(parts) >= 2:
            values[parts[0].rstrip(':')] = int(parts[1]) * 1024
    host = values.get('MemAvailable', values.get('MemFree', 0))
    limits = []
    for line in (proc / 'self/cgroup').read_text().splitlines():
        _, controllers, location = line.split(':', 2)
        if 'memory' in controllers.split(','):
            base, version = cgroups / 'memory', 'v1'
            limit_name, usage_name = 'memory.limit_in_bytes', 'memory.usage_in_bytes'
        elif not controllers:
            base, version = cgroups, 'v2'
            limit_name, usage_name = 'memory.max', 'memory.current'
        else:
            continue
        current = base / location.lstrip('/')
        if not current.is_relative_to(base):
            continue
        while True:
            try:
                raw = (current / limit_name).read_text().strip()
                limit = int(raw) if raw != 'max' else 2 ** 63
                usage = int((current / usage_name).read_text().strip())
                if 0 < limit < 2 ** 60:
                    limits.append((max(0, limit - usage), limit, usage, version))
            except (OSError, ValueError):
                pass
            if current == base:
                break
            current = current.parent
    result = {'host_available_mb': round(host / MIB, 2), 'finite_cgroup': bool(limits)}
    bounds = [host]
    if limits:
        headroom, limit, usage, version = min(limits)
        bounds.append(headroom)
        result.update(basis='cgroup_' + version, cgroup_limit_mb=round(limit / MIB, 2),
                      cgroup_usage_mb=round(usage / MIB, 2))
    if allocation_mb is not None:
        allocation_mb = int(allocation_mb)
        if allocation_mb <= 0:
            raise ValueError('Positive Slurm allocation required')
        rss = process_tree_rss(proc, parent or os.getpid())
        reserve = 1 if limits else .75
        bounds.append(max(0, allocation_mb * MIB * reserve - rss))
        result.update(slurm_allocation_mb=allocation_mb, process_tree_rss_mb=round(rss / MIB, 2))
        if not limits:
            result['basis'] = 'slurm_rss_estimate_with_25pct_reserve'
    elif not limits:
        raise ValueError('No cgroup limit or explicit Slurm allocation')
    result['headroom_mb'] = round(min(bounds) / MIB, 2)
    return result
