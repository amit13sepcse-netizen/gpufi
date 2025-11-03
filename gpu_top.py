#!/usr/bin/env python3
import curses
import time
import subprocess
import shlex
import argparse
import os
from datetime import datetime

# Utilities

def run_cmd(cmd: str) -> str:
    try:
        out = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
        return out.decode('utf-8', errors='ignore')
    except subprocess.CalledProcessError as e:
        return e.output.decode('utf-8', errors='ignore')
    except FileNotFoundError:
        return ""


def have_nvidia_smi() -> bool:
    return subprocess.call(['which', 'nvidia-smi'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0


def parse_csv(lines):
    rows = []
    for ln in lines.strip().splitlines():
        if not ln.strip():
            continue
        parts = [p.strip() for p in ln.split(',')]
        rows.append(parts)
    return rows


def get_gpu_stats():
    # Query per-GPU stats
    fields = [
        'uuid', 'index', 'name', 'temperature.gpu', 'utilization.gpu',
        'memory.used', 'memory.total', 'power.draw', 'power.limit',
        'clocks.sm', 'fan.speed'
    ]
    cmd = f"nvidia-smi --query-gpu={','.join(fields)} --format=csv,noheader,nounits"
    out = run_cmd(cmd)
    rows = parse_csv(out)
    stats = []
    for r in rows:
        try:
            stats.append({
                'uuid': r[0],
                'index': int(r[1]),
                'name': r[2],
                'temp': int(r[3]),
                'util': int(r[4]),
                'mem_used': int(r[5]),
                'mem_total': int(r[6]),
                'pwr': float(r[7]) if r[7] != 'N/A' else None,
                'pwr_lim': float(r[8]) if r[8] != 'N/A' else None,
                'sm_clock': int(r[9]) if r[9] != 'N/A' else None,
                'fan': int(r[10]) if len(r) > 10 and r[10] != 'N/A' else None,
            })
        except Exception:
            # Be forgiving on parsing errors
            continue
    return stats


def get_compute_processes():
    # Query compute processes (pid, name, used_memory, gpu_uuid)
    fields = ['pid', 'process_name', 'used_memory', 'gpu_uuid']
    cmd = f"nvidia-smi --query-compute-apps={','.join(fields)} --format=csv,noheader,nounits"
    out = run_cmd(cmd)
    rows = parse_csv(out)
    procs = []
    for r in rows:
        try:
            procs.append({
                'pid': int(r[0]),
                'name': os.path.basename(r[1]) if r[1] else '-',
                'mem': int(r[2]) if r[2] != 'N/A' else 0,
                'uuid': r[3],
                # utilization placeholder updated from pmon later
                'sm': None,
                'mem_util': None,
            })
        except Exception:
            continue
    return procs


def get_pmon_once():
    # Use pmon to get per-process SM and MEM util. One-shot capture.
    # nvidia-smi pmon columns: # gpu pid type sm mem enc dec fb command
    out = run_cmd('nvidia-smi pmon -c 1 -s um')
    pmon = []
    for ln in out.splitlines():
        ln = ln.strip()
        if not ln or ln.startswith('#'):
            continue
        parts = ln.split()
        # Expect at least: gpu pid type sm mem enc dec fb command
        if len(parts) < 9:
            continue
        try:
            gpu_idx = int(parts[0])
            pid = int(parts[1]) if parts[1] != '-' else None
            sm = None if parts[3] == '-' else int(parts[3])
            mem = None if parts[4] == '-' else int(parts[4])
            cmd = parts[8] if len(parts) >= 9 else ''
            pmon.append({'gpu': gpu_idx, 'pid': pid, 'sm': sm, 'mem': mem, 'cmd': cmd})
        except Exception:
            continue
    return pmon


def enrich_procs_with_pmon(procs, pmon, uuid_to_index):
    ix = {(e['gpu'], e['pid']): e for e in pmon if e['pid'] is not None}
    for p in procs:
        gpu_idx = uuid_to_index.get(p['uuid'])
        key = (gpu_idx, p['pid'])
        if key in ix:
            p['sm'] = ix[key]['sm']
            p['mem_util'] = ix[key]['mem']
    return procs


def human_bytes(mib):
    try:
        b = int(mib) * 1024 * 1024
    except Exception:
        return f"{mib} MiB"
    for unit in ['B','KB','MB','GB','TB']:
        if b < 1024:
            return f"{b:.0f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


def snapshot():
    gpus = get_gpu_stats()
    uuid_to_index = {g['uuid']: g['index'] for g in gpus}
    procs = get_compute_processes()
    pmon = get_pmon_once()
    procs = enrich_procs_with_pmon(procs, pmon, uuid_to_index)
    # Index processes per GPU index
    per_gpu = {}
    for g in gpus:
        per_gpu[g['index']] = []
    for p in procs:
        idx = uuid_to_index.get(p['uuid'])
        if idx is not None and idx in per_gpu:
            per_gpu[idx].append(p)
    # Sort each GPU's processes by SM util desc, then mem desc
    for idx in per_gpu:
        per_gpu[idx].sort(key=lambda x: (x['sm'] or 0, x['mem'] or 0), reverse=True)
    return gpus, per_gpu


def draw(stdscr, interval: float, max_procs: int, sort_by_util: bool):
    curses.curs_set(0)
    stdscr.nodelay(True)
    h, w = stdscr.getmaxyx()

    while True:
        stdscr.erase()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        title = f"gpu-top | {now} | refresh={interval:.1f}s | q=quit"
        stdscr.addstr(0, 0, title[:w - 1], curses.A_BOLD)

        if not have_nvidia_smi():
            stdscr.addstr(2, 0, "nvidia-smi not found. Please install NVIDIA drivers.", curses.A_BOLD)
            stdscr.refresh()
            time.sleep(interval)
            ch = stdscr.getch()
            if ch == ord('q'):
                break
            continue

        gpus, per_gpu = snapshot()

        row = 2
        # GPU summary header
        stdscr.addstr(row, 0, f"GPU Summary:", curses.A_UNDERLINE)
        row += 1
        stdscr.addstr(row, 0, f"Idx  Name                             Temp Util  Mem(Used/Total)    Power       SMClk Fan")
        row += 1
        for g in gpus:
            mem = f"{g['mem_used']} / {g['mem_total']} MiB"
            pwr = f"{g['pwr']:.0f}/{g['pwr_lim']:.0f} W" if g['pwr'] is not None and g['pwr_lim'] is not None else "N/A"
            smc = f"{g['sm_clock']} MHz" if g['sm_clock'] is not None else "N/A"
            fan = f"{g['fan']}%" if g['fan'] is not None else "N/A"
            line = f"{g['index']:<4} {g['name'][:30]:<30} {g['temp']:>3}C  {g['util']:>3}%  {mem:<16}  {pwr:<10}  {smc:<6} {fan:>4}"
            stdscr.addstr(row, 0, line[:w - 1])
            row += 1

        row += 1
        stdscr.addstr(row, 0, f"Per-GPU Processes (top {max_procs} by SM util):", curses.A_UNDERLINE)
        row += 1
        for g in gpus:
            stdscr.addstr(row, 0, f"GPU {g['index']} - {g['name']}")
            row += 1
            stdscr.addstr(row, 0, f"  PID      SM%  MEM%  VRAM       CMD")
            row += 1
            procs = per_gpu.get(g['index'], [])
            if sort_by_util:
                procs = sorted(procs, key=lambda p: (p['sm'] or 0, p['mem_util'] or 0), reverse=True)
            for p in procs[:max_procs]:
                vram = human_bytes(p['mem'] * 1024 * 1024) if isinstance(p['mem'], int) else '-'
                sm = '-' if p['sm'] is None else f"{p['sm']:>3}"
                mu = '-' if p['mem_util'] is None else f"{p['mem_util']:>3}"
                cmd = p['name']
                stdscr.addstr(row, 0, f"  {p['pid']:<8} {sm:>3}  {mu:>4}  {vram:<9}  {cmd}"[:w - 1])
                row += 1
            if not procs:
                stdscr.addstr(row, 0, "  (no compute processes)")
                row += 1
            row += 1

        stdscr.refresh()

        # Non-blocking key handling
        t0 = time.time()
        while True:
            ch = stdscr.getch()
            if ch == ord('q'):
                return
            # small sleep steps to keep UI responsive
            if time.time() - t0 >= interval:
                break
            time.sleep(0.05)


def print_once():
    if not have_nvidia_smi():
        print("nvidia-smi not found.")
        return 1
    gpus, per_gpu = snapshot()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"gpu-top snapshot {now}")
    print("Idx  Name                             Temp Util  Mem(Used/Total)    Power       SMClk Fan")
    for g in gpus:
        mem = f"{g['mem_used']} / {g['mem_total']} MiB"
        pwr = f"{g['pwr']:.0f}/{g['pwr_lim']:.0f} W" if g['pwr'] is not None and g['pwr_lim'] is not None else "N/A"
        smc = f"{g['sm_clock']} MHz" if g['sm_clock'] is not None else "N/A"
        fan = f"{g['fan']}%" if g['fan'] is not None else "N/A"
        line = f"{g['index']:<4} {g['name'][:30]:<30} {g['temp']:>3}C  {g['util']:>3}%  {mem:<16}  {pwr:<10}  {smc:<6} {fan:>4}"
        print(line)
    print()
    for g in gpus:
        print(f"GPU {g['index']} - {g['name']}")
        print("  PID      SM%  MEM%  VRAM       CMD")
        procs = per_gpu.get(g['index'], [])
        for p in procs:
            vram = human_bytes(p['mem'] * 1024 * 1024) if isinstance(p['mem'], int) else '-'
            sm = '-' if p['sm'] is None else f"{p['sm']:>3}"
            mu = '-' if p['mem_util'] is None else f"{p['mem_util']:>3}"
            cmd = p['name']
            print(f"  {p['pid']:<8} {sm:>3}  {mu:>4}  {vram:<9}  {cmd}")
        if not procs:
            print("  (no compute processes)")
        print()
    return 0


def main():
    parser = argparse.ArgumentParser(description='Top-like GPU monitor using nvidia-smi (no dependencies).')
    parser.add_argument('-i', '--interval', type=float, default=1.0, help='Refresh interval in seconds (default: 1.0)')
    parser.add_argument('-n', '--max-procs', type=int, default=10, help='Max processes shown per GPU (default: 10)')
    parser.add_argument('--once', action='store_true', help='Print a single snapshot and exit (non-interactive)')
    parser.add_argument('--sort-util', action='store_true', help='Sort processes by SM utilization descending')
    args = parser.parse_args()

    if args.once:
        code = print_once()
        raise SystemExit(code)

    if not have_nvidia_smi():
        print('nvidia-smi not found. Please install NVIDIA drivers and nvidia-utils.')
        raise SystemExit(1)

    curses.wrapper(lambda stdscr: draw(stdscr, args.interval, args.max_procs, args.sort_util))


if __name__ == '__main__':
    main()
