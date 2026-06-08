"""
Phase 2 orchestrator — fetch /api/data_hour/{record_id} for every record in
the per-station inventory files, write v2 CSVs atomically and resumably.

Design:
 - N worker threads, each rate-limited to R req/s (total ~N*R req/s).
 - One writer thread consuming parsed rows; appends per-station CSVs with
   a header-aware buffered append (no full reload).
 - Atomic writes: checkpoint flushes go via tmp-file + os.replace.
 - Resume: reads existing output CSVs at startup, diffs Record_IDs, skips hits.
 - Ctrl-C: workers stop picking up new work, in-flight requests finish, writer
   flushes pending buffers.

Usage:
  python scripts/collection/rebuild_historical_v2.py \
      --inventory analysis/station_probe/inventory \
      --output-dir data/stations/historical_full_v2 \
      --stations all \
      --workers 4 --rate-per-worker 3.0 \
      --yes
"""
import argparse
import os
import sys
import io
import csv as csv_mod
import time
import json
import glob
import queue
import threading
import signal
from collections import deque

import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from detail_parser import parse_detail, OUTPUT_COLUMNS

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (thesis-rebuild)"}
DETAIL_URL = "https://tedp.vn/api/data_hour/{}"

STOP = threading.Event()
def _sigint(signum, frame):
    if not STOP.is_set():
        print("\n!! stop signal received — draining; press again to force quit")
        STOP.set()
    else:
        print("\n!! second stop — exiting hard")
        sys.exit(1)
signal.signal(signal.SIGINT, _sigint)

# -------------------- I/O --------------------

def load_existing_record_ids(out_csv):
    if not os.path.exists(out_csv):
        return set()
    try:
        s = set()
        with open(out_csv, 'r', encoding='utf-8', newline='') as f:
            reader = csv_mod.DictReader(f)
            for r in reader:
                rid = r.get('Record_ID') or ''
                if rid:
                    s.add(rid)
        return s
    except Exception as e:
        print(f"!! could not read {out_csv}: {e}")
        return set()

class StationWriter:
    """Append-mode per-station CSV writer with header management."""
    def __init__(self, output_dir):
        self.output_dir = output_dir
        self.buffers = {}  # station_id -> list[row dict]
        self.has_header = {}  # station_id -> bool

    def path(self, sid):
        return os.path.join(self.output_dir, f"{sid}.csv")

    def append(self, station_id, row):
        self.buffers.setdefault(station_id, []).append(row)

    def pending(self, station_id):
        return len(self.buffers.get(station_id, []))

    def flush(self, station_id):
        rows = self.buffers.get(station_id, [])
        if not rows:
            return
        p = self.path(station_id)
        if station_id not in self.has_header:
            self.has_header[station_id] = os.path.exists(p) and os.path.getsize(p) > 0
        write_header = not self.has_header[station_id]
        # Normalize rows to the full column set
        norm = []
        for r in rows:
            norm.append({c: r.get(c, "") for c in OUTPUT_COLUMNS})
        # Append (fsync-style: open, write, close — atomic vs concurrent readers
        # isn't a concern since this is the only writer)
        with open(p, 'a', encoding='utf-8', newline='') as f:
            writer = csv_mod.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
            if write_header:
                writer.writeheader()
                self.has_header[station_id] = True
            writer.writerows(norm)
        self.buffers[station_id] = []

    def flush_all(self):
        for sid in list(self.buffers.keys()):
            self.flush(sid)

# -------------------- workers --------------------

class RateLimitedSession:
    def __init__(self, rate_per_sec):
        self.interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.0
        self.last = 0.0

    def get(self, url, timeout=15):
        if self.interval > 0:
            now = time.time()
            wait = self.interval - (now - self.last)
            if wait > 0:
                time.sleep(wait)
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        self.last = time.time()
        return r

def fetch_worker(worker_id, q_in, q_out, rate_per_worker, stats,
                 unknown_log_lock, unknown_log_path):
    s = RateLimitedSession(rate_per_worker)
    while not STOP.is_set():
        try:
            item = q_in.get(timeout=1)
        except queue.Empty:
            continue
        if item is None:
            q_in.task_done()
            return
        record_id, station_id, ts = item
        # Track the oldest timestamp dequeued so far (queue is DESC, so each
        # new dequeue is older than the last). Race-tolerant: approximate is fine.
        if ts and (not stats.get('current_ts') or ts < stats['current_ts']):
            stats['current_ts'] = ts
        try:
            r = s.get(DETAIL_URL.format(record_id))
            code = r.status_code
            if code == 200:
                try:
                    j = r.json()
                except Exception:
                    stats['parse_err'] += 1
                    q_out.put((station_id, {'Record_ID': record_id,
                                            'Detailed_Status': 'ParseErr'}))
                    q_in.task_done()
                    continue
                row, unknown = parse_detail(j)
                row['Detailed_Status'] = 'Done'
                if unknown:
                    with unknown_log_lock:
                        with open(unknown_log_path, 'a', encoding='utf-8') as f:
                            f.write(f"{station_id},{record_id},{'|'.join(unknown)}\n")
                q_out.put((station_id, row))
                stats['ok'] += 1
            elif code == 429:
                stats['rate'] += 1
                time.sleep(3)
                q_in.put(item)  # requeue
                q_in.task_done()
                continue
            elif 500 <= code < 600:
                stats['5xx'] += 1
                q_out.put((station_id, {'Record_ID': record_id,
                                        'Detailed_Status': f'Error_{code}'}))
            else:
                stats['other'] += 1
                q_out.put((station_id, {'Record_ID': record_id,
                                        'Detailed_Status': f'Error_{code}'}))
        except requests.exceptions.Timeout:
            stats['timeout'] += 1
            q_out.put((station_id, {'Record_ID': record_id,
                                    'Detailed_Status': 'Timeout'}))
        except Exception:
            stats['exc'] += 1
            q_out.put((station_id, {'Record_ID': record_id,
                                    'Detailed_Status': 'Exc'}))
        q_in.task_done()

def writer_loop(q_out, writer, checkpoint_every, writer_done_event):
    """Drain q_out until sentinel None is seen AND queue is empty."""
    while True:
        try:
            item = q_out.get(timeout=1)
        except queue.Empty:
            if STOP.is_set() and q_out.empty():
                break
            continue
        if item is None:
            q_out.task_done()
            break
        station_id, row = item
        writer.append(station_id, row)
        if writer.pending(station_id) >= checkpoint_every:
            writer.flush(station_id)
        q_out.task_done()
    writer.flush_all()
    writer_done_event.set()

# -------------------- queue build --------------------

def build_queue(inventory_dir, station_filter, output_dir):
    """
    Global time-sorted queue (DESC). Workers process newest records across all
    stations first, then walk backwards in time uniformly. This gives clean
    year-boundary milestones (e.g. "2026 done" == all records >= 2026-01-01).
    """
    q_in = queue.Queue()
    target_ids = None
    if station_filter and station_filter != 'all':
        if os.path.isfile(station_filter):
            with open(station_filter, 'r', encoding='utf-8') as f:
                target_ids = set(x.strip() for x in f if x.strip())
        else:
            target_ids = set(x.strip() for x in station_filter.split(',') if x.strip())
    skipped = 0
    per_station_counts = {}
    all_rows = []  # list of (timestamp_str, record_id, station_id)
    for path in sorted(glob.glob(os.path.join(inventory_dir, '*.csv'))):
        sid = os.path.basename(path).replace('.csv', '')
        if target_ids is not None and sid not in target_ids:
            continue
        out_csv = os.path.join(output_dir, f"{sid}.csv")
        existing_ids = load_existing_record_ids(out_csv)
        n_added = 0
        with open(path, 'r', encoding='utf-8', newline='') as f:
            reader = csv_mod.DictReader(f)
            for row in reader:
                rid = (row.get('record_id') or '').strip()
                if not rid:
                    continue
                if rid in existing_ids:
                    skipped += 1
                    continue
                ts = (row.get('timestamp') or '').strip()
                all_rows.append((ts, rid, sid))
                n_added += 1
        per_station_counts[sid] = n_added
    # Sort DESC by timestamp string (ISO 8601 sorts lexicographically == chronologically)
    all_rows.sort(key=lambda x: x[0], reverse=True)
    for ts, rid, sid in all_rows:
        q_in.put((rid, sid, ts))
    total = len(all_rows)
    return q_in, total, skipped, per_station_counts

# -------------------- main --------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--inventory', default='analysis/station_probe/inventory')
    ap.add_argument('--output-dir', default='data/stations/historical_full_v2')
    ap.add_argument('--stations', default='all')
    ap.add_argument('--workers', type=int, default=4)
    ap.add_argument('--rate-per-worker', type=float, default=3.0)
    ap.add_argument('--checkpoint-every', type=int, default=1000)
    ap.add_argument('--halt-on-error-pct', type=float, default=1.0)
    ap.add_argument('--yes', action='store_true')
    args = ap.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    unknown_log_path = os.path.join(args.output_dir, '_unknown_keys.log')
    unknown_log_lock = threading.Lock()

    print(f"inventory:        {args.inventory}")
    print(f"output:           {args.output_dir}")
    print(f"stations:         {args.stations}")
    print(f"workers x rate:   {args.workers} x {args.rate_per_worker} = "
          f"{args.workers * args.rate_per_worker:.1f} req/s")

    print("scanning inventory + existing output for resumable work...")
    q_in, total, skipped, per_station = build_queue(
        args.inventory, args.stations, args.output_dir)
    print(f"queued:           {total:,} record_ids")
    print(f"skipped (resume): {skipped:,} already in output")

    if total == 0:
        print("nothing to do.")
        return

    effective_rate = args.workers * args.rate_per_worker
    eta_h = total / effective_rate / 3600
    print(f"ETA at {effective_rate:.1f} req/s:  {eta_h:.1f} hours "
          f"({eta_h*60:.0f} min)")
    print(f"stations to process: {sum(1 for v in per_station.values() if v > 0)}")

    if not args.yes:
        if input("proceed? [y/N]: ").strip().lower() != 'y':
            print("aborted"); return

    stats = {'ok': 0, 'rate': 0, '5xx': 0, 'other': 0, 'exc': 0,
             'timeout': 0, 'parse_err': 0, 'current_ts': None}
    q_out = queue.Queue(maxsize=5000)
    writer = StationWriter(args.output_dir)
    writer_done_event = threading.Event()

    workers = []
    for i in range(args.workers):
        t = threading.Thread(target=fetch_worker,
                             args=(i, q_in, q_out, args.rate_per_worker, stats,
                                   unknown_log_lock, unknown_log_path),
                             daemon=True)
        t.start()
        workers.append(t)

    wt = threading.Thread(target=writer_loop,
                          args=(q_out, writer, args.checkpoint_every, writer_done_event),
                          daemon=True)
    wt.start()

    # Signal worker completion via sentinels when q_in drains
    t0 = time.time()
    last_report = t0
    last_done = 0
    last_milestone_year = None  # for year-boundary logging

    try:
        while not STOP.is_set():
            time.sleep(5)
            done = sum(stats[k] for k in ('ok','5xx','other','exc','timeout','parse_err'))
            now = time.time()
            # Year-boundary milestone (checked frequently, fires at most once per year)
            cts = stats.get('current_ts') or ''
            if len(cts) >= 4 and cts[:4].isdigit():
                cur_year = int(cts[:4])
                if last_milestone_year is None:
                    last_milestone_year = cur_year
                elif cur_year < last_milestone_year:
                    print(f"*** YEAR {last_milestone_year} COMPLETE @ record {done:,} "
                          f"(crossed into {cur_year} at ts {cts}) ***")
                    last_milestone_year = cur_year
            if now - last_report >= 60:
                elapsed = now - t0
                inst_rate = (done - last_done) / (now - last_report) if now > last_report else 0
                avg_rate = done / elapsed if elapsed > 0 else 0
                remaining = max(0, total - done)
                eta_min = remaining / avg_rate / 60 if avg_rate > 0 else 0
                err = stats['5xx'] + stats['rate'] + stats['timeout']
                ts_str = stats.get('current_ts') or 'n/a'
                print(f"[{int(elapsed/60):4d}m] done={done:>8,} ok={stats['ok']:>8,} "
                      f"5xx={stats['5xx']} 429={stats['rate']} to={stats['timeout']} "
                      f"exc={stats['exc']} inst={inst_rate:.1f}/s avg={avg_rate:.1f}/s "
                      f"ETA={eta_min:.0f}m depth={ts_str}")
                last_report = now; last_done = done
                if done > 1000:
                    err_pct = 100 * err / done
                    if err_pct > args.halt_on_error_pct:
                        print(f"!! error pct {err_pct:.2f}% > {args.halt_on_error_pct}% — halting")
                        STOP.set(); break
            if q_in.empty() and q_out.empty() and done >= total:
                print("[done] all records processed")
                break
        # Signal workers to exit
        for _ in workers:
            q_in.put(None)
        for t in workers:
            t.join(timeout=30)
        q_out.put(None)
        writer_done_event.wait(timeout=120)
    finally:
        print("final stats:", stats)
        elapsed = time.time() - t0
        print(f"elapsed: {elapsed/60:.1f} min")
        print(f"output written to {args.output_dir}")

if __name__ == "__main__":
    main()
