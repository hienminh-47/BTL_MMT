#!/usr/bin/env python3
"""Ghi CPU% và RAM của tiến trình server mỗi giây ra CSV (chạy song song lúc load test).

  python tools/cpu_monitor.py --find "uvicorn|main.py|app.main" --out results/cpu_baseline_n50.csv --seconds 70
  python tools/cpu_monitor.py --pid 1234 --out results/cpu.csv --seconds 70
Cần: pip install psutil
"""
import argparse, csv, os, re, time
import psutil

p = argparse.ArgumentParser()
p.add_argument('--pid', type=int)
p.add_argument('--find', help='regex khớp dòng lệnh của tiến trình server')
p.add_argument('--out', required=True)
p.add_argument('--seconds', type=int, default=70)
a = p.parse_args()
if a.pid:
    proc = psutil.Process(a.pid)
else:
    rx = re.compile(a.find or r'main\.py')
    cands = [q for q in psutil.process_iter(['pid', 'cmdline']) if q.info['cmdline'] and rx.search(' '.join(q.info['cmdline'])) and q.pid != os.getpid()]
    if not cands:
        raise SystemExit('Không tìm thấy tiến trình server; dùng --pid')
    proc = cands[0]
    print('Theo dõi PID', proc.pid, ' '.join(proc.info['cmdline'])[:100])
os.makedirs(os.path.dirname(a.out) or '.', exist_ok=True)
proc.cpu_percent(None)
with open(a.out, 'w', newline='') as fh:
    w = csv.writer(fh); w.writerow(['t_s', 'cpu_percent', 'rss_mb'])
    for t in range(a.seconds):
        time.sleep(1)
        w.writerow([t + 1, proc.cpu_percent(None), round(proc.memory_info().rss / 1e6, 1)])
print('Đã ghi', a.out, '(cpu_percent 100 = 1 nhân đầy)')
