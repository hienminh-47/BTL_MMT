#!/usr/bin/env python3
""" Chạy ma trận thí nghiệm: mỗi mức người chơi x số lần lặp, ghi vào results/summary.csv.

Baseline (server GỐC hoặc ?proto=1):
  python tools/run_matrix.py --url http://127.0.0.1:8000 --label baseline --proto 1 --input-mode periodic
Improved:
  python tools/run_matrix.py --url http://127.0.0.1:8000 --label improved --proto 2 --input-mode onchange
Tách từng bước (ablation):
  --label delta-only --proto 2 --input-mode periodic     |   --label input-only --proto 1 --input-mode onchange
"""
import argparse, subprocess, sys, time, os

p = argparse.ArgumentParser()
p.add_argument('--url', required=True)
p.add_argument('--label', required=True)
p.add_argument('--proto', type=int, choices=(1, 2), default=1)
p.add_argument('--input-mode', choices=('periodic', 'onchange'), default='periodic')
p.add_argument('--players', type=int, nargs='+', default=[10, 25, 50, 100])
p.add_argument('--runs', type=int, default=3)
p.add_argument('--duration', type=int, default=60)
p.add_argument('--seed', type=int, default=1)
p.add_argument('--cooldown', type=float, default=5.0, help='nghỉ giữa các lần chạy để server dọn phòng')
p.add_argument('--out', default='results/summary.csv')
p.add_argument('extra', nargs=argparse.REMAINDER, help='tham số thêm cho load_test.py, đặt sau --')
a = p.parse_args()
here = os.path.dirname(os.path.abspath(__file__))
extra = [x for x in a.extra if x != '--']
for n in a.players:
    for run in range(1, a.runs + 1):
        cmd = [sys.executable, os.path.join(here, 'load_test.py'), '--url', a.url, '--players', str(n),
               '--duration', str(a.duration), '--seed', str(a.seed), '--label', a.label, '--proto', str(a.proto),
               '--input-mode', a.input_mode, '--run', str(run), '--out', a.out] + extra
        print('>>', ' '.join(cmd), flush=True)
        subprocess.run(cmd, stdout=subprocess.DEVNULL, check=False)   # kết quả đã nằm trong CSV
        time.sleep(a.cooldown)
print('Xong. Xem', a.out)
