#!/usr/bin/env python3
""" Vẽ biểu đồ before/after từ results/summary.csv (mean ± std trên các lần lặp).

  python tools/plot_results.py --baseline baseline --improved improved --csv results/summary.csv --outdir results/charts
"""
import argparse, os
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

p = argparse.ArgumentParser()
p.add_argument('--csv', default='results/summary.csv')
p.add_argument('--baseline', default='baseline')
p.add_argument('--improved', default='improved')
p.add_argument('--outdir', default='results/charts')
a = p.parse_args()
os.makedirs(a.outdir, exist_ok=True)
df = pd.read_csv(a.csv)
# chỉ lấy lần chạy "sạch" (không chủ ý thả gói / ngắt kết nối) cho biểu đồ tải
clean = df[(df.drop_in == 0) & (df.drop_out == 0) & (df.reconnect_fraction == 0)]


def bars(data, metric, ylabel, title, fname, labels):
    g = data[data.label.isin(labels)].groupby(['players', 'label'])[metric].agg(['mean', 'std', 'count']).reset_index()
    players = sorted(g.players.unique())
    width = 0.8 / len(labels)
    fig, ax = plt.subplots(figsize=(7, 4.2))
    for i, lab in enumerate(labels):
        s = g[g.label == lab].set_index('players').reindex(players)
        xs = [k + (i - (len(labels) - 1) / 2) * width for k in range(len(players))]
        ax.bar(xs, s['mean'], width, yerr=s['std'].fillna(0), capsize=3, label=f"{lab} (n={int(s['count'].max()) if s['count'].notna().any() else 0})")
    ax.set_xticks(range(len(players)), [str(x) for x in players])
    ax.set_xlabel('Số người chơi (bot)'); ax.set_ylabel(ylabel); ax.set_title(title)
    ax.grid(axis='y', alpha=.3); ax.legend()
    fig.tight_layout(); fig.savefig(os.path.join(a.outdir, fname), dpi=160); plt.close(fig)


L = [a.baseline, a.improved]
bars(clean, 'bytes_in_total_per_s', 'Byte/s (server→client, tổng)', 'Băng thông tổng (payload ứng dụng)', 'bw_total.png', L)
bars(clean, 'bytes_in_per_player_per_s_mean', 'Byte/s mỗi người', 'Băng thông mỗi người chơi (server→client)', 'bw_per_player.png', L)
bars(clean, 'bytes_per_state_msg', 'Byte / bản tin trạng thái', 'Kích thước trung bình mỗi bản tin trạng thái', 'bytes_per_msg.png', L)
bars(clean, 'bytes_out_total_per_s', 'Byte/s (client→server, tổng)', 'Băng thông chiều lên (INPUT, ping)', 'bw_up.png', L)
bars(clean, 'rtt_p50_ms', 'ms', 'App RTT median', 'rtt_p50.png', L)
bars(clean, 'rtt_p95_ms', 'ms', 'App RTT p95', 'rtt_p95.png', L)
bars(clean, 'failed', 'Số bot lỗi', 'Bot lỗi / không kết nối được', 'failed.png', L)
rec = df[(df.reconnect_fraction > 0) & (df.drop_in == 0) & (df.drop_out == 0)]
if len(rec):
    bars(rec, 'recovery_p50_ms', 'ms', 'Recovery: từ lúc ngắt đến bản FULL đầu tiên (median)', 'recovery.png', L)
flt = df[(df.drop_in > 0) & (df.label.isin(L))]
if len(flt):
    g = flt.groupby(['label', 'drop_in'])['stale_p95_ms'].mean().reset_index()
    fig, ax = plt.subplots(figsize=(7, 4.2))
    for lab in L:
        s = g[g.label == lab]
        ax.plot(s.drop_in * 100, s.stale_p95_ms, marker='o', label=lab)
    ax.set_xlabel('Tỉ lệ drop message ứng dụng (%)'); ax.set_ylabel('Độ lệch trạng thái p95 (ms)')
    ax.set_title('Thời gian client giữ trạng thái cũ sau khi mất message'); ax.grid(alpha=.3); ax.legend()
    fig.tight_layout(); fig.savefig(os.path.join(a.outdir, 'staleness_vs_drop.png'), dpi=160)
# bảng tóm tắt để dán vào EXPERIMENTS.md
cols = ['players', 'label', 'bytes_in_total_per_s', 'bytes_in_per_player_per_s_mean', 'bytes_per_state_msg',
        'bytes_out_total_per_s', 'rtt_p50_ms', 'rtt_p95_ms', 'failed']
t = clean[clean.label.isin(L)].groupby(['players', 'label'])[cols[2:]].mean().round(1).reset_index()
t.to_csv(os.path.join(a.outdir, 'summary_table.csv'), index=False)
print(t.to_string(index=False)); print('Biểu đồ ->', a.outdir)
