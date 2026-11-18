#!/usr/bin/env python3
"""Load test mở rộng cho Stage 4: bot headless, seed cố định, xuất CSV.

Ví dụ:
  python tools/load_test.py --url http://127.0.0.1:8000 --players 50 --duration 60 \
      --label baseline --proto 1 --input-mode periodic --run 1
  python tools/load_test.py --url http://127.0.0.1:8000 --players 50 --duration 60 \
      --label improved --proto 2 --input-mode onchange --run 1
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import os
import random
import statistics
import sys
import time
import uuid
from urllib.parse import urlparse

from aiohttp import ClientSession, TCPConnector, WSMsgType

# Bản gốc chọn dx,dy độc lập trong {-1,0,1} => ~44% lần là đường chéo,
# server từ chối (parse_input: |dx|+|dy|<=1) và trả 'error' làm nhiễu byte nhận.
# Giờ chỉ dùng 4 hướng cardinal + đứng yên, đúng luật "một hướng mỗi lần".
CARDINAL = [(0, 0), (1, 0), (-1, 0), (0, 1), (0, -1)]
TICK = 0.1            # bot "ý định" 10 Hz như bản gốc
DIR_EVERY = 15        # đổi hướng mỗi 15 tick bot (1,5 s)  theo tick -> xác định, không phụ thuộc đồng hồ
CLOAK_EVERY = 37
PING_EVERY = 2.0
KEEPALIVE = 1.0       # chế độ onchange: gửi lại input cũ mỗi 1 s (khớp adapter.ts)


def pct(values: list[float], q: float) -> float | None:
    """percentile 'nearest-rank' (bản gốc dùng int(n*.95), lệch 1 phần tử)."""
    if not values:
        return None
    s = sorted(values)
    return s[max(0, min(len(s) - 1, math.ceil(q / 100 * len(s)) - 1))]


def new_rec(index: int, room: str, token: str = '') -> dict:
    return dict(index=index, room=room, token=token, dup_retries=0, connected=False, failure=None, active_s=0.0,
                bytes_in=0, bytes_out=0, msgs_in=0, msgs_out=0,
                state_msgs=0, state_bytes=0, fulls=0, deltas=0, rtts=[],
                server_errors=0, error_examples=[], drops_in=0, drops_out=0,
                gaps=0, resyncs=0, stale_ms=[], reconnect_ms=None, recovery_ms=None,
                # trạng thái nội bộ
                seq=0, tick_no=0, dx=0, dy=0, last_key=None, last_sent=0.0,
                last_state_seq=None, stale_since=None, last_resync=0.0, await_first=None,
                # góc nhìn của bot về chính mình, để tự rejoin (xem auto_rejoin)
                own_id=None, phase=None, alive=None, round=None, rejoins=0, rejoin_due=None)


async def send(socket, rec: dict, obj: dict) -> None:
    # dùng JSON gọn + đếm byte gửi (bản gốc dùng send_json, không đếm chiều lên).
    text = json.dumps(obj, separators=(',', ':'))
    rec['bytes_out'] += len(text.encode('utf-8'))
    rec['msgs_out'] += 1
    await socket.send_str(text)


async def open_session(session: ClientSession, url: str, args, rec: dict, room: str, name: str):
    # thêm danh tính bền vững: JOIN có session_token, server TỪ CHỐI kết nối thứ hai
    # cùng (token, phòng) khi kết nối cũ chưa được server thấy là đã đóng ("Duplicate tab detected").
    # Khi bot ngắt rồi nối lại ngay, có thể gặp lỗi này -> thử lại tối đa ~2 s và đếm số lần thử.
    for attempt in range(40):
        socket = await session.ws_connect(url, heartbeat=20)
        join = {'type': 'join', 'name': name, 'room': room}
        if args.session:
            join['session_token'] = rec['token']            # cùng token cho mọi lần kết nối của bot
        if args.proto == 2:
            join['proto'] = 2                               # xin giao thức delta
        await send(socket, rec, join)
        msg = await asyncio.wait_for(socket.receive(), 10)
        if msg.type != WSMsgType.TEXT:
            raise RuntimeError(f'No welcome: {msg.type}')
        rec['bytes_in'] += len(msg.data.encode('utf-8'))
        rec['msgs_in'] += 1
        welcome = json.loads(msg.data)
        if welcome.get('type') == 'error' and 'Duplicate tab' in str(welcome.get('message')):
            rec['dup_retries'] += 1
            await socket.close()
            await asyncio.sleep(0.05)
            continue
        if welcome.get('type') != 'welcome':
            raise RuntimeError(f'Join rejected: {welcome}')
        rec['last_state_seq'] = None
        rec['own_id'], rec['phase'], rec['alive'] = welcome.get('id'), None, None   
        return socket
    raise RuntimeError('Join rejected: duplicate-tab retries exhausted')


def track_self(rec: dict, data: dict) -> None:
    """ Cập nhật phase/alive của CHÍNH bot từ full hoặc delta (field theo game.ts:
    players[].id/alive, phase). Dùng chung cho v1 và v2 nên workload hai bên giống nhau."""
    if 'phase' in data:
        rec['phase'] = data['phase']
    if 'round' in data and data['round'] != rec['round']:
        rec['round'] = data['round']
        rec['last_key'] = None      # ván mới: server đặt dx,dy=0 -> gửi lại input ngay (giống adapter.ts)
    own = rec['own_id']
    if data['type'] == 'snapshot':
        me = next((p for p in data.get('players', []) if p.get('id') == own), None)
        # không thấy mình trong danh sách (vd. đang cloak ở góc nhìn người khác) -> giữ nguyên
        if me is not None:
            rec['alive'] = me.get('alive')
    else:
        for p in data.get('pu', []):
            if p.get('id') == own and 'alive' in p:
                rec['alive'] = p['alive']


def on_state(rec: dict, data: dict, nbytes: int, now: float, frng: random.Random, args):
    """Xử lý snapshot/delta. Trả về True nếu cần gửi resync."""
    kind = data['type']
    rec['state_msgs'] += 1
    rec['state_bytes'] += nbytes
    if args.drop_in and frng.random() < args.drop_in:      # mất message TẦNG ỨNG DỤNG có chủ ý
        rec['drops_in'] += 1
        if rec['stale_since'] is None:
            rec['stale_since'] = now
        return False
    last = rec['last_state_seq']
    if kind == 'delta':
        rec['deltas'] += 1
        if data['seq'] <= (last if last is not None else -1):
            return False                                   # trùng/cũ
        if last is None or data.get('base') != last:       # hụt gói -> không áp được
            rec['gaps'] += 1
            if rec['stale_since'] is None:
                rec['stale_since'] = now
            if now - rec['last_resync'] > 0.5:
                rec['last_resync'] = now
                rec['resyncs'] += 1
                return True
            return False
        rec['last_state_seq'] = data['seq']
        track_self(rec, data)
        return False
    # snapshot đầy đủ (v1 không có seq; v2 có seq + full)
    rec['fulls'] += 1
    if 'seq' in data:
        if last is not None and data['seq'] <= last:
            return False
        rec['last_state_seq'] = data['seq']
    track_self(rec, data)
    if rec['stale_since'] is not None:                     # state hội tụ lại
        rec['stale_ms'].append((now - rec['stale_since']) * 1000)
        rec['stale_since'] = None
    if rec['await_first'] is not None:                     # đo recovery
        rec['recovery_ms'] = (now - rec['await_first']) * 1000
        rec['await_first'] = None
    return False


async def play(socket, rec: dict, args, rng, frng, stop_at: float) -> None:
    async def sender():
        nxt, last_ping = time.monotonic(), 0.0
        while time.monotonic() < stop_at:
            now = time.monotonic()
            rec['tick_no'] += 1
            if rec['tick_no'] % DIR_EVERY == 1:
                rec['dx'], rec['dy'] = rng.choice(CARDINAL)
            cloak = rec['tick_no'] % CLOAK_EVERY == 0
            rec['seq'] += 1
            key = (rec['dx'], rec['dy'])
            if rec['phase'] != 'running' or rec['alive'] is False:
                rec['last_key'] = None   # engine bỏ input khi chưa 'running' / đã bị loại
            # cùng "ý định", hai cách gửi: periodic (baseline) / onchange (cải tiến)
            due = (args.input_mode == 'periodic' or key != rec['last_key'] or cloak
                   or now - rec['last_sent'] >= KEEPALIVE)
            if due:
                rec['last_key'], rec['last_sent'] = key, now   # client tin là đã gửi, kể cả khi bị drop
                if args.drop_out and frng.random() < args.drop_out:
                    rec['drops_out'] += 1                      # drop input chủ ý
                else:
                    await send(socket, rec, {'type': 'input', 'seq': rec['seq'],
                                             'dx': rec['dx'], 'dy': rec['dy'], 'cloak': cloak})
            # auto-rejoin: bị loại hoặc ván kết thúc -> gửi REJOIN (tối đa 1 lần/2 s).
            # Nếu không, thế giới đứng yên sau ván đầu, delta gần như rỗng còn baseline vẫn gửi đủ
            # => so sánh bị thổi phồng. Cùng luật cho baseline và improved.
            # engine.rejoin(): chỉ thành công nếu đã chết hoặc ván 'finished'; người đầu tiên mở ván mới
            # nên nếu cả phòng cùng gửi, số còn lại nhận lỗi 'Rejoin unavailable'. Vì vậy mỗi bot chờ
            # một độ trễ ngẫu nhiên (luồng frng riêng) và huỷ nếu thấy ván đã chạy lại.
            if args.auto_rejoin:
                need = rec['alive'] is False or rec['phase'] == 'finished'
                if not need:
                    rec['rejoin_due'] = None
                elif rec['rejoin_due'] is None:
                    rec['rejoin_due'] = now + frng.uniform(0.1, 1.5)
                elif now >= rec['rejoin_due']:
                    rec['rejoin_due'] = now + 2.0      # nếu chưa hiệu lực, thử lại sau 2 s
                    rec['rejoins'] += 1
                    await send(socket, rec, {'type': 'rejoin'})
            if now - last_ping >= PING_EVERY:
                last_ping = now
                await send(socket, rec, {'type': 'ping', 'nonce': time.monotonic_ns()})
            nxt += TICK
            await asyncio.sleep(max(0.0, nxt - time.monotonic()))

    async def receiver():
        # đọc liên tục ở tác vụ riêng, tách khỏi tác vụ gửi. Đây là thay đổi về CẤU TRÚC
        # (để thêm resync/rejoin/reconnect gọn hơn), KHÔNG phải sửa lỗi: đo thử 50 bot cho RTT p50
        # gần như bằng bản gốc; chỉ p95 khác (1 lần chạy, chưa kết luận được nguyên nhân).
        while True:
            remaining = stop_at - time.monotonic()
            if remaining <= 0:
                return
            try:
                msg = await socket.receive(timeout=min(0.5, remaining))
            except asyncio.TimeoutError:
                continue
            now = time.monotonic()
            if msg.type == WSMsgType.TEXT:
                nbytes = len(msg.data.encode('utf-8'))
                rec['bytes_in'] += nbytes
                rec['msgs_in'] += 1
                data = json.loads(msg.data)
                kind = data.get('type')
                if kind == 'pong':
                    rec['rtts'].append((time.monotonic_ns() - data['nonce']) / 1e6)
                elif kind in ('snapshot', 'delta'):
                    if on_state(rec, data, nbytes, now, frng, args):
                        await send(socket, rec, {'type': 'resync'})
                elif kind == 'error':                      # đếm lỗi, không giấu
                    rec['server_errors'] += 1
                    if len(rec['error_examples']) < 3:
                        rec['error_examples'].append(data.get('message'))
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                raise RuntimeError('Socket closed during test')

    await asyncio.gather(sender(), receiver())


async def bot(session, url, index, args, end, rejoin_set, reconnect_abs):
    room = f'load-{index // args.room_size:03d}'
    rec = new_rec(index, room, f'bot-{args.nonce}-{index}')   # token riêng mỗi bot, mới mỗi lần chạy
    # seed theo (seed, bot): luồng hướng đi của mỗi bot cố định giữa các lần chạy.
    rng = random.Random(args.seed * 100003 + index)
    frng = random.Random(args.seed * 7919 + index)         # luồng riêng cho drop để không làm lệch rng hướng đi
    socket = None
    try:
        await asyncio.sleep(index * args.ramp)             # vào dần, tránh dội 100 kết nối cùng lúc
        socket = await open_session(session, url, args, rec, room, f'bot-{index}')
        rec['connected'] = True
        t_join = time.monotonic()
        split = reconnect_abs if index in rejoin_set else end
        await play(socket, rec, args, rng, frng, min(split, end))
        if index in rejoin_set and time.monotonic() < end:
            # ngắt rồi vào lại. LƯU Ý: baseline chưa có danh tính bền vững nên
            # đây là JOIN mới (id mới). Khi làm session id, sửa open_session cho gửi id cũ.
            t_close = time.monotonic()
            await socket.close()
            socket = await open_session(session, url, args, rec, room, f'bot-{index}')
            rec['reconnect_ms'] = (time.monotonic() - t_close) * 1000
            rec['await_first'] = t_close
            await play(socket, rec, args, rng, frng, end)
        rec['active_s'] = time.monotonic() - t_join
    except Exception as exc:                               # giữ lại số đo dở dang thay vì mất hết
        rec['failure'] = f'{type(exc).__name__}: {exc}'
        if rec['connected']:
            rec['active_s'] = max(rec['active_s'], 0.001)
    finally:
        if socket is not None and not socket.closed:
            await socket.close()
    return rec


async def fetch_metrics(session, ws_url: str):
    """đọc /metrics của server (transport.py mới). Server cũ không có -> None."""
    u = urlparse(ws_url)
    http = f"{'https' if u.scheme == 'wss' else 'http'}://{u.netloc}/metrics"
    try:
        async with session.get(http) as resp:
            return await resp.json() if resp.status == 200 else None
    except Exception:
        return None


def summarize(args, recs, wall, m0, m1, rooms) -> dict:
    ok = [r for r in recs if r['connected'] and r['active_s'] > 0]
    failed = [r for r in recs if r['failure'] is not None]
    in_rate = [r['bytes_in'] / r['active_s'] for r in ok]
    out_rate = [r['bytes_out'] / r['active_s'] for r in ok]
    rtts = [x for r in recs for x in r['rtts']]
    stale = [x for r in recs for x in r['stale_ms']]
    rec_ms = [r['recovery_ms'] for r in recs if r['recovery_ms'] is not None]
    rcn_ms = [r['reconnect_ms'] for r in recs if r['reconnect_ms'] is not None]
    st_msgs = sum(r['state_msgs'] for r in recs)
    row = {
        'label': args.label, 'proto': args.proto, 'input_mode': args.input_mode, 'run': args.run,
        'seed': args.seed, 'players': args.players, 'rooms': rooms, 'duration_s': args.duration,
        'drop_in': args.drop_in, 'drop_out': args.drop_out, 'reconnect_fraction': args.reconnect_fraction,
        'connected': len(ok), 'failed': len(failed),
        'bytes_in_total_per_s': round(sum(in_rate), 1),            # S->C, cộng mọi bot
        'bytes_out_total_per_s': round(sum(out_rate), 1),          # C->S
        'bytes_in_per_player_per_s_mean': round(statistics.mean(in_rate), 1) if in_rate else None,
        'bytes_in_per_player_per_s_p95': round(pct(in_rate, 95), 1) if in_rate else None,
        'bytes_out_per_player_per_s_mean': round(statistics.mean(out_rate), 1) if out_rate else None,
        'state_msgs': st_msgs,
        'bytes_per_state_msg': round(sum(r['state_bytes'] for r in recs) / st_msgs, 1) if st_msgs else None,
        'fulls': sum(r['fulls'] for r in recs), 'deltas': sum(r['deltas'] for r in recs),
        'rtt_samples': len(rtts),
        'rtt_p50_ms': round(statistics.median(rtts), 2) if rtts else None,
        'rtt_p95_ms': round(pct(rtts, 95), 2) if rtts else None,
        'rejoins': sum(r['rejoins'] for r in recs),
        'dup_retries': sum(r['dup_retries'] for r in recs),
        'server_errors': sum(r['server_errors'] for r in recs),
        'drops_in': sum(r['drops_in'] for r in recs), 'drops_out': sum(r['drops_out'] for r in recs),
        'gaps': sum(r['gaps'] for r in recs), 'resyncs': sum(r['resyncs'] for r in recs),
        'stale_n': len(stale),
        'stale_p50_ms': round(statistics.median(stale), 1) if stale else None,
        'stale_p95_ms': round(pct(stale, 95), 1) if stale else None,
        'reconnect_n': len(rcn_ms),
        'reconnect_p50_ms': round(statistics.median(rcn_ms), 1) if rcn_ms else None,
        'recovery_p50_ms': round(statistics.median(rec_ms), 1) if rec_ms else None,
        'recovery_p95_ms': round(pct(rec_ms, 95), 1) if rec_ms else None,
        'server_bytes_out_per_s': None,
        'wall_s': round(wall, 1),
        'failure_examples': ' | '.join(sorted({r['failure'] for r in failed if r['failure']}))[:300],
        'error_examples': ' | '.join(sorted({str(e) for r in recs for e in r['error_examples']}))[:200],
    }
    if m0 and m1 and wall > 0:
        row['server_bytes_out_per_s'] = round((m1['bytes_out'] - m0['bytes_out']) / wall, 1)
    return row


def write_csv(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    exists = os.path.exists(path) and os.path.getsize(path) > 0
    with open(path, 'a', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


async def run(args):
    url = args.url
    if url.startswith('http://'): url = 'ws://' + url[7:].rstrip('/') + '/ws'
    if url.startswith('https://'): url = 'wss://' + url[8:].rstrip('/') + '/ws'
    # TCPConnector(limit=0): mặc định aiohttp chỉ cho 100 kết nối đồng thời (đã đo: 110 socket ->
    # chỉ 100 nối được). Với đúng 100 bot vẫn đủ; chỉ cần khi chạy >100 bot hoặc có kết nối thêm cùng lúc.
    async with ClientSession(connector=TCPConnector(limit=0)) as session:
        m0 = await fetch_metrics(session, url)
        start = time.monotonic()
        end = start + args.duration + args.ramp * (args.players - 1)   # bot vào cuối vẫn chơi đủ --duration
        k = round(args.players * args.reconnect_fraction)
        rejoin_set = set(random.Random(args.seed).sample(range(args.players), k)) if k else set()
        reconnect_abs = start + args.ramp * (args.players - 1) + args.reconnect_at
        recs = await asyncio.gather(*(bot(session, url, i, args, end, rejoin_set, reconnect_abs)
                                      for i in range(args.players)))
        wall = time.monotonic() - start
        m1 = await fetch_metrics(session, url)
    rooms = math.ceil(args.players / args.room_size)
    row = summarize(args, recs, wall, m0, m1, rooms)
    write_csv(args.out, [row])
    if args.players_out:                                    # số liệu từng bot
        write_csv(args.players_out, [
            {'label': args.label, 'run': args.run, 'bot': r['index'], 'room': r['room'],
             'bytes_in': r['bytes_in'], 'bytes_out': r['bytes_out'], 'active_s': round(r['active_s'], 2),
             'state_msgs': r['state_msgs'], 'rtt_n': len(r['rtts']), 'failure': r['failure'] or ''}
            for r in recs])
    print(json.dumps(row, indent=2, ensure_ascii=False))
    if row['failed'] and args.strict:
        raise SystemExit(1)                                 # mặc định KHÔNG thoát lỗi: stress test cần ghi cả lỗi


def main():
    p = argparse.ArgumentParser(description='Load-test + đo đạc cho Stage 4')
    p.add_argument('--url', default='ws://127.0.0.1:8000/ws')
    p.add_argument('--players', type=int, default=10)
    p.add_argument('--room-size', type=int, default=20)
    p.add_argument('--duration', type=int, default=15)
    # Các tham số mới
    p.add_argument('--seed', type=int, default=1)
    p.add_argument('--label', default=None, help='baseline / improved / ... (mặc định tự ghép từ proto+input-mode)')
    p.add_argument('--proto', type=int, choices=(1, 2), default=1, help='1=snapshot đầy đủ, 2=delta')
    p.add_argument('--input-mode', choices=('periodic', 'onchange'), default='periodic')
    p.add_argument('--run', type=int, default=1, help='số thứ tự lần lặp')
    p.add_argument('--ramp', type=float, default=0.02, help='giây giữa 2 bot kết nối liên tiếp')
    p.add_argument('--drop-in', type=float, default=0.0, help='xác suất bỏ 1 snapshot/delta nhận được (ứng dụng)')
    p.add_argument('--drop-out', type=float, default=0.0, help='xác suất không gửi 1 input (ứng dụng)')
    p.add_argument('--reconnect-fraction', type=float, default=0.0, help='tỉ lệ bot ngắt và vào lại')
    p.add_argument('--reconnect-at', type=float, default=20.0, help='giây (sau khi bot cuối vào) để ngắt')
    p.add_argument('--no-auto-rejoin', dest='auto_rejoin', action='store_false', help='tắt tự rejoin khi bị loại/hết ván')
    p.add_argument('--no-session', dest='session', action='store_false', help='không gửi session_token (server cũ chưa có phiên bản)')
    p.add_argument('--out', default='results/summary.csv')
    p.add_argument('--players-out', default=None)
    p.add_argument('--strict', action='store_true', help='thoát mã 1 nếu có bot lỗi')
    a = p.parse_args()
    if not 1 <= a.players <= 1000 or not 1 <= a.room_size <= 20 or a.duration < 1:
        p.error('Use 1-1000 players, room-size 1-20, and positive duration')
    if not (0 <= a.drop_in < 1 and 0 <= a.drop_out < 1 and 0 <= a.reconnect_fraction <= 1):
        p.error('drop rates in [0,1), reconnect-fraction in [0,1]')
    a.nonce = uuid.uuid4().hex[:8]   # token không trùng giữa các lần chạy (server nhớ session cũ)
    a.label = a.label or f'proto{a.proto}-{a.input_mode}'
    asyncio.run(run(a))


if __name__ == '__main__':
    main()
