"""Kiểm tra: áp chuỗi delta lên client phải tái tạo đúng snapshot server.

Đây là bằng chứng "cải tiến không đổi trạng thái hiển thị" (ngoài scenario lockstep).
Hàm apply() dưới đây là bản Python của SnapshotAssembler trong protocol.ts.
"""
import copy
import random

from app.network.delta import DeltaEncoder, quantize


def _patch(lst, ups, rem):
    m = {e['id']: e for e in lst or []}
    for i in rem or []:
        m.pop(i, None)
    for u in ups or []:
        m[u['id']] = {**m[u['id']], **u} if u['id'] in m else u
    return list(m.values())


class Client:
    def __init__(self):
        self.state, self.seq = None, -1
        self.seen = set()   # id event đã nhận (UI lọc trùng theo id như game.ts)

    def apply(self, msg):
        if msg['type'] == 'snapshot':
            if msg['seq'] <= self.seq:
                return False
            self.state, self.seq = msg, msg['seq']
            self.seen |= {e['id'] for e in msg.get('events', [])}
            return True
        if msg['seq'] <= self.seq:
            return False
        if self.state is None or msg['base'] != self.seq:
            return None                       # gap -> cần resync
        skip = {'type', 'seq', 'base', 'events', 'pu', 'pr', 'iu', 'ir'}
        nxt = dict(self.state)
        nxt.update({k: v for k, v in msg.items() if k not in skip})
        nxt['events'] = msg.get('events', [])
        nxt['players'] = _patch(nxt.get('players'), msg.get('pu'), msg.get('pr'))
        nxt['items'] = _patch(nxt.get('items'), msg.get('iu'), msg.get('ir'))
        self.state, self.seq = nxt, msg['seq']
        self.seen |= {e['id'] for e in nxt['events']}
        return True


def _world(rng, tick, players, items):
    for p in players:
        if rng.random() < 0.7:
            p['x'] += rng.uniform(-8, 8)
        if rng.random() < 0.1:
            p['credits'] += rng.randint(1, 7)
    if rng.random() < 0.2 and len(players) > 2:
        players.pop(rng.randrange(len(players)))
    if rng.random() < 0.1:
        players.append({'id': f'p{rng.randrange(10**6)}', 'x': 1.0, 'y': 2.0, 'credits': 0})
    if rng.random() < 0.3 and items:
        items.pop(rng.randrange(len(items)))
    if rng.random() < 0.3:
        items.append({'id': f'i{rng.randrange(10**6)}', 'x': rng.random() * 960, 'y': 5.0, 'v': 1})
    return {'type': 'snapshot', 'tick': tick, 'round': 1, 'phase': 'active',
            'winner': None, 'players': copy.deepcopy(players), 'items': copy.deepcopy(items),
            'events': [{'id': t, 'type': 'collect'} for t in range(max(1, tick - 4), tick + 1) if t % 3 == 0]}  # log trượt có id


def test_delta_stream_reconstructs_snapshot():
    rng = random.Random(7)
    players = [{'id': f'p{i}', 'x': 10.0 * i, 'y': 3.0, 'credits': 0} for i in range(6)]
    items = [{'id': f'i{i}', 'x': 5.5 * i, 'y': 9.0, 'v': 1} for i in range(20)]
    enc, cl = DeltaEncoder(), Client()
    for tick in range(1, 400):
        snap = _world(rng, tick, players, items)
        assert cl.apply(enc.next(snap)) is True
        want = quantize(snap)
        assert cl.state['players'] == want['players'] or \
            sorted(cl.state['players'], key=lambda e: e['id']) == sorted(want['players'], key=lambda e: e['id'])
        assert sorted(cl.state['items'], key=lambda e: e['id']) == sorted(want['items'], key=lambda e: e['id'])
        assert cl.state['tick'] == tick
        assert {e['id'] for e in want['events']} <= cl.seen      # mọi event hiện có đều đã tới UI


def test_gap_detected_and_resync_recovers():
    rng = random.Random(3)
    players = [{'id': f'p{i}', 'x': 1.0, 'y': 1.0, 'credits': 0} for i in range(4)]
    items = [{'id': 'i0', 'x': 1.0, 'y': 1.0, 'v': 1}]
    enc, cl = DeltaEncoder(), Client()
    cl.apply(enc.next(_world(rng, 1, players, items)))
    enc.next(_world(rng, 2, players, items))                      # delta bị "mất"
    assert cl.apply(enc.next(_world(rng, 3, players, items))) is None   # phát hiện gap
    enc.request_full()                                            # client gửi resync
    snap = _world(rng, 4, players, items)
    assert cl.apply(enc.next(snap)) is True
    assert cl.state['tick'] == 4


def test_duplicate_and_stale_ignored():
    enc, cl = DeltaEncoder(), Client()
    snap = {'type': 'snapshot', 'tick': 1, 'players': [{'id': 'a', 'x': 1.0}], 'items': [], 'events': []}
    m1 = enc.next(snap)
    assert cl.apply(m1) is True
    assert cl.apply(m1) is False                                   # trùng
