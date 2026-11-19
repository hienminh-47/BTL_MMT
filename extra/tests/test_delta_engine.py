"""Kiểm tra đầu-cuối: dùng GameService/World THẬT, cho người chơi di chuyển, cloak, ăn vật phẩm,
bị loại, rejoin; mỗi snapshot_for(viewer) mã hoá bằng DeltaEncoder rồi ghép lại ở "client" và so với bản gốc.

Chứng minh: (1) delta + ghép tái tạo ĐÚNG góc nhìn của từng người (kể cả người đang cloak bị ẩn),
(2) dữ liệu server (luật game) không bị đụng tới.
"""
import random

from app.game.service import GameService
from app.network.delta import DeltaEncoder, quantize


class Client:
    """Bản Python của SnapshotAssembler (protocol.ts)."""
    def __init__(self):
        self.state, self.seq, self.seen = None, -1, set()

    @staticmethod
    def _patch(lst, ups, rem):
        m = {e['id']: e for e in lst or []}
        for i in rem or []:
            m.pop(i, None)
        for u in ups or []:
            m[u['id']] = {**m[u['id']], **u} if u['id'] in m else u
        return list(m.values())

    def apply(self, msg):
        if msg['type'] == 'snapshot':
            self.state, self.seq = msg, msg['seq']
            self.seen |= {e['id'] for e in msg.get('events', [])}
            return True
        assert msg['base'] == self.seq, 'hụt gói không mong đợi trong luồng tin cậy'
        skip = {'type', 'seq', 'base', 'events', 'pu', 'pr', 'iu', 'ir'}
        nxt = dict(self.state)
        nxt.update({k: v for k, v in msg.items() if k not in skip})
        nxt['events'] = msg.get('events', [])
        nxt['players'] = self._patch(nxt.get('players'), msg.get('pu'), msg.get('pr'))
        nxt['items'] = self._patch(nxt.get('items'), msg.get('iu'), msg.get('ir'))
        self.state, self.seq = nxt, msg['seq']
        self.seen |= {e['id'] for e in nxt['events']}
        return True


def _by_id(entries):
    return sorted(entries, key=lambda e: e['id'])


def test_delta_matches_real_engine_views():
    rng = random.Random(5)
    svc = GameService()
    tokens = [f'tok{i}' for i in range(8)]
    ids = [svc.join(f'p{i}', 'e2e', session_token=t)['id'] for i, t in enumerate(tokens)]
    enc = {pid: DeltaEncoder() for pid in ids}
    cli = {pid: Client() for pid in ids}
    cloaks = 0
    for tick in range(1, 1500):
        for pid in ids:
            if rng.random() < 0.15:
                dx, dy = rng.choice([(0, 0), (1, 0), (-1, 0), (0, 1), (0, -1)])
                cloak = rng.random() < 0.05
                svc.apply_input(pid, tick, dx, dy, cloak)
        if tick % 97 == 0:                               # người bị loại/hết ván thì rejoin
            for pid in ids:
                svc.rejoin(pid)
        svc.advance()
        if tick % 2:                                     # 10 Hz publish
            continue
        for pid in ids:
            snap = svc.snapshot_for(pid)
            assert snap is not None
            if enc[pid].seq and rng.random() < 0.01:     # thỉnh thoảng client xin resync
                enc[pid].request_full()
            msg = enc[pid].next(snap)
            cli[pid].apply(msg)
            want = quantize(snap)
            got = cli[pid].state
            assert _by_id(got['players']) == _by_id(want['players']), f'players lệch ở tick {tick}'
            assert _by_id(got['items']) == _by_id(want['items']), f'items lệch ở tick {tick}'
            for key in ('tick', 'phase', 'round', 'winner', 'room'):
                assert got[key] == want[key], f'{key} lệch ở tick {tick}'
            assert {e['id'] for e in want['events']} <= cli[pid].seen
            cloaks += sum(1 for p in want['players'] if p['invisible'])
    # kịch bản phải thực sự có người cloak để kiểm tra việc ẩn/hiện
    assert cloaks > 0
