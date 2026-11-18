"""Mã hoá delta cho snapshot (giao thức v2).

Ý tưởng: với mỗi client, server nhớ snapshot cuối cùng đã gửi cho CHÍNH client đó
(đã lọc cloak vì dùng snapshot_for). Mỗi tick chỉ gửi phần khác biệt. Cứ FULL_EVERY
snapshot thì gửi lại một bản đầy đủ để chặn trên độ lệch nếu client lỡ mất delta.

Thông điệp server -> client:
  full  : {"type":"snapshot", ...như baseline..., "seq":N, "full":true}
  delta : {"type":"delta", "seq":N, "base":N-1, "tick":T, <scalar đổi>,
           "pu":[player đổi], "pr":[id player bị xoá],
           "iu":[item đổi],   "ir":[id item bị xoá], "events":[...]}
Client chỉ áp delta khi delta.base == seq đã áp gần nhất; nếu không thì xin "resync".

GIẢ ĐỊNH còn lại (cần engine.py để xác nhận): mỗi phần tử trong
snapshot['players'] và snapshot['items'] là dict có khoá 'id' duy nhất. Nếu không có
'id' thì make_delta tự lùi về gửi nguyên danh sách (đúng nhưng ít tiết kiệm).
"""
from __future__ import annotations

from typing import Any

FULL_EVERY = 10        # số snapshot giữa 2 bản full (10 Hz -> 1 giây)
FLOAT_DIGITS = 1       # làm tròn toạ độ khi gửi: 0.1 đơn vị, arena chỉ 960x600
LIST_FIELDS = ('players', 'items')
ABBR = {'players': ('pu', 'pr'), 'items': ('iu', 'ir')}
_SKIP = {'type', 'events', 'seq', 'full', 'base'} | set(LIST_FIELDS)


def quantize(value: Any, digits: int = FLOAT_DIGITS) -> Any:
    """Làm tròn mọi float (đệ quy). Chỉ ảnh hưởng hình ảnh gửi đi, KHÔNG đụng mô phỏng."""
    if isinstance(value, float):
        return round(value, digits)
    if isinstance(value, dict):
        return {k: quantize(v, digits) for k, v in value.items()}
    if isinstance(value, list):
        return [quantize(v, digits) for v in value]
    return value


def _index(entries: Any) -> dict | None:
    if not isinstance(entries, list):
        return None
    out: dict = {}
    for entry in entries:
        if not isinstance(entry, dict) or 'id' not in entry:
            return None
        out[entry['id']] = entry
    return out


def make_delta(prev: dict, cur: dict) -> dict:
    out: dict = {}
    for key, value in cur.items():
        if key in _SKIP:
            continue
        if prev.get(key) != value:
            out[key] = value                      # scalar đổi (tick luôn đổi)
    for name in LIST_FIELDS:
        up_key, rm_key = ABBR[name]
        old, new = _index(prev.get(name)), _index(cur.get(name))
        if old is None or new is None:            # không có 'id' -> gửi cả danh sách
            if prev.get(name) != cur.get(name):
                out[name] = cur.get(name)
            continue
        removed = [ident for ident in old if ident not in new]
        upserts = []
        for ident, entry in new.items():
            before = old.get(ident)
            if before is None:
                upserts.append(entry)             # phần tử mới: gửi đủ
            elif before.keys() != entry.keys():   # đổi tập trường: xoá rồi thêm lại
                removed.append(ident)
                upserts.append(entry)
            else:
                changed = {k: v for k, v in entry.items() if before[k] != v}
                if changed:
                    changed['id'] = ident
                    upserts.append(changed)       # chỉ trường đổi + id
        if upserts:
            out[up_key] = upserts
        if removed:
            out[rm_key] = removed
    return out


class DeltaEncoder:
    """Một encoder cho mỗi kết nối (mỗi player)."""

    def __init__(self) -> None:
        self.base: dict | None = None
        self.seq = 0
        self.since_full = 0
        self.need_full = True
        self._event_ids: set = set()   # id các event của bản tin trước (game.ts cho thấy events là log có id)

    def request_full(self) -> None:
        self.need_full = True

    def next(self, snapshot: dict) -> dict:
        cur = quantize(snapshot)
        self.seq += 1
        events = cur.get('events') or []
        prev_ids = self._event_ids
        self._event_ids = {e['id'] for e in events if isinstance(e, dict) and 'id' in e}
        if self.need_full or self.base is None or self.since_full >= FULL_EVERY:
            self.base, self.since_full, self.need_full = cur, 0, False
            return {**cur, 'seq': self.seq, 'full': True}
        msg = {'type': 'delta', 'seq': self.seq, 'base': self.seq - 1, **make_delta(self.base, cur)}
        # Chỉ gửi event CHƯA gửi ở bản tin trước (event không có id thì gửi hết).
        fresh = [e for e in events if not (isinstance(e, dict) and 'id' in e and e['id'] in prev_ids)]
        if fresh:
            msg['events'] = fresh
        self.base = cur
        self.since_full += 1
        return msg
