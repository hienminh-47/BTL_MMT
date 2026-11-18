/** STUDENT-EDITABLE: baseline JSON message encoding and decoding. */
import type { Action, Snapshot, Welcome } from '../core/contracts';

// Khai báo kiểu hợp (Discriminated Union Type) đại diện cho tất cả các loại gói tin hợp lệ mà Client có thể nhận được từ Server
// Thông điệp delta của giao thức v2 (xem server/app/network/delta.py)
export type DeltaMessage = { type: 'delta'; seq: number; base: number; tick: number } & Record<string, unknown>;
// Thêm DeltaMessage vào union
export type ServerMessage = Welcome | Snapshot | DeltaMessage | { type: 'pong'; nonce: number } | { type: 'error'; message: string };

export function encodeInput(action: Action): string {
  return JSON.stringify({ type: 'input', ...action });
}

export function decodeServerMessage(raw: string): ServerMessage | null {
  try {
    const data: unknown = JSON.parse(raw);
    if (data === null || typeof data !== 'object' || !('type' in data)) return null;
    const record = data as Record<string, unknown>;
    if (record.type === 'welcome' && typeof record.id === 'string') return record as Welcome;
    if (record.type === 'snapshot' && typeof record.tick === 'number' && Array.isArray(record.players)) return record as Snapshot;
    // Nhận dạng delta
    if (record.type === 'delta' && typeof record.seq === 'number' && typeof record.base === 'number') return record as DeltaMessage;
    if (record.type === 'pong' && typeof record.nonce === 'number') return { type: 'pong', nonce: record.nonce };
    if (record.type === 'error' && typeof record.message === 'string') return { type: 'error', message: record.message };
    return null;
  } catch { return null; }
}

// ---------------------------------------------------------------------------
// SnapshotAssembler: ghép full + delta thành Snapshot đầy đủ cho UI.
// Quy tắc: bỏ gói trùng/cũ (seq <= đã áp); delta chỉ áp khi base == seq đã áp,
// nếu không là "hụt" -> báo needResync để adapter gửi {type:'resync'}.
// ---------------------------------------------------------------------------
type Entry = Record<string, unknown>;
const META_KEYS = new Set(['type', 'seq', 'base', 'full', 'events', 'pu', 'pr', 'iu', 'ir']);

function patchList(list: unknown, ups: unknown, rem: unknown): Entry[] {
  const map = new Map<unknown, Entry>();
  for (const e of (Array.isArray(list) ? list : []) as Entry[]) map.set(e.id, e);
  if (Array.isArray(rem)) for (const id of rem) map.delete(id);               // xoá trước
  if (Array.isArray(ups)) for (const u of ups as Entry[]) {                   // rồi upsert
    const old = map.get(u.id);
    map.set(u.id, old ? { ...old, ...u } : u);
  }
  return [...map.values()];
}

export class SnapshotAssembler {
  private base: Entry | null = null;
  private seq = -1;

  reset(): void { this.base = null; this.seq = -1; }

  accept(msg: Snapshot | DeltaMessage): { snapshot: Snapshot | null; needResync: boolean } {
    const raw = msg as unknown as Entry;
    if (msg.type === 'snapshot') {
      if (typeof raw.seq !== 'number') {                  // snapshot v1 (baseline): không có seq
        this.base = null;
        return { snapshot: msg, needResync: false };
      }
      if (raw.seq <= this.seq) return { snapshot: null, needResync: false };   // trùng / cũ
      this.seq = raw.seq; this.base = raw;
      return { snapshot: msg, needResync: false };
    }
    if (msg.seq <= this.seq) return { snapshot: null, needResync: false };     // trùng / cũ
    if (!this.base || msg.base !== this.seq) return { snapshot: null, needResync: true }; // hụt gói
    const next: Entry = { ...this.base };
    for (const [k, v] of Object.entries(raw)) if (!META_KEYS.has(k)) next[k] = v;  // scalar đổi (kể cả cả danh sách dự phòng)
    next.players = patchList(next.players, raw.pu, raw.pr);
    next.items = patchList(next.items, raw.iu, raw.ir);
    next.events = Array.isArray(raw.events) ? raw.events : [];
    next.type = 'snapshot';
    this.base = next; this.seq = msg.seq;
    return { snapshot: next as unknown as Snapshot, needResync: false };
  }
}
