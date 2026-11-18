/** STUDENT-EDITABLE: WebSocket is an example, not a mandated solution.
 * You may implement polling, long polling, or another transport here while
 * preserving the public ClientTransport interface in main.ts.
 */
import type { Action, ConnectionOptions, NetworkHooks, Welcome, Snapshot } from '../core/contracts';
import { decodeServerMessage, encodeInput, SnapshotAssembler } from './protocol';  // Thêm SnapshotAssembler

//   Chọn chế độ bằng URL để chạy baseline/improved trên CÙNG một bản build:
//   ?proto=1 -> snapshot đầy đủ (baseline) | mặc định -> proto 2 (delta)
//   ?input=periodic -> gửi INPUT mỗi lần game gọi (baseline) | mặc định -> chỉ gửi khi đổi hướng
const QUERY = new URLSearchParams(location.search);
const PROTO: 1 | 2 = QUERY.get('proto') === '1' ? 1 : 2;
const INPUT_ON_CHANGE = QUERY.get('input') !== 'periodic';
const INPUT_KEEPALIVE_MS = 1000;   // vẫn gửi lại input giống cũ mỗi 1s để chịu mất gói

// Hàm sinh/lấy session_token cố định cho trình duyệt
function getSessionToken(): string {
  let token = localStorage.getItem('session_token');
  if (!token) {
    token = typeof crypto !== 'undefined' && crypto.randomUUID
      ? crypto.randomUUID()
      : 'sess_' + Math.random().toString(36).substring(2) + Date.now().toString(36);
    localStorage.setItem('session_token', token);
  }
  return token;
}

export class ClientTransport {
  private socket: WebSocket | null = null;
  private probe: number | undefined;
  private hooks: NetworkHooks;
  // Trạng thái cho delta và cho input-khi-đổi
  private assembler = new SnapshotAssembler();
  private lastResync = 0;
  private lastInputKey = '';
  private lastInputAt = 0;
  private ownId = '';       // id của mình (từ welcome) để biết mình còn sống không
  private lastRound = -1;

  constructor(hooks: NetworkHooks) { this.hooks = hooks; }

  // 1. Khởi tạo kết nối WebSocket mới tới máy chủ và đăng ký lắng nghe các sự kiện mạng
  connect({ name, room }: ConnectionOptions): void {
    this.disconnect();
    this.assembler.reset();          // Kết nối mới -> chờ bản full đầu tiên (kể cả reload/rejoin cùng session_token)
    this.lastInputKey = '';
    this.lastRound = -1;
    const sessionToken = getSessionToken(); // Lấy session_token từ localStorage
    const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${scheme}//${location.host}/ws`;
    const socket = new WebSocket(url);
    this.socket = socket;
    this.hooks.onStatus('Connecting...');

    socket.onopen = () => {
      // Gửi kèm session_token trong gói tin join
      socket.send(JSON.stringify({ type: 'join', name, room, session_token: sessionToken, proto: PROTO }));   // Thêm proto
      this.hooks.onStatus('Connected');
      this.probe = window.setInterval(() => {
        if (socket.readyState === WebSocket.OPEN) {
          socket.send(JSON.stringify({ type: 'ping', nonce: Math.round(performance.now()) }));
        }
      }, 2000);
    };

    socket.onmessage = (event: MessageEvent<string>) => {
      const msg = decodeServerMessage(event.data);
      if (!msg) return;
      if (msg.type === 'welcome') { this.ownId = msg.id; this.hooks.onWelcome(msg); }   // Lưu id
      // Snapshot/delta đều qua assembler; hụt seq thì xin resync
      if (msg.type === 'snapshot' || msg.type === 'delta') {
        const { snapshot, needResync } = this.assembler.accept(msg);
        if (snapshot) { this.hooks.onSnapshot(snapshot); this.rearmInput(snapshot); }
        if (needResync) this.requestResync();
      }
      if (msg.type === 'pong') this.hooks.onPong(Math.max(0, performance.now() - msg.nonce));
      if (msg.type === 'error') this.hooks.onStatus(`Server error: ${msg.message}`);
    };
    socket.onclose = () => {
      if (this.socket !== socket) return;
      this.clearProbe();
      this.hooks.onStatus('Disconnected. Click Join to reconnect.');
    };
    socket.onerror = () => this.hooks.onStatus('Connection error');
  }

  // Chỉ gửi INPUT khi đổi hướng / có cloak / quá INPUT_KEEPALIVE_MS.
  // `seq` do game tăng, ta chỉ bỏ bớt lần gửi nên seq vẫn tăng dần (engine loại input có seq <= đã nhận).
  sendInput(action: Action): void {
    if (this.socket?.readyState !== WebSocket.OPEN) return;
    if (INPUT_ON_CHANGE) {
      const key = `${action.dx},${action.dy}`;
      const cloak = Boolean(action.cloak);
      const now = performance.now();
      if (!cloak && key === this.lastInputKey && now - this.lastInputAt < INPUT_KEEPALIVE_MS) return;
      this.lastInputKey = key;
      this.lastInputAt = now;
    }
    this.socket.send(encodeInput(action));
  }

  // engine.py: _start_round()/rejoin() đặt lại dx,dy=0 và accept_input() bỏ input khi
  // phase != 'running'. Nếu không "nạp lại" thì người đang giữ phím sẽ đứng yên tới 1 s (keepalive).
  private rearmInput(snapshot: Snapshot): void {
    const me = snapshot.players.find(p => p.id === this.ownId);
    if (snapshot.phase !== 'running' || snapshot.round !== this.lastRound || me?.alive === false) {
      this.lastInputKey = '';
    }
    this.lastRound = snapshot.round;
  }

  // xin server gửi lại bản FULL (tối đa 2 lần/giây để không dội server)
  private requestResync(): void {
    const now = performance.now();
    if (now - this.lastResync < 500) return;
    this.lastResync = now;
    if (this.socket?.readyState === WebSocket.OPEN) this.socket.send(JSON.stringify({ type: 'resync' }));
  }

  rejoin(): void {
    if (this.socket?.readyState === WebSocket.OPEN) this.socket.send(JSON.stringify({ type: 'rejoin' }));
  }
  
  disconnect(): void {
    this.clearProbe();
    const previous = this.socket;
    this.socket = null;
    previous?.close();
  }

  private clearProbe(): void {
    if (this.probe !== undefined) window.clearInterval(this.probe);
    this.probe = undefined;
  }
}


/** Network adapter for browser scenario players.
 * Unlike the /sim/ws control channel, this uses the real gameplay endpoint.
 * Teams changing transport must adapt this as well as ClientTransport.
 */
export type ScenarioAction = {
  tick: number; op: 'join'|'leave'|'rejoin'|'input'; player: string;
  name?: string; seq?: number; dx?: number; dy?: number; cloak?: boolean;
};

export class ScenarioDataTransport {
  private socket: WebSocket | null = null;
  private opening: Promise<void> | null = null;
  private awaiting = new Map<string, {resolve: () => void; reject: (reason: Error) => void}>();
  private queue: Promise<void> = Promise.resolve();
  constructor(private hooks: {
    onWelcome: (message: Welcome) => void;
    onSnapshot: (message: Snapshot) => void;
    onError: (message: string) => void;
  }) {}

  private ensureSocket(): Promise<void> {
    if (this.socket?.readyState === WebSocket.OPEN) return Promise.resolve();
    if (this.opening) return this.opening;
    const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(scheme + '//' + location.host + '/ws');
    this.socket = ws;
    this.opening = new Promise<void>((resolve, reject) => {
      const fail = () => reject(new Error('Gameplay network connection failed'));
      ws.addEventListener('open', () => resolve(), {once: true});
      ws.addEventListener('error', fail, {once: true});
    }).finally(() => { this.opening = null; });
    ws.addEventListener('message', (event: MessageEvent<string>) => {
      try {
        const message = JSON.parse(event.data) as Record<string, unknown>;
        if (message.type === 'welcome') this.hooks.onWelcome(message as unknown as Welcome);
        else if (message.type === 'snapshot') this.hooks.onSnapshot(message as unknown as Snapshot);
        else if (message.type === 'sim_game_ack') {
          const key = String(message.tick) + ':' + String(message.index);
          const pending = this.awaiting.get(key);
          if (pending) { this.awaiting.delete(key); pending.resolve(); }
        } else if (message.type === 'error') {
          const err = new Error(String(message.message));
          this.hooks.onError(err.message);
          for (const [, pending] of this.awaiting) pending.reject(err);
          this.awaiting.clear();
        }
      } catch (err) { this.hooks.onError(String(err)); }
    });
    ws.addEventListener('close', () => {
      this.socket = null;
      for (const [, pending] of this.awaiting) pending.reject(new Error('Gameplay socket closed'));
      this.awaiting.clear();
    });
    return this.opening;
  }

  private async execute(runId: string, actor: string, room: string,
                        tick: number, actions: ScenarioAction[]): Promise<void> {
    if (!actions.length) return;
    await this.ensureSocket();
    for (const [index, action] of actions.entries()) {
      if (!this.socket || this.socket.readyState !== WebSocket.OPEN)
        throw new Error('Gameplay transport is disconnected');
      const key = String(tick) + ':' + String(index);
      const payload: Record<string, unknown> = {
        type: action.op, sim_run: runId, sim_actor: actor,
        sim_tick: tick, sim_index: index,
      };
      if (action.op === 'join') {
        payload.name = action.name ?? actor;
        payload.room = room;
      } else if (action.op === 'input') {
        payload.seq = action.seq; payload.dx = action.dx; payload.dy = action.dy;
        payload.cloak = action.cloak ?? false;
      }
      await new Promise<void>((resolve, reject) => {
        const timeout = window.setTimeout(() => {
          this.awaiting.delete(key); reject(new Error('Game action ACK timed out at tick ' + tick));
        }, 7000);
        this.awaiting.set(key, {
          resolve: () => { window.clearTimeout(timeout); resolve(); },
          reject: (error) => { window.clearTimeout(timeout); reject(error); },
        });
        this.socket!.send(JSON.stringify(payload));
      });
    }
  }

  sendBatch(runId: string, actor: string, room: string,
            tick: number, actions: ScenarioAction[]): Promise<void> {
    const current = this.queue.then(() => this.execute(runId, actor, room, tick, actions));
    this.queue = current.catch(() => undefined);
    return current;
  }

  close(): void {
    this.socket?.close();
    this.socket = null;
  }
}
