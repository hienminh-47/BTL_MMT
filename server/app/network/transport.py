"""Student-editable baseline WebSocket game transport.

The simulator control channel (/sim/ws) never changes game state.
Scripted browser actions must traverse this *ordinary* application transport.
Teams replacing WebSocket must implement equivalent scenario action/ack support
in their own data-plane adapter, keeping the published input/output contract.
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING
from aiohttp import WSMsgType, web
from .protocol import decode_message, encode_message, parse_input
from .delta import DeltaEncoder   # encoder delta cho giao thức v2
from ..game.service import GameService

if TYPE_CHECKING:
    from ..simulator.transport import SimulatorTransport, SimRun

logger = logging.getLogger(__name__)


class GameTransport:
    # 1. Khởi tạo lớp quản lý mạng
    def __init__(self, service: GameService):
        self.service = service
        self.simulator: SimulatorTransport | None = None
        self.sockets: dict[str, web.WebSocketResponse] = {}
        # encoder delta theo player_id (chỉ có với client gửi proto=2 trong JOIN)
        self.encoders: dict[str, DeltaEncoder] = {}
        # bộ đếm byte phía server, để đối chiếu với số đo của bot (xem /metrics)
        self.bytes_out = 0
        self.msgs_out = 0

    # 2. Đăng ký định tuyến HTTP/WebSocket vào aiohttp web application
    def install_routes(self, app: web.Application) -> None:
        app.router.add_get('/ws', self.websocket)
        app.router.add_get('/metrics', self.metrics)  

    async def metrics(self, request: web.Request) -> web.Response:
        """Số byte snapshot server đã gửi (payload UTF-8, chưa tính header WS/TCP)."""
        return web.json_response({'bytes_out': self.bytes_out, 'msgs_out': self.msgs_out,
                                  'clients': len(self.sockets)})

    # 3. Xử lý toàn bộ vòng đời kết nối WebSocket của một client (Handshake, nhận tin nhắn, xử lý logic, và dọn dẹp khi ngắt kết nối)
    async def websocket(self, request: web.Request) -> web.WebSocketResponse:
        # Step 1: Handshake
        ws = web.WebSocketResponse(heartbeat=30, max_msg_size=8192)
        await ws.prepare(request)
        
        player_id: str | None = None
        sim_run: str | None = None
        sim_actor: str | None = None
        proto = 1   # 1 = baseline (snapshot đầy đủ), 2 = delta
        
        try:
            # Vòng lặp nhận dữ liệu
            async for packet in ws:
                if packet.type != WSMsgType.TEXT:
                    continue
                data = decode_message(packet.data)
                if data is None:
                    await ws.send_json({'type': 'error', 'message': 'Invalid message JSON'})
                    continue
                kind = data.get('type')
                try:
                    ctx = None
                    event = None
                    tick = index = None
                    # Xử lý Chế độ Mô phỏng Kiểm thử
                    if 'sim_run' in data:
                        if not self.simulator:
                            raise ValueError('Scenario adapter disabled')
                        token, actor = data.get('sim_run'), data.get('sim_actor')
                        if not isinstance(token, str) or not isinstance(actor, str):
                            raise ValueError('Missing simulated run and actor')
                        if sim_run is not None and (token != sim_run or actor != sim_actor):
                            raise ValueError('Scenario actor cannot change on a connection')
                        ctx = self.simulator.authorize(token, actor)
                        tick, index = data.get('sim_tick'), data.get('sim_index')
                        if kind == 'join':
                            event = {'tick': tick, 'op': 'join', 'player': actor,
                                     'name': str(data.get('name', actor))}
                        elif kind == 'input':
                            action = parse_input(data)
                            if action is None:
                                raise ValueError('Invalid input action')
                            seq, dx, dy, cloak = action
                            event = {'tick': tick, 'op': 'input', 'player': actor,
                                     'seq': seq, 'dx': dx, 'dy': dy, 'cloak': cloak}
                        elif kind in ('leave', 'rejoin'):
                            event = {'tick': tick, 'op': kind, 'player': actor}
                        else:
                            raise ValueError('Unsupported scenario operation')
                        repeated = ctx.validate_data_action(actor, tick, index, event)
                        if repeated:
                            await ws.send_json({'type': 'sim_game_ack', 'tick': tick,
                                                'index': index, 'duplicate': True})
                            continue

                    if kind == 'join' and player_id is None:
                        if ctx:
                            player_id = ctx.player_id(actor)
                            welcome = self.service.sim_join(
                                actor, ctx.scenario['room'], token, event['name'])
                            sim_run, sim_actor = token, actor
                        else:
                            #UPDATE:
                            session_token = data.get('session_token')
                            room_name = str(data.get('room', 'main'))

                            if session_token and session_token in self.service.sessions:
                                has_active_tab = any(
                                    (existing_ws := self.sockets.get(pid)) is not None and not existing_ws.closed
                                    for pid in self.service.sessions[session_token].values()
                                )
                                if has_active_tab:
                                    await ws.send_json({
                                        'type': 'error',
                                        'message': 'Duplicate tab detected. Another tab in this browser session is already in a room.'
                                    })
                                    await ws.close()
                                    return ws
                            
                            welcome = self.service.join(
                                str(data.get('name', ''))[:18],
                                room_name,
                                session_token=session_token
                            )
                            player_id = welcome['id']
                            # client xin giao thức v2 trong JOIN ("proto": 2).
                            # Scenario mode (nhánh ctx ở trên) luôn dùng v1 -> kết quả kiểm thử không đổi.
                            proto = 2 if data.get('proto') == 2 else 1
                            
                            
                        self.sockets[player_id] = ws
                        await ws.send_json(welcome)
                        snapshot = self.service.snapshot_for(player_id)
                        if snapshot is not None:
                            if proto == 2:
                                # v2: gói đầu tiên luôn là bản FULL (seq=1)
                                enc = self.encoders[player_id] = DeltaEncoder()
                                await ws.send_str(encode_message(enc.next(snapshot)))
                            else:
                                await ws.send_json(snapshot)
                    
                    # Xử lý các tin nhắn ứng dụng khác (input, rejoin, leave, ping)        
                    elif kind == 'input' and player_id is not None:
                        action = parse_input(data)
                        if action is None:
                            raise ValueError('Invalid movement or ability input')
                        self.service.apply_input(player_id, *action)
                        
                    elif kind == 'rejoin' and player_id is not None:
                        if not self.service.rejoin(player_id):
                            raise ValueError('Rejoin unavailable')
                        # ván mới: ép gửi bản FULL ở lần publish kế tiếp
                        if player_id in self.encoders:
                            self.encoders[player_id].request_full()
                        
                    elif kind == 'leave' and player_id is not None and ctx:
                        self.service.leave(player_id)
                        
                    elif kind == 'resync' and player_id is not None:
                        # client phát hiện hụt seq -> xin bản FULL ở lần publish kế tiếp
                        if player_id in self.encoders:
                            self.encoders[player_id].request_full()
                        continue

                    elif kind == 'ping':
                        nonce = data.get('nonce')
                        if isinstance(nonce, (float, int)) and not isinstance(nonce, bool):
                            await ws.send_json({'type': 'pong', 'nonce': nonce})
                        continue
                    
                    else:
                        raise ValueError('Join first or unknown message')
                    
                    if ctx and event is not None:
                        ctx.confirm_data_action(actor, tick, index)
                        await ws.send_json({'type': 'sim_game_ack', 'tick': tick,
                                            'index': index, 'duplicate': False})
                except (ValueError, KeyError, TypeError) as exc:
                    await ws.send_json({'type': 'error', 'message': str(exc)})
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        
        finally:
            if player_id:
                # chỉ xoá encoder nếu socket đăng ký cho player_id vẫn là socket NÀY (hoặc đã mất).
                # Nếu kết nối mới (reload/rejoin cùng session_token) đã thay thế, giữ encoder mới.
                if self.sockets.get(player_id) in (ws, None):
                    self.encoders.pop(player_id, None)
                # dòng dưới xoá socket theo player_id mà không kiểm tra socket nào;
                # nếu kết nối cũ đóng trễ sau khi kết nối mới đã join thì sẽ xoá nhầm kết nối mới.
                self.sockets.pop(player_id, None)
                self.service.leave(player_id)
        return ws

    # 4. Broadcast trạng thái mới nhất của các phòng chơi tới tất cả các client đang kết nối theo từng tick
    async def publish(self, snapshots: dict[str, dict]) -> None:
        jobs = []
        for player_id, ws in list(self.sockets.items()):
            room = self.service.player_rooms.get(player_id)
            if room and room in snapshots and not ws.closed:
                snapshot = self.service.snapshot_for(player_id)
                if snapshot is not None:
                    # v2: gửi full/delta; v1: giữ nguyên baseline (snapshot đầy đủ)
                    enc = self.encoders.get(player_id)
                    payload = encode_message(enc.next(snapshot) if enc else snapshot)
                    self.bytes_out += len(payload.encode('utf-8'))   # đếm byte
                    self.msgs_out += 1
                    jobs.append(ws.send_str(payload))
        if jobs:
            results = await asyncio.gather(*jobs, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.debug('Snapshot write error: %r', result)

    # 5. Tắt server an toàn
    async def close(self) -> None:
        await asyncio.gather(*(ws.close() for ws in list(self.sockets.values())),
                             return_exceptions=True)
