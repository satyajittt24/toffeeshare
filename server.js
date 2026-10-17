import express from 'express';
import http from 'http';
import { WebSocketServer } from 'ws';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const rooms = new Map();
setInterval(() => {
  const now = Date.now();
  for (const [id, room] of rooms.entries()) {
    if (now - room.createdAt > 1000 * 60 * 30) {
      try { room.sender?.close(); } catch {}
      try { room.receiver?.close(); } catch {}
      rooms.delete(id);
    }
  }
}, 60 * 1000);
function send(ws, type, payload={}) {
  if (ws.readyState === 1) ws.send(JSON.stringify({ type, ...payload }));
}
wss.on('connection', (ws) => {
  let currentRoom = null;
  let role = null;
  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'create-room') {
        const roomId = msg.roomId || Math.random().toString(36).substring(2,8).toUpperCase();
        rooms.set(roomId, { sender: ws, receiver: null, meta: msg.meta, createdAt: Date.now() });
        currentRoom = roomId; role = 'sender';
        send(ws, 'room-created', { roomId });
      }
      if (msg.type === 'join-room') {
        const room = rooms.get(msg.roomId);
        if (!room) { send(ws, 'error', { message: 'Room not found or expired.' }); return; }
        room.receiver = ws;
        currentRoom = msg.roomId; role = 'receiver';
        send(room.sender, 'peer-joined', { meta: room.meta });
        send(ws, 'room-joined', { meta: room.meta });
      }
      if (msg.type === 'signal') {
        const room = rooms.get(msg.roomId);
        if (!room) return;
        const target = role === 'sender' ? room.receiver : room.sender;
        if (target) send(target, 'signal', { data: msg.data });
      }
    } catch (e) { console.error(e); }
  });
  ws.on('close', () => {
    if (!currentRoom) return;
    const room = rooms.get(currentRoom);
    if (!room) return;
    if (role === 'sender') {
      if (room.receiver) send(room.receiver, 'peer-left', { reason: 'Sender closed' });
      rooms.delete(currentRoom);
    } else {
      if (room.sender) send(room.sender, 'peer-left', { reason: 'Receiver left' });
      room.receiver = null;
    }
  });
});
app.get('/api/stats', (req, res) => res.json({ activeRooms: rooms.size }));
app.get('/health', (req, res) => res.send('ok'));
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Toffee signal on '+PORT));
