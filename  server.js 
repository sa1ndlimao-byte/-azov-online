'use strict';

const http = require('http');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const rooms = new Map();
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const MAX_ROOMS = 100;
const LIMIT = 120;

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store'
    });
    res.end(JSON.stringify({
      status: 'ok',
      service: 'azovskaya-parasha',
      rooms: rooms.size
    }));
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});

const wss = new WebSocketServer({
  server,
  maxPayload: 16384
});

const send = (ws, payload) => {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
};

function profile(x) {
  if (!x || typeof x !== 'object') {
    return { name: 'Игрок', avatar: 0 };
  }

  return {
    name: String(x.name || 'Игрок')
      .replace(/[\x00-\x1f\x7f]/g, '')
      .trim()
      .slice(0, 16) || 'Игрок',
    avatar: Number.isInteger(x.avatar)
      ? Math.max(0, Math.min(13, x.avatar))
      : 0
  };
}

function newCode() {
  for (let i = 0; i < 100; i++) {
    let code = '';
    for (let j = 0; j < 6; j++) {
      code += ALPHABET[crypto.randomInt(ALPHABET.length)];
    }
    if (!rooms.has(code)) return code;
  }
  throw new Error('No room codes available');
}

function leave(ws) {
  const room = ws.room;
  if (!room) return;
  ws.room = null;

  if (rooms.get(room.code) !== room) return;
  rooms.delete(room.code);

  for (const player of room.players) {
    if (player !== ws) {
      player.room = null;
      send(player, { type: 'opponent_left' });
    }
  }
}

function fail(ws, code, message) {
  send(ws, { type: 'error', code, message });
}

wss.on('connection', ws => {
  ws.room = null;
  ws.role = null;
  ws.alive = true;
  ws.rateAt = Date.now();
  ws.rateCount = 0;

  ws.on('pong', () => {
    ws.alive = true;
  });

  send(ws, { type: 'hello', protocol: 1 });

  ws.on('message', raw => {
    const now = Date.now();

    if (now - ws.rateAt >= 1000) {
      ws.rateAt = now;
      ws.rateCount = 0;
    }

    if (++ws.rateCount > LIMIT) {
      ws.close(1008, 'Rate limit');
      return;
    }

    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return fail(ws, 'bad_json', 'Неверный формат сообщения');
    }

    if (!msg || typeof msg !== 'object' || Array.isArray(msg)) {
      return fail(ws, 'bad_message', 'Неверное сообщение');
    }

    if (msg.type === 'create') {
      if (ws.room) {
        return fail(ws, 'already_in_room', 'Сначала выйди из комнаты');
      }
      if (rooms.size >= MAX_ROOMS) {
        return fail(ws, 'server_full', 'Сервер заполнен');
      }
      if (!['killer', 'survivor'].includes(msg.role)) {
        return fail(ws, 'bad_role', 'Выбери роль');
      }

      const code = newCode();
      const seed = crypto.randomInt(1, 2147483000);
      const room = {
        code,
        seed,
        players: [ws],
        started: false
      };

      ws.role = msg.role;
      ws.profile = profile(msg.profile);
      ws.room = room;
      rooms.set(code, room);

      return send(ws, {
        type: 'created',
        code,
        seed,
        role: ws.role
      });
    }

    if (msg.type === 'join') {
      if (ws.room) {
        return fail(ws, 'already_in_room', 'Сначала выйди из комнаты');
      }

      const code = String(msg.code || '').trim().toUpperCase();
      const room = rooms.get(code);

      if (!room) {
        return fail(ws, 'not_found', 'Комната не найдена');
      }
      if (room.players.length >= 2) {
        return fail(ws, 'room_full', 'В комнате уже два игрока');
      }

      const host = room.players[0];
      ws.role = host.role === 'killer' ? 'survivor' : 'killer';
      ws.profile = profile(msg.profile);
      ws.room = room;
      room.players.push(ws);
      room.started = true;

      send(ws, {
        type: 'joined',
        code,
        seed: room.seed,
        role: ws.role
      });

      const players = room.players.map(p => ({
        role: p.role,
        profile: p.profile
      }));

      for (const p of room.players) {
        send(p, {
          type: 'start',
          code,
          seed: room.seed,
          role: p.role,
          players
        });
      }
      return;
    }

    if (msg.type === 'leave') {
      leave(ws);
      return send(ws, { type: 'left' });
    }

    if (msg.type === 'event') {
      const room = ws.room;

      if (!room || !room.started || room.players.length !== 2) {
        return fail(ws, 'no_match', 'Матч не начался');
      }

      if (!['state', 'action', 'world', 'hit', 'result', 'ready'].includes(msg.event)) {
        return fail(ws, 'bad_event', 'Неизвестное действие');
      }

      return send(room.players.find(p => p !== ws), {
        type: 'event',
        event: msg.event,
        data: msg.data ?? null,
        from: ws.role
      });
    }

    fail(ws, 'unknown_type', 'Неизвестная команда');
  });

  ws.on('close', () => leave(ws));
  ws.on('error', () => leave(ws));
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) {
      ws.terminate();
      continue;
    }
    ws.alive = false;
    ws.ping();
  }
}, 25000);

heartbeat.unref();

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Game relay listening on ${PORT}`);
});
