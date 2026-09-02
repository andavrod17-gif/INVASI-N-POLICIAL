const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = 8000;
const DIR = __dirname;

const MIME = {
    '.html': 'text/html',
    '.js': 'text/javascript',
    '.css': 'text/css',
    '.json': 'application/json',
    '.glb': 'model/gltf-binary',
    '.gltf': 'model/gltf+json',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
};

const server = http.createServer((req, res) => {
    let filePath = path.join(DIR, req.url === '/' ? 'index.html' : req.url);
    const ext = path.extname(filePath);
    const contentType = MIME[ext] || 'application/octet-stream';

    fs.readFile(filePath, (err, data) => {
        if (err) {
            res.writeHead(404);
            res.end('Not found');
            return;
        }
        res.writeHead(200, { 'Content-Type': contentType, 'Access-Control-Allow-Origin': '*' });
        res.end(data);
    });
});

// ---------------- Sistema multijugador en línea (WebSocket) ----------------
// Cada sala admite hasta MAX_PLAYERS jugadores. Cada jugador simula su propio
// coche y envía su estado; el servidor lo difunde a TODOS los demás del cuarto.
// Roles: POLICÍA vs FUGITIVO (se reparten al unirse).
const wss = new WebSocketServer({ server, path: '/ws' });

const MAX_PLAYERS = 4;           // jugadores por sala
// rooms: code -> { id, players: [ {ws, slot, role, nick} | null, ... ] }
const rooms = new Map();

function genCode() {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    let c;
    do {
        c = '';
        for (let i = 0; i < 4; i++) c += chars[Math.floor(Math.random() * chars.length)];
    } while (rooms.has(c));
    return c;
}

function send(ws, obj) {
    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

// Difunde a todas las demás personas del cuarto (opcionalmente a todas si includeSelf)
function broadcast(room, msg, exceptSlot) {
    room.players.forEach(p => {
        if (p && p.slot !== exceptSlot) send(p.ws, msg);
    });
}

// Lista resumida de jugadores (para "rooms_list" y "peer_joined")
function playerList(room, exceptSlot) {
    const list = [];
    room.players.forEach(p => {
        if (p && p.slot !== exceptSlot) list.push({ slot: p.slot, role: p.role, nick: p.nick || 'Jugador' });
    });
    return list;
}

// Roles disponibles: repartir de modo balanceado (máximo pivote de cada rol)
function assignRole(room) {
    let cops = room.players.filter(p => p && p.role === 'policia').length;
    let fugis = room.players.filter(p => p && p.role === 'fugitivo').length;
    // En salas pares, mitades iguales; de lo contrario el siguiente entra al rol con menos gente.
    const target = Math.round(room.players.length / 2);
    if (cops < target) return 'policia';
    return 'fugitivo';
}

wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.room = null;
    ws.slot = -1;
    ws.nick = 'Jugador';

    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw.toString()); } catch (e) { return; }

        switch (msg.type) {
            case 'create': {
                if (ws.room) return;
                const code = genCode();
                const desiredRole = (msg.role === 'policia' ? 'policia' : 'fugitivo');
                const room = { id: code, players: [] };
                rooms.set(code, room);
                ws.room = room;
                ws.slot = 0;
                ws.nick = msg.nick || 'Jugador';
                room.players[0] = { ws, slot: 0, role: desiredRole, nick: ws.nick };
                send(ws, { type: 'created', code, slot: 0, role: desiredRole, maxPlayers: MAX_PLAYERS });
                break;
            }
            case 'join': {
                if (ws.room) return;
                const room = rooms.get(String(msg.code || '').toUpperCase());
                if (!room) { send(ws, { type: 'error', message: 'Sala no encontrada' }); break; }
                if (room.players.length >= MAX_PLAYERS) { send(ws, { type: 'error', message: 'Sala llena (' + MAX_PLAYERS + ' jugadores)' }); break; }
                const slot = room.players.length;
                ws.room = room;
                ws.slot = slot;
                ws.nick = msg.nick || 'Jugador';
                const role = assignRole(room);
                room.players[slot] = { ws, slot, role, nick: ws.nick };
                send(ws, { type: 'joined', code: room.id, slot, role, players: playerList(room, slot), maxPlayers: MAX_PLAYERS });
                // Avisar a los ya presentes de la llegada del nuevo jugador
                broadcast(room, { type: 'peer_joined', player: { slot, role, nick: ws.nick }, players: playerList(room, slot) }, slot);
                break;
            }
            case 'nick': {
                ws.nick = msg.nick || 'Jugador';
                const room = ws.room;
                if (room && room.players[ws.slot]) room.players[ws.slot].nick = ws.nick;
                if (room) broadcast(room, { type: 'nick_changed', slot: ws.slot, nick: ws.nick }, ws.slot);
                break;
            }
            case 'chat': {
                const room = ws.room;
                if (room) {
                    const text = String(msg.text || '').slice(0, 200);
                    broadcast(room, { type: 'chat', slot: ws.slot, nick: ws.nick, text }, ws.slot);
                }
                break;
            }
            default: {
                // Cualquier otro mensaje (state, event, start, result...) se difunde a los demás
                const room = ws.room;
                if (room) {
                    if (msg.type === 'state') {
                        broadcast(room, { type: 'opponent_state', slot: ws.slot, nick: ws.nick, state: msg.state }, ws.slot);
                    } else {
                        broadcast(room, { type: msg.type, ...msg, slot: ws.slot, nick: ws.nick }, ws.slot);
                    }
                }
            }
        }
    });

    ws.on('close', () => {
        if (ws.room) {
            const room = ws.room;
            room.players[ws.slot] = null;
            broadcast(room, { type: 'peer_left', slot: ws.slot, players: playerList(room, ws.slot) }, ws.slot);
            // Compactar y liberar sala vacía
            room.players = room.players.filter(p => p != null);
            if (room.players.length === 0) rooms.delete(room.id);
            ws.room = null;
        }
    });
});

// Heartbeat para limpiar conexiones muertas
setInterval(() => {
    wss.clients.forEach((ws) => {
        if (!ws.isAlive) { ws.terminate(); return; }
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

server.listen(PORT, '0.0.0.0', () => {
    console.log('Servidor activo en http://localhost:' + PORT);
    console.log('(Modo multijugador en línea habilitado: salas de hasta ' + MAX_PLAYERS + ' jugadores)');
});