const http = require('http');
const net = require('net');
const { WebSocketServer, createWebSocketStream } = require('ws');

// 1. 读取环境变量与配置
const PORT = process.env.PORT || 8000;
const UUID = (process.env.UUID || '').replace(/-/g, '').toLowerCase();

// 2. 先创建 HTTP 服务（提供健康检查，防止平台杀死进程）
const server = http.createServer((req, res) => {
    if (req.url === '/' || req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Hostless VLESS Node is Running!');
    } else {
        res.writeHead(404);
        res.end();
    }
});

// 3. 创建 WebSocket 服务
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
    ws.once('message', (chunk) => {
        try {
            const buffer = Buffer.from(chunk);
            if (buffer.length < 18) return ws.close();

            const version = buffer[0];
            const reqUUID = buffer.subarray(1, 17).toString('hex').toLowerCase();

            // 校验 UUID
            if (reqUUID !== UUID) {
                return ws.close();
            }

            const optLen = buffer[17];
            let cursor = 18 + optLen;

            const command = buffer[cursor]; // 1: TCP
            cursor += 1;

            if (command !== 1) return ws.close();

            const remotePort = buffer.readUInt16BE(cursor);
            cursor += 2;

            const atyp = buffer[cursor];
            cursor += 1;

            let remoteHost = '';
            if (atyp === 1) { // IPv4
                remoteHost = buffer.subarray(cursor, cursor + 4).join('.');
                cursor += 4;
            } else if (atyp === 2) { // Domain
                const domainLen = buffer[cursor];
                cursor += 1;
                remoteHost = buffer.subarray(cursor, cursor + domainLen).toString('utf-8');
                cursor += domainLen;
            } else if (atyp === 3) { // IPv6
                const ipv6Buf = buffer.subarray(cursor, cursor + 16);
                const ipv6Arr = [];
                for (let i = 0; i < 16; i += 2) {
                    ipv6Arr.push(ipv6Buf.readUInt16BE(i).toString(16));
                }
                remoteHost = ipv6Arr.join(':');
                cursor += 16;
            } else {
                return ws.close();
            }

            // 返回 VLESS 响应头
            ws.send(Buffer.from([version, 0]));

            // 建立双向 TCP 管道转发
            const duplex = createWebSocketStream(ws);
            const socket = net.connect({ host: remoteHost, port: remotePort }, () => {
                const payload = buffer.subarray(cursor);
                if (payload.length > 0) socket.write(payload);
                duplex.pipe(socket);
                socket.pipe(duplex);
            });

            socket.on('error', () => ws.close());
            duplex.on('error', () => socket.destroy());
            ws.on('close', () => socket.destroy());
        } catch (e) {
            ws.close();
        }
    });
});

// 4.【必须在最后】开启端口监听
server.listen(PORT, '0.0.0.0', () => {
    console.log(`VLESS Node 正在运行于端口: ${PORT}`);
});
