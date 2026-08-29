const http = require('http');
const net = require('net');
const { WebSocketServer } = require('ws');

// 获取平台注入的端口，若无则默认 8000
const port = process.env.PORT || 8000;

// 监听 0.0.0.0 保证外部网络能够通透
server.listen(port, '0.0.0.0', () => {
    console.log(`VLESS Node 正在运行于端口: ${port}`);
});
const RAW_UUID = process.env.UUID || 'd3b07384-d113-424a-a726-02232d00748b';
const UUID = RAW_UUID.replaceAll('-', '');

const server = http.createServer((req, res) => {
  // 健康检查接口，供 PaaS 探测状态
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Hostless VLESS Node is Running!');
  } else {
    res.writeHead(404);
    res.end();
  }
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  let tcpSocket = null;

  ws.on('message', (chunk) => {
    // 若 TCP 连接已建立，后续数据直接透明转发
    if (tcpSocket) {
      tcpSocket.write(chunk);
      return;
    }

    // 校验 VLESS 请求头部长度（最少 24 字节）
    if (chunk.length < 24) return ws.close();

    const version = chunk[0];
    const clientUUID = chunk.slice(1, 17).toString('hex');

    // UUID 鉴权
    if (clientUUID !== UUID) {
      console.log('非法 UUID 访问');
      return ws.close();
    }

    const addonsLen = chunk[17];
    let cursor = 18 + addonsLen;

    const command = chunk[cursor]; // 1: TCP, 2: UDP
    cursor += 1;

    const port = chunk.readUInt16BE(cursor);
    cursor += 2;

    const addrType = chunk[cursor]; // 1: IPv4, 2: Domain, 3: IPv6
    cursor += 1;

    let host = '';
    if (addrType === 1) {
      host = chunk.slice(cursor, cursor + 4).join('.');
      cursor += 4;
    } else if (addrType === 2) {
      const domainLen = chunk[cursor];
      cursor += 1;
      host = chunk.slice(cursor, cursor + domainLen).toString('utf-8');
      cursor += domainLen;
    } else if (addrType === 3) {
      host = chunk.slice(cursor, cursor + 16).reduce((acc, val, i) => {
        return acc + (i % 2 === 0 && i > 0 ? ':' : '') + val.toString(16).padStart(2, '0');
      }, '');
      cursor += 16;
    } else {
      return ws.close();
    }

    const initialPayload = chunk.slice(cursor);

    // 返回 VLESS 响应头: [version, addonsLen]
    ws.send(Buffer.from([version, 0]));

    if (command === 1) {
      // 建立出站 TCP 连接
      tcpSocket = net.connect({ host, port }, () => {
        if (initialPayload.length > 0) {
          tcpSocket.write(initialPayload);
        }
      });

      tcpSocket.on('data', (data) => {
        if (ws.readyState === ws.OPEN) {
          ws.send(data);
        }
      });

      tcpSocket.on('error', () => ws.close());
      tcpSocket.on('close', () => ws.close());
    } else {
      // 暂不支持 UDP/Mux
      ws.close();
    }
  });

  ws.on('close', () => {
    if (tcpSocket) tcpSocket.destroy();
  });

  ws.on('error', () => {
    if (tcpSocket) tcpSocket.destroy();
  });
});

server.listen(PORT, () => {
  console.log(`VLESS Node 正在运行于端口: ${PORT}`);
});
