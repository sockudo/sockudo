import { createServer } from 'node:http';
import { createReadStream, statSync } from 'node:fs';
import { extname, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('.', import.meta.url));
const port = Number(process.env.ABLY_CHAT_DEMO_PORT ?? '4173');

const contentTypes = new Map([
  ['.html', 'text/html; charset=utf-8'],
  ['.js', 'text/javascript; charset=utf-8'],
  ['.map', 'application/json; charset=utf-8'],
  ['.css', 'text/css; charset=utf-8'],
]);

function resolvePath(url) {
  const pathname = new URL(url, 'http://127.0.0.1').pathname;
  const relative = pathname === '/' ? 'pubsub-chat-demo.html' : pathname.slice(1);
  const candidate = normalize(join(root, relative));
  if (!candidate.startsWith(root)) {
    return undefined;
  }
  return candidate;
}

const server = createServer((request, response) => {
  const path = resolvePath(request.url ?? '/');
  if (!path) {
    response.writeHead(403).end('forbidden');
    return;
  }

  try {
    const stat = statSync(path);
    if (!stat.isFile()) {
      response.writeHead(404).end('not found');
      return;
    }
    response.writeHead(200, {
      'content-type': contentTypes.get(extname(path)) ?? 'application/octet-stream',
      'content-length': stat.size,
      'cache-control': 'no-store',
    });
    createReadStream(path).pipe(response);
  } catch {
    response.writeHead(404).end('not found');
  }
});

server.listen(port, '127.0.0.1', () => {
  console.log(`Sockudo Ably Pub/Sub chat demo: http://127.0.0.1:${port}/`);
});
