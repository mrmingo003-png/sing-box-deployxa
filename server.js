'use strict';
/**
 * sing-box-deployxa — Node.js runtime wrapper for running sing-box
 * (VLESS + WS) on Deployxa. Same design as the xray wrapper in
 * ~/workspace/deploy-proxy/voroa/ (server.js), with sing-box in place
 * of xray: Deployxa's gateway terminates TLS, so sing-box listens on
 * plain WebSocket on 127.0.0.1 and the wrapper fronts $PORT.
 *
 * What it does
 *  1. Finds the sing-box binary: bundled binary first (several
 *     candidate paths); if it is missing, downloads the pinned
 *     release from GitHub at runtime and extracts it with pure JS
 *     (see below).
 *  2. Writes a sing-box config from env vars (PORT / UUID / WS_PATH)
 *     and starts sing-box listening on 127.0.0.1:<a free local port>.
 *     Inbound is VLESS over plain WebSocket at $WS_PATH; outbound is
 *     "direct" (the platform has real internet egress).
 *  3. Listens on $PORT (the port Deployxa assigns). Plain HTTP
 *     requests (platform health checks) get 200. WebSocket upgrades
 *     are forwarded byte-for-byte to the local sing-box port — the
 *     handshake is never rewritten or re-parsed.
 *
 * Differences vs the xray wrapper
 *  - sing-box releases ship as .tar.gz, not .zip, so the runtime
 *    fallback extracts with pure JS too: zlib gunzip + a hand-written
 *    tar reader (512-byte headers, octal sizes). No external archive
 *    tool is invoked anywhere in this file.
 *  - The runtime download uses Node's built-in fetch only (no curl),
 *    so the wrapper depends on no external command except spawning
 *    sing-box itself.
 *  - Before starting, the wrapper runs `sing-box check -c <config>`
 *    and aborts on a config error instead of failing at first
 *    connection.
 *
 * Env vars
 *  PORT                  port assigned by the platform (default 8080)
 *  UUID                  VLESS user id (required)
 *  WS_PATH               WebSocket path, e.g. /ws-xxxx (default /ws)
 *  SINGBOX_PORT          local sing-box port (default: pick a free one)
 *  SINGBOX_VERSION       release to download if no bundled binary
 *                        (default v1.14.4 — the version verified on
 *                        2026-11-19 against the GitHub releases API)
 *  SINGBOX_DOWNLOAD_URL  full URL of the .tar.gz to download instead
 *                        of the constructed release URL (test-only;
 *                        lets the fallback be exercised against a
 *                        local HTTP server on machines whose Node
 *                        cannot reach GitHub directly)
 *  SINGBOX_HTTP_PROXY    optional http://user:pass@host:port — if set,
 *                        the outbound goes through that HTTP proxy
 *                        instead of direct. Local-test-only knob;
 *                        production uses direct.
 *
 * No third-party dependencies: Node built-ins only.
 */

const { spawn, spawnSync } = require('node:child_process');
const fs = require('node:fs');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const { Readable } = require('node:stream');
const zlib = require('node:zlib');

const log = (...a) => console.log(new Date().toISOString(), '[wrapper]', ...a);
const logErr = (...a) => console.error(new Date().toISOString(), '[wrapper]', ...a);

const PORT = parseInt(process.env.PORT || '8080', 10);
const UUID = process.env.UUID || '';
const WS_PATH = process.env.WS_PATH || '/ws';
const SINGBOX_VERSION = process.env.SINGBOX_VERSION || 'v1.14.4';
const SINGBOX_DOWNLOAD_URL = process.env.SINGBOX_DOWNLOAD_URL || '';
const SINGBOX_HTTP_PROXY = process.env.SINGBOX_HTTP_PROXY || '';
const CONFIG_PATH = path.join(__dirname, 'sing-box-config.json');
const IDLE_TIMEOUT_MS = 10000; // close connections that send nothing

if (!UUID) {
  logErr('FATAL: UUID env var is required');
  process.exit(1);
}
if (!WS_PATH.startsWith('/')) {
  logErr('FATAL: WS_PATH must start with /');
  process.exit(1);
}
if (!Number.isInteger(PORT) || PORT <= 0 || PORT > 65535) {
  logErr('FATAL: invalid PORT:', process.env.PORT);
  process.exit(1);
}

function releaseUrl(version) {
  const v = version.replace(/^v/, '');
  return `https://github.com/SagerNet/sing-box/releases/download/${version}/sing-box-${v}-linux-amd64.tar.gz`;
}

async function isExecutable(p) {
  try {
    await fs.promises.access(p, fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

// --- pure-JS tar.gz extraction (no external archive tool) -------------
// gunzip with zlib, then walk 512-byte tar headers. Handles regular
// files (typeflag '0' / '\0'), skips ustar prefix, PAX ('x'/'g') and
// GNU longname ('L'/'K') metadata entries. Paths in the sing-box
// release tarball are short (< 100 chars), so no longname handling
// beyond skipping is needed.

function parseOctal(buf, off, len) {
  const s = buf.subarray(off, off + len).toString('latin1').replace(/\0/g, '').trim();
  if (!s) return 0;
  const n = parseInt(s, 8);
  return Number.isNaN(n) ? 0 : n;
}

function readCString(buf, off, len) {
  const end = buf.indexOf(0, off);
  const stop = end === -1 || end > off + len ? off + len : end;
  return buf.subarray(off, stop).toString('utf8');
}

function extractTarGzEntry(tgzBuffer, wantedBasename) {
  let tar;
  try {
    tar = zlib.gunzipSync(tgzBuffer);
  } catch (e) {
    throw new Error('gunzip failed: ' + e.message);
  }
  let off = 0;
  while (off + 512 <= tar.length) {
    const header = tar.subarray(off, off + 512);
    // Two zero blocks mark the end of the archive.
    if (header.every((b) => b === 0)) break;
    const name = readCString(header, 0, 100);
    const size = parseOctal(header, 124, 12);
    const typeflag = String.fromCharCode(header[156]);
    const prefix = readCString(header, 345, 155);
    const fullName = prefix ? prefix + '/' + name : name;
    const dataStart = off + 512;
    const dataEnd = dataStart + size;
    if (dataEnd > tar.length) throw new Error(`truncated tar entry '${fullName}'`);
    const isFile = typeflag === '0' || typeflag === '\0';
    const base = fullName.split('/').pop();
    if (isFile && base === wantedBasename) {
      return Buffer.from(tar.subarray(dataStart, dataEnd));
    }
    // Skip this entry's data (rounded up to a 512-byte boundary).
    off = dataStart + Math.ceil(size / 512) * 512;
  }
  throw new Error(`entry '${wantedBasename}' not found in tarball`);
}

async function extractTarGzFile(tgzPath, wantedBasename, destPath) {
  const buf = await fs.promises.readFile(tgzPath);
  const data = extractTarGzEntry(buf, wantedBasename);
  await fs.promises.writeFile(destPath, data);
  await fs.promises.chmod(destPath, 0o755);
  return data.length;
}

// --- sing-box binary ---------------------------------------------------

async function downloadRelease(url, dest) {
  const res = await fetch(url, { redirect: 'follow', signal: AbortSignal.timeout(300000) });
  if (!res.ok) throw new Error(`HTTP ${res.status} downloading ${url}`);
  const ws = fs.createWriteStream(dest);
  await new Promise((resolve, reject) => {
    Readable.fromWeb(res.body).pipe(ws).on('finish', resolve).on('error', reject);
  });
}

// Check the bundled binary in several places: the platform may run from
// a different working directory than the script's directory, or may
// strip the executable bit when it unpacks the upload.
async function findBundledBinary() {
  const candidates = [
    path.join(__dirname, 'sing-box'),
    path.resolve('sing-box'),
    path.join(process.cwd(), 'sing-box'),
  ];
  const seen = new Set();
  for (const p of candidates) {
    if (seen.has(p)) continue;
    seen.add(p);
    let st;
    try {
      st = await fs.promises.stat(p);
    } catch (e) {
      if (e.code !== 'ENOENT') log(`bundled-binary candidate ${p}: ${e.message}`);
      continue;
    }
    if (!st.isFile()) {
      log(`bundled-binary candidate ${p}: not a regular file`);
      continue;
    }
    // The platform may unpack the upload without the exec bit; fix it.
    try {
      await fs.promises.chmod(p, 0o755);
    } catch {
      /* ignore */
    }
    if (await isExecutable(p)) {
      log(`using bundled sing-box binary: ${p} (${st.size} bytes)`);
      return p;
    }
    log(`bundled-binary candidate ${p}: exists (${st.size} bytes) but not executable even after chmod`);
  }
  return null;
}

async function logDirectoryContents() {
  for (const dir of [process.cwd(), __dirname]) {
    try {
      const entries = await fs.promises.readdir(dir, { withFileTypes: true });
      log(
        `contents of ${dir}:`,
        entries.map((e) => (e.isDirectory() ? e.name + '/' : e.name)).join(', ') || '(empty)',
      );
    } catch (e) {
      log(`cannot list ${dir}: ${e.message}`);
    }
  }
}

// Prefer the bundled binary; fall back to a runtime download + pure-JS
// extraction. Never shells out to an external archive tool.
async function ensureSingBox() {
  const bundled = await findBundledBinary();
  if (bundled) return bundled;
  const url = SINGBOX_DOWNLOAD_URL || releaseUrl(SINGBOX_VERSION);
  log('bundled sing-box binary not found, downloading', url);
  const tgzPath = path.join(os.tmpdir(), 'sing-box-linux-amd64.tar.gz');
  try {
    await downloadRelease(url, tgzPath);
  } catch (e) {
    logErr('FATAL: failed to download sing-box release:', e.message);
    process.exit(1);
  }
  const dests = [path.join(__dirname, 'sing-box'), path.join(os.tmpdir(), 'sing-box')];
  let dest = null;
  for (const d of dests) {
    try {
      const n = await extractTarGzFile(tgzPath, 'sing-box', d);
      dest = d;
      log(`extracted sing-box (${n} bytes) to ${d} using built-in zlib`);
      break;
    } catch (e) {
      log(`extraction to ${d} failed: ${e.message}`);
    }
  }
  await fs.promises.unlink(tgzPath).catch(() => {});
  if (!dest) {
    logErr('FATAL: could not extract sing-box from the downloaded release');
    process.exit(1);
  }
  if (!(await isExecutable(dest))) {
    logErr('FATAL: sing-box binary still not executable after download + extraction');
    process.exit(1);
  }
  log('downloaded and extracted sing-box', SINGBOX_VERSION);
  return dest;
}

function httpProxyOutbound(proxyUrl) {
  let u;
  try {
    u = new URL(proxyUrl);
  } catch {
    throw new Error('SINGBOX_HTTP_PROXY is not a valid URL: ' + proxyUrl);
  }
  const server = { type: 'http', tag: 'proxy-out', server: u.hostname, server_port: parseInt(u.port || '8080', 10) };
  if (u.username) {
    server.username = decodeURIComponent(u.username);
    server.password = decodeURIComponent(u.password || '');
  }
  return server;
}

function buildConfig(singBoxPort) {
  return {
    log: { level: 'warn' },
    inbounds: [
      {
        type: 'vless',
        tag: 'vless-in',
        listen: '127.0.0.1',
        listen_port: singBoxPort,
        users: [{ uuid: UUID }],
        transport: { type: 'ws', path: WS_PATH },
      },
    ],
    outbounds: [SINGBOX_HTTP_PROXY ? httpProxyOutbound(SINGBOX_HTTP_PROXY) : { type: 'direct', tag: 'direct' }],
  };
}

function freePort() {
  return new Promise((resolve, reject) => {
    const s = net.createServer();
    s.once('error', reject);
    s.listen(0, '127.0.0.1', () => {
      const p = s.address().port;
      s.close(() => resolve(p));
    });
  });
}

function checkConfig(bin, configPath) {
  const r = spawnSync(bin, ['check', '-c', configPath], { encoding: 'utf8', timeout: 30000 });
  if (r.error) throw new Error('sing-box check failed to run: ' + r.error.message);
  if (r.status !== 0) {
    throw new Error(
      `sing-box check rejected the config (exit ${r.status}): ${(r.stdout || '') + (r.stderr || '')}`.trim(),
    );
  }
  log('sing-box check passed');
}

function startSingBox(bin, configPath) {
  const child = spawn(bin, ['run', '-c', configPath], { stdio: ['ignore', 'pipe', 'pipe'] });
  child.stdout.on('data', (d) => process.stdout.write('[sing-box] ' + d));
  child.stderr.on('data', (d) => process.stderr.write('[sing-box] ' + d));
  child.on('error', (e) => {
    logErr('FATAL: failed to spawn sing-box:', e.message);
    process.exit(1);
  });
  child.on('exit', (code, signal) => {
    logErr(`sing-box exited (code=${code} signal=${signal}); wrapper exiting`);
    process.exit(code ?? 1);
  });
  return child;
}

async function waitForPort(port, timeoutMs = 15000) {
  const start = Date.now();
  for (;;) {
    const ok = await new Promise((resolve) => {
      const s = net.connect(port, '127.0.0.1');
      s.once('connect', () => {
        s.end();
        resolve(true);
      });
      s.once('error', () => resolve(false));
    });
    if (ok) return;
    if (Date.now() - start > timeoutMs) {
      throw new Error(`sing-box did not start listening on 127.0.0.1:${port} within ${timeoutMs}ms`);
    }
    await new Promise((r) => setTimeout(r, 200));
  }
}

// Raw TCP front door on $PORT. Sniffs the request head: a WebSocket
// upgrade is forwarded byte-for-byte to sing-box; anything else (health
// checks, stray probes) gets a plain 200.
function startFront(singBoxPort) {
  const server = net.createServer((socket) => {
    socket.setTimeout(IDLE_TIMEOUT_MS, () => socket.destroy());
    let buf = Buffer.alloc(0);
    let decided = false;

    function onData(chunk) {
      if (decided) return;
      buf = Buffer.concat([buf, chunk]);
      const end = buf.indexOf('\r\n\r\n');
      if (end === -1) {
        if (buf.length > 16384) {
          socket.end(
            'HTTP/1.1 431 Request Header Fields Too Large\r\nConnection: close\r\nContent-Length: 0\r\n\r\n',
          );
        }
        return; // wait for the full header block
      }
      decided = true;
      socket.removeListener('data', onData);
      const headText = buf.subarray(0, end).toString('latin1');
      const isWsUpgrade =
        /^GET\s+\S+\s+HTTP\/1\.\d/i.test(headText) &&
        /connection\s*:[^\r\n]*upgrade/i.test(headText) &&
        /upgrade\s*:\s*websocket/i.test(headText);
      if (isWsUpgrade) {
        socket.setTimeout(0); // long-lived WS connection
        const upstream = net.connect(singBoxPort, '127.0.0.1');
        upstream.once('connect', () => {
          upstream.write(buf); // original request bytes, untouched
          socket.pipe(upstream);
          upstream.pipe(socket);
        });
        upstream.on('error', (e) => {
          log('upstream error:', e.message);
          socket.destroy();
        });
        socket.on('error', () => upstream.destroy());
        upstream.on('close', () => socket.destroy());
        socket.on('close', () => upstream.destroy());
      } else {
        socket.end('HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok');
      }
    }

    socket.on('data', onData);
    socket.on('error', () => {});
  });

  server.on('error', (e) => {
    logErr('FATAL: front server error:', e.message);
    process.exit(1);
  });
  server.listen(PORT, '0.0.0.0', () => {
    log(`listening on 0.0.0.0:${PORT} (WS ${WS_PATH} -> 127.0.0.1:${singBoxPort}, other HTTP -> 200)`);
  });
  return server;
}

async function main() {
  log(`starting (node=${process.version} PORT=${PORT} WS_PATH=${WS_PATH} sing-box=${SINGBOX_VERSION})`);
  log('cwd:', process.cwd());
  log('__dirname:', __dirname);
  await logDirectoryContents();
  const bin = await ensureSingBox();
  const singBoxPort = process.env.SINGBOX_PORT
    ? parseInt(process.env.SINGBOX_PORT, 10)
    : await freePort();
  if (!Number.isInteger(singBoxPort) || singBoxPort <= 0 || singBoxPort > 65535) {
    logErr('FATAL: invalid SINGBOX_PORT:', process.env.SINGBOX_PORT);
    process.exit(1);
  }
  const config = buildConfig(singBoxPort);
  let configPath = CONFIG_PATH;
  try {
    await fs.promises.writeFile(configPath, JSON.stringify(config, null, 2));
  } catch {
    configPath = path.join(os.tmpdir(), 'sing-box-config.json');
    await fs.promises.writeFile(configPath, JSON.stringify(config, null, 2));
  }
  log('sing-box config written to', configPath);
  checkConfig(bin, configPath);
  const child = startSingBox(bin, configPath);
  const shutdown = () => {
    try {
      child.kill('SIGTERM');
    } catch {
      /* ignore */
    }
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
  await waitForPort(singBoxPort);
  log('sing-box is listening on 127.0.0.1:' + singBoxPort);
  startFront(singBoxPort);
}

main().catch((e) => {
  logErr('FATAL:', e.message);
  process.exit(1);
});
