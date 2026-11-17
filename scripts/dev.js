/**
 * `npm run dev` - launches both local services with prefixed, interleaved logs.
 * No browser or external database required; everything is localhost + SQLite.
 */
const { spawn } = require('node:child_process');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const args = ['--import', 'tsx'];

const prefix = (name, color) => {
  const reset = '\x1b[0m';
  return (buf) => {
    const text = buf.toString().replace(/\n$/, '');
    if (!text) return;
    for (const line of text.split('\n')) process.stdout.write(`${color}[${name}]${reset} ${line}\n`);
  };
};

const source = spawn(process.execPath, [...args, 'src/source/main.ts'], {
  cwd: root,
  env: { ...process.env, SOURCE_PORT: process.env.SOURCE_PORT ?? '8080' },
  stdio: ['ignore', 'pipe', 'pipe'],
});
const sync = spawn(process.execPath, [...args, 'src/sync/main.ts'], {
  cwd: root,
  env: {
    ...process.env,
    SYNC_PORT: process.env.SYNC_PORT ?? '8081',
    SOURCE_URL: process.env.SOURCE_URL ?? 'http://127.0.0.1:8080',
    SYNC_DB: process.env.SYNC_DB ?? path.join(root, 'data', 'sync.db'),
  },
  stdio: ['ignore', 'pipe', 'pipe'],
});

source.stdout.on('data', prefix('source', '\x1b[36m'));
source.stderr.on('data', prefix('source', '\x1b[31m'));
sync.stdout.on('data', prefix(' sync ', '\x1b[35m'));
sync.stderr.on('data', prefix(' sync ', '\x1b[31m'));

const stop = () => {
  source.kill('SIGTERM');
  sync.kill('SIGTERM');
};
process.on('SIGINT', () => {
  stop();
  process.exit(0);
});
process.on('SIGTERM', () => {
  stop();
  process.exit(0);
});
source.on('exit', (code) => {
  console.log(`[dev] source exited (${code})`);
  stop();
});
sync.on('exit', (code) => {
  console.log(`[dev] sync exited (${code})`);
  stop();
});
