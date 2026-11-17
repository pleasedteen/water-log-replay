/** Source simulator process entry. */
import { createSourceServer } from './server';

const port = Number(process.env.SOURCE_PORT ?? 8080);
const overlap = process.env.SOURCE_OVERLAP === '1';
const failEvery = Number(process.env.SOURCE_FAIL_EVERY ?? 0);

const { server, logSize } = createSourceServer({ overlap, failEvery });
server.listen(port, () => {
  console.log(JSON.stringify({ svc: 'source', event: 'listening', port, logSize, overlap, failEvery }));
});

const shutdown = () => server.close(() => process.exit(0));
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
