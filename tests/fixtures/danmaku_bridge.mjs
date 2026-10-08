import { createInterface } from 'node:readline';
for await (const line of createInterface({ input: process.stdin, crlfDelay: Infinity })) {
  const request = JSON.parse(line);
  if (request.action === 'crash') process.exit(7);
  if (request.action === 'invalid') { process.stdout.write('not-json\n'); continue; }
  if (request.action === 'error') { process.stdout.write(JSON.stringify({ ok: false, error: 'source failed' }) + '\n'); continue; }
  if (request.data?.delay) await new Promise(resolve => setTimeout(resolve, request.data.delay));
  process.stdout.write(JSON.stringify({ ok: true, result: { pid: process.pid, source: request.source } }) + '\n');
}
