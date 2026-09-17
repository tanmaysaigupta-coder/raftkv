/**
 * Minimal CLI client. Talks to the cluster coordinator's HTTP API (which
 * already knows how to find the current leader), so this file stays tiny.
 *
 *   npm run cli -- set foo bar
 *   npm run cli -- get foo
 *   npm run cli -- delete foo
 */

const DASHBOARD = process.env.DASHBOARD_URL ?? 'http://localhost:4000';

async function main() {
  const [cmd, key, value] = process.argv.slice(2);

  if (cmd === 'set') {
    if (!key || value === undefined) throw new Error('usage: cli set <key> <value>');
    const res = await fetch(`${DASHBOARD}/api/write`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ key, value }),
    });
    console.log(await res.json());
  } else if (cmd === 'get') {
    if (!key) throw new Error('usage: cli get <key>');
    const res = await fetch(`${DASHBOARD}/api/read?key=${encodeURIComponent(key)}`);
    console.log(await res.json());
  } else if (cmd === 'delete') {
    if (!key) throw new Error('usage: cli delete <key>');
    const res = await fetch(`${DASHBOARD}/api/write`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ key, _delete: true }),
    });
    console.log(await res.json());
  } else {
    console.log('usage: cli <set|get|delete> <key> [value]');
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
