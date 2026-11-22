import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { setImmediate as settleRequests } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';

let server;
let monitoring;
const originalFetch = globalThis.fetch;
const originalDocument = globalThis.document;
const originalWindow = globalThis.window;

const stats = {
  database: { nodes: 1, edges: 2 },
  server: { uptime_seconds: 60, requests: 0, errors: 0, active: 0 },
};

before(async () => {
  const root = fileURLToPath(new URL('..', import.meta.url));
  server = await createServer({
    root,
    configFile: `${root}/vite.config.ts`,
    appType: 'custom',
    logLevel: 'error',
    server: { middlewareMode: true },
  });
  globalThis.window = { location: { protocol: 'http:', hostname: 'localhost' } };
  globalThis.document = Object.assign(new EventTarget(), { hidden: false });
  monitoring = await server.ssrLoadModule('/src/store/monitoringStore.ts');
});

after(async () => {
  globalThis.fetch = originalFetch;
  globalThis.document = originalDocument;
  globalThis.window = originalWindow;
  await server?.close();
});

function fixture(t, { healthPending = false, ignoreAbort = false, statusOK = true, healthOK = true } = {}) {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval'] });
  const health = [];
  const state = monitoring.useMonitoringStore;
  state.setState({ stats: null, connected: false, error: '', metrics: null, history: [], updatedAt: null });
  globalThis.fetch = async (url, options = {}) => {
    if (url === '/auth/me') return Response.json({ username: 'reader' });
    if (url === '/status') return Response.json(statusOK ? stats : {}, { status: statusOK ? 200 : 503 });
    if (url === '/health') {
      if (!healthPending) return Response.json({}, { status: healthOK ? 200 : 503 });
      return new Promise((resolve, reject) => {
        health.push({ signal: options.signal, resolve });
        if (!ignoreAbort) {
          options.signal.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), { once: true });
        }
      });
    }
    throw new Error(`Unexpected request ${url}`);
  };

  const stop = monitoring.acquireMonitoring(false);
  let active = true;
  const release = () => {
    if (active) {
      active = false;
      stop();
    }
  };
  t.after(async () => {
    release();
    for (const pending of health) pending.resolve(Response.json({}));
    await settleRequests();
  });
  return { health, state, release };
}

test('pending health remains owned until timeout and the next poll can recover', async t => {
  const { health, state } = fixture(t, { healthPending: true });
  await settleRequests();
  assert.equal(state.getState().connected, true, 'successful status remains usable');
  assert.equal(state.getState().refreshing, true, 'health is still part of this poll');
  await monitoring.refreshMonitoring();
  assert.equal(health.length, 1, 'a second refresh must not overlap pending health');

  t.mock.timers.tick(10000);
  await settleRequests();
  assert.equal(health[0].signal.aborted, true, 'the existing deadline covers health');
  assert.equal(state.getState().refreshing, false, 'abort settles the polling owner');
  assert.equal(state.getState().connected, true, 'health timeout preserves successful status');

  const next = monitoring.refreshMonitoring();
  await settleRequests();
  assert.equal(health.length, 2, 'polling resumes after cancellation');
  health[1].resolve(Response.json({}));
  await next;
  assert.equal(state.getState().refreshing, false);
});

test('unmount cancels health and a late response cannot change the next generation', async t => {
  const { health, state, release } = fixture(t, { healthPending: true, ignoreAbort: true });
  await settleRequests();
  release();
  const reacquire = monitoring.acquireMonitoring(false);
  t.after(reacquire);
  await settleRequests();
  assert.equal(health[0].signal.aborted, true);

  state.setState({ connected: false, updatedAt: null });
  health[0].resolve(Response.json({}));
  await settleRequests();
  assert.equal(state.getState().connected, false, 'old generation cannot publish health');
  health[1].resolve(Response.json({}));
  await settleRequests();
  assert.equal(state.getState().connected, true, 'current generation can publish health');
});

for (const healthOK of [true, false]) {
  test(`status failure retains health availability=${healthOK}`, async t => {
    const { state } = fixture(t, { statusOK: false, healthOK });
    await settleRequests();
    assert.equal(state.getState().connected, healthOK);
    assert.equal(state.getState().stats, null);
    assert.match(state.getState().error, /503/);
    assert.equal(state.getState().refreshing, false);
  });
}
