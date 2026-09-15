// SPDX-License-Identifier: MPL-2.0
const assert = require('node:assert/strict');
const {once} = require('node:events');
const http = require('node:http');
const {createRequire} = require('node:module');
const test = require('node:test');
const vm = require('node:vm');

// Resolve through the actual consumers so nested installs exercise the same
// overrides as Docusaurus, rather than an unrelated hoisted dependency.
const bundlerRequire = createRequire(require.resolve('@docusaurus/bundler'));
for (const plugin of ['copy-webpack-plugin', 'css-minimizer-webpack-plugin']) {
  test(`${plugin} can serialize executable build options`, () => {
    const pluginRequire = createRequire(bundlerRequire.resolve(plugin));
    const serialize = pluginRequire('serialize-javascript');
    const options = {
      pattern: /\.css$/gi,
      date: new Date('2026-01-02T03:04:05Z'),
      transform: function(input) { return input.trim(); },
      text: '</script>\u2028\u2029',
    };
    const encoded = serialize(options);
    assert.equal(encoded.includes('</script>'), false);
    const decoded = vm.runInNewContext(`(${encoded})`);
    assert.equal(decoded.pattern.source, options.pattern.source);
    assert.equal(decoded.pattern.flags, options.pattern.flags);
    assert.equal(decoded.date.toISOString(), options.date.toISOString());
    assert.equal(decoded.transform('  css  '), 'css');
    assert.equal(decoded.text, options.text);
  });
}

test('SockJS creates a UUID connection and exchanges messages', {timeout: 5000}, async (t) => {
  const coreRequire = createRequire(require.resolve('@docusaurus/core/package.json'));
  const devServerRequire = createRequire(coreRequire.resolve('webpack-dev-server'));
  const sockjs = devServerRequire('sockjs');
  const WebSocket = devServerRequire('ws');
  const server = http.createServer();
  const endpoint = sockjs.createServer({
    log() {},
    disconnect_delay: 1000,
  });
  let connection;
  let socket;
  endpoint.on('connection', (client) => {
    connection = client;
    client.on('data', (message) => client.write(message));
  });
  endpoint.installHandlers(server, {prefix: '/sockjs'});
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => {
    socket?.terminate();
    connection?.close();
    server.closeAllConnections();
    await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  });

  const base = `http://127.0.0.1:${server.address().port}/sockjs/000/compat`;
  const request = (path, body) => fetch(`${base}/${path}`, {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body,
    signal: AbortSignal.timeout(3000),
  });
  const opened = await request('xhr');
  assert.equal(opened.status, 200);
  assert.equal(await opened.text(), 'o\n');
  assert.match(connection.id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);

  const sent = await request('xhr_send', JSON.stringify(['dependency smoke test']));
  assert.equal(sent.status, 204);
  await sent.text();
  const received = await request('xhr');
  assert.equal(received.status, 200);
  assert.equal(await received.text(), 'a["dependency smoke test"]\n');

  // Exercise the websocket-driver upgrade path as well as the XHR fallback.
  socket = new WebSocket(`ws://127.0.0.1:${server.address().port}/sockjs/000/websocket-test/websocket`);
  const openedFrame = once(socket, 'message');
  await once(socket, 'open');
  assert.equal((await openedFrame)[0].toString(), 'o');
  const echoedFrame = once(socket, 'message');
  socket.send(JSON.stringify(['websocket smoke test']));
  assert.equal((await echoedFrame)[0].toString(), 'a["websocket smoke test"]');
  const closed = once(socket, 'close');
  socket.close();
  await closed;
});
