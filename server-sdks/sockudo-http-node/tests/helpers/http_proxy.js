const net = require("net");
const url = require("url");

function handleInit(client) {
  let inBuffer = Buffer.from("");

  function onData(chunk) {
    inBuffer = Buffer.concat([inBuffer, chunk]);

    const lines = splitBufferOnce(inBuffer, Buffer.from("\r\n"));
    if (lines[1] === null) {
      // still reading the first line
      return;
    }

    const firstLine = lines[0].toString();

    const requestInfo = firstLine.match(/([A-Z]+) (.*) HTTP\/1\.1/);
    const method = requestInfo[1];
    const destination = requestInfo[2];

    unbind();
    if (method === "CONNECT") {
      handleConnectInit(
        client,
        destination.split(":")[0],
        parseInt(destination.split(":")[1]) || 443,
        inBuffer,
      );
    } else {
      const destinationUrl = url.parse(destination);
      handleConnecting(
        client,
        destinationUrl.hostname,
        destinationUrl.port || 80,
        inBuffer,
      );
    }
  }

  function unbind() {
    client.removeListener("data", onData);
    client.removeListener("end", unbind);
  }

  client.addListener("data", onData);
  client.addListener("end", unbind);
}

function handleConnectInit(client, hostname, port, inBuffer) {
  function advanceIfHeadersWereSent() {
    const blocks = splitBufferOnce(inBuffer, Buffer.from("\r\n\r\n"));
    if (blocks[1] !== null) {
      // discard the headers
      unbind();
      handleConnecting(client, hostname, port, blocks[1], function () {
        client.write(
          Buffer.from("HTTP/1.0 200 Connection established\r\n\r\n"),
        );
      });
    }
  }

  function onData(chunk) {
    inBuffer = Buffer.concat([inBuffer, chunk]);
    advanceIfHeadersWereSent();
  }

  function unbind() {
    client.removeListener("data", onData);
    client.removeListener("end", unbind);
  }

  client.addListener("data", onData);
  client.addListener("end", unbind);

  advanceIfHeadersWereSent();
}

function handleConnecting(client, hostname, port, inBuffer, callback) {
  const server = net.connect(port, hostname);

  function onConnected() {
    unbind();
    handleConnected(client, server, inBuffer, callback);
  }

  function onData(chunk) {
    inBuffer = Buffer.concat([inBuffer, chunk]);
  }

  function onEnd() {
    unbind();
    client.end();
    server.end();
  }

  function unbind() {
    client.removeListener("data", onData);
    client.removeListener("end", onEnd);
    server.removeListener("connect", onConnected);
    server.removeListener("end", onEnd);
  }

  client.addListener("data", onData);
  client.addListener("end", onEnd);
  server.addListener("connect", onConnected);
  server.addListener("end", onEnd);
}

function handleConnected(client, server, inBuffer, callback) {
  server.write(inBuffer, "binary");
  inBuffer = null;

  function onClientData(chunk) {
    server.write(chunk, "binary");
  }

  function onServerData(chunk) {
    client.write(chunk, "binary");
  }

  function onEnd() {
    unbind();
    client.end();
    server.end();
  }

  function unbind() {
    client.removeListener("data", onClientData);
    client.removeListener("end", onEnd);
    server.removeListener("data", onServerData);
    server.removeListener("end", onEnd);
  }

  client.addListener("data", onClientData);
  client.addListener("end", onEnd);
  server.addListener("data", onServerData);
  server.addListener("end", onEnd);

  if (callback) {
    callback();
  }
}

function start(callback) {
  const proxy = {};

  proxy.requests = 0;
  proxy.server = net.createServer(function (connection) {
    proxy.requests += 1;
    handleInit(connection);
  });
  proxy.server.listen(8321, callback);

  return proxy;
}

function stop(proxy, callback) {
  proxy.server.close();
  proxy.server.addListener("close", callback);
}

function splitBufferOnce(buffer, searchValue) {
  for (let i = 0; i < buffer.length - searchValue.length + 1; i++) {
    for (let j = 0; j < searchValue.length; j++) {
      if (buffer[i + j] !== searchValue[j]) {
        break;
      }
      if (j === searchValue.length - 1) {
        return [buffer.slice(0, i), buffer.slice(i + searchValue.length)];
      }
    }
  }
  return [buffer, null];
}

exports.start = start;
exports.stop = stop;
