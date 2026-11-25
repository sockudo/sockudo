const expect = require("expect.js");
const sinon = require("sinon");

// The example reads credentials when it is imported, so set them first.
process.env["SOCKUDO_APP_ID"] = "123456";
process.env["SOCKUDO_KEY"] = "123456789";
process.env["SOCKUDO_SECRET"] = "tofu";

const app = require("../../examples/webhook_endpoint");
const Token = require("../../dist/token");

const token = new Token("123456789", "tofu");

describe("examples/webhook_endpoint", function () {
  let server;
  let log;

  before(function (done) {
    log = sinon.stub(console, "log");
    server = app.listen(0, "127.0.0.1", done);
  });

  after(function (done) {
    log.restore();
    // fetch pools connections, so close them before waiting on the server
    server.closeAllConnections();
    server.close(done);
  });

  const payload = JSON.stringify({
    time_ms: 1403175510755,
    events: [{ name: "channel_occupied", channel: "my-channel" }],
  });

  function send(signature) {
    return fetch("http://127.0.0.1:" + server.address().port + "/webhook", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-pusher-key": "123456789",
        "x-pusher-signature": signature,
      },
      body: payload,
    });
  }

  it("accepts a correctly signed webhook payload", async function () {
    const response = await send(token.sign(payload));

    expect(response.status).to.be(200);
    expect(await response.text()).to.be("OK");
  });

  it("rejects a webhook payload with a tampered signature", async function () {
    const signature = token.sign(payload);
    const tampered = (signature[0] === "a" ? "b" : "a") + signature.slice(1);

    const response = await send(tampered);

    expect(response.status).to.be(401);
    expect(await response.text()).to.be("Invalid signature");
  });
});
