const expect = require("expect.js");
const nock = require("nock");
const nacl = require("tweetnacl");
const naclUtil = require("tweetnacl-util");
const sinon = require("sinon");

const Sockudo = require("../../../dist/sockudo");
const events = require("../../../dist/events");

describe("Sockudo", function () {
  let sockudo;

  beforeEach(function () {
    sockudo = new Sockudo({
      appId: 1234,
      key: "f00d",
      secret: "tofu",
      autoIdempotencyKey: false,
    });
    nock.disableNetConnect();
  });

  afterEach(function () {
    nock.cleanAll();
    nock.enableNetConnect();
  });

  describe("#trigger", function () {
    it("should send the event to a single channel", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/1234/events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=e95168baf497b2e54b2c6cadd41a6a3f&auth_signature=Y",
          { name: "my_event", data: '{"some":"data "}', channels: ["one"] },
        )
        .reply(200, "{}");

      sockudo
        .trigger("one", "my_event", { some: "data " })
        .then(() => done())
        .catch(done);
    });

    it("should send the event to multiple channels", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/1234/events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=530dac0aa045e5f8e51c470aed0ce325&auth_signature=Y",
          {
            name: "my_event",
            data: '{"some":"data "}',
            channels: ["one", "two", "three"],
          },
        )
        .reply(200, "{}");

      sockudo
        .trigger(["one", "two", "three"], "my_event", { some: "data " })
        .then(() => done())
        .catch(done);
    });

    it("should serialize arrays into JSON", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/1234/events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=18e64b2fed38726915d79ebb4f8feb5b&auth_signature=Y",
          { name: "my_event", data: "[1,2,4]", channels: ["one"] },
        )
        .reply(200, "{}");

      sockudo
        .trigger("one", "my_event", [1, 2, 4])
        .then(() => done())
        .catch(done);
    });

    it("should not serialize strings into JSON", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/1234/events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=f358a562d00e1bfe1859132d932cd706&auth_signature=Y",
          { name: "test_event", data: "test string", channels: ["test"] },
        )
        .reply(200, "{}");

      sockudo
        .trigger("test", "test_event", "test string")
        .then(() => done())
        .catch(done);
    });

    it("should add params to the request body", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/1234/events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=2e4f053f1c325dedbe21abd8f1852b53&auth_signature=Y",
          {
            name: "my_event",
            data: '{"some":"data "}',
            channels: ["test_channel"],
            socket_id: "123.567",
            info: "user_count,subscription_count",
          },
        )
        .reply(200, "{}");

      const params = {
        socket_id: "123.567",
        info: "user_count,subscription_count",
      };
      sockudo
        .trigger("test_channel", "my_event", { some: "data " }, params)
        .then(() => done())
        .catch(done);
    });

    it("should resolve to the response", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/1234/events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=d3a47b3241328a6432adf60c8e91b6fb&auth_signature=Y",
          {
            name: "my_event",
            data: '{"some":"data "}',
            channels: ["test_channel"],
            info: "subscription_count",
          },
        )
        .reply(200, '{"channels":{"test_channel":{"subscription_count":123}}}');

      sockudo
        .trigger(
          "test_channel",
          "my_event",
          { some: "data " },
          { info: "subscription_count" },
        )
        .then((response) => {
          expect(response.status).to.equal(200);
          return response.text().then((body) => {
            expect(body).to.equal(
              '{"channels":{"test_channel":{"subscription_count":123}}}',
            );
            done();
          });
        })
        .catch(done);
    });

    it("should reject with a RequestError if Sockudo responds with 4xx", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y")
            .replace(/body_md5=[0-9a-f]{32}/, "body_md5=Z");
        })
        .post(
          "/apps/1234/events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=Z&auth_signature=Y",
          {
            name: "my_event",
            data: '{"some":"data "}',
            channels: ["test_channel"],
          },
        )
        .reply(400, "Error");

      sockudo
        .trigger("test_channel", "my_event", { some: "data " })
        .catch((error) => {
          expect(error).to.be.a(Sockudo.RequestError);
          expect(error.message).to.equal("Unexpected status code 400");
          expect(error.url).to.match(
            /^http:\/\/localhost\/apps\/1234\/events\?auth_key=f00d&auth_timestamp=[0-9]+&auth_version=1\.0&body_md5=[a-f0-9]{32}&auth_signature=[a-f0-9]+$/,
          );
          expect(error.status).to.equal(400);
          expect(error.body).to.equal("Error");
          done();
        });
    });

    it("should allow channel names with special characters: _ - = @ , . ;", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/1234/events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=024f0f297e27e131c8ec2c8817d153f4&auth_signature=Y",
          {
            name: "my_event",
            data: '{"some":"data "}',
            channels: ["test_-=@,.;channel"],
          },
        )
        .reply(200, "OK");

      sockudo
        .trigger("test_-=@,.;channel", "my_event", { some: "data " })
        .then((response) => {
          expect(response.status).to.equal(200);
          done();
        })
        .catch(done);
    });

    it("should throw an error if called with more than 100 channels", function () {
      expect(function () {
        const channels = [];
        for (let i = 0; i < 101; i++) {
          channels.push(i.toString());
        }
        sockudo.trigger(channels, "x", {});
      }).to.throwError(function (e) {
        expect(e).to.be.an(Error);
        expect(e.message).to.equal(
          "Can't trigger a message to more than 100 channels",
        );
      });
    });

    it("should throw an error if channel name is empty", function () {
      expect(function () {
        sockudo.trigger("", "test");
      }).to.throwError(function (e) {
        expect(e).to.be.an(Error);
        expect(e.message).to.equal("Invalid channel name: ''");
      });
    });

    it("should throw an error if channel name is invalid", function () {
      expect(function () {
        sockudo.trigger("abc$", "test");
      }).to.throwError(function (e) {
        expect(e).to.be.an(Error);
        expect(e.message).to.equal("Invalid channel name: 'abc$'");
      });
    });

    it("should throw an error if channel name is longer than 200 characters", function () {
      const channel = "x".repeat(201);
      expect(function () {
        sockudo.trigger(channel, "test");
      }).to.throwError(function (e) {
        expect(e).to.be.an(Error);
        expect(e.message).to.equal("Channel name too long: '" + channel + "'");
      });
    });

    it("should throw an error if event name is longer than 200 characters", function () {
      const event = "x".repeat(201);
      expect(function () {
        sockudo.trigger("test", event);
      }).to.throwError(function (e) {
        expect(e).to.be.an(Error);
        expect(e.message).to.equal("Too long event name: '" + event + "'");
      });
    });

    it("should respect the encryption, host and port config", function (done) {
      const sockudo = new Sockudo({
        appId: 1234,
        key: "f00d",
        secret: "tofu",
        useTLS: true,
        host: "example.com",
        port: 1234,
        autoIdempotencyKey: false,
      });
      nock("https://example.com:1234")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y")
            .replace(/body_md5=[0-9a-f]{32}/, "body_md5=Z");
        })
        .post(
          "/apps/1234/events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=Z&auth_signature=Y",
          {
            name: "my_event",
            data: '{"some":"data "}',
            channels: ["test_channel"],
            socket_id: "123.567",
          },
        )
        .reply(200, "{}");

      sockudo
        .trigger(
          "test_channel",
          "my_event",
          { some: "data " },
          { socket_id: "123.567" },
        )
        .then(() => done())
        .catch(done);
    });

    it("should include idempotency_key in body and X-Idempotency-Key header when provided as string", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y")
            .replace(/body_md5=[0-9a-f]{32}/, "body_md5=Z");
        })
        .post(
          "/apps/1234/events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=Z&auth_signature=Y",
          function (body) {
            return (
              body.name === "my_event" &&
              body.channels[0] === "test_channel" &&
              body.idempotency_key === "my-unique-key-123"
            );
          },
        )
        .reply(200, "{}");

      sockudo
        .trigger(
          "test_channel",
          "my_event",
          { some: "data " },
          { idempotency_key: "my-unique-key-123" },
        )
        .then(() => done())
        .catch(done);
    });

    it("should auto-generate a UUID v4 idempotency_key when set to true", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y")
            .replace(/body_md5=[0-9a-f]{32}/, "body_md5=Z");
        })
        .post(
          "/apps/1234/events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=Z&auth_signature=Y",
          function (body) {
            const uuidRegex =
              /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
            return (
              body.name === "my_event" &&
              body.channels[0] === "test_channel" &&
              typeof body.idempotency_key === "string" &&
              uuidRegex.test(body.idempotency_key)
            );
          },
        )
        .reply(200, "{}");

      sockudo
        .trigger(
          "test_channel",
          "my_event",
          { some: "data " },
          { idempotency_key: true },
        )
        .then(() => done())
        .catch(done);
    });

    it("should respect the timeout when specified", function (done) {
      const sockudo = new Sockudo({
        appId: 1234,
        key: "f00d",
        secret: "tofu",
        timeout: 100,
        autoIdempotencyKey: false,
      });
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y")
            .replace(/body_md5=[0-9a-f]{32}/, "body_md5=Z");
        })
        .post(
          "/apps/1234/events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=Z&auth_signature=Y",
          {
            name: "my_event",
            data: '{"some":"data "}',
            channels: ["test_channel"],
            socket_id: "123.567",
          },
        )
        .delayConnection(101)
        .reply(200);

      sockudo
        .trigger(
          "test_channel",
          "my_event",
          { some: "data " },
          { socket_id: "123.567" },
        )
        .catch((error) => {
          expect(error).to.be.a(Sockudo.RequestError);
          expect(error.message).to.equal("Request failed with an error");
          expect(error.error.name).to.eql("AbortError");
          expect(error.url).to.match(
            /^http:\/\/localhost\/apps\/1234\/events\?auth_key=f00d&auth_timestamp=[0-9]+&auth_version=1\.0&body_md5=[a-f0-9]{32}&auth_signature=[a-f0-9]+$/,
          );
          expect(error.status).to.equal(undefined);
          expect(error.body).to.equal(undefined);
          done();
        });
    });
  });

  describe("#triggerBatch", function () {
    it("should trigger multiple events in a single call", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/1234/batch_events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=fd5ab5fd40237f27555c4d2564470fdd&auth_signature=Y",
          JSON.stringify({
            batch: [
              { channel: "integration", name: "event", data: "test" },
              { channel: "integration2", name: "event2", data: "test2" },
            ],
          }),
        )
        .reply(200, "{}");

      sockudo
        .triggerBatch([
          {
            channel: "integration",
            name: "event",
            data: "test",
          },
          {
            channel: "integration2",
            name: "event2",
            data: "test2",
          },
        ])
        .then(() => done())
        .catch(done);
    });

    it("should include idempotency_key per event in batch", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y")
            .replace(/body_md5=[0-9a-f]{32}/, "body_md5=Z");
        })
        .post(
          "/apps/1234/batch_events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=Z&auth_signature=Y",
          function (body) {
            return (
              body.batch.length === 2 &&
              body.batch[0].idempotency_key === "key-1" &&
              body.batch[1].idempotency_key === "key-2"
            );
          },
        )
        .reply(200, "{}");

      sockudo
        .triggerBatch([
          {
            channel: "integration",
            name: "event",
            data: "test",
            idempotency_key: "key-1",
          },
          {
            channel: "integration2",
            name: "event2",
            data: "test2",
            idempotency_key: "key-2",
          },
        ])
        .then(() => done())
        .catch(done);
    });

    it("should auto-generate UUID v4 for batch events with idempotency_key set to true", function (done) {
      const uuidRegex =
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y")
            .replace(/body_md5=[0-9a-f]{32}/, "body_md5=Z");
        })
        .post(
          "/apps/1234/batch_events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=Z&auth_signature=Y",
          function (body) {
            return (
              body.batch.length === 2 &&
              typeof body.batch[0].idempotency_key === "string" &&
              uuidRegex.test(body.batch[0].idempotency_key) &&
              typeof body.batch[1].idempotency_key === "string" &&
              uuidRegex.test(body.batch[1].idempotency_key) &&
              body.batch[0].idempotency_key !== body.batch[1].idempotency_key
            );
          },
        )
        .reply(200, "{}");

      sockudo
        .triggerBatch([
          {
            channel: "integration",
            name: "event",
            data: "test",
            idempotency_key: true,
          },
          {
            channel: "integration2",
            name: "event2",
            data: "test2",
            idempotency_key: true,
          },
        ])
        .then(() => done())
        .catch(done);
    });

    it("should stringify data before posting", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/1234/batch_events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=ade2e9d64d936215c2b2d6a6f4606ef9&auth_signature=Y",
          JSON.stringify({
            batch: [
              {
                channel: "integration",
                name: "event",
                data: '{"hello":"world"}',
              },
              {
                channel: "integration2",
                name: "event2",
                data: '{"hello2":"another world"}',
              },
            ],
          }),
        )
        .reply(200, "{}");

      sockudo
        .triggerBatch([
          {
            channel: "integration",
            name: "event",
            data: {
              hello: "world",
            },
          },
          {
            channel: "integration2",
            name: "event2",
            data: {
              hello2: "another world",
            },
          },
        ])
        .then(() => done())
        .catch(done);
    });
  });

  describe("#sendToUser", function () {
    it("should trigger an event on #server-to-user-{userId}", function () {
      sinon.stub(events, "trigger");
      sockudo.sendToUser("abc123", "halo", { foo: "bar" });
      expect(events.trigger.called).to.be(true);
      expect(events.trigger.getCall(0).args[1]).eql(["#server-to-user-abc123"]);
      expect(events.trigger.getCall(0).args[2]).equal("halo");
      expect(events.trigger.getCall(0).args[3]).eql({ foo: "bar" });
      events.trigger.restore();
    });

    it("should throw an error if user id is empty", function () {
      expect(function () {
        sockudo.sendToUser("", "halo", { foo: "bar" });
      }).to.throwError(function (e) {
        expect(e).to.be.an(Error);
        expect(e.message).to.equal("Invalid user id: ''");
      });
    });

    it("should throw an error if user id is not a string", function () {
      expect(function () {
        sockudo.sendToUser(123, "halo", { foo: "bar" });
      }).to.throwError(function (e) {
        expect(e).to.be.an(Error);
        expect(e.message).to.equal("Invalid user id: '123'");
      });
    });

    it("should throw an error if event name is longer than 200 characters", function () {
      const event = "x".repeat(201);
      expect(function () {
        sockudo.sendToUser("abc123", event, { foo: "bar" });
      }).to.throwError(function (e) {
        expect(e).to.be.an(Error);
        expect(e.message).to.equal("Too long event name: '" + event + "'");
      });
    });
  });
});

describe("Sockudo with encryptionMasterKey", function () {
  let sockudo;

  const testMasterKey = Buffer.from(
    "01234567890123456789012345678901",
  ).toString("base64");

  beforeEach(function () {
    sockudo = new Sockudo({
      appId: 1234,
      key: "f00d",
      secret: "tofu",
      encryptionMasterKeyBase64: testMasterKey,
      autoIdempotencyKey: false,
    });
    nock.disableNetConnect();
  });

  afterEach(function () {
    nock.cleanAll();
    nock.enableNetConnect();
  });

  describe("#trigger", function () {
    it("should not encrypt the body of an event triggered on a single channel", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/1234/events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=e95168baf497b2e54b2c6cadd41a6a3f&auth_signature=Y",
          { name: "my_event", data: '{"some":"data "}', channels: ["one"] },
        )
        .reply(200, "{}");

      sockudo
        .trigger("one", "my_event", { some: "data " })
        .then(() => done())
        .catch(done);
    });

    it("should encrypt the body of an event triggered on a private-encrypted- channel", function (done) {
      const sentPlaintext = "Hello!";

      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y")
            .replace(/body_md5=[0-9a-f]{32}/, "body_md5=Z");
        })
        .post(
          "/apps/1234/events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=Z&auth_signature=Y",
          function (body) {
            if (body.name !== "test_event") return false;
            if (body.channels.length !== 1) return false;
            const channel = body.channels[0];
            if (channel !== "private-encrypted-bla") return false;
            const encrypted = JSON.parse(body.data);
            const nonce = naclUtil.decodeBase64(encrypted.nonce);
            const ciphertext = naclUtil.decodeBase64(encrypted.ciphertext);
            const channelSharedSecret = sockudo.channelSharedSecret(channel);
            const receivedPlaintextBytes = nacl.secretbox.open(
              ciphertext,
              nonce,
              channelSharedSecret,
            );
            const receivedPlaintextJson = naclUtil.encodeUTF8(
              receivedPlaintextBytes,
            );
            const receivedPlaintext = JSON.parse(receivedPlaintextJson);
            return receivedPlaintext === sentPlaintext;
          },
        )
        .reply(200, "{}");

      sockudo
        .trigger("private-encrypted-bla", "test_event", sentPlaintext)
        .then(() => done())
        .catch(done);
    });
  });

  describe("#triggerBatch", function () {
    it("should encrypt the bodies of an events triggered on a private-encrypted- channels", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y")
            .replace(/body_md5=[0-9a-f]{32}/, "body_md5=Z");
        })
        .post(
          "/apps/1234/batch_events?auth_key=f00d&auth_timestamp=X&auth_version=1.0&body_md5=Z&auth_signature=Y",
          function (body) {
            if (body.batch.length !== 2) return false;
            const event1 = body.batch[0];
            if (event1.channel !== "integration") return false;
            if (event1.name !== "event") return false;
            if (event1.data !== "test") return false;
            const event2 = body.batch[1];
            if (event2.channel !== "private-encrypted-integration2")
              return false;
            if (event2.name !== "event2") return false;
            const encrypted = JSON.parse(event2.data);
            const nonce = naclUtil.decodeBase64(encrypted.nonce);
            const ciphertext = naclUtil.decodeBase64(encrypted.ciphertext);
            const channelSharedSecret = sockudo.channelSharedSecret(
              event2.channel,
            );
            const receivedPlaintextBytes = nacl.secretbox.open(
              ciphertext,
              nonce,
              channelSharedSecret,
            );
            const receivedPlaintextJson = naclUtil.encodeUTF8(
              receivedPlaintextBytes,
            );
            const receivedPlaintext = JSON.parse(receivedPlaintextJson);
            return receivedPlaintext === "test2";
          },
        )
        .reply(200, "{}");

      sockudo
        .triggerBatch([
          {
            channel: "integration",
            name: "event",
            data: "test",
          },
          {
            channel: "private-encrypted-integration2",
            name: "event2",
            data: "test2",
          },
        ])
        .then(() => done())
        .catch(done);
    });
  });
});
