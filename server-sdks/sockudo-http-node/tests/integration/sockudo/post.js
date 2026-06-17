const expect = require("expect.js");
const nock = require("nock");

const Sockudo = require("../../../dist/sockudo");

describe("Sockudo", function () {
  let sockudo;

  beforeEach(function () {
    sockudo = new Sockudo({ appId: 10000, key: "aaaa", secret: "tofu" });
    nock.disableNetConnect();
  });

  afterEach(function () {
    nock.cleanAll();
    nock.enableNetConnect();
  });

  describe("#post", function () {
    it("should set the correct path and include the body", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/10000/test?auth_key=aaaa&auth_timestamp=X&auth_version=1.0&body_md5=12bf5995ac4b1285a6d87b2dafb92590&auth_signature=Y",
          { foo: "one", bar: [1, 2, 3], baz: 4321 },
        )
        .reply(200, "{}");

      sockudo
        .post({
          path: "/test",
          body: { foo: "one", bar: [1, 2, 3], baz: 4321 },
        })
        .then(() => done())
        .catch(done);
    });

    it("should set the request content type to application/json", function (done) {
      nock("http://localhost", {
        reqheaders: {
          "content-type": "application/json",
          host: "localhost",
          "content-length": 2,
        },
      })
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/10000/test?auth_key=aaaa&auth_timestamp=X&auth_version=1.0&body_md5=99914b932bd37a50b983c5e7c90ae93b&auth_signature=Y",
          {},
        )
        .reply(201, '{"returned key": 101010101}');

      sockudo
        .post({ path: "/test", body: {} })
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
          "/apps/10000/test?auth_key=aaaa&auth_timestamp=X&auth_version=1.0&body_md5=99914b932bd37a50b983c5e7c90ae93b&auth_signature=Y",
          {},
        )
        .reply(201, '{"returned key": 101010101}');

      sockudo
        .post({ path: "/test", body: {} })
        .then((response) => {
          expect(response.status).to.equal(201);
          return response.text().then((body) => {
            expect(body).to.equal('{"returned key": 101010101}');
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
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/10000/test?auth_key=aaaa&auth_timestamp=X&auth_version=1.0&body_md5=99914b932bd37a50b983c5e7c90ae93b&auth_signature=Y",
          {},
        )
        .reply(403, "NOPE");

      sockudo.post({ path: "/test", body: {} }).catch((error) => {
        expect(error).to.be.a(Sockudo.RequestError);
        expect(error.message).to.equal("Unexpected status code 403");
        expect(error.url).to.match(
          /^http:\/\/localhost\/apps\/10000\/test\?auth_key=aaaa&auth_timestamp=[0-9]+&auth_version=1\.0&body_md5=99914b932bd37a50b983c5e7c90ae93b&auth_signature=[a-f0-9]+$/,
        );
        expect(error.status).to.equal(403);
        expect(error.body).to.equal("NOPE");
        done();
      });
    });

    it("should respect the encryption, host and port config", function (done) {
      const sockudo = new Sockudo({
        appId: 10000,
        key: "aaaa",
        secret: "tofu",
        useTLS: true,
        host: "example.com",
        port: 1234,
      });
      nock("https://example.com:1234")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/10000/test?auth_key=aaaa&auth_timestamp=X&auth_version=1.0&body_md5=99914b932bd37a50b983c5e7c90ae93b&auth_signature=Y",
          {},
        )
        .reply(201, '{"returned key": 101010101}');

      sockudo
        .post({ path: "/test", body: {} })
        .then(() => done())
        .catch(done);
    });

    it("should respect the timeout when specified", function (done) {
      const sockudo = new Sockudo({
        appId: 10000,
        key: "aaaa",
        secret: "tofu",
        timeout: 100,
      });
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/10000/test?auth_key=aaaa&auth_timestamp=X&auth_version=1.0&body_md5=99914b932bd37a50b983c5e7c90ae93b&auth_signature=Y",
          {},
        )
        .delayConnection(101)
        .reply(200);

      sockudo.post({ path: "/test", body: {} }).catch((error) => {
        expect(error).to.be.a(Sockudo.RequestError);
        expect(error.message).to.equal("Request failed with an error");
        expect(error.error.name).to.eql("AbortError");
        expect(error.url).to.match(
          /^http:\/\/localhost\/apps\/10000\/test\?auth_key=aaaa&auth_timestamp=[0-9]+&auth_version=1\.0&body_md5=99914b932bd37a50b983c5e7c90ae93b&auth_signature=[a-f0-9]+$/,
        );
        expect(error.status).to.equal(undefined);
        expect(error.body).to.equal(undefined);
        done();
      });
    });
  });

  describe("#updateMessage", function () {
    it("should call the message update endpoint", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/10000/channels/chat:room-1/messages/msg:1/update?auth_key=aaaa&auth_timestamp=X&auth_version=1.0&body_md5=5cd6e1d8509654b09eb942c6e1b3efe4&auth_signature=Y",
          '{"data":"hello brave","description":"replace base"}',
        )
        .reply(200, {
          channel: "chat:room-1",
          message_serial: "msg:1",
          action: "update",
          accepted: true,
          status: "applied",
        });

      sockudo
        .updateMessage("chat:room-1", "msg:1", {
          data: "hello brave",
          description: "replace base",
        })
        .then((payload) => {
          expect(payload.action).to.equal("update");
          done();
        })
        .catch(done);
    });
  });

  describe("#deleteMessage", function () {
    it("should call the message delete endpoint", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/10000/channels/chat:room-1/messages/msg:1/delete?auth_key=aaaa&auth_timestamp=X&auth_version=1.0&body_md5=ca60a32ee8dbfc527332ba3b02a6457a&auth_signature=Y",
          '{"clear_fields":["data","extras"],"description":"soft delete"}',
        )
        .reply(200, {
          channel: "chat:room-1",
          message_serial: "msg:1",
          action: "delete",
          accepted: true,
          status: "applied",
        });

      sockudo
        .deleteMessage("chat:room-1", "msg:1", {
          clear_fields: ["data", "extras"],
          description: "soft delete",
        })
        .then((payload) => {
          expect(payload.action).to.equal("delete");
          done();
        })
        .catch(done);
    });
  });

  describe("#appendMessage", function () {
    it("should call the message append endpoint", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/10000/channels/chat:room-1/messages/msg:1/append?auth_key=aaaa&auth_timestamp=X&auth_version=1.0&body_md5=5f02618871c7f264671e16b581f9a4b6&auth_signature=Y",
          '{"data":" world","description":"append suffix"}',
        )
        .reply(200, {
          channel: "chat:room-1",
          message_serial: "msg:1",
          action: "append",
          accepted: true,
          status: "applied",
        });

      sockudo
        .appendMessage("chat:room-1", "msg:1", {
          data: " world",
          description: "append suffix",
        })
        .then((payload) => {
          expect(payload.action).to.equal("append");
          done();
        })
        .catch(done);
    });
  });

  describe("#publishAnnotation", function () {
    it("should call the annotation publish endpoint", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .post(
          "/apps/10000/channels/chat:room-1/messages/msg:1/annotations?auth_key=aaaa&auth_timestamp=X&auth_version=1.0&body_md5=6212b152d0b9bfe76ea0cd5e73367410&auth_signature=Y",
          '{"type":"reaction:distinct.v1","name":"like","client_id":"alice"}',
        )
        .reply(200, {
          channel: "chat:room-1",
          message_serial: "msg:1",
          annotation_serial: "ann:1",
          accepted: true,
        });

      sockudo
        .publishAnnotation("chat:room-1", "msg:1", {
          type: "reaction:distinct.v1",
          name: "like",
          client_id: "alice",
        })
        .then((payload) => {
          expect(payload.annotation_serial).to.equal("ann:1");
          done();
        })
        .catch(done);
    });
  });

  describe("#deleteAnnotation", function () {
    it("should call the annotation delete endpoint", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .delete(
          "/apps/10000/channels/chat:room-1/messages/msg:1/annotations/ann:1?auth_key=aaaa&auth_timestamp=X&auth_version=1.0&socket_id=123.456&auth_signature=Y",
        )
        .reply(200, {
          channel: "chat:room-1",
          message_serial: "msg:1",
          annotation_serial: "ann:1",
          deleted: true,
        });

      sockudo
        .deleteAnnotation("chat:room-1", "msg:1", "ann:1", {
          socket_id: "123.456",
        })
        .then((payload) => {
          expect(payload.deleted).to.equal(true);
          done();
        })
        .catch(done);
    });
  });
});
