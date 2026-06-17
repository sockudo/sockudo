const expect = require("expect.js");
const nock = require("nock");

const Sockudo = require("../../../dist/sockudo");

describe("Sockudo", function () {
  let sockudo;

  beforeEach(function () {
    sockudo = new Sockudo({ appId: 999, key: "111111", secret: "tofu" });
    nock.disableNetConnect();
  });

  afterEach(function () {
    nock.cleanAll();
    nock.enableNetConnect();
  });

  describe("#get", function () {
    it("should set the correct path and include all params", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .get(
          "/apps/999/channels?auth_key=111111&auth_timestamp=X&auth_version=1.0&filter_by_prefix=presence-&info=user_count,subscription_count&auth_signature=Y",
        )
        .reply(200, "{}");

      sockudo
        .get({
          path: "/channels",
          params: {
            filter_by_prefix: "presence-",
            info: "user_count,subscription_count",
          },
        })
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
        .get(
          "/apps/999/test?auth_key=111111&auth_timestamp=X&auth_version=1.0&auth_signature=Y",
        )
        .reply(200, '{"test key": "test value"}');

      sockudo
        .get({ path: "/test", params: {} })
        .then((response) => {
          expect(response.status).to.equal(200);
          return response.text().then((body) => {
            expect(body).to.equal('{"test key": "test value"}');
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
        .get(
          "/apps/999/test?auth_key=111111&auth_timestamp=X&auth_version=1.0&auth_signature=Y",
        )
        .reply(400, "Error");

      sockudo.get({ path: "/test", params: {} }).catch((error) => {
        expect(error).to.be.a(Sockudo.RequestError);
        expect(error.message).to.equal("Unexpected status code 400");
        expect(error.url).to.match(
          /^http:\/\/localhost\/apps\/999\/test\?auth_key=111111&auth_timestamp=[0-9]+&auth_version=1\.0&auth_signature=[a-f0-9]+$/,
        );
        expect(error.status).to.equal(400);
        expect(error.body).to.equal("Error");
        done();
      });
    });

    it("should respect the encryption, host and port config", function (done) {
      const sockudo = new Sockudo({
        appId: 999,
        key: "111111",
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
        .get(
          "/apps/999/test?auth_key=111111&auth_timestamp=X&auth_version=1.0&auth_signature=Y",
        )
        .reply(200, '{"test key": "test value"}');

      sockudo
        .get({ path: "/test", params: {} })
        .then(() => done())
        .catch(done);
    });

    it("should respect the timeout when specified", function (done) {
      const sockudo = new Sockudo({
        appId: 999,
        key: "111111",
        secret: "tofu",
        timeout: 100,
      });
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .get(
          "/apps/999/test?auth_key=111111&auth_timestamp=X&auth_version=1.0&auth_signature=Y",
        )
        .delayConnection(101)
        .reply(200);

      sockudo.get({ path: "/test", params: {} }).catch((error) => {
        expect(error).to.be.a(Sockudo.RequestError);
        expect(error.message).to.equal("Request failed with an error");
        expect(error.error.name).to.eql("AbortError");
        expect(error.url).to.match(
          /^http:\/\/localhost\/apps\/999\/test\?auth_key=111111&auth_timestamp=[0-9]+&auth_version=1\.0&auth_signature=[a-f0-9]+$/,
        );
        expect(error.status).to.equal(undefined);
        expect(error.body).to.equal(undefined);
        done();
      });
    });
  });

  describe("#channelHistory", function () {
    it("should call the channel history endpoint with expected query params", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .get(
          "/apps/999/channels/history-room/history?auth_key=111111&auth_timestamp=X&auth_version=1.0&cursor=abc&direction=newest_first&end_serial=20&end_time_ms=2000&limit=50&start_serial=10&start_time_ms=1000&auth_signature=Y",
        )
        .reply(200, "{}");

      sockudo
        .channelHistory("history-room", {
          limit: 50,
          direction: "newest_first",
          cursor: "abc",
          start_serial: 10,
          end_serial: 20,
          start_time_ms: 1000,
          end_time_ms: 2000,
        })
        .then(() => done())
        .catch(done);
    });
  });

  describe("#getMessage", function () {
    it("should call the latest-message endpoint and decode the payload", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .get(
          "/apps/999/channels/chat:room-1/messages/msg:1?auth_key=111111&auth_timestamp=X&auth_version=1.0&auth_signature=Y",
        )
        .reply(200, {
          channel: "chat:room-1",
          item: {
            message_serial: "msg:1",
            action: "update",
            data: "hello brave",
          },
        });

      sockudo
        .getMessage("chat:room-1", "msg:1")
        .then((payload) => {
          expect(payload.item.message_serial).to.equal("msg:1");
          expect(payload.item.data).to.equal("hello brave");
          done();
        })
        .catch(done);
    });
  });

  describe("#getMessageVersions", function () {
    it("should call the versions endpoint with expected query params", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .get(
          "/apps/999/channels/chat:room-1/messages/msg:1/versions?auth_key=111111&auth_timestamp=X&auth_version=1.0&cursor=abc&direction=oldest_first&limit=10&auth_signature=Y",
        )
        .reply(200, {
          channel: "chat:room-1",
          direction: "oldest_first",
          limit: 10,
          has_more: false,
          items: [{ message_serial: "msg:1", action: "update", data: "hello" }],
        });

      sockudo
        .getMessageVersions("chat:room-1", "msg:1", {
          limit: 10,
          direction: "oldest_first",
          cursor: "abc",
        })
        .then((payload) => {
          expect(payload.limit).to.equal(10);
          expect(payload.items.length).to.equal(1);
          done();
        })
        .catch(done);
    });
  });

  describe("#listAnnotations", function () {
    it("should call the annotation events endpoint with expected query params", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .get(
          "/apps/999/channels/chat:room-1/messages/msg:1/annotations?auth_key=111111&auth_timestamp=X&auth_version=1.0&cursor=abc&direction=oldest_first&limit=10&type=reaction%3Adistinct.v1&auth_signature=Y",
        )
        .reply(200, {
          channel: "chat:room-1",
          message_serial: "msg:1",
          direction: "oldest_first",
          limit: 10,
          has_more: false,
          items: [
            {
              annotation_serial: "ann:1",
              type: "reaction:distinct.v1",
              name: "like",
              client_id: "alice",
            },
          ],
        });

      sockudo
        .listAnnotations("chat:room-1", "msg:1", {
          limit: 10,
          direction: "oldest_first",
          cursor: "abc",
          type: "reaction:distinct.v1",
        })
        .then((payload) => {
          expect(payload.items[0].annotation_serial).to.equal("ann:1");
          done();
        })
        .catch(done);
    });
  });

  describe("#channelPresenceHistory", function () {
    it("should call the presence history endpoint with expected query params", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .get(
          "/apps/999/channels/presence-history-room/presence/history?auth_key=111111&auth_timestamp=X&auth_version=1.0&cursor=abc&direction=newest_first&end_serial=20&end_time_ms=2000&limit=50&start_serial=10&start_time_ms=1000&auth_signature=Y",
        )
        .reply(200, "{}");

      sockudo
        .channelPresenceHistory("presence-history-room", {
          limit: 50,
          direction: "newest_first",
          cursor: "abc",
          start_serial: 10,
          end_serial: 20,
          start_time_ms: 1000,
          end_time_ms: 2000,
        })
        .then(() => done())
        .catch(done);
    });

    it("should resolve to the parsed presence history page", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .get(
          "/apps/999/channels/presence-history-room/presence/history?auth_key=111111&auth_timestamp=X&auth_version=1.0&auth_signature=Y",
        )
        .reply(200, {
          items: [
            {
              stream_id: "stream-1",
              serial: 2,
              published_at_ms: 1700000000002,
              event: "member_removed",
              cause: "disconnect",
              user_id: "user-2",
              connection_id: "230423.3434",
              dead_node_id: null,
              payload_size_bytes: 123,
              presence_event: {
                stream_id: "stream-1",
                serial: 2,
                published_at_ms: 1700000000002,
                event: "member_removed",
                cause: "disconnect",
                user_id: "user-2",
                connection_id: "230423.3434",
                user_info: { name: "Ada" },
                dead_node_id: null,
              },
            },
          ],
          direction: "newest_first",
          limit: 100,
          has_more: false,
          next_cursor: null,
          bounds: {
            start_serial: null,
            end_serial: null,
            start_time_ms: null,
            end_time_ms: null,
          },
          continuity: {
            stream_id: "stream-1",
            oldest_available_serial: 1,
            newest_available_serial: 2,
            oldest_available_published_at_ms: 1700000000001,
            newest_available_published_at_ms: 1700000000002,
            retained_events: 2,
            retained_bytes: 512,
            complete: true,
            truncated_by_retention: false,
          },
        });

      sockudo
        .channelPresenceHistory("presence-history-room")
        .then((page) => {
          expect(page.items).to.have.length(1);
          expect(page.items[0].event).to.equal("member_removed");
          expect(page.items[0].presence_event.user_id).to.equal("user-2");
          expect(page.continuity.retained_events).to.equal(2);
          done();
        })
        .catch(done);
    });

    it("should reject non-presence channels before making a request", function () {
      expect(function () {
        sockudo.channelPresenceHistory("public-room");
      }).to.throwError(/Presence history is only available/);
    });
  });

  describe("#getMessage", function () {
    it("should call the latest-message endpoint", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .get(
          "/apps/999/channels/chat:room-1/messages/msg:1?auth_key=111111&auth_timestamp=X&auth_version=1.0&auth_signature=Y",
        )
        .reply(200, {
          channel: "chat:room-1",
          item: { message_serial: "msg:1", action: "update" },
        });

      sockudo
        .getMessage("chat:room-1", "msg:1")
        .then((payload) => {
          expect(payload.item.message_serial).to.equal("msg:1");
          done();
        })
        .catch(done);
    });
  });

  describe("#getMessageVersions", function () {
    it("should call the message versions endpoint with params", function (done) {
      nock("http://localhost")
        .filteringPath(function (path) {
          return path
            .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
            .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y");
        })
        .get(
          "/apps/999/channels/chat:room-1/messages/msg:1/versions?auth_key=111111&auth_timestamp=X&auth_version=1.0&cursor=abc&direction=oldest_first&limit=10&auth_signature=Y",
        )
        .reply(200, {
          channel: "chat:room-1",
          direction: "oldest_first",
          limit: 10,
          has_more: false,
          items: [],
        });

      sockudo
        .getMessageVersions("chat:room-1", "msg:1", {
          limit: 10,
          direction: "oldest_first",
          cursor: "abc",
        })
        .then((payload) => {
          expect(payload.limit).to.equal(10);
          done();
        })
        .catch(done);
    });
  });
});
