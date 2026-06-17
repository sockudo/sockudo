const expect = require("expect.js");
const nock = require("nock");

const Sockudo = require("../../../dist/sockudo");

describe("Sockudo push", function () {
  let sockudo;

  beforeEach(function () {
    sockudo = new Sockudo({ appId: 10000, key: "aaaa", secret: "tofu" });
    nock.disableNetConnect();
  });

  afterEach(function () {
    nock.cleanAll();
    nock.enableNetConnect();
  });

  function signed(path) {
    return path
      .replace(/auth_timestamp=[0-9]+/, "auth_timestamp=X")
      .replace(/auth_signature=[0-9a-f]{64}/, "auth_signature=Y")
      .replace(/body_md5=[0-9a-f]{32}/, "body_md5=MD5");
  }

  it("activates a device through the admin registration endpoint", function (done) {
    const device = {
      id: "device-1",
      formFactor: "phone",
      platform: "android",
      timezone: "UTC",
      locale: "en",
      push: {
        recipient: {
          transportType: "gcm",
          registrationToken: "secret-token",
        },
      },
    };

    nock("http://localhost", {
      reqheaders: {
        "x-sockudo-push-capability": "push-admin",
      },
    })
      .filteringPath(signed)
      .post(
        "/apps/10000/push/deviceRegistrations?auth_key=aaaa&auth_timestamp=X&auth_version=1.0&body_md5=MD5&auth_signature=Y",
        device,
      )
      .reply(201, {
        change: "inserted",
        tokenHash: "hash",
        device: { id: "device-1" },
        deviceIdentityToken: "identity",
      });

    sockudo
      .activateDevice(device)
      .then((body) => {
        expect(body.deviceIdentityToken).to.equal("identity");
        done();
      })
      .catch(done);
  });

  it("updates a device registration with a device identity token", function (done) {
    nock("http://localhost", {
      reqheaders: {
        "x-sockudo-push-capability": "push-subscribe",
        "x-sockudo-device-identity-token": "identity",
      },
    })
      .filteringPath(signed)
      .post(
        "/apps/10000/push/deviceRegistrations?auth_key=aaaa&auth_timestamp=X&auth_version=1.0&body_md5=MD5&auth_signature=Y",
      )
      .reply(201, {
        change: "updated",
        tokenHash: "hash",
        device: { id: "device-1" },
      });

    sockudo
      .updateDeviceRegistration(
        {
          id: "device-1",
          formFactor: "phone",
          platform: "android",
          timezone: "UTC",
          locale: "en",
          push: {
            recipient: {
              transportType: "gcm",
              registrationToken: "rotated-token",
            },
          },
        },
        "identity",
      )
      .then((body) => {
        expect(body.change).to.equal("updated");
        done();
      })
      .catch(done);
  });

  it("publishes push asynchronously by default and returns a publish id", function (done) {
    const request = {
      recipients: [{ type: "channel", channel: "orders" }],
      payload: { title: "Order", body: "Updated" },
      providerOverrides: [{ provider: "fcm", payload: { android: {} } }],
    };

    nock("http://localhost", {
      reqheaders: {
        "x-sockudo-push-capability": "push-admin",
      },
    })
      .filteringPath(signed)
      .post(
        "/apps/10000/push/publish?auth_key=aaaa&auth_timestamp=X&auth_version=1.0&body_md5=MD5&auth_signature=Y",
        { ...request, sync: false },
      )
      .reply(202, {
        publish_id: "pub_123",
        status: "queued",
        expectedRecipients: 1,
        fanoutRegime: "FastPath",
      });

    sockudo
      .publishPush(request)
      .then((body) => {
        expect(body.publish_id).to.equal("pub_123");
        done();
      })
      .catch(done);
  });

  it("reads push status and uses cursor pagination for registry lists", function (done) {
    nock("http://localhost")
      .filteringPath(signed)
      .get(
        "/apps/10000/push/deviceRegistrations?auth_key=aaaa&auth_timestamp=X&auth_version=1.0&cursor=c1&limit=10&auth_signature=Y",
      )
      .reply(200, { items: [], next_cursor: null, has_more: false })
      .get(
        "/apps/10000/push/publish/pub_123/status?auth_key=aaaa&auth_timestamp=X&auth_version=1.0&auth_signature=Y",
      )
      .reply(200, { appId: "10000", publishId: "pub_123", state: "queued" });

    sockudo
      .listDeviceRegistrations({ limit: 10, cursor: "c1" })
      .then((page) => {
        expect(page.has_more).to.equal(false);
        return sockudo.getPublishStatus("pub_123");
      })
      .then((status) => {
        expect(status.publishId).to.equal("pub_123");
        done();
      })
      .catch(done);
  });
});
