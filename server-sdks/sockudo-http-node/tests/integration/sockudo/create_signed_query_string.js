const expect = require("expect.js");
const crypto = require("crypto");
const sinon = require("sinon");

const Sockudo = require("../../../dist/sockudo");

describe("Sockudo", function () {
  let sockudo;

  beforeEach(function () {
    sockudo = new Sockudo({ appId: 1234, key: "f00d", secret: "tofu" });
  });

  describe("#createSignedQueryString", function () {
    let clock;

    beforeEach(function () {
      clock = sinon.useFakeTimers(1234567890000, "Date");
    });

    afterEach(function () {
      clock.restore();
    });

    describe("when signing a body", function () {
      it("should set the auth_key param to the app key", function () {
        const queryString = sockudo.createSignedQueryString({
          method: "GET",
          path: "/event",
          body: "example body",
        });
        expect(queryString).to.match(/^(.*&)?auth_key=f00d(&.*)?$/);
      });

      it("should set the auth_timestamp param to the current timestamp (in seconds)", function () {
        const queryString = sockudo.createSignedQueryString({
          method: "GET",
          path: "/event",
          body: "example body",
        });
        // Date.now is mocked
        expect(queryString).to.match(/^(.*&)?auth_timestamp=1234567890(&.*)?$/);
      });

      it("should set the auth_version param to 1.0", function () {
        const queryString = sockudo.createSignedQueryString({
          method: "GET",
          path: "/event",
          body: "example body",
        });
        expect(queryString).to.match(/^(.*&)?auth_version=1\.0(&.*)?$/);
      });

      it("should set the body_md5 param to a correct hash", function () {
        const queryString = sockudo.createSignedQueryString({
          method: "GET",
          path: "/event",
          body: "example body",
        });
        expect(queryString).to.match(/^(.*&)?body_md5=165d5e6d7ca8f73b3853ce45addf42fc(&.*)?$/);
      });

      it("should set the auth_signature to a correct hash", function () {
        const queryString = sockudo.createSignedQueryString({
          method: "GET",
          path: "/event",
          body: "example body",
        });
        // Date.now is mocked, so the signature can be hardcoded
        expect(queryString).to.match(
          /^(.*&)?auth_signature=a650196dc427ebe837226f8565ca9232198c6d1b9455eaa72374a9dc0b620e7b(&.*)?$/,
        );
      });
    });

    describe("when signing params", function () {
      it("should set the auth_key param to the app key", function () {
        const queryString = sockudo.createSignedQueryString({
          method: "GET",
          path: "/event",
          params: { foo: "bar" },
        });
        expect(queryString).to.match(/^(.*&)?auth_key=f00d(&.*)?$/);
      });

      it("should set the auth_timestamp param to the current timestamp (in seconds)", function () {
        const queryString = sockudo.createSignedQueryString({
          method: "GET",
          path: "/event",
          params: { foo: "bar" },
        });
        // Date.now is mocked
        expect(queryString).to.match(/^(.*&)?auth_timestamp=1234567890(&.*)?$/);
      });

      it("should set the auth_version param to 1.0", function () {
        const queryString = sockudo.createSignedQueryString({
          method: "GET",
          path: "/event",
          params: { foo: "bar" },
        });
        expect(queryString).to.match(/^(.*&)?auth_version=1\.0(&.*)?$/);
      });

      it("should set all given params", function () {
        const queryString = sockudo.createSignedQueryString({
          method: "GET",
          path: "/event",
          params: { foo: "bar", baz: 123454321 },
        });
        expect(queryString).to.match(/^(.*&)?foo=bar(&.*)?$/);
        expect(queryString).to.match(/^(.*&)?baz=123454321(&.*)?$/);
      });

      it("should set the auth_signature to a correct hash", function () {
        const queryString = sockudo.createSignedQueryString({
          method: "GET",
          path: "/event",
          params: { foo: "bar", baz: 123454321 },
        });
        // Date.now is mocked, so the signature can be hardcoded
        expect(queryString).to.match(
          /^(.*&)?auth_signature=d8bdbd31911fb7400a14fbd36fd7de053494725b64400ae5856deafe304b34a9(&.*)?$/,
        );
      });

      it("should sign mixed-case params with lowercase canonical keys while preserving URL params", function () {
        const queryString = sockudo.createSignedQueryString({
          method: "GET",
          path: "/apps/1234/push/channelSubscriptions",
          params: { deviceId: "device-1", limit: 10 },
        });
        const params = new URLSearchParams(queryString);
        const canonical =
          "GET\n/apps/1234/push/channelSubscriptions\n" +
          "auth_key=f00d&auth_timestamp=1234567890&auth_version=1.0&deviceid=device-1&limit=10";
        const expectedSignature = crypto
          .createHmac("sha256", "tofu")
          .update(canonical)
          .digest("hex");

        expect(params.get("deviceId")).to.equal("device-1");
        expect(params.get("deviceid")).to.equal(null);
        expect(params.get("auth_signature")).to.equal(expectedSignature);
      });

      it("should raise an expcetion when overriding the auth_key param", function () {
        expect(function () {
          sockudo.createSignedQueryString({
            method: "GET",
            path: "/event",
            params: { auth_key: "NOPE" },
          });
        }).to.throwException(/^auth_key is a required parameter and cannot be overidden$/);
      });

      it("should raise an expcetion when overriding the auth_timestamp param", function () {
        expect(function () {
          sockudo.createSignedQueryString({
            method: "GET",
            path: "/event",
            params: { auth_timestamp: "NOPE" },
          });
        }).to.throwException(/^auth_timestamp is a required parameter and cannot be overidden$/);
      });

      it("should raise an expcetion when overriding the auth_version param", function () {
        expect(function () {
          sockudo.createSignedQueryString({
            method: "GET",
            path: "/event",
            params: { auth_version: "NOPE" },
          });
        }).to.throwException(/^auth_version is a required parameter and cannot be overidden$/);
      });

      it("should raise an expcetion when overriding the auth_signature param", function () {
        expect(function () {
          sockudo.createSignedQueryString({
            method: "GET",
            path: "/event",
            params: { auth_signature: "NOPE" },
          });
        }).to.throwException(/^auth_signature is a required parameter and cannot be overidden$/);
      });
    });
  });
});
