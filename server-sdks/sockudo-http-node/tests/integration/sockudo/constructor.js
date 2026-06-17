const expect = require("expect.js");
const HttpsProxyAgent = require("https-proxy-agent");

const Sockudo = require("../../../dist/sockudo");

describe("Sockudo", function () {
  describe("constructor attributes", function () {
    it("should support `appId`", function () {
      const sockudo = new Sockudo({ appId: 12345 });
      expect(sockudo.config.appId).to.equal(12345);
    });

    it("should support `token`", function () {
      const sockudo = new Sockudo({
        key: "1234567890abcdef",
        secret: "fedcba0987654321",
      });
      expect(sockudo.config.token.key).to.equal("1234567890abcdef");
      expect(sockudo.config.token.secret).to.equal("fedcba0987654321");
    });

    it("should default `useTLS` to false", function () {
      const sockudo = new Sockudo({});
      expect(sockudo.config.scheme).to.equal("http");
    });

    it("should support `useTLS`", function () {
      const sockudo = new Sockudo({ useTLS: true });
      expect(sockudo.config.scheme).to.equal("https");
    });

    it("should support deprecated `encrypted`", function () {
      const sockudo = new Sockudo({ encrypted: true });
      expect(sockudo.config.scheme).to.equal("https");
    });

    it("should throw an exception if `useTLS` and `encrypted` are set", function () {
      expect(function () {
        new Sockudo({ useTLS: true, encrypted: false });
      }).to.throwException(
        /^Cannot set both `useTLS` and `encrypted` configuration options$/,
      );
    });

    it("should default `host` to 'localhost'", function () {
      const sockudo = new Sockudo({});
      expect(sockudo.config.host).to.equal("localhost");
    });

    it("should support `host`", function () {
      const sockudo = new Sockudo({ host: "example.org" });
      expect(sockudo.config.host).to.equal("example.org");
    });

    it("should support `cluster`", function () {
      const sockudo = new Sockudo({ cluster: "eu" });
      expect(sockudo.config.host).to.equal("api-eu.sockudo.com");
    });

    it("should have `host` override `cluster`", function () {
      const sockudo = new Sockudo({
        host: "api.staging.sockudo.com",
        cluster: "eu",
      });
      expect(sockudo.config.host).to.equal("api.staging.sockudo.com");
    });

    it("should default `port` to undefined", function () {
      const sockudo = new Sockudo({ useTLS: true });
      expect(sockudo.config.port).to.be(undefined);
    });

    it("should support `port`", function () {
      let sockudo = new Sockudo({ port: 8080 });
      expect(sockudo.config.port).to.equal(8080);

      sockudo = new Sockudo({ useTLS: true, port: 8080 });
      expect(sockudo.config.port).to.equal(8080);
    });

    it("should default `agent` to `undefined`", function () {
      const sockudo = new Sockudo({});
      expect(sockudo.config.agent).to.be(undefined);
    });

    it("should support `agent`", function () {
      const agent = new HttpsProxyAgent("https://test:tset@example.com");
      const sockudo = new Sockudo({ agent });
      expect(sockudo.config.agent).to.equal(agent);
    });

    it("should default `timeout` to `undefined`", function () {
      const sockudo = new Sockudo({});
      expect(sockudo.config.timeout).to.be(undefined);
    });

    it("should support `timeout`", function () {
      const sockudo = new Sockudo({ timeout: 1001 });
      expect(sockudo.config.timeout).to.equal(1001);
    });

    it("should support `encryptionMasterKey` of 32 bytes", function () {
      const key = "01234567890123456789012345678901";
      const sockudo = new Sockudo({ encryptionMasterKey: key });
      expect(sockudo.config.encryptionMasterKey.toString()).to.equal(key);
    });

    it("should reject `encryptionMasterKey` of 31 bytes", function () {
      const key = "0123456789012345678901234567890";
      expect(function () {
        new Sockudo({ encryptionMasterKey: key });
      }).to.throwException(/31 bytes/);
    });

    it("should reject `encryptionMasterKey` of 33 bytes", function () {
      const key = "012345678901234567890123456789012";
      expect(function () {
        new Sockudo({ encryptionMasterKey: key });
      }).to.throwException(/33 bytes/);
    });

    it("should support `encryptionMasterKeyBase64` which decodes to 32 bytes", function () {
      const key = "01234567890123456789012345678901";
      const keyBase64 = Buffer.from(key).toString("base64");
      const sockudo = new Sockudo({ encryptionMasterKeyBase64: keyBase64 });
      expect(sockudo.config.encryptionMasterKey.toString()).to.equal(key);
    });

    it("should reject `encryptionMasterKeyBase64` which decodes to 31 bytes", function () {
      const key = "0123456789012345678901234567890";
      const keyBase64 = Buffer.from(key).toString("base64");
      expect(function () {
        new Sockudo({ encryptionMasterKeyBase64: keyBase64 });
      }).to.throwException(/31 bytes/);
    });

    it("should reject `encryptionMasterKeyBase64` which decodes to 33 bytes", function () {
      const key = "012345678901234567890123456789012";
      const keyBase64 = Buffer.from(key).toString("base64");
      expect(function () {
        new Sockudo({ encryptionMasterKeyBase64: keyBase64 });
      }).to.throwException(/33 bytes/);
    });

    it("should reject `encryptionMasterKeyBase64` which is invalid base64", function () {
      const keyBase64 = "aGkgd(GhlcmUK";
      expect(function () {
        new Sockudo({ encryptionMasterKeyBase64: keyBase64 });
      }).to.throwException(/valid base64/);
    });
  });
});
