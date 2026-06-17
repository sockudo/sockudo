const expect = require("expect.js");

const Sockudo = require("../../../dist/sockudo");

describe("Sockudo", function () {
  describe(".forUrl", function () {
    it("should set the `appId` attribute", function () {
      const sockudo = Sockudo.forURL(
        "https://123abc:def456@example.org/apps/4321",
      );
      expect(sockudo.config.appId).to.equal(4321);
    });

    it("should set the `token` attribute", function () {
      const sockudo = Sockudo.forURL(
        "https://123abc:def456@example.org/apps/4321",
      );
      expect(sockudo.config.token.key).to.equal("123abc");
      expect(sockudo.config.token.secret).to.equal("def456");
    });

    it("should set the `scheme` attribute", function () {
      const sockudo = Sockudo.forURL(
        "https://123abc:def456@example.org/apps/4321",
      );
      expect(sockudo.config.scheme).to.equal("https");
    });

    it("should set the `host` attribute", function () {
      const sockudo = Sockudo.forURL(
        "https://123abc:def456@example.org/apps/4321",
      );
      expect(sockudo.config.host).to.equal("example.org");
    });

    it("should set the `port` attribute if specified", function () {
      const sockudo = Sockudo.forURL(
        "https://123abc:def456@example.org:999/apps/4321",
      );
      expect(sockudo.config.port).to.equal(999);
    });

    it("should default the `port` attribute to undefined", function () {
      const sockudo = Sockudo.forURL(
        "http://123abc:def456@example.org/apps/4321",
      );
      expect(sockudo.config.port).to.be(undefined);
    });
  });
});
