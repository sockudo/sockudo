const expect = require("expect.js");

const Sockudo = require("../../../dist/sockudo");
const WebHook = require("../../../dist/webhook");

describe("Sockudo", function () {
  let sockudo;

  beforeEach(function () {
    sockudo = new Sockudo({ appId: 10000, key: "aaaa", secret: "tofu" });
  });

  describe("#webhook", function () {
    it("should return a WebHook instance", function () {
      expect(sockudo.webhook({ headers: {}, body: "" })).to.be.a(WebHook);
    });

    it("should pass the token to the WebHook", function () {
      expect(sockudo.webhook({ headers: {}, body: "" }).token).to.be(
        sockudo.config.token,
      );
    });
  });
});
