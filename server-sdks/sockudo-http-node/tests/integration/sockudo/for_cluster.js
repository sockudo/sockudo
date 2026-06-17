const expect = require("expect.js");

const Sockudo = require("../../../dist/sockudo");

describe("Sockudo", function () {
  describe(".forCluster", function () {
    it("should generate a hostname for the cluster", function () {
      const sockudo = Sockudo.forCluster("test");
      expect(sockudo.config.host).to.equal("api-test.sockudo.com");
    });

    it("should override the hostname if set in the extra options", function () {
      const sockudo = Sockudo.forCluster("eu", {
        host: "api.staging.sockudo.com",
      });
      expect(sockudo.config.host).to.equal("api-eu.sockudo.com");
    });

    it("should use the cluster option passed as first param not the option", function () {
      const sockudo = Sockudo.forCluster("eu", {
        cluster: "mt1",
      });
      expect(sockudo.config.host).to.equal("api-eu.sockudo.com");
    });
  });
});
