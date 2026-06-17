const expect = require("expect.js");

const HttpsProxyAgent = require("https-proxy-agent");

const http_proxy = require("../../helpers/http_proxy");
const Sockudo = require("../../../dist/sockudo");

describe("Sockudo (integration)", function () {
  describe("with configured proxy", function () {
    let sockudo;
    let proxy;

    before(function (done) {
      proxy = http_proxy.start(done);
    });

    beforeEach(function () {
      if (!process.env.SOCKUDO_URL) {
        this.skip();
      }
      sockudo = Sockudo.forURL(process.env.SOCKUDO_URL, {
        agent: new HttpsProxyAgent("http://localhost:8321"),
      });
    });

    afterEach(function () {
      proxy.requests = 0;
    });

    after(function (done) {
      http_proxy.stop(proxy, done);
    });

    describe("#get", function () {
      it("should go through the proxy", function (done) {
        expect(proxy.requests).to.equal(0);
        sockudo
          .get({ path: "/channels" })
          .then((response) => {
            expect(proxy.requests).to.equal(1);
            expect(response.status).to.equal(200);
            response.json().then((body) => {
              expect(body.channels).to.be.an(Object);
              done();
            });
          })
          .catch(done);
      });
    });

    describe("#trigger", function () {
      it("should go through the proxy", function (done) {
        expect(proxy.requests).to.equal(0);
        sockudo
          .trigger("integration", "event", "test", null)
          .then((response) => {
            expect(proxy.requests).to.equal(1);
            expect(response.status).to.equal(200);
            response.json().then((body) => {
              expect(body).to.eql({});
              done();
            });
          })
          .catch(done);
      });
    });
  });
});
