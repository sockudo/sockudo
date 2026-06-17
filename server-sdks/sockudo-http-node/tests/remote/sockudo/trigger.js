const expect = require("expect.js");

const Sockudo = require("../../../dist/sockudo");

describe("Sockudo (integration)", function () {
  let sockudo;

  beforeEach(function () {
    if (!process.env.SOCKUDO_URL) {
      this.skip();
    }
    sockudo = Sockudo.forURL(process.env.SOCKUDO_URL);
  });

  describe("#trigger", function () {
    it("should return code 200", function (done) {
      sockudo
        .trigger("integration", "event", "test", null)
        .then((response) => {
          expect(response.status).to.equal(200);
          return response.json().then((body) => {
            expect(body).to.eql({});
            done();
          });
        })
        .catch(done);
    });
  });

  describe("#triggerBatch", function () {
    it("should return code 200", function (done) {
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
        .then((response) => {
          expect(response.status).to.equal(200);
          return response.json().then((body) => {
            expect(body).to.eql({});
            done();
          });
        })
        .catch(done);
    });
  });
});
