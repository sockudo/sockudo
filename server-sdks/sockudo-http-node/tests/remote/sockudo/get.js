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

  describe("#get", function () {
    describe("/channels", function () {
      it("should return channels as an object", function (done) {
        sockudo
          .get({ path: "/channels" })
          .then((response) => {
            expect(response.status).to.equal(200);
            return response.json().then((body) => {
              expect(body.channels).to.be.an(Object);
              done();
            });
          })
          .catch(done);
      });
    });

    describe("/channels/CHANNEL", function () {
      it("should return if the channel is occupied", function (done) {
        sockudo
          .get({ path: "/channels/CHANNEL" })
          .then((response) => {
            expect(response.status).to.equal(200);
            return response.json().then((body) => {
              expect(body.occupied).to.be.a("boolean");
              done();
            });
          })
          .catch(done);
      });
    });

    describe("/channels/CHANNEL/users", function () {
      it("should return code 400 for non-presence channels", function (done) {
        sockudo.get({ path: "/channels/CHANNEL/users" }).catch((error) => {
          expect(error).to.be.a(Sockudo.RequestError);
          expect(error.message).to.equal("Unexpected status code 400");
          expect(error.status).to.equal(400);
          done();
        });
      });
    });
  });
});
