const expect = require("expect.js");
const nock = require("nock");

const Sockudo = require("../../../dist/sockudo");
const sinon = require("sinon");

describe("Sockudo", function () {
  let sockudo;

  beforeEach(function () {
    sockudo = new Sockudo({ appId: 1234, key: "f00d", secret: "tofu" });
    nock.disableNetConnect();
  });

  afterEach(function () {
    nock.cleanAll();
    nock.enableNetConnect();
  });

  describe("#terminateUserConnections", function () {
    it("should throw an error if user id is empty", function () {
      expect(function () {
        sockudo.terminateUserConnections("");
      }).to.throwError(function (e) {
        expect(e).to.be.an(Error);
        expect(e.message).to.equal("Invalid user id: ''");
      });
    });

    it("should throw an error if user id is not a string", function () {
      expect(function () {
        sockudo.terminateUserConnections(123);
      }).to.throwError(function (e) {
        expect(e).to.be.an(Error);
        expect(e.message).to.equal("Invalid user id: '123'");
      });
    });
  });

  it("should call /terminate_connections endpoint", function (done) {
    sinon.stub(sockudo, "post");
    sockudo.appId = 1234;
    const userId = "testUserId";

    sockudo.terminateUserConnections(userId);

    expect(sockudo.post.called).to.be(true);
    expect(sockudo.post.getCall(0).args[0]).eql({
      path: `/users/${userId}/terminate_connections`,
      body: {},
    });
    sockudo.post.restore();
    done();
  });
});
