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

  describe("#forceReconnectUser", function () {
    it("should throw an error if user id is empty", function () {
      expect(function () {
        sockudo.forceReconnectUser("");
      }).to.throwError(function (e) {
        expect(e).to.be.an(Error);
        expect(e.message).to.equal("Invalid user id: ''");
      });
    });

    it("should throw an error if user id is not a string", function () {
      expect(function () {
        sockudo.forceReconnectUser(123);
      }).to.throwError(function (e) {
        expect(e).to.be.an(Error);
        expect(e.message).to.equal("Invalid user id: '123'");
      });
    });
  });

  it("should call /force_reconnect endpoint", function (done) {
    sinon.stub(sockudo, "post");
    sockudo.appId = 1234;
    const userId = "testUserId";

    sockudo.forceReconnectUser(userId);

    expect(sockudo.post.called).to.be(true);
    expect(sockudo.post.getCall(0).args[0]).eql({
      path: `/users/${userId}/force_reconnect`,
      body: {},
    });
    sockudo.post.restore();
    done();
  });
});
