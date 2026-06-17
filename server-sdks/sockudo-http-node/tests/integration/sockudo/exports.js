const expect = require("expect.js");

const errors = require("../../../dist/errors");
const Sockudo = require("../../../dist/sockudo");
const Token = require("../../../dist/token");

describe("Sockudo", function () {
  it("should export `Token`", function () {
    expect(Sockudo.Token).to.be(Token);
  });

  it("should export `RequestError`", function () {
    expect(Sockudo.RequestError).to.be(errors.RequestError);
  });

  it("should export `WebHookError`", function () {
    expect(Sockudo.WebHookError).to.be(errors.WebHookError);
  });
});
