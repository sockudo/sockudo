const expect = require("expect.js");
const fs = require("fs");
const path = require("path");

const Sockudo = require("../../dist/sockudo");
const Token = require("../../dist/token");
const WebHook = require("../../dist/webhook");

describe("WebHook", function () {
  let token;

  beforeEach(function () {
    token = new Token("123456789", "tofu");
  });

  describe("#isValid", function () {
    it("should return true for a webhook with correct signature", function () {
      const webhook = new WebHook(token, {
        headers: {
          "x-pusher-key": "123456789",
          "x-pusher-signature": "c17257e92037cd7de407ebc1ed174ceb7b2e518db127f44411b9ffc4f5b28cc5",
          "content-type": "application/json",
        },
        rawBody: JSON.stringify({
          time_ms: 1403175510755,
          events: [{ channel: "test_channel", name: "channel_vacated" }],
        }),
      });
      expect(webhook.isValid()).to.be(true);
    });

    it("should return false for a webhook with incorrect key", function () {
      const webhook = new WebHook(token, {
        headers: {
          "x-pusher-key": "000",
          "x-pusher-signature": "df1465f5ff93f83238152fd002cb904f9562d39569e68f00a6bfa0d8ccf88334",
          "content-type": "application/json",
        },
        rawBody: JSON.stringify({
          time_ms: 1403175510755,
          events: [{ channel: "test_channel", name: "channel_vacated" }],
        }),
      });
      expect(webhook.isValid()).to.be(false);
    });

    it("should return false for a webhook with incorrect signature", function () {
      const webhook = new WebHook(token, {
        headers: {
          "x-pusher-key": "123456789",
          "x-pusher-signature": "000",
          "content-type": "application/json",
        },
        rawBody: JSON.stringify({
          time_ms: 1403175510755,
          events: [{ channel: "test_channel", name: "channel_vacated" }],
        }),
      });
      expect(webhook.isValid()).to.be(false);
    });

    it("should return true if webhook is signed with the extra token", function () {
      const webhook = new WebHook(token, {
        headers: {
          "x-pusher-key": "1234",
          "x-pusher-signature": "c17257e92037cd7de407ebc1ed174ceb7b2e518db127f44411b9ffc4f5b28cc5",
          "content-type": "application/json",
        },
        rawBody: JSON.stringify({
          time_ms: 1403175510755,
          events: [{ channel: "test_channel", name: "channel_vacated" }],
        }),
      });
      expect(webhook.isValid(new Token("1234", "tofu"))).to.be(true);
    });

    it("should return true if webhook is signed with one of the extra tokens", function () {
      const webhook = new WebHook(token, {
        headers: {
          "x-pusher-key": "3",
          "x-pusher-signature": "c17257e92037cd7de407ebc1ed174ceb7b2e518db127f44411b9ffc4f5b28cc5",
          "content-type": "application/json",
        },
        rawBody: JSON.stringify({
          time_ms: 1403175510755,
          events: [{ channel: "test_channel", name: "channel_vacated" }],
        }),
      });
      expect(
        webhook.isValid([
          new Token("1", "nope"),
          new Token("2", "not really"),
          new Token("3", "tofu"),
        ]),
      ).to.be(true);
    });
  });

  describe("#isContentTypeValid", function () {
    it("should return true if content type is `application/json`", function () {
      const webhook = new WebHook(token, {
        headers: {
          "content-type": "application/json",
        },
        rawBody: JSON.stringify({}),
      });
      expect(webhook.isContentTypeValid()).to.be(true);
    });

    it("should return false if content type is not `application/json`", function () {
      const webhook = new WebHook(token, {
        headers: {
          "content-type": "application/weird",
        },
        rawBody: JSON.stringify({}),
      });
      expect(webhook.isContentTypeValid()).to.be(false);
    });
  });

  describe("#isBodyValid", function () {
    it("should return true if content type is `application/json` and body is valid JSON", function () {
      const webhook = new WebHook(token, {
        headers: {
          "content-type": "application/json",
        },
        rawBody: JSON.stringify({}),
      });
      expect(webhook.isBodyValid()).to.be(true);
    });

    it("should return false if content type is `application/json` and body is not valid JSON", function () {
      const webhook = new WebHook(token, {
        headers: {
          "content-type": "application/json",
        },
        rawBody: "not json!",
      });
      expect(webhook.isBodyValid()).to.be(false);
    });

    it("should return false if content type is not `application/json`", function () {
      const webhook = new WebHook(token, {
        headers: {
          "content-type": "application/weird",
        },
        rawBody: JSON.stringify({}),
      });
      expect(webhook.isContentTypeValid()).to.be(false);
    });
  });

  describe("#getData", function () {
    it("should return a parsed JSON body", function () {
      const webhook = new WebHook(token, {
        headers: { "content-type": "application/json" },
        rawBody: JSON.stringify({ foo: 9 }),
      });
      expect(webhook.getData()).to.eql({ foo: 9 });
    });

    it("should throw an error if content type is not `application/json`", function () {
      const body = JSON.stringify({ foo: 9 });
      const webhook = new WebHook(token, {
        headers: {
          "content-type": "application/weird",
          "x-pusher-signature": "f000000",
        },
        rawBody: body,
      });
      expect(function () {
        webhook.getData();
      }).to.throwError(function (e) {
        expect(e).to.be.a(Sockudo.WebHookError);
        expect(e.message).to.equal("Invalid WebHook body");
        expect(e.contentType).to.equal("application/weird");
        expect(e.body).to.equal(body);
        expect(e.signature).to.equal("f000000");
      });
    });

    it("should throw an error if body is not valid JSON", function () {
      const webhook = new WebHook(token, {
        headers: {
          "content-type": "application/json",
          "x-pusher-signature": "b00",
        },
        rawBody: "not json",
      });
      expect(function () {
        webhook.getData();
      }).to.throwError(function (e) {
        expect(e).to.be.a(Sockudo.WebHookError);
        expect(e.contentType).to.equal("application/json");
        expect(e.body).to.equal("not json");
        expect(e.signature).to.equal("b00");
      });
    });
  });

  describe("#getTime", function () {
    it("should return a correct date object", function () {
      const webhook = new WebHook(token, {
        headers: { "content-type": "application/json" },
        rawBody: JSON.stringify({ time_ms: 1403172023361 }),
      });
      expect(webhook.getTime()).to.eql(new Date(1403172023361));
    });
  });

  describe("#getEvents", function () {
    it("should return an array of events", function () {
      const webhook = new WebHook(token, {
        headers: { "content-type": "application/json" },
        rawBody: JSON.stringify({
          events: [1, 2, 3],
        }),
      });
      expect(webhook.getEvents()).to.eql([1, 2, 3]);
    });

    it("should accept shared future webhook event fixtures", function () {
      const body = fs.readFileSync(
        path.join(
          __dirname,
          "..",
          "..",
          "..",
          "..",
          "tests",
          "ai-conformance",
          "fixtures",
          "forward-compat",
          "future-webhook-events.json",
        ),
        "utf8",
      );
      const webhook = new WebHook(token, {
        headers: {
          "x-pusher-key": "123456789",
          "x-pusher-signature": token.sign(body),
          "content-type": "application/json",
        },
        rawBody: body,
      });

      expect(webhook.isValid()).to.be(true);
      expect(webhook.getTime()).to.eql(new Date(1710000000000));
      expect(webhook.getEvents()[0].name).to.equal("member_updated");
      expect(webhook.getEvents()[0].future_field).to.equal("must-pass-through");
      expect(webhook.getEvents()[1].turn_id).to.equal("turn-1");
      expect(webhook.getEvents()[2].version_serial).to.equal("ver-1");
    });

    it("should preserve nested future webhook values without coercion", function () {
      const body = JSON.stringify({
        time_ms: 1710000000000,
        events: [
          {
            name: "ai_turn_started",
            channel: "private-ai-forward",
            data: {
              turn_id: "turn-1",
              tokens: ["hello", "world"],
              done: false,
              nullable: null,
            },
            future_field: { nested: true },
          },
        ],
      });
      const webhook = new WebHook(token, {
        headers: {
          "x-pusher-key": "123456789",
          "x-pusher-signature": token.sign(body),
          "content-type": "application/json",
        },
        rawBody: body,
      });

      expect(webhook.isValid()).to.be(true);
      expect(webhook.getEvents()[0].name).to.equal("ai_turn_started");
      expect(webhook.getEvents()[0].data.tokens).to.eql(["hello", "world"]);
      expect(webhook.getEvents()[0].data.done).to.be(false);
      expect(webhook.getEvents()[0].data.nullable).to.be(null);
      expect(webhook.getEvents()[0].future_field).to.eql({ nested: true });
    });
  });
});
