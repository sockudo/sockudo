const expect = require("expect.js");

const Sockudo = require("../../../dist/sockudo");

describe("Sockudo", function () {
  let sockudo;

  beforeEach(function () {
    sockudo = new Sockudo({ appId: 10000, key: "aaaa", secret: "tofu" });
  });

  describe("#authenticateUser", function () {
    it("should prefix the signature with the app key", function () {
      let sockudo = new Sockudo({ appId: 10000, key: "1234", secret: "tofu" });
      expect(sockudo.authenticateUser("123.456", { id: "45678" })).to.eql({
        auth: "1234:f4b1fdeea7c93e32648c7230e32b172057c5623cace6cfce791c6e7035e0babd",
        user_data: '{"id":"45678"}',
      });

      sockudo = new Sockudo({ appId: 10000, key: "abcdef", secret: "tofu" });
      expect(sockudo.authenticateUser("123.456", { id: "45678" })).to.eql({
        auth: "abcdef:f4b1fdeea7c93e32648c7230e32b172057c5623cace6cfce791c6e7035e0babd",
        user_data: '{"id":"45678"}',
      });
    });

    it("should return correct authentication signatures for different user data", function () {
      expect(sockudo.authenticateUser("123.456", { id: "45678" })).to.eql({
        auth: "aaaa:f4b1fdeea7c93e32648c7230e32b172057c5623cace6cfce791c6e7035e0babd",
        user_data: '{"id":"45678"}',
      });
      expect(
        sockudo.authenticateUser("123.456", { id: "55555", user_name: "test" }),
      ).to.eql({
        auth: "aaaa:b8a9f173455903792ae2b788add0c4c78ad7372b3ae7fb5769479276a1993743",
        user_data: JSON.stringify({ id: "55555", user_name: "test" }),
      });
    });

    it("should return correct authentication signatures for different secrets", function () {
      let sockudo = new Sockudo({ appId: 10000, key: "11111", secret: "1" });
      expect(sockudo.authenticateUser("123.456", { id: "45678" })).to.eql({
        auth: "11111:79bddf29fe8e2153dd5d8d569b3f45e5aeb26ae2eb4758879d844791b466cfa2",
        user_data: '{"id":"45678"}',
      });
      sockudo = new Sockudo({ appId: 10000, key: "11111", secret: "2" });
      expect(sockudo.authenticateUser("123.456", { id: "45678" })).to.eql({
        auth: "11111:a542498ffa6faf6de7c17a8106b923c042319bd73acfd1d274df32e269b55d1f",
        user_data: '{"id":"45678"}',
      });
    });

    it("should return correct authentication signature with utf-8 in user data", function () {
      expect(sockudo.authenticateUser("1.1", { id: "ą§¶™€łü€ß£" })).to.eql({
        auth: "aaaa:620494cee53d6c568b49598313194088afda37218f0d059af03c0c898ed61ff4",
        user_data: '{"id":"ą§¶™€łü€ß£"}',
      });
    });

    it("should raise an exception if socket id is not a string", function () {
      expect(function () {
        sockudo.authenticateUser(undefined, { id: "123" });
      }).to.throwException(/^Invalid socket id: 'undefined'$/);
      expect(function () {
        sockudo.authenticateUser(null, { id: "123" });
      }).to.throwException(/^Invalid socket id: 'null'$/);
      expect(function () {
        sockudo.authenticateUser(111, { id: "123" });
      }).to.throwException(/^Invalid socket id: '111'$/);
    });

    it("should raise an exception if socket id is an empty string", function () {
      expect(function () {
        sockudo.authenticateUser("", { id: "123" });
      }).to.throwException(/^Invalid socket id: ''$/);
    });

    it("should raise an exception if socket id is invalid", function () {
      expect(function () {
        sockudo.authenticateUser("1.1:", { id: "123" });
      }).to.throwException(/^Invalid socket id/);
      expect(function () {
        sockudo.authenticateUser(":1.1", { id: "123" });
      }).to.throwException(/^Invalid socket id/);
      expect(function () {
        sockudo.authenticateUser(":\n1.1", { id: "123" });
      }).to.throwException(/^Invalid socket id/);
      expect(function () {
        sockudo.authenticateUser("1.1\n:", { id: "123" });
      }).to.throwException(/^Invalid socket id/);
    });

    it("should raise an exception if user data is not a non-null object", function () {
      expect(function () {
        sockudo.authenticateUser("111.222", undefined);
      }).to.throwException(/^Invalid user data: 'undefined'$/);
      expect(function () {
        sockudo.authenticateUser("111.222", null);
      }).to.throwException(/^Invalid user data: 'null'$/);
      expect(function () {
        sockudo.authenticateUser("111.222", 111);
      }).to.throwException(/^Invalid user data: '111'$/);
      expect(function () {
        sockudo.authenticateUser("111.222", "");
      }).to.throwException(/^Invalid user data: ''$/);
      expect(function () {
        sockudo.authenticateUser("111.222", "abc");
      }).to.throwException(/^Invalid user data: 'abc'$/);
    });

    it("should raise an exception if user data doesn't have a valid id field", function () {
      expect(function () {
        sockudo.authenticateUser("111.222", {});
      }).to.throwException(/^Invalid user id: 'undefined'$/);
      expect(function () {
        sockudo.authenticateUser("111.222", { id: "" });
      }).to.throwException(/^Invalid user id: ''$/);
      expect(function () {
        sockudo.authenticateUser("111.222", { id: 123 });
      }).to.throwException(/^Invalid user id: '123'$/);
    });
  });

  describe("#authenticate", function () {
    it("should be the exactly the same as authorizeChannel", function () {
      expect(sockudo.authenticate).to.eql(sockudo.authorizeChannel);
    });
  });

  describe("#authorizeChannel", function () {
    it("should prefix the signature with the app key", function () {
      let sockudo = new Sockudo({ appId: 10000, key: "1234", secret: "tofu" });
      expect(sockudo.authorizeChannel("123.456", "test")).to.eql({
        auth: "1234:efa6cf7644a0b35cba36aa0f776f3cbf7bb60e95ea2696bde1dbe8403b61bd7c",
      });

      sockudo = new Sockudo({ appId: 10000, key: "abcdef", secret: "tofu" });
      expect(sockudo.authorizeChannel("123.456", "test")).to.eql({
        auth: "abcdef:efa6cf7644a0b35cba36aa0f776f3cbf7bb60e95ea2696bde1dbe8403b61bd7c",
      });
    });

    it("should return correct authentication signatures for different socket ids", function () {
      expect(sockudo.authorizeChannel("123.456", "test")).to.eql({
        auth: "aaaa:efa6cf7644a0b35cba36aa0f776f3cbf7bb60e95ea2696bde1dbe8403b61bd7c",
      });
      expect(sockudo.authorizeChannel("321.654", "test")).to.eql({
        auth: "aaaa:f6ecb0a17d3e4f68aca28f1673197a7608587c09deb0208faa4b5519aee0a777",
      });
    });

    it("should return correct authentication signatures for different channels", function () {
      expect(sockudo.authorizeChannel("123.456", "test1")).to.eql({
        auth: "aaaa:d5ab857f805433cb50562da96afa41688d7742a3c3a021ed15a4d991a4d8cf94",
      });
      expect(sockudo.authorizeChannel("123.456", "test2")).to.eql({
        auth: "aaaa:43affa6a09af1fb9ce1cadf176171346beaf7366673ec1e5920f68b3e97a466d",
      });
    });

    it("should return correct authentication signatures for different secrets", function () {
      let sockudo = new Sockudo({ appId: 10000, key: "11111", secret: "1" });
      expect(sockudo.authorizeChannel("123.456", "test")).to.eql({
        auth: "11111:584828bd6e80b2d177d2b28fde07b8e170abf87ccb5a791a50c933711fb8eb28",
      });
      sockudo = new Sockudo({ appId: 10000, key: "11111", secret: "2" });
      expect(sockudo.authorizeChannel("123.456", "test")).to.eql({
        auth: "11111:269bbf3f7625db4e0d0525b617efa5915c3ae667fd222dc8e4cb94bc531f26f2",
      });
    });

    it("should return the channel data", function () {
      expect(
        sockudo.authorizeChannel("123.456", "test", { foo: "bar" })
          .channel_data,
      ).to.eql('{"foo":"bar"}');
    });

    it("should return correct authentication signatures with and without the channel data", function () {
      expect(sockudo.authorizeChannel("123.456", "test")).to.eql({
        auth: "aaaa:efa6cf7644a0b35cba36aa0f776f3cbf7bb60e95ea2696bde1dbe8403b61bd7c",
      });
      expect(
        sockudo.authorizeChannel("123.456", "test", { foo: "bar" }),
      ).to.eql({
        auth: "aaaa:f41faf9ead2ea76772cc6b1168363057459f02499ae4d92e88229dc7f4efa2d4",
        channel_data: '{"foo":"bar"}',
      });
    });

    it("should return correct authentication signature with utf-8 in channel data", function () {
      expect(sockudo.authorizeChannel("1.1", "test", "ą§¶™€łü€ß£")).to.eql({
        auth: "aaaa:2a229263e89d9c50524fd80c2e88be2843379f6931e28995e2cc214282c9db0c",
        channel_data: '"ą§¶™€łü€ß£"',
      });
    });

    it("should raise an exception if socket id is not a string", function () {
      expect(function () {
        sockudo.authorizeChannel(undefined, "test");
      }).to.throwException(/^Invalid socket id: 'undefined'$/);
      expect(function () {
        sockudo.authorizeChannel(null, "test");
      }).to.throwException(/^Invalid socket id: 'null'$/);
      expect(function () {
        sockudo.authorizeChannel(111, "test");
      }).to.throwException(/^Invalid socket id: '111'$/);
    });

    it("should raise an exception if socket id is an empty string", function () {
      expect(function () {
        sockudo.authorizeChannel("", "test");
      }).to.throwException(/^Invalid socket id: ''$/);
    });

    it("should raise an exception if socket id is invalid", function () {
      expect(function () {
        sockudo.authorizeChannel("1.1:", "test");
      }).to.throwException(/^Invalid socket id/);
      expect(function () {
        sockudo.authorizeChannel(":1.1", "test");
      }).to.throwException(/^Invalid socket id/);
      expect(function () {
        sockudo.authorizeChannel(":\n1.1", "test");
      }).to.throwException(/^Invalid socket id/);
      expect(function () {
        sockudo.authorizeChannel("1.1\n:", "test");
      }).to.throwException(/^Invalid socket id/);
    });

    it("should raise an exception if channel name is not a string", function () {
      expect(function () {
        sockudo.authorizeChannel("111.222", undefined);
      }).to.throwException(/^Invalid channel name: 'undefined'$/);
      expect(function () {
        sockudo.authorizeChannel("111.222", null);
      }).to.throwException(/^Invalid channel name: 'null'$/);
      expect(function () {
        sockudo.authorizeChannel("111.222", 111);
      }).to.throwException(/^Invalid channel name: '111'$/);
    });

    it("should raise an exception if channel name is an empty string", function () {
      expect(function () {
        sockudo.authorizeChannel("111.222", "");
      }).to.throwException(/^Invalid channel name: ''$/);
    });

    it("should throw an error for private-encrypted- channels", function () {
      expect(function () {
        sockudo.authorizeChannel("123.456", "private-encrypted-bla", "foo");
      }).to.throwException(
        "Cannot generate shared_secret because encryptionMasterKey is not set",
      );
    });
  });
});

describe("Sockudo with encryptionMasterKey", function () {
  let sockudo;

  const testMasterKeyBase64 = "zyrm8pvV2C9fJcBfhyXzvxbJVN/H7QLmbe0xJi1GhPU=";

  beforeEach(function () {
    sockudo = new Sockudo({
      appId: 1234,
      key: "f00d",
      secret: "tofu",
      encryptionMasterKeyBase64: testMasterKeyBase64,
    });
  });

  describe("#authorizeChannel", function () {
    it("should return a shared_secret for private-encrypted- channels", function () {
      expect(
        sockudo.authorizeChannel("123.456", "private-encrypted-bla", "foo"),
      ).to.eql({
        auth: "f00d:962c48b78bf93d98ff4c92ee7dff04865821455b7b401e9d60a9e0a90af2c105",
        channel_data: '"foo"',
        shared_secret: "nlr49ISQHz91yS3cy/yWmW8wFMNeTnNL5tNHnbPJcLQ=",
      });
    });
    it("should not return a shared_secret for non-encrypted channels", function () {
      expect(sockudo.authorizeChannel("123.456", "bla", "foo")).to.eql({
        auth: "f00d:013ad3da0d88e0df6ae0a8184bef50b9c3933f2344499e6e3d1ad67fad799e20",
        channel_data: '"foo"',
      });
    });
  });
});
