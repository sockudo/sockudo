using System;
using Newtonsoft.Json;
using NUnit.Framework;
using PusherServer.Exceptions;
using PusherServer.Tests.Helpers;

namespace PusherServer.Tests.UnitTests
{
    [TestFixture]
    public class When_authorizing_a_private_channel
    {
        IPusher _pusher;

        [OneTimeSetUp]
        public void Setup()
        {
            _pusher = new Pusher(Config.AppId, Config.AppKey, Config.AppSecret);
        }

        [Test]
        public void the_auth_response_is_valid()
        {
            string channelName = "my-channel";
            string socketId = "123.456";

            string expectedAuthString = Config.AppKey + ":" + CreateSignedString(channelName, socketId);

            IChannelAuthorizationResponse result = _pusher.AuthorizeChannel(channelName, socketId);
            Assert.AreEqual(expectedAuthString, result.auth);
            Assert.IsNull(result.shared_secret, nameof(result.shared_secret));
        }

        [Test]
        [ExpectedException(typeof(SocketIdFormatException))]
        public void socket_id_cannot_contain_colon_prefix()
        {
            _pusher.AuthorizeChannel("private-test", ":444.444");
        }

        [Test]
        [ExpectedException(typeof(SocketIdFormatException))]
        public void socket_id_cannot_contain_colon_suffix()
        {
            _pusher.AuthorizeChannel("private-test", "444.444:");
        }

        [Test]
        [ExpectedException(typeof(SocketIdFormatException))]
        public void socket_id_cannot_contain_letters_suffix()
        {
            _pusher.AuthorizeChannel("private-test", "444.444a");
        }

        [Test]
        [ExpectedException(typeof(SocketIdFormatException))]
        public void socket_id_must_contain_a_period_point()
        {
            _pusher.AuthorizeChannel("private-test", "444");
        }

        [Test]
        [ExpectedException(typeof(SocketIdFormatException))]
        public void socket_id_must_not_contain_newline_prefix()
        {
            _pusher.AuthorizeChannel("private-test", "\n444.444");
        }

        [Test]
        [ExpectedException(typeof(SocketIdFormatException))]
        public void socket_id_must_not_contain_newline_suffix()
        {
            _pusher.AuthorizeChannel("private-test", "444.444\n");
        }

        [Test]
        [ExpectedException(typeof(SocketIdFormatException))]
        public void socket_id_must_not_be_empty_string()
        {
            _pusher.AuthorizeChannel("private-test", string.Empty);
        }

        [Test]
        [ExpectedException(typeof(ChannelNameFormatException))]
        public void channel_must_not_have_trailing_colon()
        {
            AuthWithChannelName("private-channel:");
        }
        [Test]
        [ExpectedException(typeof(ChannelNameFormatException))]
        public void channel_name_must_not_have_leading_colon()
        {
            AuthWithChannelName(":private-channel");
        }

        [Test]
        [ExpectedException(typeof(ChannelNameFormatException))]
        public void channel_name_must_not_have_leading_colon_newline()
        {
            AuthWithChannelName(":\nprivate-channel");
        }

        [Test]
        [ExpectedException(typeof(ChannelNameFormatException))]
        public void channel_name_must_not_have_trailing_colon_newline()
        {
            AuthWithChannelName("private-channel\n:");
        }

        [Test]
        [ExpectedException(typeof(ChannelNameLengthExceededException))]
        public void channel_names_must_not_exceed_allowed_length()
        {
            var channelName = new String('a', ValidationHelper.CHANNEL_NAME_MAX_LENGTH + 1);
            AuthWithChannelName(channelName);
        }

        private void AuthWithChannelName(string channelName)
        {
            _pusher.AuthorizeChannel(channelName, "123.456");
        }

        private string CreateSignedString(string channelName, string socketId)
        {
            // null for presence data
            var stringToSign = socketId + ":" + channelName;
            return CryptoHelper.GetHmac256(Config.AppSecret, stringToSign);
        }
    }

    [TestFixture]
    public class When_authorizing_a_private_encrypted_channel
    {
        [Test]
        public void the_auth_response_is_valid()
        {
            string channelName = "private-encrypted-channel";
            Pusher pusher = new Pusher(Config.AppId, Config.AppKey, Config.AppSecret, new PusherOptions
            {
                EncryptionMasterKey = DataHelper.GenerateEncryptionMasterKey(),
            });

            IChannelAuthorizationResponse result = pusher.AuthorizeChannel(channelName, "123.456");

            Assert.IsNotNull(result, nameof(IAuthenticationData));
            Assert.IsNotNull(result.shared_secret, nameof(result.shared_secret));
            Assert.IsNotNull(Convert.FromBase64String(result.shared_secret));
        }

        [Test]
        [ExpectedException(typeof(EncryptionMasterKeyException))]
        public void encryption_master_key_should_be_defined()
        {
            string channelName = "private-encrypted-channel";
            Pusher pusher = new Pusher(Config.AppId, Config.AppKey, Config.AppSecret);
            pusher.AuthorizeChannel(channelName, "123.456");
        }


        [Test]
        [ExpectedException(typeof(EncryptionMasterKeyException))]
        public void encryption_master_key_should_be_well_defined()
        {
            string channelName = "private-encrypted-channel";
            Pusher pusher = new Pusher(Config.AppId, Config.AppKey, Config.AppSecret, new PusherOptions
            {
                EncryptionMasterKey = new byte[] { 1, 2 },
            });
            pusher.AuthorizeChannel(channelName, "123.456");
        }
    }

    [TestFixture]
    public class When_authorizing_a_presence_channel
    {
        private IPusher _pusher;

        [OneTimeSetUp]
        public void Setup()
        {
            _pusher = new Pusher(Config.AppId, Config.AppKey, Config.AppSecret);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void null_presence_data_throw_Exception()
        {
            string channelName = "my-channel";
            string socketId = "some_socket_id";

            _pusher.AuthorizeChannel(channelName, socketId, null);
        }

        [Test]
        public void the_auth_response_is_valid()
        {
            string channelName = "my-channel";
            string socketId = "123.456";

            PresenceChannelData data = new PresenceChannelData()
            {
                user_id = "unique_user_id",
                user_info = new { twitter_id = "@leggetter" }
            };
            string presenceJson = DefaultSerializer.Default.Serialize(data);

            string expectedAuthString = Config.AppKey + ":" + CreateSignedString(channelName, socketId, presenceJson);

            IChannelAuthorizationResponse result = _pusher.AuthorizeChannel(channelName, socketId, data);
            Assert.AreEqual(expectedAuthString, result.auth);
            Assert.IsNull(result.shared_secret, nameof(result.shared_secret));
        }

        [Test]
        public void channel_data_is_encoded_as_JSON()
        {
            string channelName = "my-channel";
            string socketId = "123.456";

            PresenceChannelData data = new PresenceChannelData()
            {
                user_id = "unique_user_id",
                user_info = new { twitter_id = "@leggetter" }
            };

            string expectedChannelData = DefaultSerializer.Default.Serialize(data);

            IChannelAuthorizationResponse result = _pusher.AuthorizeChannel(channelName, socketId, data);
            Assert.AreEqual(expectedChannelData, result.channel_data);
        }

        private string CreateSignedString(string channelName, string socketId, string presenceJson)
        {
            var stringToSign = socketId + ":" + channelName + ":" + presenceJson;
            return CryptoHelper.GetHmac256(Config.AppSecret, stringToSign);
        }
    }
}
