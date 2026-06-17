using System;
using Newtonsoft.Json;
using NUnit.Framework;
using PusherServer.Exceptions;
using PusherServer.Tests.Helpers;

namespace PusherServer.Tests.UnitTests
{
    [TestFixture]
    public class When_authenticating_a_user
    {
        private IPusher _pusher;

        [OneTimeSetUp]
        public void Setup()
        {
            _pusher = new Pusher(Config.AppId, Config.AppKey, Config.AppSecret);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void null_user_data_throw_Exception()
        {
            string socketId = "some_socket_id";

            _pusher.AuthenticateUser(socketId, null);
        }

        [Test]
        public void the_auth_response_is_valid()
        {
            string socketId = "123.456";

            UserData userData = new UserData()
            {
                id = "unique_user_id",
            };
            string userDataJson = DefaultSerializer.Default.Serialize(userData);

            string expectedAuthString = Config.AppKey + ":" + CreateSignedString(socketId, userDataJson);

            IUserAuthenticationResponse result = _pusher.AuthenticateUser(socketId, userData);
            Assert.AreEqual(expectedAuthString, result.auth);
            Assert.AreEqual(userDataJson, result.user_data);
        }

        [Test]
        public void with_watchlist_the_auth_response_is_valid()
        {
            string socketId = "123.456";

            UserData userData = new UserData()
            {
                id = "unique_user_id",
                watchlist = new string[] { "user1", "user2" },
                user_info = new { twitter_id = "@leggetter" }
            };
            string userDataJson = DefaultSerializer.Default.Serialize(userData);

            string expectedAuthString = Config.AppKey + ":" + CreateSignedString(socketId, userDataJson);

            IUserAuthenticationResponse result = _pusher.AuthenticateUser(socketId, userData);
            Assert.AreEqual(expectedAuthString, result.auth);
            Assert.AreEqual(userDataJson, result.user_data);
        }

        [Test]
        public void with_userinfo_the_auth_response_is_valid()
        {
            string socketId = "123.456";

            UserData userData = new UserData()
            {
                id = "unique_user_id",
                user_info = new { twitter_id = "@leggetter" }
            };
            string userDataJson = DefaultSerializer.Default.Serialize(userData);

            string expectedAuthString = Config.AppKey + ":" + CreateSignedString(socketId, userDataJson);

            IUserAuthenticationResponse result = _pusher.AuthenticateUser(socketId, userData);
            Assert.AreEqual(expectedAuthString, result.auth);
            Assert.AreEqual(userDataJson, result.user_data);
        }

        [Test]
        public void with_userinfo_and_watchlist_the_auth_response_is_valid()
        {
            string socketId = "123.456";

            UserData userData = new UserData()
            {
                id = "unique_user_id",
                watchlist = new string[] { "user1", "user2" },
                user_info = new { twitter_id = "@leggetter" }
            };
            string userDataJson = DefaultSerializer.Default.Serialize(userData);

            string expectedAuthString = Config.AppKey + ":" + CreateSignedString(socketId, userDataJson);

            IUserAuthenticationResponse result = _pusher.AuthenticateUser(socketId, userData);
            Assert.AreEqual(expectedAuthString, result.auth);
            Assert.AreEqual(userDataJson, result.user_data);
        }

        private string CreateSignedString(string socketId, string userDataJson)
        {
            var stringToSign = socketId + "::user::" + userDataJson;
            return CryptoHelper.GetHmac256(Config.AppSecret, stringToSign);
        }
    }
}
