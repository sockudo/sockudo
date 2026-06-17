using System;
using System.Net;
using System.Threading.Tasks;
using NUnit.Framework;
using PusherServer.Tests.Helpers;

namespace PusherServer.Tests.AcceptanceTests
{
    [TestFixture]
    public class When_querying_a_channel
    {
        [Test]
        public async Task It_should_return_the_state_asynchronously_when_given_a_channel_name_that_exists()
        {
            var channelName = "presence-state-channel-async-1";

            var pusherServer = ClientServerFactory.CreateServer();
            await ClientServerFactory.CreateClientAsync(pusherServer, channelName).ConfigureAwait(false);

            var info = new { info = "user_count" };
            var response = await pusherServer.FetchStateForChannelAsync<ChannelStateMessage>(channelName, info).ConfigureAwait(false);

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            Assert.AreEqual(1, response.Data.User_Count);
        }

        [Test]
        public async Task It_should_return_the_state_asynchronously_when_given_a_channel_name_that_exists_and_no_info_object_is_provided()
        {
            var channelName = "presence-state-channel-async-1";

            var pusherServer = ClientServerFactory.CreateServer();
            await ClientServerFactory.CreateClientAsync(pusherServer, channelName).ConfigureAwait(false);

            var response = await pusherServer.FetchStateForChannelAsync<ChannelStateMessage>(channelName).ConfigureAwait(false);

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            Assert.IsTrue(response.Data.Occupied);
        }

        [Test]
        public async Task It_should_not_return_the_state_based_asynchronously_when_given_a_channel_name_that_exists_an_bad_attributes()
        {
            var channelName = "presence-state-channel-async-2";

            var pusherServer = ClientServerFactory.CreateServer();
            await ClientServerFactory.CreateClientAsync(pusherServer, channelName).ConfigureAwait(false);

            var info = new { info = "does-not-exist" };

            var result = await pusherServer.FetchStateForChannelAsync<ChannelStateMessage>(channelName, info).ConfigureAwait(false);
            Assert.AreEqual(HttpStatusCode.BadRequest, result.StatusCode);
            StringAssert.IsMatch("info should be a comma separated list of attributes", result.Body);
        }

        [Test]
        public async Task It_should_throw_an_exception_when_given_an_empty_string_as_a_channel_name_async()
        {
            var pusherServer = ClientServerFactory.CreateServer();

            var info = new { info = "user_count" };

            ArgumentException caughtException = null;

            try
            {
                var response = await pusherServer.FetchStateForChannelAsync<ChannelStateMessage>(string.Empty, info).ConfigureAwait(false);
            }
            catch (ArgumentException ex)
            {
                caughtException = ex;
            }

            StringAssert.IsMatch("channelName cannot be null or empty", caughtException.Message);
        }

        [Test]
        public async Task It_should_throw_an_exception_when_given_a_null_as_a_channel_name_async()
        {
            var pusherServer = ClientServerFactory.CreateServer();

            var info = new { info = "user_count" };

            ArgumentException caughtException = null;

            try
            {
                var response = await pusherServer.FetchStateForChannelAsync<ChannelStateMessage>(null, info).ConfigureAwait(false);
            }
            catch (ArgumentException ex)
            {
                caughtException = ex;
            }

            StringAssert.IsMatch("channelName cannot be null or empty", caughtException.Message);
        }

        private class ChannelStateMessage
        {
            public bool Occupied { get; set; }
            public int User_Count { get; set; }
        }
    }

    [TestFixture]
    public class When_querying_multiple_channels
    {
        [Test]
        public async Task It_should_return_the_state_asynchronously_when_given_a_channel_name_that_exists()
        {
            var channelName = "presence-multiple-state-channel-async-3";

            var pusherServer = ClientServerFactory.CreateServer();
            await ClientServerFactory.CreateClientAsync(pusherServer, channelName).ConfigureAwait(false);

            var info = new { info = "user_count", filter_by_prefix = "presence-" };

            var result = await pusherServer.FetchStateForChannelsAsync<object>(info).ConfigureAwait(false);

            Assert.AreEqual(HttpStatusCode.OK, result.StatusCode);
            Assert.AreEqual(1, ((Newtonsoft.Json.Linq.JValue)((result.Data as Newtonsoft.Json.Linq.JObject)["channels"]["presence-multiple-state-channel-async-3"]["user_count"])).Value);
        }

        [Test]
        public async Task It_should_return_the_state_asynchronously_when_given_a_channel_name_that_exists_and_no_info_object_is_provided()
        {
            var channelName = "presence-multiple-state-channel-async-3";

            var pusherServer = ClientServerFactory.CreateServer();
            await ClientServerFactory.CreateClientAsync(pusherServer, channelName).ConfigureAwait(false);

            var result = await pusherServer.FetchStateForChannelsAsync<object>().ConfigureAwait(false);

            Assert.AreEqual(HttpStatusCode.OK, result.StatusCode);
        }

        [Test]
        public async Task It_should_not_return_the_state_asynchronously_based_when_given_a_channel_name_that_exists_an_bad_attributes()
        {
            string channelName = "presence-multiple-state-channel-async-4";

            var pusherServer = ClientServerFactory.CreateServer();
            await ClientServerFactory.CreateClientAsync(pusherServer, channelName).ConfigureAwait(false);

            var info = new { info = "does-not-exist" };

            var result = await pusherServer.FetchStateForChannelsAsync<object>(info).ConfigureAwait(false);

            Assert.AreEqual(HttpStatusCode.BadRequest, result.StatusCode);
            StringAssert.IsMatch("info should be a comma separated list of attributes", result.Body);
        }
    }
}
