using System.Net;
using System.Threading.Tasks;
using NUnit.Framework;

namespace SockudoServer.Tests.AcceptanceTests
{
    [TestFixture]
    public class When_application_channels_are_queried
    {
        [Test]
        public async Task It_should_return_a_200_response()
        {
            IPusher pusher = new Pusher(Config.AppId, Config.AppKey, Config.AppSecret, new PusherOptions()
            {
                HostName = Config.HttpHost
            });

            IGetResult<object> result = await pusher.GetAsync<object>("/channels").ConfigureAwait(false);

            Assert.AreEqual(HttpStatusCode.OK, result.StatusCode);
        }

        [Test]
        public async Task It_should_be_possible_to_deserialize_the_request_result_body_as_an_object()
        {
            IPusher pusher = new Pusher(Config.AppId, Config.AppKey, Config.AppSecret, new PusherOptions()
            {
                HostName = Config.HttpHost
            });

            IGetResult<object> result = await pusher.GetAsync<object>("/channels").ConfigureAwait(false);

            Assert.NotNull(result.Data);
        }

        [Test]
        public async Task It_should_be_possible_to_deserialize_the_a_channels_result_body_as_an_ChannelsList()
        {
            IPusher pusher = new Pusher(Config.AppId, Config.AppKey, Config.AppSecret, new PusherOptions()
            {
                HostName = Config.HttpHost
            });

            IGetResult<ChannelsList> result = await pusher.GetAsync<ChannelsList>("/channels").ConfigureAwait(false);

            Assert.IsTrue(result.Data.Channels.Count >= 0);
        }
    }
}
