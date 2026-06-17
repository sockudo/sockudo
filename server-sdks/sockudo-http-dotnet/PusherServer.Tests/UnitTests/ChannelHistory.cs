using NSubstitute;
using NUnit.Framework;
using PusherServer.RestfulClient;
using PusherServer.Tests.Helpers;
using System.Threading.Tasks;

namespace PusherServer.Tests.UnitTests
{
    [TestFixture]
    public class When_querying_channel_history
    {
        private IPusher _pusher;
        private IPusherRestClient _subPusherClient;
        private IApplicationConfig _config;

        [SetUp]
        public void Setup()
        {
            _subPusherClient = Substitute.For<IPusherRestClient>();
            IPusherOptions options = new PusherOptions() { RestClient = _subPusherClient };
            _config = new ApplicationConfig { AppId = "test-app-id", AppKey = "test-app-key", AppSecret = "test-app-secret" };
            _pusher = new Pusher(_config.AppId, _config.AppKey, _config.AppSecret, options);
        }

        [Test]
        public async Task url_is_in_expected_format()
        {
            await _pusher.FetchHistoryForChannelAsync<object>("history-room", new { limit = 50, direction = "newest_first" }).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecuteGetAsync<object>(
#pragma warning restore 4014
                Arg.Is<IPusherRestRequest>(
                    x => x.ResourceUri.StartsWith("/apps/" + _config.AppId + "/channels/history-room/history") &&
                         x.ResourceUri.Contains("&limit=50") &&
                         x.ResourceUri.Contains("&direction=newest_first")
                )
            );
        }

        [Test]
        public async Task presence_history_url_is_in_expected_format()
        {
            await _pusher.FetchPresenceHistoryForChannelAsync<object>("presence-room", new { limit = 50, direction = "newest_first" }).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecuteGetAsync<object>(
#pragma warning restore 4014
                Arg.Is<IPusherRestRequest>(
                    x => x.ResourceUri.StartsWith("/apps/" + _config.AppId + "/channels/presence-room/presence/history") &&
                         x.ResourceUri.Contains("&limit=50") &&
                         x.ResourceUri.Contains("&direction=newest_first")
                )
            );
        }

        [Test]
        public async Task presence_snapshot_url_is_in_expected_format()
        {
            await _pusher.FetchPresenceSnapshotForChannelAsync<object>("presence-room", new { at_serial = 4 }).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecuteGetAsync<object>(
#pragma warning restore 4014
                Arg.Is<IPusherRestRequest>(
                    x => x.ResourceUri.StartsWith("/apps/" + _config.AppId + "/channels/presence-room/presence/history/snapshot") &&
                         x.ResourceUri.Contains("&at_serial=4")
                )
            );
        }
    }
}
