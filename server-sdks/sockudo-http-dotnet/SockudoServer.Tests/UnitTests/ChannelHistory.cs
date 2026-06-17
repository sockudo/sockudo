using NSubstitute;
using NUnit.Framework;
using SockudoServer.RestfulClient;
using SockudoServer.Tests.Helpers;
using System.Threading.Tasks;

namespace SockudoServer.Tests.UnitTests
{
    [TestFixture]
    public class When_querying_channel_history
    {
        private ISockudo _sockudo;
        private ISockudoRestClient _subSockudoClient;
        private IApplicationConfig _config;

        [SetUp]
        public void Setup()
        {
            _subSockudoClient = Substitute.For<ISockudoRestClient>();
            ISockudoOptions options = new SockudoOptions() { RestClient = _subSockudoClient };
            _config = new ApplicationConfig { AppId = "test-app-id", AppKey = "test-app-key", AppSecret = "test-app-secret" };
            _sockudo = new Sockudo(_config.AppId, _config.AppKey, _config.AppSecret, options);
        }

        [Test]
        public async Task url_is_in_expected_format()
        {
            await _sockudo.FetchHistoryForChannelAsync<object>("history-room", new { limit = 50, direction = "newest_first" }).ConfigureAwait(false);

#pragma warning disable 4014
            _subSockudoClient.Received().ExecuteGetAsync<object>(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(
                    x => x.ResourceUri.StartsWith("/apps/" + _config.AppId + "/channels/history-room/history") &&
                         x.ResourceUri.Contains("&limit=50") &&
                         x.ResourceUri.Contains("&direction=newest_first")
                )
            );
        }
    }
}
