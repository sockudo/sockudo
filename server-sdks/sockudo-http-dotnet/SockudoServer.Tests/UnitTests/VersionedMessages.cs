using NSubstitute;
using NUnit.Framework;
using SockudoServer.RestfulClient;
using SockudoServer.Tests.Helpers;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace SockudoServer.Tests.UnitTests
{
    [TestFixture]
    public class When_querying_versioned_messages
    {
        private ISockudo _sockudo;
        private ISockudoRestClient _subSockudoClient;
        private IApplicationConfig _config;

        private HttpResponseMessage _successfulResponse;

        [OneTimeSetUp]
        public void FixtureSetUp()
        {
            _successfulResponse = Substitute.For<HttpResponseMessage>();
            _successfulResponse.Content = new StringContent(TriggerResultHelper.TRIGGER_RESPONSE_JSON);
            _successfulResponse.StatusCode = HttpStatusCode.OK;
        }

        [SetUp]
        public void Setup()
        {
            _subSockudoClient = Substitute.For<ISockudoRestClient>();
            ISockudoOptions options = new SockudoOptions() { RestClient = _subSockudoClient };
            _config = new ApplicationConfig { AppId = "test-app-id", AppKey = "test-app-key", AppSecret = "test-app-secret" };
            _sockudo = new Sockudo(_config.AppId, _config.AppKey, _config.AppSecret, options);

            _subSockudoClient
                .ExecutePostAsync(Arg.Any<ISockudoRestRequest>())
                .Returns(Task.FromResult(new TriggerResult(_successfulResponse, TriggerResultHelper.TRIGGER_RESPONSE_JSON)));
        }

        [Test]
        public async Task get_message_url_is_in_expected_format()
        {
            await _sockudo.GetMessageAsync<object>("chat:room-1", "msg:1").ConfigureAwait(false);

#pragma warning disable 4014
            _subSockudoClient.Received().ExecuteGetAsync<object>(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(
                    x => x.ResourceUri.StartsWith("/apps/" + _config.AppId + "/channels/chat:room-1/messages/msg:1")
                )
            );
        }

        [Test]
        public async Task get_message_versions_url_is_in_expected_format()
        {
            await _sockudo.GetMessageVersionsAsync<object>("chat:room-1", "msg:1", new { limit = 10, direction = "oldest_first" }).ConfigureAwait(false);

#pragma warning disable 4014
            _subSockudoClient.Received().ExecuteGetAsync<object>(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(
                    x => x.ResourceUri.StartsWith("/apps/" + _config.AppId + "/channels/chat:room-1/messages/msg:1/versions") &&
                         x.ResourceUri.Contains("&limit=10") &&
                         x.ResourceUri.Contains("&direction=oldest_first")
                )
            );
        }

        [Test]
        public async Task update_message_url_is_in_expected_format()
        {
            await _sockudo.UpdateMessageAsync<object>("chat:room-1", "msg:1", new { data = "hello brave" }).ConfigureAwait(false);

#pragma warning disable 4014
            _subSockudoClient.Received().ExecutePostAsync(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(
                    x => x.ResourceUri.StartsWith("/apps/" + _config.AppId + "/channels/chat:room-1/messages/msg:1/update")
                )
            );
        }

        [Test]
        public async Task delete_message_url_is_in_expected_format()
        {
            await _sockudo.DeleteMessageAsync<object>("chat:room-1", "msg:1", new { clear_fields = new[] { "data", "extras" } }).ConfigureAwait(false);

#pragma warning disable 4014
            _subSockudoClient.Received().ExecutePostAsync(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(
                    x => x.ResourceUri.StartsWith("/apps/" + _config.AppId + "/channels/chat:room-1/messages/msg:1/delete")
                )
            );
        }

        [Test]
        public async Task append_message_url_is_in_expected_format()
        {
            await _sockudo.AppendMessageAsync<object>("chat:room-1", "msg:1", new { data = " world" }).ConfigureAwait(false);

#pragma warning disable 4014
            _subSockudoClient.Received().ExecutePostAsync(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(
                    x => x.ResourceUri.StartsWith("/apps/" + _config.AppId + "/channels/chat:room-1/messages/msg:1/append")
                )
            );
        }
    }
}
