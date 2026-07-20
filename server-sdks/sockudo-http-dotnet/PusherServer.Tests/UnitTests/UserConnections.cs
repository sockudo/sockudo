using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using NSubstitute;
using NUnit.Framework;
using PusherServer.RestfulClient;
using PusherServer.Tests.Helpers;

namespace PusherServer.Tests.UnitTests
{
    [TestFixture]
    public class When_managing_user_connections
    {
        private Pusher _pusher;
        private IPusherRestClient _restClient;
        private IApplicationConfig _config;

        [SetUp]
        public void Setup()
        {
            _restClient = Substitute.For<IPusherRestClient>();

            var options = new PusherOptions
            {
                RestClient = _restClient
            };

            _config = new ApplicationConfig
            {
                AppId = "test-app-id",
                AppKey = "test-app-key",
                AppSecret = "test-app-secret",
            };

            _pusher = new Pusher(_config.AppId, _config.AppKey, _config.AppSecret, options);

            var okResponse = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{}")
            };

            _restClient.ExecutePostRawAsync(Arg.Any<IPusherRestRequest>())
                .Returns(Task.FromResult(new RawRequestResult(okResponse, "{}")));
        }

        [Test]
        public async Task terminate_user_connections_posts_to_correct_path()
        {
            await _pusher.TerminateUserConnectionsAsync("user-123").ConfigureAwait(false);

#pragma warning disable 4014
            _restClient.Received().ExecutePostRawAsync(
#pragma warning restore 4014
                Arg.Is<IPusherRestRequest>(request =>
                    request.ResourceUri.StartsWith("/apps/" + _config.AppId + "/users/user-123/terminate_connections?") &&
                    request.Method == PusherMethod.POST
                )
            );
        }

        [Test]
        public async Task force_reconnect_user_posts_to_correct_path()
        {
            await _pusher.ForceReconnectUserAsync("user-456").ConfigureAwait(false);

#pragma warning disable 4014
            _restClient.Received().ExecutePostRawAsync(
#pragma warning restore 4014
                Arg.Is<IPusherRestRequest>(request =>
                    request.ResourceUri.StartsWith("/apps/" + _config.AppId + "/users/user-456/force_reconnect?") &&
                    request.Method == PusherMethod.POST
                )
            );
        }

        [Test]
        public void terminate_user_connections_throws_on_null_user_id()
        {
            Assert.ThrowsAsync<ArgumentException>(async () =>
                await _pusher.TerminateUserConnectionsAsync(null).ConfigureAwait(false));
        }

        [Test]
        public void terminate_user_connections_throws_on_empty_user_id()
        {
            Assert.ThrowsAsync<ArgumentException>(async () =>
                await _pusher.TerminateUserConnectionsAsync(string.Empty).ConfigureAwait(false));
        }

        [Test]
        public void force_reconnect_user_throws_on_null_user_id()
        {
            Assert.ThrowsAsync<ArgumentException>(async () =>
                await _pusher.ForceReconnectUserAsync(null).ConfigureAwait(false));
        }

        [Test]
        public void force_reconnect_user_throws_on_empty_user_id()
        {
            Assert.ThrowsAsync<ArgumentException>(async () =>
                await _pusher.ForceReconnectUserAsync(string.Empty).ConfigureAwait(false));
        }
    }
}
