using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using NSubstitute;
using NUnit.Framework;
using SockudoServer.RestfulClient;
using SockudoServer.Tests.Helpers;

namespace SockudoServer.Tests.UnitTests
{
    [TestFixture]
    public class When_managing_user_connections
    {
        private Sockudo _sockudo;
        private ISockudoRestClient _restClient;
        private IApplicationConfig _config;

        [SetUp]
        public void Setup()
        {
            _restClient = Substitute.For<ISockudoRestClient>();

            var options = new SockudoOptions
            {
                RestClient = _restClient
            };

            _config = new ApplicationConfig
            {
                AppId = "test-app-id",
                AppKey = "test-app-key",
                AppSecret = "test-app-secret",
            };

            _sockudo = new Sockudo(_config.AppId, _config.AppKey, _config.AppSecret, options);

            var okResponse = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{}")
            };

            _restClient.ExecutePostRawAsync(Arg.Any<ISockudoRestRequest>())
                .Returns(Task.FromResult(new RawRequestResult(okResponse, "{}")));
        }

        [Test]
        public async Task terminate_user_connections_posts_to_correct_path()
        {
            await _sockudo.TerminateUserConnectionsAsync("user-123").ConfigureAwait(false);

#pragma warning disable 4014
            _restClient.Received().ExecutePostRawAsync(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(request =>
                    request.ResourceUri.StartsWith("/apps/" + _config.AppId + "/users/user-123/terminate_connections?") &&
                    request.Method == SockudoMethod.POST
                )
            );
        }

        [Test]
        public async Task force_reconnect_user_posts_to_correct_path()
        {
            await _sockudo.ForceReconnectUserAsync("user-456").ConfigureAwait(false);

#pragma warning disable 4014
            _restClient.Received().ExecutePostRawAsync(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(request =>
                    request.ResourceUri.StartsWith("/apps/" + _config.AppId + "/users/user-456/force_reconnect?") &&
                    request.Method == SockudoMethod.POST
                )
            );
        }

        [Test]
        public void terminate_user_connections_throws_on_null_user_id()
        {
            Assert.ThrowsAsync<ArgumentException>(async () =>
                await _sockudo.TerminateUserConnectionsAsync(null).ConfigureAwait(false));
        }

        [Test]
        public void terminate_user_connections_throws_on_empty_user_id()
        {
            Assert.ThrowsAsync<ArgumentException>(async () =>
                await _sockudo.TerminateUserConnectionsAsync(string.Empty).ConfigureAwait(false));
        }

        [Test]
        public void force_reconnect_user_throws_on_null_user_id()
        {
            Assert.ThrowsAsync<ArgumentException>(async () =>
                await _sockudo.ForceReconnectUserAsync(null).ConfigureAwait(false));
        }

        [Test]
        public void force_reconnect_user_throws_on_empty_user_id()
        {
            Assert.ThrowsAsync<ArgumentException>(async () =>
                await _sockudo.ForceReconnectUserAsync(string.Empty).ConfigureAwait(false));
        }
    }
}
