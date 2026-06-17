using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using NSubstitute;
using NUnit.Framework;
using PusherServer.RestfulClient;
using PusherServer.Tests.Helpers;

namespace PusherServer.Tests.UnitTests
{
    [TestFixture]
    public class When_using_push_helpers
    {
        private Pusher _pusher;
        private IPusherRestClient _restClient;
        private IApplicationConfig _config;
        private HttpResponseMessage _okResponse;
        private HttpResponseMessage _acceptedResponse;
        private HttpResponseMessage _noContentResponse;

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

            _okResponse = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{\"items\":[],\"next_cursor\":null,\"has_more\":false}")
            };
            _acceptedResponse = new HttpResponseMessage(HttpStatusCode.Accepted)
            {
                Content = new StringContent("{\"publish_id\":\"pub_123\",\"status\":\"accepted\"}")
            };
            _noContentResponse = new HttpResponseMessage(HttpStatusCode.NoContent)
            {
                Content = new StringContent(string.Empty)
            };

            _restClient.ExecuteGetAsync<object>(Arg.Any<IPusherRestRequest>())
                .Returns(Task.FromResult(new GetResult<object>(_okResponse, "{\"items\":[],\"next_cursor\":null,\"has_more\":false}")));
            _restClient.ExecutePostRawAsync(Arg.Any<IPusherRestRequest>())
                .Returns(Task.FromResult(new RawRequestResult(_acceptedResponse, "{\"publish_id\":\"pub_123\",\"status\":\"accepted\"}")));
            _restClient.ExecuteDeleteRawAsync(Arg.Any<IPusherRestRequest>())
                .Returns(Task.FromResult(new RawRequestResult(_noContentResponse, string.Empty)));
        }

        [Test]
        public async Task publish_push_forces_async_admission_and_preserves_provider_overrides()
        {
            await _pusher.PublishPushAsync<object>(new
            {
                recipients = new[] { new { type = "channel", channel = "orders" } },
                payload = new { title = "Order", body = "Updated" },
                providerOverrides = new[] { new { provider = "fcm", payload = new { android = new { } } } }
            }).ConfigureAwait(false);

#pragma warning disable 4014
            _restClient.Received().ExecutePostRawAsync(
#pragma warning restore 4014
                Arg.Is<IPusherRestRequest>(request =>
                    request.ResourceUri.StartsWith("/apps/" + _config.AppId + "/push/publish?") &&
                    request.Headers["X-Sockudo-Push-Capability"] == "push-admin" &&
                    RequestBodyHasAsyncPublishDefaults(request)
                )
            );
        }

        [Test]
        public async Task list_device_registrations_adds_cursor_pagination_query()
        {
            await _pusher.ListDeviceRegistrationsAsync<object>(new { limit = 10, cursor = "c1" }).ConfigureAwait(false);

#pragma warning disable 4014
            _restClient.Received().ExecuteGetAsync<object>(
#pragma warning restore 4014
                Arg.Is<IPusherRestRequest>(request =>
                    request.ResourceUri.StartsWith("/apps/" + _config.AppId + "/push/deviceRegistrations?") &&
                    request.ResourceUri.Contains("&cursor=c1") &&
                    request.ResourceUri.Contains("&limit=10") &&
                    request.Headers["X-Sockudo-Push-Capability"] == "push-admin"
                )
            );
        }

        [Test]
        public async Task subscribe_scoped_list_uses_device_identity_and_filter_params()
        {
            await _pusher.ListChannelPushSubscriptionsAsync<object>(
                new { limit = 10, cursor = "c1", deviceId = "device-1" },
                "identity-token"
            ).ConfigureAwait(false);

#pragma warning disable 4014
            _restClient.Received().ExecuteGetAsync<object>(
#pragma warning restore 4014
                Arg.Is<IPusherRestRequest>(request =>
                    request.ResourceUri.StartsWith("/apps/" + _config.AppId + "/push/channelSubscriptions?") &&
                    request.ResourceUri.Contains("&cursor=c1") &&
                    request.ResourceUri.Contains("&limit=10") &&
                    request.ResourceUri.Contains("&deviceId=device-1") &&
                    request.Headers["X-Sockudo-Push-Capability"] == "push-subscribe" &&
                    request.Headers["X-Sockudo-Device-Identity-Token"] == "identity-token"
                )
            );
        }

        [Test]
        public async Task cancel_scheduled_push_uses_delete_with_push_admin_scope()
        {
            await _pusher.CancelScheduledPushAsync<object>("pub_123").ConfigureAwait(false);

#pragma warning disable 4014
            _restClient.Received().ExecuteDeleteRawAsync(
#pragma warning restore 4014
                Arg.Is<IPusherRestRequest>(request =>
                    request.ResourceUri.StartsWith("/apps/" + _config.AppId + "/push/scheduled/pub_123?") &&
                    request.Headers["X-Sockudo-Push-Capability"] == "push-admin"
                )
            );
        }

        [Test]
        public void schedule_push_requires_notBeforeMs()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
                await _pusher.SchedulePushAsync<object>(new
                {
                    recipients = new[] { new { type = "channel", channel = "orders" } },
                    payload = new { title = "Order" }
                }).ConfigureAwait(false));

            Assert.AreEqual("scheduled push requires notBeforeMs", ex.Message.Replace(" (Parameter 'request')", string.Empty));
        }

        private static bool RequestBodyHasAsyncPublishDefaults(IPusherRestRequest request)
        {
            var body = JObject.Parse(request.GetContentAsJsonString());
            return body.Value<bool?>("sync") == false &&
                   body["providerOverrides"] is JArray overrides &&
                   overrides.Count == 1 &&
                   overrides[0]?["provider"]?.Value<string>() == "fcm";
        }
    }
}
