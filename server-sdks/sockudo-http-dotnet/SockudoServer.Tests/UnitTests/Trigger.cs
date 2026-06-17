using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using NSubstitute;
using NUnit.Framework;
using SockudoServer.Exceptions;
using SockudoServer.RestfulClient;
using SockudoServer.Tests.Helpers;

namespace SockudoServer.Tests.UnitTests
{
    [TestFixture]
    public class When_Triggering_an_Event
    {
        private IPusher _pusher;
        private IPusherRestClient _subPusherClient;
        private IApplicationConfig _config;

        private readonly string _channelName = "my-channel";
        private readonly string _eventName = "my_event";
        private readonly object _eventData = new { hello = "world" };

        private HttpResponseMessage _v7ProtocolSuccessfulResponse;
        private HttpResponseMessage _v8ProtocolSuccessfulResponse;

        [OneTimeSetUp]
        public void FixtureSetUp()
        {
            _v7ProtocolSuccessfulResponse = Substitute.For<HttpResponseMessage>();
            _v7ProtocolSuccessfulResponse.Content = new StringContent("{}");
            _v7ProtocolSuccessfulResponse.StatusCode = HttpStatusCode.OK;

            _v8ProtocolSuccessfulResponse = Substitute.For<HttpResponseMessage>();
            _v8ProtocolSuccessfulResponse.Content = new StringContent(TriggerResultHelper.TRIGGER_RESPONSE_JSON);
            _v8ProtocolSuccessfulResponse.StatusCode = HttpStatusCode.OK;
        }

        [SetUp]
        public void Setup()
        {
            _subPusherClient = Substitute.For<IPusherRestClient>();

            IPusherOptions options = new PusherOptions()
            {
                RestClient = _subPusherClient
            };

            _config = new ApplicationConfig
            {
                AppId = "test-app-id",
                AppKey = "test-app-key",
                AppSecret = "test-app-secret",
            };

            _pusher = new Pusher(_config.AppId, _config.AppKey, _config.AppSecret, options);

            _subPusherClient.ExecutePostAsync(Arg.Any<ISockudoRestRequest>()).Returns(Task.FromResult(new TriggerResult(_v8ProtocolSuccessfulResponse, TriggerResultHelper.TRIGGER_RESPONSE_JSON)));
        }

        [Test]
        public async Task url_resource_is_in_expected_format()
        {
            await _pusher.TriggerAsync(_channelName, _eventName, _eventData).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecutePostAsync(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(
                    x => x.ResourceUri.StartsWith("/apps/" + _config.AppId + "/events?")
                )
            );
        }

        [Test]
        public async Task post_payload_contains_channelName_eventName_and_eventData()
        {
            await _pusher.TriggerAsync(_channelName, _eventName, _eventData).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecutePostAsync(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(
                    x => CheckRequestContainsPayload(x, _channelName, _eventName, _eventData)
                )
            );
        }

        [Test]
        public async Task with_async_and_a_single_channel_the_request_is_made()
        {
            await _pusher.TriggerAsync(_channelName, _eventName, _eventData).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecutePostAsync(Arg.Any<ISockudoRestRequest>());
#pragma warning restore 4014
        }

        [Test]
        public async Task with_async_and_a_single_channel_and_trigger_options_the_request_is_made()
        {
            await _pusher.TriggerAsync(_channelName, _eventName, _eventData).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecutePostAsync(Arg.Any<ISockudoRestRequest>());
#pragma warning restore 4014
        }

        [Test]
        public async Task with_async_and_multiple_channels_the_request_is_made()
        {
            await _pusher.TriggerAsync(new[] { "fish", "pie" }, _eventName, _eventData).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecutePostAsync(Arg.Any<ISockudoRestRequest>());
#pragma warning restore 4014
        }

        [Test]
        public async Task with_async_and_multiple_channels_and_trigger_options_the_request_is_made()
        {
            await _pusher.TriggerAsync(new[] { "fish", "pie" }, _eventName, _eventData).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecutePostAsync(Arg.Any<ISockudoRestRequest>());
#pragma warning restore 4014
        }

        [Test]
        public async Task on_a_single_channel_the_socket_id_parameter_should_be_present_in_the_querystring()
        {
            var expectedSocketId = "123.098";

            await _pusher.TriggerAsync(_channelName, _eventName, _eventData, new TriggerOptions()
            {
                SocketId = expectedSocketId
            }).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecutePostAsync(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(
                    x => CheckRequestContainsSocketIdParameter(x, expectedSocketId)
                )
            );
        }

        [Test]
        public async Task on_a_single_channel_the_socket_id_parameter_should_be_present_in_the_querystring_async()
        {
            var expectedSocketId = "123.098";

            await _pusher.TriggerAsync(
                _channelName,
                _eventName,
                _eventData,
                new TriggerOptions()
                {
                    SocketId = expectedSocketId
                }
            ).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecutePostAsync(Arg.Is<ISockudoRestRequest>(
#pragma warning restore 4014
                x => CheckRequestContainsSocketIdParameter(x, expectedSocketId)
                )
            );
        }

        [Test]
        public async Task on_a_multiple_channels_the_socket_id_parameter_should_be_present_in_the_querystring()
        {
            var expectedSocketId = "123.456";

            await _pusher.TriggerAsync(new[] { "my-channel", "my-channel-2" }, _eventName, _eventData, new TriggerOptions()
            {
                SocketId = expectedSocketId
            }).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecutePostAsync(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(
                    x => CheckRequestContainsSocketIdParameter(x, expectedSocketId)
                )
            );
        }

        [Test]
        public async Task on_a_multiple_channels_the_socket_id_parameter_should_be_present_in_the_querystring_async()
        {
            var expectedSocketId = "123.456";

            await _pusher.TriggerAsync(
                    new[] { "my-channel", "my-channel-2" },
                    _eventName,
                    _eventData,
                    new TriggerOptions()
                    {
                        SocketId = expectedSocketId
                    }
                ).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecutePostAsync(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(
                    x => CheckRequestContainsSocketIdParameter(x, expectedSocketId)
                )
            );
        }

        [Test]
        public async Task socket_id_cannot_contain_colon_prefix()
        {
            FormatException caughtException = null;

            try
            {
                await TriggerWithSocketId(":444.444").ConfigureAwait(false);
            }
            catch (FormatException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.AreEqualIgnoringCase("The socket id \":444.444\" was not in the form: \\A\\d+\\.\\d+\\z", caughtException.Message);
        }

        [Test]
        public async Task socket_id_cannot_contain_colon_suffix()
        {
            FormatException caughtException = null;

            try
            {
                await TriggerWithSocketId("444.444:").ConfigureAwait(false);
            }
            catch (FormatException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.AreEqualIgnoringCase("The socket id \"444.444:\" was not in the form: \\A\\d+\\.\\d+\\z", caughtException.Message);
        }

        [Test]
        public async Task socket_id_cannot_contain_letters_suffix()
        {
            FormatException caughtException = null;

            try
            {
                await TriggerWithSocketId("444.444a").ConfigureAwait(false);
            }
            catch (FormatException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.AreEqualIgnoringCase("The socket id \"444.444a\" was not in the form: \\A\\d+\\.\\d+\\z", caughtException.Message);
        }

        [Test]
        public async Task socket_id_must_contain_a_period_point()
        {
            FormatException caughtException = null;

            try
            {
                await TriggerWithSocketId("444").ConfigureAwait(false);
            }
            catch (FormatException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.AreEqualIgnoringCase("The socket id \"444\" was not in the form: \\A\\d+\\.\\d+\\z", caughtException.Message);
        }

        [Test]
        public async Task socket_id_must_not_contain_newline_prefix()
        {
            FormatException caughtException = null;

            try
            {
                await TriggerWithSocketId("\n444.444").ConfigureAwait(false);
            }
            catch (FormatException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.AreEqualIgnoringCase("The socket id \"\n444.444\" was not in the form: \\A\\d+\\.\\d+\\z", caughtException.Message);
        }

        [Test]
        public async Task socket_id_must_not_contain_newline_suffix()
        {
            FormatException caughtException = null;

            try
            {
                await TriggerWithSocketId("444.444\n").ConfigureAwait(false);
            }
            catch (FormatException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.AreEqualIgnoringCase("The socket id \"444.444\n\" was not in the form: \\A\\d+\\.\\d+\\z", caughtException.Message);
        }

        [Test]
        public async Task socket_id_must_not_be_empty_string()
        {
            FormatException caughtException = null;

            try
            {
                await TriggerWithSocketId(string.Empty).ConfigureAwait(false);
            }
            catch (FormatException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.AreEqualIgnoringCase("The socket id \"\" was not in the form: \\A\\d+\\.\\d+\\z", caughtException.Message);
        }

        [Test]
        public async Task channel_must_not_have_trailing_colon()
        {
            FormatException caughtException = null;

            try
            {
                await TriggerWithChannelName("test_channel:").ConfigureAwait(false);
            }
            catch (FormatException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.AreEqualIgnoringCase("The channel name \"test_channel:\" was not in the form: \\A[a-zA-Z0-9_=@,.;\\-]+\\z", caughtException.Message);
        }

        [Test]
        public async Task channel_name_must_not_have_leading_colon()
        {
            FormatException caughtException = null;

            try
            {
                await TriggerWithChannelName(":test_channel").ConfigureAwait(false);
            }
            catch (FormatException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.AreEqualIgnoringCase("The channel name \":test_channel\" was not in the form: \\A[a-zA-Z0-9_=@,.;\\-]+\\z", caughtException.Message);
        }

        [Test]
        public async Task channel_name_must_not_have_leading_colon_newline()
        {
            FormatException caughtException = null;

            try
            {
                await TriggerWithChannelName(":\ntest_channel").ConfigureAwait(false);
            }
            catch (FormatException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.AreEqualIgnoringCase("The channel name \":\ntest_channel\" was not in the form: \\A[a-zA-Z0-9_=@,.;\\-]+\\z", caughtException.Message);
        }

        [Test]
        public async Task channel_name_must_not_have_trailing_colon_newline()
        {
            FormatException caughtException = null;

            try
            {
                await TriggerWithChannelName("test_channel\n:").ConfigureAwait(false);
            }
            catch (FormatException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.AreEqualIgnoringCase("The channel name \"test_channel\n:\" was not in the form: \\A[a-zA-Z0-9_=@,.;\\-]+\\z", caughtException.Message);
        }

        [Test]
        public async Task channel_names_in_array_must_be_validated()
        {
            FormatException caughtException = null;

            try
            {
                await _pusher.TriggerAsync(new[] { "this_one_is_okay", "test_channel\n:" }, _eventName, _eventData).ConfigureAwait(false);
            }
            catch (FormatException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.AreEqualIgnoringCase("The channel name \"test_channel\n:\" was not in the form: \\A[a-zA-Z0-9_=@,.;\\-]+\\z", caughtException.Message);
        }

        [Test]
        public async Task channel_names_must_not_exceed_allowed_length()
        {
            ArgumentOutOfRangeException caughtException = null;

            string channelName = new string('a', ValidationHelper.CHANNEL_NAME_MAX_LENGTH + 1);
            try
            {
                await TriggerWithChannelName(channelName).ConfigureAwait(false);
            }
            catch (ArgumentOutOfRangeException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.Contains($"greater than the allowed {ValidationHelper.CHANNEL_NAME_MAX_LENGTH} characters", caughtException.Message);
            StringAssert.Contains("channelName", caughtException.Message);
            StringAssert.Contains(channelName.Length.ToString(), caughtException.Message);
        }

        [Test]
        public async Task event_arrays_must_not_exceed_allowed_length()
        {
            EventBatchSizeExceededException caughtException = null;

            try
            {
                var events = DataHelper.CreateEvents(numberOfEvents: 11);

                await TriggerWithBatch(events.ToArray()).ConfigureAwait(false);
            }
            catch (EventBatchSizeExceededException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.Contains("Only 10 events permitted per batch", caughtException.Message);
            StringAssert.Contains("events", caughtException.Message);
            StringAssert.Contains("11", caughtException.Message);
        }

        [Test]
        public async Task event_arrays_will_be_rejected_if_a_channel_name_is_too_long()
        {
            ArgumentOutOfRangeException caughtException = null;

            string channelName = new string('a', ValidationHelper.CHANNEL_NAME_MAX_LENGTH + 1);
            try
            {
                var events = DataHelper.CreateEvents(numberOfEvents: 9);
                events.Add(new Event { Channel = channelName });

                await TriggerWithBatch(events.ToArray()).ConfigureAwait(false);
            }
            catch (ArgumentOutOfRangeException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.Contains($"greater than the allowed {ValidationHelper.CHANNEL_NAME_MAX_LENGTH} characters", caughtException.Message);
            StringAssert.Contains("channelName", caughtException.Message);
            StringAssert.Contains(channelName.Length.ToString(), caughtException.Message);
        }

        [Test]
        public async Task idempotency_key_should_be_included_in_body_and_header_for_single_channel_trigger()
        {
            var idempotencyKey = Pusher.GenerateIdempotencyKey();

            await _pusher.TriggerAsync(_channelName, _eventName, _eventData, new TriggerOptions()
            {
                IdempotencyKey = idempotencyKey
            }).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecutePostAsync(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(
                    x => x.GetContentAsJsonString().Contains("idempotency_key") &&
                         x.GetContentAsJsonString().Contains(idempotencyKey) &&
                         x.Headers.ContainsKey("X-Idempotency-Key") &&
                         x.Headers["X-Idempotency-Key"] == idempotencyKey
                )
            );
        }

        [Test]
        public async Task idempotency_key_should_be_included_in_body_and_header_for_multi_channel_trigger()
        {
            var idempotencyKey = Pusher.GenerateIdempotencyKey();

            await _pusher.TriggerAsync(new[] { "channel-1", "channel-2" }, _eventName, _eventData, new TriggerOptions()
            {
                IdempotencyKey = idempotencyKey
            }).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecutePostAsync(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(
                    x => x.GetContentAsJsonString().Contains("idempotency_key") &&
                         x.GetContentAsJsonString().Contains(idempotencyKey) &&
                         x.Headers.ContainsKey("X-Idempotency-Key") &&
                         x.Headers["X-Idempotency-Key"] == idempotencyKey
                )
            );
        }

        [Test]
        public async Task idempotency_key_should_not_be_present_when_not_set()
        {
            await _pusher.TriggerAsync(_channelName, _eventName, _eventData).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecutePostAsync(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(
                    x => x.GetContentAsJsonString().Contains("idempotency_key") &&
                         x.Headers.ContainsKey("X-Idempotency-Key") &&
                         !string.IsNullOrWhiteSpace(x.Headers["X-Idempotency-Key"])
                )
            );
        }

        [Test]
        public async Task batch_events_should_include_per_event_idempotency_keys_in_body()
        {
            var key1 = Pusher.GenerateIdempotencyKey();
            var key2 = Pusher.GenerateIdempotencyKey();

            var events = new[]
            {
                new Event { Channel = "channel-1", EventName = "event-1", Data = new { msg = "hello" }, IdempotencyKey = key1 },
                new Event { Channel = "channel-2", EventName = "event-2", Data = new { msg = "world" }, IdempotencyKey = key2 },
            };

            await _pusher.TriggerAsync(events).ConfigureAwait(false);

#pragma warning disable 4014
            _subPusherClient.Received().ExecutePostAsync(
#pragma warning restore 4014
                Arg.Is<ISockudoRestRequest>(
                    x => x.GetContentAsJsonString().Contains(key1) &&
                         x.GetContentAsJsonString().Contains(key2) &&
                         x.GetContentAsJsonString().Contains("idempotency_key")
                )
            );
        }

        [Test]
        public void generate_idempotency_key_returns_valid_guid_string()
        {
            var key = Pusher.GenerateIdempotencyKey();
            Assert.IsNotNull(key);
            Assert.IsNotEmpty(key);
            Assert.IsTrue(Guid.TryParse(key, out _), "GenerateIdempotencyKey should return a valid GUID string");
        }

        [Test]
        public void generate_idempotency_key_returns_unique_values()
        {
            var key1 = Pusher.GenerateIdempotencyKey();
            var key2 = Pusher.GenerateIdempotencyKey();
            Assert.AreNotEqual(key1, key2, "Each call to GenerateIdempotencyKey should produce a unique value");
        }

        private bool CheckRequestContainsPayload(ISockudoRestRequest request, string channelName, string eventName, object eventData)
        {
            var payload = JObject.Parse(request.GetContentAsJsonString());
            var expectedData = JToken.Parse(DefaultSerializer.Default.Serialize(eventData));
            var actualData = JToken.Parse(payload.Value<string>("data"));

            return payload.Value<string>("name") == eventName &&
                   payload["channels"] is JArray channels &&
                   channels.Count == 1 &&
                   channels[0]?.Value<string>() == channelName &&
                   JToken.DeepEquals(actualData, expectedData);
        }

        private async Task<ITriggerResult> TriggerWithSocketId(string socketId)
        {
            var response = await _pusher.TriggerAsync(_channelName, _eventName, _eventData, new TriggerOptions()
            {
                SocketId = socketId
            }).ConfigureAwait(false);

            return response;
        }

        private async Task<ITriggerResult> TriggerWithChannelName(string channelName)
        {
            var response = await _pusher.TriggerAsync(channelName, _eventName, _eventData).ConfigureAwait(false);

            return response;
        }

        private async Task<ITriggerResult> TriggerWithBatch(Event[] events)
        {
            var response = await _pusher.TriggerAsync(events).ConfigureAwait(false);

            return response;
        }

        private static bool CheckRequestContainsSocketIdParameter(ISockudoRestRequest request, string expectedSocketId)
        {
            var parameter = request.GetContentAsJsonString();
            return parameter.Contains("socket_id") &&
                   parameter.Contains(expectedSocketId);
        }
    }
}
