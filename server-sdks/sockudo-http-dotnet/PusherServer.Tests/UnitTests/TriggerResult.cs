using System;
using System.Net;
using System.Net.Http;
using NSubstitute;
using NUnit.Framework;
using PusherServer.Exceptions;
using PusherServer.Tests.Helpers;

namespace PusherServer.Tests.UnitTests
{
    public class TriggerResultHelper
    {
        public static string TRIGGER_RESPONSE_JSON = "{" +
                            "\"event_ids\": {" +
                                "\"ch1\": \"ch1_event_id\"," +
                                "\"ch2\": \"ch2_event_id\"," +
                                "\"ch3\": \"ch3_event_id\"" +
                            "}" +
                        "}";

        public static string TRIGGER_RESPONSE_WITH_CHANNEL_ATTRIBUTES = "{" +
                            "\"channels\": {" +
                                "\"any-channel\": {" +
                                    "\"user_count\": 1," +
                                    "\"subscription_count\": 2" +
                                "}" +
                            "}," +
                            "\"event_ids\": {" +
                                "\"any-channel\": \"test-id\"" +
                            "}" +
                        "}";
    }

    [TestFixture]
    public class When_initialisation_a_TriggerEvent
    {
        private HttpResponseMessage V7_PROTOCOL_SUCCESSFUL_RESPONSE;

        [OneTimeSetUp]
        public void FixtureSetUp()
        {
            V7_PROTOCOL_SUCCESSFUL_RESPONSE = Substitute.For<HttpResponseMessage>();
            V7_PROTOCOL_SUCCESSFUL_RESPONSE.Content = new StringContent("{}");
            V7_PROTOCOL_SUCCESSFUL_RESPONSE.StatusCode = HttpStatusCode.OK;
        }

        [Test]
        [ExpectedException(typeof (ArgumentNullException))]
        public void it_should_not_accept_a_null_response()
        {
            new TriggerResult(null, null);
        }

        [Test]
        public void it_should_treat_a_v7_protocol_200_response_as_a_successful_request()
        {
            var triggerResult = new TriggerResult(V7_PROTOCOL_SUCCESSFUL_RESPONSE, "{}");
            Assert.AreEqual(HttpStatusCode.OK, triggerResult.StatusCode);
        }

        [Test]
        public void it_should_have_no_event_id_value_when_a_v7_protocol_200_response_is_returned()
        {
            var triggerResult = new TriggerResult(V7_PROTOCOL_SUCCESSFUL_RESPONSE, "{}");
            Assert.AreEqual(0, triggerResult.EventIds.Count);
        }

        [Test]
        [ExpectedException(typeof (TriggerResponseException))]
        public void it_should_treat_non_JSON_content_in_the_request_body_as_a_failed_request()
        {
            HttpResponseMessage response = Substitute.For<HttpResponseMessage>();
            response.Content = new StringContent("FISH");
            response.StatusCode = HttpStatusCode.OK;

            new TriggerResult(response, "FISH");
            try
            {
                new TriggerResult(response, "FISH");
                Assert.Fail("Exception was not thrown");
            }
            catch (TriggerResponseException ex)
            {
                Assert.IsTrue(ex.Message.Contains("FISH"), "exception message includes response");
            }
        }

        [Test]
        [ExpectedException(typeof (NotSupportedException))]
        public void it_should_not_be_possible_to_change_EventIds()
        {
            HttpResponseMessage response = Substitute.For<HttpResponseMessage>();
            response.StatusCode = HttpStatusCode.OK;
            response.Content = new StringContent(TriggerResultHelper.TRIGGER_RESPONSE_JSON);
            var triggerResult = new TriggerResult(response, TriggerResultHelper.TRIGGER_RESPONSE_JSON);

            triggerResult.EventIds.Add("fish", "pie");
        }

        [Test]
        public void it_should_parse_channel_attributes()
        {
            HttpResponseMessage response = Substitute.For<HttpResponseMessage>();
            response.StatusCode = HttpStatusCode.OK;
            response.Content = new StringContent(TriggerResultHelper.TRIGGER_RESPONSE_WITH_CHANNEL_ATTRIBUTES);

            var triggerResult = new TriggerResult(response, TriggerResultHelper.TRIGGER_RESPONSE_WITH_CHANNEL_ATTRIBUTES);

            Assert.AreEqual(2, triggerResult.ChannelAttributes["any-channel"].subscription_count);
            Assert.AreEqual(1, triggerResult.ChannelAttributes["any-channel"].user_count);
            Assert.AreEqual(1, triggerResult.EventIds.Count);
        }
    }
}