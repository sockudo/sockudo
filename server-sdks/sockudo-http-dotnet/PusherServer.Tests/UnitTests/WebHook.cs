using System;
using System.IO;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace PusherServer.Tests.UnitTests
{
    [TestFixture]
    public class when_creating_a_webhook
    {
        private static string GenerateValidSignature(string secret, string stringToSign)
        {
            return CryptoHelper.GetHmac256(secret, stringToSign);
        }

        static string secret = "some_crazy_secret";

        static string validBody = "{\"time_ms\": 1327078148132, \"events\": [{\"name\": \"channel_occupied\", \"channel\": \"test_channel\" }]}";
        static string validSignature = GenerateValidSignature(secret, validBody);

        private static string LoadForwardCompatFixture(string name)
        {
            var directory = new DirectoryInfo(TestContext.CurrentContext.TestDirectory);
            while (directory != null)
            {
                var candidate = Path.Combine(
                    directory.FullName,
                    "tests",
                    "ai-conformance",
                    "fixtures",
                    "forward-compat",
                    name);
                if (File.Exists(candidate))
                {
                    return File.ReadAllText(candidate);
                }

                directory = directory.Parent;
            }

            throw new FileNotFoundException("Forward compatibility fixture not found.", name);
        }

        [Test]
        public void the_WebHook_will_be_valid_if_all_params_are_as_expected()
        {
            var webHook = new WebHook(secret, validSignature, validBody);

            Assert.IsTrue(webHook.IsValid);
        }

        [Test]
        public void the_event_name_can_be_retrieved_from_the_WebHook()
        {
            var webHook = new WebHook(secret, validSignature, validBody);

            Assert.AreEqual("channel_occupied", webHook.Events[0]["name"]);
        }

        [Test]
        public void the_channel_name_can_be_retrieved_from_the_WebHook()
        {
            var webHook = new WebHook(secret, validSignature, validBody);

            Assert.AreEqual("test_channel", webHook.Events[0]["channel"]);
        }

        [Test]
        public void the_WebHook_can_contain_multiple_events()
        {
            var body = "{\"time_ms\": 1327078148132, \"events\": " +
                "[" +
                    "{\"name\": \"channel_occupied\", \"channel\": \"test_channel\" }," +
                    "{\"name\": \"channel_vacated\", \"channel\": \"test_channel2\" }" +
                "]}";

            var webHook = new WebHook(secret, validSignature, body);
            Assert.AreEqual("test_channel", webHook.Events[0]["channel"]);
            Assert.AreEqual("channel_occupied", webHook.Events[0]["name"]);

            Assert.AreEqual("test_channel2", webHook.Events[1]["channel"]);
            Assert.AreEqual("channel_vacated", webHook.Events[1]["name"]);
        }

        [Test]
        public void the_WebHook_will_throw_exception_if_secret_is_null()
        {
            ArgumentException caughtException = null;

            try
            {
                new WebHook(null, validSignature, validBody);
            }
            catch (ArgumentException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.Contains("A secret must be provided", caughtException.Message);
            StringAssert.Contains("secret", caughtException.Message);
        }

        [Test]
        public void the_WebHook_will_throw_exception_if_secret_is_empty()
        {
            ArgumentException caughtException = null;

            try
            {
                new WebHook(string.Empty, validSignature, validBody);
            }
            catch (ArgumentException ex)
            {
                caughtException = ex;
            }

            Assert.IsNotNull(caughtException);
            StringAssert.Contains("A secret must be provided", caughtException.Message);
            StringAssert.Contains("secret", caughtException.Message);
        }

        [Test]
        public void the_WebHook_will_be_invalid_if_they_signature_is_null()
        {
            var webHook = new WebHook(secret, null, validBody);

            Assert.IsFalse(webHook.IsValid);
            Assert.AreEqual(2, webHook.ValidationErrors.Length);
            StringAssert.IsMatch(@"The supplied signature to check was null or empty\. A signature to check must be provided\.", webHook.ValidationErrors[0]);
            StringAssert.IsMatch(@"The signature did not validate\. Expected \. Got 003a63ce4da20830c4fecffb63d5b3944b64989b6458e15b26e08e244f758954", webHook.ValidationErrors[1]);
        }

        [Test]
        public void the_WebHook_will_be_invalid_if_they_signature_is_empty()
        {
            var webHook = new WebHook(secret, string.Empty, validBody);

            Assert.IsFalse(webHook.IsValid);
            Assert.AreEqual(2, webHook.ValidationErrors.Length);
            StringAssert.IsMatch(@"The supplied signature to check was null or empty\. A signature to check must be provided\.", webHook.ValidationErrors[0]);
            StringAssert.IsMatch(@"The signature did not validate\. Expected \. Got 003a63ce4da20830c4fecffb63d5b3944b64989b6458e15b26e08e244f758954", webHook.ValidationErrors[1]);
        }

        [Test]
        public void the_WebHook_will_be_invalid_if_they_body_is_null()
        {
            var webHook = new WebHook(secret, validSignature, null);

            Assert.IsFalse(webHook.IsValid);
            Assert.AreEqual(1, webHook.ValidationErrors.Length);
            StringAssert.IsMatch(@"The supplied body to check was null or empty\. A body to check must be provided\.", webHook.ValidationErrors[0]);
        }

        [Test]
        public void the_WebHook_will_be_invalid_if_they_body_is_empty()
        {
            var webHook = new WebHook(secret, validSignature, string.Empty);

            Assert.IsFalse(webHook.IsValid);
            Assert.AreEqual(1, webHook.ValidationErrors.Length);
            StringAssert.IsMatch(@"The supplied body to check was null or empty\. A body to check must be provided\.", webHook.ValidationErrors[0]);
        }

        [Test]
        public void the_WebHook_will_not_be_valid_when_given_invalid_JSON_for_the_body()
        {
            var secret = "1c9c753dddfd049dd7f1";
            var body = "{Invalid JSON}";
            var expectedSignature = GenerateValidSignature(secret, body);

            var webHook = new WebHook(secret, expectedSignature, body);

            Assert.IsFalse(webHook.IsValid);
            StringAssert.IsMatch("Exception occurred parsing the body as JSON: .*", webHook.ValidationErrors[0]);
        }

        [Test]
        public void the_WebHook_will_be_valid_given_alternative_values()
        {
            var signature = "851f492bab8f7652a2e4c82cd0212d97b4e678edf085c06bf640ed45ee7b1169";
            var secret = "1c9c753dddfd049dd7f1";
            var body = "{\"time_ms\":1423778833207,\"events\":[{\"channel\":\"test_channel\",\"name\":\"channel_occupied\"}]}";

            var webHook = new WebHook(secret, signature, body);
            Assert.IsTrue(webHook.IsValid);
        }

        [Test]
        public void the_WebHook_time_in_ms_is_correctly_parsed()
        {
            var fakeMillis = "1423850522000";
            var expectedDate = new DateTime(2015, 2, 13, 18, 2, 2, DateTimeKind.Utc);
            var secret = "1c9c753dddfd049dd7f1";
            var body = "{\"time_ms\":" + fakeMillis + ",\"events\":[{\"channel\":\"test_channel\",\"name\":\"channel_occupied\"}]}";
            var expectedSignature = GenerateValidSignature(secret, body);

            var webHook = new WebHook(secret, expectedSignature, body);
            Assert.AreEqual(expectedDate, webHook.Time);
        }

        [Test]
        public void future_webhook_fixture_events_are_accepted_and_preserved()
        {
            var body = LoadForwardCompatFixture("future-webhook-events.json");
            IWebHook webHook = new WebHook(secret, GenerateValidSignature(secret, body), body);

            Assert.IsTrue(webHook.IsValid);
            Assert.AreEqual("member_updated", webHook.Events[0]["name"]);
            Assert.AreEqual("must-pass-through", webHook.Events[0]["future_field"]);
            Assert.AreEqual("ai_run_started", webHook.Events[1]["name"]);
            Assert.AreEqual("run-1", webHook.GetRawEvents()[1]["run_id"].ToString());
            Assert.AreEqual("message_version_created", webHook.Events[2]["name"]);
        }

        [Test]
        public void nested_future_webhook_values_do_not_invalidate_the_batch()
        {
            var body = "{\"time_ms\":1710000000000,\"events\":[{\"name\":\"ai_turn_started\",\"channel\":\"private-ai-forward\",\"data\":{\"turn_id\":\"turn-1\",\"tokens\":[\"hello\",\"world\"],\"done\":false,\"nullable\":null},\"future_field\":{\"nested\":true}}]}";
            IWebHook webHook = new WebHook(secret, GenerateValidSignature(secret, body), body);

            Assert.IsTrue(webHook.IsValid);
            Assert.AreEqual("ai_turn_started", webHook.Events[0]["name"]);
            Assert.AreEqual("{\"turn_id\":\"turn-1\",\"tokens\":[\"hello\",\"world\"],\"done\":false,\"nullable\":null}", webHook.Events[0]["data"]);
            var rawData = (JObject)webHook.GetRawEvents()[0]["data"];
            Assert.AreEqual("turn-1", rawData.Value<string>("turn_id"));
            CollectionAssert.AreEqual(new[] { "hello", "world" }, rawData["tokens"].ToObject<string[]>());
            Assert.AreEqual(false, rawData.Value<bool>("done"));
            Assert.AreEqual(JTokenType.Null, rawData["nullable"].Type);
            Assert.AreEqual(true, ((JObject)webHook.GetRawEvents()[0]["future_field"]).Value<bool>("nested"));
        }
    }
}
