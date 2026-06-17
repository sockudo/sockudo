using System;
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

            StringAssert.IsMatch("A secret must be provided" + Environment.NewLine + "Parameter name: secret", caughtException.Message);
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

            StringAssert.IsMatch("A secret must be provided" + Environment.NewLine + "Parameter name: secret", caughtException.Message);
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
    }
}
