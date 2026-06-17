using System.Threading.Tasks;
using NUnit.Framework;
using PusherServer.RestfulClient;
using PusherServer.Tests.RestfulClient.Fakes;

namespace PusherServer.Tests.RestfulClient
{
    [TestFixture]
    public class When_making_a_request
    {
        [Test]
        public async Task then_the_get_request_should_be_made_with_a_valid_resource()
        {
            var factory = new AuthenticatedRequestFactory(Config.AppKey, Config.AppId, Config.AppSecret);
            var request = factory.Build(PusherMethod.GET, "/channels/newRestClient");

            var client = new PusherRestClient($"http://{Config.HttpHost}", "pusher-http-dotnet", Pusher.VERSION);
            var response = await client.ExecuteGetAsync<TestOccupied>(request).ConfigureAwait(false);

            Assert.IsNotNull(response);
            Assert.IsFalse(response.Data.Occupied);
            Assert.AreEqual(30d, client.Timeout.TotalSeconds);
        }
    }
}