using System.Threading.Tasks;
using NUnit.Framework;
using PusherServer.Tests.Helpers;

namespace PusherServer.Tests.AcceptanceTests
{
    [TestFixture]
    public class When_authenticating_a_user
    {
        [Test]
        public async Task the_auth_token_for_a_user_should_be_accepted_by_Pusher()
        {
            PusherServer.Pusher pusherServer = new Pusher(Config.AppId, Config.AppKey, Config.AppSecret, new PusherOptions()
            {
                HostName = Config.HttpHost
            });
            PusherClient.Pusher pusherClient = new PusherClient.Pusher(Config.AppKey, new PusherClient.PusherOptions
            {
                UserAuthenticator = new InMemoryUserAuthenticator(
                    pusherServer,
                    new UserData()
                    {
                        id = "leggetter",
                        watchlist = new string[] { "user_1", "user_2" },
                        user_info = new { twitter_id = "@leggetter" }
                    }),
                Cluster = Config.Cluster,
                TraceLogger = new PusherClient.TraceLogger(),
            });

            await pusherClient.ConnectAsync().ConfigureAwait(false);
            pusherClient.User.Signin();

            await pusherClient.User.SigninDoneAsync().ConfigureAwait(false);

            // No assertions for now. If the above code executes without error then the test passes.
        }

    }
}
