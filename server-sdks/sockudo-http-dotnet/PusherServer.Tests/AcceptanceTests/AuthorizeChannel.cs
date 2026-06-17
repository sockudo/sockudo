using System.Threading.Tasks;
using NUnit.Framework;
using PusherServer.Tests.Helpers;

namespace PusherServer.Tests.AcceptanceTests
{
    [TestFixture]
    public class When_authorizing_a_private_subscription
    {
        [Test]
        public async Task the_auth_token_for_a_private_channel_should_be_accepted_by_Pusher()
        {
            PusherServer.Pusher pusherServer = new Pusher(Config.AppId, Config.AppKey, Config.AppSecret, new PusherOptions()
            {
                HostName = Config.HttpHost
            });
            PusherClient.Pusher pusherClient = new PusherClient.Pusher(Config.AppKey, new PusherClient.PusherOptions
            {
                ChannelAuthorizer = new InMemoryChannelAuthorizer(pusherServer),
                Cluster = Config.Cluster,
                TraceLogger = new PusherClient.TraceLogger(),
            });

            string channelName = "private-channel";

            await pusherClient.ConnectAsync().ConfigureAwait(false);

            var channel = await pusherClient.SubscribeAsync(channelName).ConfigureAwait(false);

            Assert.IsTrue(channel.IsSubscribed, nameof(channel.IsSubscribed));
        }

        [Test]
        public async Task the_auth_token_for_a_private_encrypted_channel_should_be_accepted_by_Pusher()
        {
            PusherServer.Pusher pusherServer = new Pusher(Config.AppId, Config.AppKey, Config.AppSecret, new PusherOptions()
            {
                HostName = Config.HttpHost,
                EncryptionMasterKey = DataHelper.GenerateEncryptionMasterKey(),
            });

            PusherClient.Pusher pusherClient = new PusherClient.Pusher(Config.AppKey, new PusherClient.PusherOptions
            {
                ChannelAuthorizer = new InMemoryChannelAuthorizer(pusherServer),
                Cluster = Config.Cluster,
                TraceLogger = new PusherClient.TraceLogger(),
            });

            string channelName = "private-encrypted-channel";

            await pusherClient.ConnectAsync().ConfigureAwait(false);

            var channel = await pusherClient.SubscribeAsync(channelName).ConfigureAwait(false);

            Assert.IsTrue(channel.IsSubscribed, nameof(channel.IsSubscribed));
        }

        [Test]
        public async Task the_auth_token_for_a_presence_channel_should_be_accepted_by_Pusher()
        {
            Pusher pusherServer = new Pusher(Config.AppId, Config.AppKey, Config.AppSecret, new PusherOptions
            {
                HostName = Config.HttpHost,
            });
            PusherClient.Pusher pusherClient = new PusherClient.Pusher(Config.AppKey, new PusherClient.PusherOptions
            {
                ChannelAuthorizer = new InMemoryChannelAuthorizer(
                    pusherServer,
                    new PresenceChannelData()
                    {
                        user_id = "leggetter",
                        user_info = new { twitter_id = "@leggetter" }
                    }),
                Cluster = Config.Cluster,
                TraceLogger = new PusherClient.TraceLogger(),
            });

            string channelName = "presence-channel";

            await pusherClient.ConnectAsync().ConfigureAwait(false);

            var channel = await pusherClient.SubscribePresenceAsync<PresenceChannelData>(channelName).ConfigureAwait(false);

            Assert.IsTrue(channel.IsSubscribed, nameof(channel.IsSubscribed));
        }
    }
}
