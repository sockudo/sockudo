using System;
using System.Threading;
using System.Threading.Tasks;

namespace SockudoServer.Tests.Helpers
{
    internal sealed class ClientServerFactory
    {
        /// <summary>
        /// Create a Pusher Client, and subscribes a user
        /// </summary>
        /// <param name="pusherServer">Server to connect to</param>
        /// <param name="channelName">The name of the channel to subscribe to</param>
        /// <returns>An awaitable task</returns>
        public static async Task CreateClientAsync(Pusher pusherServer, string channelName)
        {
            PusherClient.Pusher pusherClient = new PusherClient.Pusher(Config.AppKey, new PusherClient.PusherOptions
            {
                ChannelAuthorizer = new InMemoryChannelAuthorizer(
                    pusherServer,
                    new PresenceChannelData()
                    {
                        user_id = "Mr Pusher",
                        user_info = new { twitter_id = "@pusher" }
                    }),
                Cluster = Config.Cluster,
                TraceLogger = new PusherClient.TraceLogger(),
            });

            await pusherClient.ConnectAsync().ConfigureAwait(false);

            AutoResetEvent subscribedEvent = new AutoResetEvent(false);
            await pusherClient.SubscribeAsync(channelName, (sender) => { subscribedEvent.Set(); }).ConfigureAwait(false);
            subscribedEvent.WaitOne(TimeSpan.FromSeconds(5));
        }

        /// <summary>
        /// Create a Pusher Server instance
        /// </summary>
        /// <returns></returns>
        public static Pusher CreateServer()
        {
            return new Pusher(Config.AppId, Config.AppKey, Config.AppSecret, new PusherOptions
            {
                HostName = Config.HttpHost
            });
        }
    }
}
