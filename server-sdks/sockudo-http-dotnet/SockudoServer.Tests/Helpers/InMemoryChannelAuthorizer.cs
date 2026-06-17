using PusherClient;

namespace SockudoServer.Tests.Helpers
{
    internal class InMemoryChannelAuthorizer : IChannelAuthorizer
    {
        private readonly SockudoServer.Pusher _pusher;
        private readonly PresenceChannelData _presenceData;

        public InMemoryChannelAuthorizer(SockudoServer.Pusher pusher) :
            this(pusher, null)
        {
        }

        public InMemoryChannelAuthorizer(SockudoServer.Pusher pusher, PresenceChannelData presenceData)
        {
            _pusher = pusher;
            _presenceData = presenceData;
        }

        public string Authorize(string channelName, string socketId)
        {
            IAuthenticationData auth;
            if (_presenceData != null)
            {
                auth = _pusher.Authenticate(channelName, socketId, _presenceData);
            }
            else
            {
                auth = _pusher.Authenticate(channelName, socketId);
            }
            return auth.ToJson();
        }
    }
}
