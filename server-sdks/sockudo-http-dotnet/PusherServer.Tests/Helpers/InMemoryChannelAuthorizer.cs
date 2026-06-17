using PusherClient;

namespace PusherServer.Tests.Helpers
{
    internal class InMemoryChannelAuthorizer: IChannelAuthorizer
    {
        private readonly PusherServer.Pusher _pusher;
        private readonly PresenceChannelData _presenceData;

        public InMemoryChannelAuthorizer(PusherServer.Pusher pusher):
            this(pusher, null)
        {
        }

        public InMemoryChannelAuthorizer(PusherServer.Pusher pusher, PresenceChannelData presenceData)
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
