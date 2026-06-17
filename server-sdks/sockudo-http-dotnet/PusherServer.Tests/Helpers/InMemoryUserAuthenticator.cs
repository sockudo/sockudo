using PusherClient;

namespace PusherServer.Tests.Helpers
{
    internal class InMemoryUserAuthenticator: IUserAuthenticator
    {
        private readonly PusherServer.Pusher _pusher;
        private readonly UserData _userData;

        public InMemoryUserAuthenticator(PusherServer.Pusher pusher, UserData userData)
        {
            _pusher = pusher;
            _userData = userData;
        }

        public string Authenticate(string socketId)
        {
            IUserAuthenticationResponse authResponse;
            authResponse = _pusher.AuthenticateUser(socketId, _userData);
            return authResponse.ToJson();
        }
    }
}
