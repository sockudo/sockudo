using System;

namespace SockudoServer.Tests.Helpers
{
    /// <summary>
    /// Loads configuration from system environment variables.
    /// </summary>
    public class EnvironmentVariableConfigLoader : IApplicationConfigLoader
    {
        private const string PUSHER_APP_ID = "PUSHER_APP_ID";
        private const string PUSHER_APP_KEY = "PUSHER_APP_KEY";
        private const string PUSHER_APP_SECRET = "PUSHER_APP_SECRET";
        private const string PUSHER_APP_CLUSTER = "PUSHER_APP_CLUSTER";

        public static IApplicationConfigLoader Default { get; } = new EnvironmentVariableConfigLoader();

        /// <summary>
        /// Loads configuration from system environment variables.
        /// </summary>
        /// <returns>An <see cref="IApplicationConfig"/> instance.</returns>
        public IApplicationConfig Load()
        {
            ApplicationConfig result = new ApplicationConfig
            {
                AppId = Environment.GetEnvironmentVariable(PUSHER_APP_ID),
                AppKey = Environment.GetEnvironmentVariable(PUSHER_APP_KEY),
                AppSecret = Environment.GetEnvironmentVariable(PUSHER_APP_SECRET),
                Cluster = Environment.GetEnvironmentVariable(PUSHER_APP_CLUSTER),
            };
            return result;
        }
    }
}
