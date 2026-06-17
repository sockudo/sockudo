namespace SockudoServer.Tests.Helpers
{
    /// <summary>
    /// Loads application configuration.
    /// </summary>
    public interface IApplicationConfigLoader
    {
        /// <summary>
        /// Loads application configuration.
        /// </summary>
        /// <returns>An <see cref="IApplicationConfig"/> instance.</returns>
        IApplicationConfig Load();
    }
}
