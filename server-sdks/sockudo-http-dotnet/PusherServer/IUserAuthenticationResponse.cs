namespace PusherServer
{
    /// <summary>
    /// Interface for Authenticaton Data
    /// </summary>
    public interface IUserAuthenticationResponse
    {
        /// <summary>
        /// Gets the Authetication String
        /// </summary>
        string auth { get; }

        /// <summary>
        /// Double encoded JSON containing user information.
        /// </summary>
        string user_data { get; }

        /// <summary>
        /// Returns a Json representation of the authentication data.
        /// </summary>
        /// <returns></returns>
        string ToJson();
    }
}
