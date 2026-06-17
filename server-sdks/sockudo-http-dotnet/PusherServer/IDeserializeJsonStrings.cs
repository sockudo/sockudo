namespace PusherServer
{
    /// <summary>
    /// Contract that allows a JSON deserializer to be injected
    /// </summary>
    public interface IDeserializeJsonStrings
    {
        /// <summary>
        /// Deserialize a JSON string into an object
        /// </summary>
        /// <param name="stringToDeserialize">The JSON string to be deserialized into an object instance</param>
        /// <returns>A populated object</returns>
        T Deserialize<T>(string stringToDeserialize);
    }
}