namespace PusherServer
{
    /// <summary>
    /// Contract that allows a JSON serializer to be injected
    /// </summary>
    public interface ISerializeObjectsToJson
    {
        /// <summary>
        /// Serialize an object
        /// </summary>
        /// <param name="objectToSerialize">The object to be serialized into a JSON string</param>
        /// <returns>The passed in object as a JSON string</returns>
        string Serialize(object objectToSerialize);
    }
}
