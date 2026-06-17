using Newtonsoft.Json;

namespace PusherServer
{
    /// <summary>
    /// Default implmentation for serializing an object
    /// </summary>
    public class DefaultSerializer : ISerializeObjectsToJson
    {
        private static readonly JsonSerializerSettings _settings = new JsonSerializerSettings() { NullValueHandling = NullValueHandling.Ignore };

        /// <summary>
        /// Gets the static default serializer.
        /// </summary>
        public static ISerializeObjectsToJson Default { get; } = new DefaultSerializer();

        /// <inheritDoc/>
        public string Serialize(object objectToSerialize)
        {
            return JsonConvert.SerializeObject(objectToSerialize, _settings);
        }
    }
}