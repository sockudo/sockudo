using System;
using SockudoServer.RestfulClient;
using SockudoServer.Util;

namespace SockudoServer
{
    /// <summary>
    /// Interface for Sockudo Options
    /// </summary>
    public interface ISockudoOptions
    {
        /// <summary>
        /// Gets or sets a value indicating whether calls to the Sockudo REST API are over HTTP or HTTPS.
        /// </summary>
        /// <value>
        ///   <c>true</c> if encrypted; otherwise, <c>false</c>.
        /// </value>
        bool Encrypted { get; set; }

        /// <summary>
        /// Gets or sets the REST API port that the HTTP calls will be made to.
        /// </summary>
        /// <value>
        /// The port.
        /// </value>
        int Port { get; set; }

        /// <summary>
        /// Gets or sets the Json Serializer
        /// </summary>
        ISerializeObjectsToJson JsonSerializer { get; set; }

        /// <summary>
        /// Gets or sets the Json Deserializer
        /// </summary>
        IDeserializeJsonStrings JsonDeserializer { get; set; }

        /// <summary>
        /// Gets or sets the Sockudo rest client. Generally only expected to be used for testing.
        /// </summary>
        /// <value>
        /// The Sockudo REST client.
        /// </value>
        ISockudoRestClient RestClient { get; set; }

        /// <summary>
        /// Gets or sets the Sockudo rest client timeout. The default timeout is 30 seconds.
        /// </summary>
        TimeSpan RestClientTimeout { get; set; }

        /// <summary>
        /// The host of the HTTP API endpoint excluding the scheme e.g. api.sockudo.io
        /// </summary>
        /// <exception cref="FormatException">If a scheme is found at the start of the host value</exception>
        string HostName { get; set; }

        /// <summary>
        /// The cluster where the application was created, e.g. eu
        /// </summary>
        string Cluster { get; set; }

        /// <summary>
        /// Gets or sets the size limit for the <c>Data</c> property of an <see cref="Event"/>.
        /// This is normally 10KB but SDK customers can request a larger limit.
        /// </summary>
        /// <remarks>
        /// If this value is specified each <c>Event.Data</c> field will be validated before triggering an event on the server.
        /// The validation check will happen client side rather than server side if this value is specified.
        /// </remarks>
        int? BatchEventDataSizeLimit { get; set; }

        /// <summary>
        /// Gets or sets the <see cref="ITraceLogger"/> to use for tracing debug messages.
        /// </summary>
        ITraceLogger TraceLogger { get; set; }

        /// <summary>
        /// Gets or sets the encryption master key. The key is required to be 32 bytes in length.
        /// </summary>
        byte[] EncryptionMasterKey { get; set; }

        /// <summary>
        /// Gets the base Url based on the set Options
        /// </summary>
        /// <returns>The constructed URL</returns>
        Uri GetBaseUrl();
    }
}
