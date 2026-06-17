using System;
using System.Text.RegularExpressions;
using PusherServer.RestfulClient;
using PusherServer.Util;

namespace PusherServer
{
    /// <summary>
    /// Options to be set on the <see cref="Pusher">Pusher</see> instance.
    /// </summary>
    public class PusherOptions : IPusherOptions
    {
        /// <summary>
        /// The default Rest API Host for contacting the Pusher server, it does not contain a cluster name
        /// </summary>
        public const string DEFAULT_REST_API_HOST = "api.pusherapp.com";

        /// <summary>
        /// The default batch event data size limit in bytes.
        /// </summary>
        public const int DEFAULT_BATCH_EVENT_DATA_SIZE_LIMIT = 10 * 1024;

        private const int DEFAULT_HTTPS_PORT = 443;
        private const int DEFAULT_HTTP_PORT = 80;

        IPusherRestClient _pusherClient;
        bool _encrypted;
        bool _portModified;
        bool _hostSet;
        int _port = DEFAULT_HTTP_PORT;
        string _hostName;
        string _cluster;
        ISerializeObjectsToJson _jsonSerializer;
        IDeserializeJsonStrings _jsonDeserializer;

        /// <inheritedDoc/>
        public bool Encrypted
        {
            get
            {
                return _encrypted;
            }
            set
            {
                _encrypted = value;
                if (_encrypted && _portModified == false)
                {
                    _port = DEFAULT_HTTPS_PORT;
                }
            }
        }

        /// <inheritDoc/>
        public int Port
        {
            get
            {
                return _port;
            }
            set
            {
                _port = value;
                _portModified = true;
            }
        }

        /// <inheritDoc/>
        public string Cluster
        {
          get
          {
            return _cluster;
          }
          set
          {
            if (_hostSet == false) {
              _cluster = value;
              _hostName = "api-"+_cluster+".pusher.com";
            }
          }
        }

        /// <inheritDoc/>
        public IPusherRestClient RestClient
        {
            get
            {
                if (_pusherClient == null)
                {
                    _pusherClient = new PusherRestClient(GetBaseUrl(), Pusher.LIBRARY_NAME, Pusher.VERSION)
                    {
                        Timeout = RestClientTimeout,
                    };
                }

                return _pusherClient;
            }
            set { _pusherClient = value; }
        }

        /// <inheritDoc/>
        public TimeSpan RestClientTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <inheritDoc/>
        public string HostName
        {
            get
            {
                return _hostName ?? DEFAULT_REST_API_HOST;
            }
            set
            {
                if (Regex.IsMatch(value, "^.*://"))
                {
                    string msg = string.Format("The scheme should not be present in the host value: {0}", value);
                    throw new FormatException(msg);
                }

                _hostSet = true;
                _cluster = null;
                _hostName = value;
            }
        }

        /// <inheritDoc/>
        public ISerializeObjectsToJson JsonSerializer
        {
            get
            {
                if (_jsonSerializer == null)
                {
                    _jsonSerializer = new DefaultSerializer();
                }

                return _jsonSerializer;

            }
            set { _jsonSerializer = value; }
        }

        /// <inheritDoc/>
        public IDeserializeJsonStrings JsonDeserializer
        {
            get
            {
                if (_jsonDeserializer == null)
                {
                    _jsonDeserializer = new DefaultDeserializer();
                }

                return _jsonDeserializer;
            }
            set
            {
                _jsonDeserializer = value;
            }
        }

        /// <inheritDoc/>
        public int? BatchEventDataSizeLimit { get; set; }

        /// <inheritDoc/>
        public ITraceLogger TraceLogger { get; set; }

        /// <inheritDoc/>
        public byte[] EncryptionMasterKey { get; set; }

        /// <inheritDoc/>
        public Uri GetBaseUrl()
        {
            string baseUrl = (Encrypted ? "https" : "http") + "://" + HostName + GetPort();

            return new Uri(baseUrl);
        }

        private string GetPort()
        {
            var port = string.Empty;

            if (Port != DEFAULT_HTTP_PORT)
            {
                port += (":" + Port);
            }

            return port;
        }
    }
}
