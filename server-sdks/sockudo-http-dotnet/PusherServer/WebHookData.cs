using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace PusherServer
{
    /// <summary>
    /// Represents the Data payload of a Web Hook
    /// </summary>
    public class WebHookData
    {
        private DateTime _time;

        /// <summary>
        /// Gets or sets the Time the Web Hook was created in Milliseconds
        /// </summary>
        public string time_ms
        {
            get
            {
                // This should not be used.
                return GetUnixTimestampMillis(this.Time).ToString();
            }
            set
            {
                long unixTimeStamp = long.Parse(value);
                _time = DateTimeFromUnixTimestampMillis(unixTimeStamp);
            }
        }
        /// <summary>
        /// Gets or sets the raw Events being triggered.
        /// </summary>
        [JsonProperty("events")]
        public Dictionary<string, object>[] RawEvents { get; set; } = Array.Empty<Dictionary<string, object>>();

        /// <summary>
        /// Gets or sets the Events being triggered as the legacy string projection.
        /// </summary>
        [JsonIgnore]
        public Dictionary<string, string>[] events
        {
            get
            {
                return (RawEvents ?? Array.Empty<Dictionary<string, object>>())
                    .Select(ProjectStringEvent)
                    .ToArray();
            }
            set
            {
                RawEvents = value?
                    .Select(evt => evt.ToDictionary(
                        item => item.Key,
                        item => (object)item.Value))
                    .ToArray();
            }
        }

        /// <summary>
        /// Gets the Time the Web Hook was created
        /// </summary>
        public DateTime Time
        {
            get
            {
                return this._time;
            }
        }

        private static readonly DateTime UnixEpoch =
            new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private static long GetUnixTimestampMillis(DateTime dateTime)
        {
            return (long)(dateTime - UnixEpoch).TotalMilliseconds;
        }

        private static DateTime DateTimeFromUnixTimestampMillis(long millis)
        {
            return UnixEpoch.AddMilliseconds(millis);
        }

        private static Dictionary<string, string> ProjectStringEvent(Dictionary<string, object> rawEvent)
        {
            return rawEvent.ToDictionary(
                item => item.Key,
                item => StringValue(item.Value));
        }

        private static string StringValue(object value)
        {
            if (value == null)
            {
                return string.Empty;
            }

            if (value is string stringValue)
            {
                return stringValue;
            }

            if (value is JToken token)
            {
                return token.ToString(Formatting.None);
            }

            return JsonConvert.SerializeObject(value);
        }
    }
}
