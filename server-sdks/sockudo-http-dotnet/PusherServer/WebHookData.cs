using System;
using System.Collections.Generic;

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
        /// Gets or sets the Events being triggered
        /// </summary>
        public Dictionary<string, string>[] events { get; set; }

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
    }
}
