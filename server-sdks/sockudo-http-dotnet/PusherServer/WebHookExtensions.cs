using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace PusherServer
{
    /// <summary>
    /// Forward-compatible helpers for Web Hook payloads.
    /// </summary>
    public static class WebHookExtensions
    {
        /// <summary>
        /// Gets the raw webhook events without stringifying nested future payload fields.
        /// </summary>
        public static IReadOnlyList<IReadOnlyDictionary<string, object>> GetRawEvents(this IWebHook webHook)
        {
            if (webHook is WebHook concreteWebHook)
            {
                return concreteWebHook.RawEvents;
            }

            return webHook.Events
                .Select(evt => new ReadOnlyDictionary<string, object>(
                    evt.ToDictionary(item => item.Key, item => (object)item.Value)))
                .Cast<IReadOnlyDictionary<string, object>>()
                .ToArray();
        }
    }
}
