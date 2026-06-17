using System;
using System.Collections.Generic;

namespace PusherServer
{
    /// <summary>
    /// Interface for Web Hooks
    /// </summary>
    public interface IWebHook
    {
        /// <summary>
        /// Indicates if the WebHook has validated.
        /// </summary>
        bool IsValid
        {
            get;
        }

        /// <summary>
        /// The Events in the WebHook
        /// </summary>
        Dictionary<string, string>[] Events
        {
            get;
        }


        /// <summary>
        /// The timestamp of the WebHook
        /// </summary>
        DateTime Time
        {
            get;
        }

        /// <summary>
        /// An array of validation errors. If <see cref="IsValid"/> is true then the array
        /// will have no elements.
        /// </summary>
        string[] ValidationErrors
        {
            get;
        }

    }
}