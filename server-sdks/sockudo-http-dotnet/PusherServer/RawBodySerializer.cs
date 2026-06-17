using System;

namespace PusherServer
{
    /// <summary>
    /// An implementation of the <see cref="ISerializeObjectsToJson"/> that passes through the raw string.
    /// </summary>
    public class RawBodySerializer : ISerializeObjectsToJson
    {
        /// <summary>
        /// Presumes we are getting a string as the body, and passes it through.
        /// </summary>
        /// <param name="body">The string body to pass through.</param>
        /// <returns>The body passed in.</returns>
        /// <exception cref="ArgumentException">Thrown if the body provided is not a string.</exception>
        public string Serialize(object body)
        {
            if (body == null)
            {
                return string.Empty;
            }

            var bodyAsString = body as string;
            if(bodyAsString == null)
            {
                throw new ArgumentException("The RawBodySerializer only supports strings for messages.  The body type was: " + body.GetType().Name, "body");
            }

            return bodyAsString;
        }
    }
}