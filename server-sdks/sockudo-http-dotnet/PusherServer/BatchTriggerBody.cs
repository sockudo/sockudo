
namespace PusherServer
{
    /// <summary>
    /// Represents the payload to be sent when triggering events
    /// </summary>
    class BatchTriggerBody
    {
        public BatchEvent[] batch { get; set; }
    }
}
