namespace Sockudo.Client;

public enum MutableMessageAction
{
    Create,
    Update,
    Delete,
    Append,
}

public sealed record MutableMessageVersionInfo(
    MutableMessageAction Action,
    string Event,
    string MessageSerial,
    string? VersionSerial = null,
    long? HistorySerial = null,
    long? VersionTimestampMs = null
);

public sealed record MutableMessageState(
    string MessageSerial,
    MutableMessageAction Action,
    object? Data,
    string Event,
    int? Serial = null,
    string? StreamId = null,
    string? MessageId = null,
    string? VersionSerial = null,
    long? HistorySerial = null,
    long? VersionTimestampMs = null
);

public static class MutableMessageReducer
{
    private static readonly Dictionary<string, MutableMessageAction> ActionMap = new(StringComparer.Ordinal)
    {
        ["message.create"] = MutableMessageAction.Create,
        ["message.update"] = MutableMessageAction.Update,
        ["message.delete"] = MutableMessageAction.Delete,
        ["message.append"] = MutableMessageAction.Append,
    };

    private static long? ParseNumericHeader(object? value) =>
        value switch
        {
            long l => l,
            int i => (long)i,
            double d when d == Math.Truncate(d) => (long)d,
            string s when double.TryParse(s.Trim(), out var d) && d == Math.Truncate(d) => (long)d,
            _ => null,
        };

    public static bool IsMutableMessageEvent(SockudoEvent @event) =>
        GetMutableMessageInfo(@event) is not null;

    public static MutableMessageVersionInfo? GetMutableMessageInfo(SockudoEvent @event)
    {
        var headers = @event.Extras?.Headers;
        if (headers is null) return null;

        if (!headers.TryGetValue("sockudo_action", out var actionObj)) return null;
        if (!headers.TryGetValue("sockudo_message_serial", out var messageSerialObj)) return null;

        var actionRaw = actionObj as string;
        var messageSerial = messageSerialObj as string;

        if (actionRaw is null || messageSerial is null) return null;
        if (!ActionMap.TryGetValue(actionRaw, out var action)) return null;

        var versionSerial = headers.TryGetValue("sockudo_version_serial", out var vs) ? vs as string : null;
        var historySerial = headers.TryGetValue("sockudo_history_serial", out var hs) ? ParseNumericHeader(hs) : null;
        var versionTimestampMs = headers.TryGetValue("sockudo_version_timestamp_ms", out var vt) ? ParseNumericHeader(vt) : null;

        return new MutableMessageVersionInfo(
            Action: action,
            Event: @event.Event,
            MessageSerial: messageSerial,
            VersionSerial: versionSerial,
            HistorySerial: historySerial,
            VersionTimestampMs: versionTimestampMs
        );
    }

    public static MutableMessageState ReduceMutableMessageEvent(
        MutableMessageState? current,
        SockudoEvent @event)
    {
        var info = GetMutableMessageInfo(@event)
            ?? throw new InvalidOperationException("Event is not a mutable-message event");

        if (current is not null && current.MessageSerial != info.MessageSerial)
        {
            throw new InvalidOperationException(
                $"Mutable-message reducer expected message_serial '{current.MessageSerial}'" +
                $" but received '{info.MessageSerial}'"
            );
        }

        object? nextData = info.Action switch
        {
            MutableMessageAction.Append => ReduceAppend(current, @event),
            _ => @event.Data,
        };

        return new MutableMessageState(
            MessageSerial: info.MessageSerial,
            Action: info.Action,
            Data: nextData,
            Event: info.Event,
            Serial: @event.Serial,
            StreamId: @event.StreamId,
            MessageId: @event.MessageId,
            VersionSerial: info.VersionSerial,
            HistorySerial: info.HistorySerial,
            VersionTimestampMs: info.VersionTimestampMs
        );
    }

    private static string ReduceAppend(MutableMessageState? current, SockudoEvent @event)
    {
        if (current?.Data is not string baseValue)
        {
            throw new InvalidOperationException(
                "message.append requires an existing string base;" +
                " seed state from a create/update payload or latest-view history first"
            );
        }

        if (@event.Data is not string fragment)
        {
            throw new InvalidOperationException(
                "message.append payload must be a string fragment when applying" +
                " client-side concatenation"
            );
        }

        return baseValue + fragment;
    }

    public static MutableMessageState? ReduceMutableMessageEvents(IEnumerable<SockudoEvent> events)
    {
        MutableMessageState? state = null;
        foreach (var @event in events)
        {
            state = ReduceMutableMessageEvent(state, @event);
        }
        return state;
    }
}
