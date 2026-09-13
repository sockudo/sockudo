namespace Sockudo.Client;

/// <summary>
/// Apple Live Activity helpers. ActivityKit itself lives in the iOS host application; these
/// types validate and serialize the token uploads and proxy publish requests that a .NET
/// client sends to an authenticated backend. Field names match the Sockudo push API.
/// </summary>
public enum ApnsChannelStoragePolicy { NoStorage, MostRecent }

public enum ApnsLiveActivityEvent { Start, Update, End }

public enum ApnsLiveActivityPriority { LowPower, ConservePower, Immediate }

public enum ApnsLiveActivityTokenKind { PushToStart, Update }

internal static class ApnsLiveActivityWire
{
    public static string Name(ApnsChannelStoragePolicy value) => value switch
    {
        ApnsChannelStoragePolicy.NoStorage => "noStorage",
        ApnsChannelStoragePolicy.MostRecent => "mostRecent",
        _ => throw new ArgumentOutOfRangeException(nameof(value)),
    };

    public static string Name(ApnsLiveActivityEvent value) => value switch
    {
        ApnsLiveActivityEvent.Start => "start",
        ApnsLiveActivityEvent.Update => "update",
        ApnsLiveActivityEvent.End => "end",
        _ => throw new ArgumentOutOfRangeException(nameof(value)),
    };

    public static string Name(ApnsLiveActivityPriority value) => value switch
    {
        ApnsLiveActivityPriority.LowPower => "lowPower",
        ApnsLiveActivityPriority.ConservePower => "conservePower",
        ApnsLiveActivityPriority.Immediate => "immediate",
        _ => throw new ArgumentOutOfRangeException(nameof(value)),
    };

    public static string Name(ApnsLiveActivityTokenKind value) => value switch
    {
        ApnsLiveActivityTokenKind.PushToStart => "pushToStart",
        ApnsLiveActivityTokenKind.Update => "update",
        _ => throw new ArgumentOutOfRangeException(nameof(value)),
    };
}

/// <summary>
/// A token rotation emitted by the app's ActivityKit bridge. Upload it to an authenticated
/// backend immediately; Live Activity tokens are credentials and must not be logged.
/// </summary>
public sealed class ApnsLiveActivityTokenUpdate
{
    private ApnsLiveActivityTokenUpdate(ApnsLiveActivityTokenKind kind, string token, string? activityId)
    {
        Kind = kind;
        Token = token;
        ActivityId = activityId;
    }

    public static ApnsLiveActivityTokenUpdate PushToStart(string token) =>
        new(ApnsLiveActivityTokenKind.PushToStart, token, null);

    public static ApnsLiveActivityTokenUpdate Activity(string activityId, string token) =>
        new(ApnsLiveActivityTokenKind.Update, token, activityId);

    public ApnsLiveActivityTokenKind Kind { get; }
    public string Token { get; }
    public string? ActivityId { get; }

    public Dictionary<string, object?> ToJson()
    {
        if (string.IsNullOrWhiteSpace(Token))
        {
            throw new ArgumentException("token must not be empty", nameof(Token));
        }
        if (Kind == ApnsLiveActivityTokenKind.Update && string.IsNullOrWhiteSpace(ActivityId))
        {
            throw new ArgumentException("activityId is required for activity update tokens", nameof(ActivityId));
        }
        var json = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["kind"] = ApnsLiveActivityWire.Name(Kind),
            ["token"] = Token,
        };
        if (ActivityId is not null)
        {
            json["activityId"] = ActivityId;
        }
        return json;
    }

    /// <summary>Formats raw ActivityKit token bytes as the lowercase hex APNs expects.</summary>
    public static string EncodeHex(ReadOnlySpan<byte> bytes) => Convert.ToHexString(bytes).ToLowerInvariant();
}

public abstract class ApnsLiveActivityRecipient
{
    public abstract bool IsBroadcast { get; }
    public abstract Dictionary<string, object?> ToJson();
}

public sealed class ApnsLiveActivityTokenRecipient : ApnsLiveActivityRecipient
{
    public ApnsLiveActivityTokenRecipient(string activityToken)
    {
        ActivityToken = activityToken;
    }

    public string ActivityToken { get; }
    public override bool IsBroadcast => false;

    public override Dictionary<string, object?> ToJson()
    {
        if (string.IsNullOrWhiteSpace(ActivityToken))
        {
            throw new ArgumentException("activityToken must not be empty", nameof(ActivityToken));
        }
        return new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["transportType"] = "apnsLiveActivity",
            ["activityToken"] = ActivityToken,
        };
    }
}

public sealed class ApnsLiveActivityBroadcastRecipient : ApnsLiveActivityRecipient
{
    public ApnsLiveActivityBroadcastRecipient(
        string channelId,
        ApnsChannelStoragePolicy storagePolicy = ApnsChannelStoragePolicy.NoStorage)
    {
        ChannelId = channelId;
        StoragePolicy = storagePolicy;
    }

    public string ChannelId { get; }
    public ApnsChannelStoragePolicy StoragePolicy { get; }
    public override bool IsBroadcast => true;

    public override Dictionary<string, object?> ToJson()
    {
        if (string.IsNullOrWhiteSpace(ChannelId))
        {
            throw new ArgumentException("channelId must not be empty", nameof(ChannelId));
        }
        return new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["transportType"] = "apnsLiveActivityBroadcast",
            ["channelId"] = ChannelId,
            ["storagePolicy"] = ApnsLiveActivityWire.Name(StoragePolicy),
        };
    }
}

/// <summary>Typed ActivityKit content; Sockudo converts it into the APNs wire payload.</summary>
public sealed record ApnsLiveActivityPayload(
    ApnsLiveActivityEvent Event,
    long Timestamp,
    IReadOnlyDictionary<string, object?> ContentState,
    string? AttributesType = null,
    IReadOnlyDictionary<string, object?>? Attributes = null,
    IReadOnlyDictionary<string, object?>? Alert = null,
    long? StaleDate = null,
    long? DismissalDate = null,
    double? RelevanceScore = null,
    bool InputPushToken = false,
    string? InputPushChannel = null,
    ApnsLiveActivityPriority Priority = ApnsLiveActivityPriority.ConservePower)
{
    public Dictionary<string, object?> ToJson(bool broadcast)
    {
        Validate(broadcast);
        var json = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["event"] = ApnsLiveActivityWire.Name(Event),
            ["timestamp"] = Timestamp,
            ["contentState"] = ContentState,
        };
        if (AttributesType is not null) json["attributesType"] = AttributesType;
        if (Attributes is not null) json["attributes"] = Attributes;
        if (Alert is not null) json["alert"] = Alert;
        if (StaleDate is not null) json["staleDate"] = StaleDate;
        if (DismissalDate is not null) json["dismissalDate"] = DismissalDate;
        if (RelevanceScore is not null) json["relevanceScore"] = RelevanceScore;
        if (InputPushToken) json["inputPushToken"] = true;
        if (InputPushChannel is not null) json["inputPushChannel"] = InputPushChannel;
        json["priority"] = ApnsLiveActivityWire.Name(Priority);
        return json;
    }

    private void Validate(bool broadcast)
    {
        if (Timestamp <= 0)
        {
            throw new ArgumentException("timestamp must be positive", nameof(Timestamp));
        }
        if (ContentState is null)
        {
            throw new ArgumentException("contentState is required", nameof(ContentState));
        }
        if (InputPushToken && InputPushChannel is not null)
        {
            throw new ArgumentException("inputPushToken and inputPushChannel are mutually exclusive");
        }
        if (InputPushChannel is not null && string.IsNullOrWhiteSpace(InputPushChannel))
        {
            throw new ArgumentException("inputPushChannel must not be empty", nameof(InputPushChannel));
        }
        if (Priority == ApnsLiveActivityPriority.LowPower && !broadcast)
        {
            throw new ArgumentException("lowPower priority is broadcast-only", nameof(Priority));
        }
        switch (Event)
        {
            case ApnsLiveActivityEvent.Start:
                if (broadcast)
                {
                    throw new ArgumentException("broadcast notifications cannot start an activity", nameof(Event));
                }
                if (string.IsNullOrWhiteSpace(AttributesType))
                {
                    throw new ArgumentException("attributesType is required for start events", nameof(AttributesType));
                }
                if (Attributes is null || Alert is null)
                {
                    throw new ArgumentException("attributes and alert are required for start events");
                }
                if (StaleDate is not null || DismissalDate is not null)
                {
                    throw new ArgumentException("start events cannot include stale or dismissal dates");
                }
                break;
            case ApnsLiveActivityEvent.Update:
                if (AttributesType is not null || Attributes is not null || DismissalDate is not null
                    || InputPushToken || InputPushChannel is not null)
                {
                    throw new ArgumentException("update event contains start or end-only fields");
                }
                break;
            default:
                if (AttributesType is not null || Attributes is not null || StaleDate is not null
                    || InputPushToken || InputPushChannel is not null)
                {
                    throw new ArgumentException("end event contains start or update-only fields");
                }
                break;
        }
        if (RelevanceScore is { } score && (!double.IsFinite(score) || score < 0))
        {
            throw new ArgumentException("relevanceScore must be finite and nonnegative", nameof(RelevanceScore));
        }
    }
}

public sealed record ApnsLiveActivityPublishRequest(
    ApnsLiveActivityRecipient Recipient,
    ApnsLiveActivityPayload LiveActivity,
    string? PublishId = null,
    long? NotBeforeMs = null,
    long? ExpiresAtMs = null)
{
    public Dictionary<string, object?> ToJson()
    {
        var json = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (PublishId is not null) json["publishId"] = PublishId;
        json["recipients"] = new object?[]
        {
            new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["type"] = "recipient",
                ["recipient"] = Recipient.ToJson(),
            },
        };
        json["payload"] = new Dictionary<string, object?>(StringComparer.Ordinal);
        json["liveActivity"] = LiveActivity.ToJson(Recipient.IsBroadcast);
        if (NotBeforeMs is not null) json["notBeforeMs"] = NotBeforeMs;
        if (ExpiresAtMs is not null) json["expiresAtMs"] = ExpiresAtMs;
        return json;
    }
}
