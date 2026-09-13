using System.Net.Http.Json;
using System.Text.Json;
using Xunit;

namespace Sockudo.Client.Tests;

public sealed class LiveActivityTests
{
    [Fact]
    public void SerializesValidatedDirectAndBroadcastRequests()
    {
        var direct = new ApnsLiveActivityPublishRequest(
            PublishId: "ride-42-start",
            Recipient: new ApnsLiveActivityTokenRecipient("push-to-start-token"),
            LiveActivity: new ApnsLiveActivityPayload(
                Event: ApnsLiveActivityEvent.Start,
                Timestamp: 1_725_000_000,
                ContentState: new Dictionary<string, object?> { ["status"] = "driverAssigned", ["etaMinutes"] = 4 },
                AttributesType: "RideAttributes",
                Attributes: new Dictionary<string, object?> { ["rideID"] = "ride-42" },
                Alert: new Dictionary<string, object?> { ["title"] = "Driver assigned" },
                InputPushChannel: "channel-1",
                Priority: ApnsLiveActivityPriority.Immediate)).ToJson();

        var recipients = Assert.IsType<object?[]>(direct["recipients"]);
        var recipient = Assert.IsType<Dictionary<string, object?>>(
            Assert.IsType<Dictionary<string, object?>>(recipients[0])["recipient"]);
        Assert.Equal("apnsLiveActivity", recipient["transportType"]);
        Assert.Equal("push-to-start-token", recipient["activityToken"]);
        var liveActivity = Assert.IsType<Dictionary<string, object?>>(direct["liveActivity"]);
        Assert.Equal("start", liveActivity["event"]);
        Assert.Equal("immediate", liveActivity["priority"]);
        Assert.Equal("channel-1", liveActivity["inputPushChannel"]);
        Assert.False(liveActivity.ContainsKey("inputPushToken"));
        Assert.False(direct.ContainsKey("expiresAtMs"));
        Assert.Empty(Assert.IsType<Dictionary<string, object?>>(direct["payload"]));

        var broadcast = new ApnsLiveActivityPublishRequest(
            Recipient: new ApnsLiveActivityBroadcastRecipient("channel-1", ApnsChannelStoragePolicy.MostRecent),
            ExpiresAtMs: 1_725_003_600_000,
            LiveActivity: new ApnsLiveActivityPayload(
                Event: ApnsLiveActivityEvent.Update,
                Timestamp: 1_725_000_000,
                ContentState: new Dictionary<string, object?> { ["home"] = 3 },
                Priority: ApnsLiveActivityPriority.LowPower)).ToJson();
        var broadcastRecipient = Assert.IsType<Dictionary<string, object?>>(
            Assert.IsType<Dictionary<string, object?>>(Assert.IsType<object?[]>(broadcast["recipients"])[0])["recipient"]);
        Assert.Equal("apnsLiveActivityBroadcast", broadcastRecipient["transportType"]);
        Assert.Equal("mostRecent", broadcastRecipient["storagePolicy"]);
        Assert.Equal(1_725_003_600_000L, broadcast["expiresAtMs"]);
        Assert.Equal("lowPower", Assert.IsType<Dictionary<string, object?>>(broadcast["liveActivity"])["priority"]);

        // Wire names must be camelCase after JSON serialization.
        var encoded = JsonSerializer.Serialize(broadcast);
        Assert.Contains("\"storagePolicy\":\"mostRecent\"", encoded);
        Assert.Contains("\"priority\":\"lowPower\"", encoded);
    }

    [Fact]
    public void RejectsInvalidCombinationsBeforeProxyUpload()
    {
        var state = new Dictionary<string, object?> { ["status"] = "x" };

        Assert.Throws<ArgumentException>(() =>
            new ApnsLiveActivityPayload(ApnsLiveActivityEvent.Update, 1, state, Priority: ApnsLiveActivityPriority.LowPower)
                .ToJson(broadcast: false));
        Assert.Throws<ArgumentException>(() =>
            new ApnsLiveActivityPayload(ApnsLiveActivityEvent.Update, 0, state).ToJson(broadcast: false));
        Assert.Throws<ArgumentException>(() =>
            new ApnsLiveActivityPayload(ApnsLiveActivityEvent.Start, 1, state, AttributesType: "A",
                Attributes: state, Alert: state, InputPushToken: true, InputPushChannel: "c").ToJson(broadcast: false));
        Assert.Throws<ArgumentException>(() =>
            new ApnsLiveActivityPayload(ApnsLiveActivityEvent.Start, 1, state).ToJson(broadcast: false));
        Assert.Throws<ArgumentException>(() =>
            new ApnsLiveActivityPayload(ApnsLiveActivityEvent.Start, 1, state, AttributesType: "A",
                Attributes: state, Alert: state).ToJson(broadcast: true));
        Assert.Throws<ArgumentException>(() =>
            new ApnsLiveActivityPayload(ApnsLiveActivityEvent.Update, 1, state, DismissalDate: 5).ToJson(broadcast: false));
        Assert.Throws<ArgumentException>(() =>
            new ApnsLiveActivityPayload(ApnsLiveActivityEvent.End, 1, state, StaleDate: 5).ToJson(broadcast: false));
        Assert.Throws<ArgumentException>(() =>
            new ApnsLiveActivityPayload(ApnsLiveActivityEvent.Update, 1, state, RelevanceScore: -1).ToJson(broadcast: false));
        Assert.Throws<ArgumentException>(() => new ApnsLiveActivityTokenRecipient(" ").ToJson());
        Assert.Throws<ArgumentException>(() => new ApnsLiveActivityBroadcastRecipient("").ToJson());
        Assert.Throws<ArgumentException>(() => ApnsLiveActivityTokenUpdate.Activity("", "abc").ToJson());
        Assert.Throws<ArgumentException>(() => ApnsLiveActivityTokenUpdate.PushToStart("").ToJson());
    }

    [Fact]
    public void SerializesTokenUpdatesAndHexTokens()
    {
        var pushToStart = ApnsLiveActivityTokenUpdate.PushToStart("abcd").ToJson();
        Assert.Equal("pushToStart", pushToStart["kind"]);
        Assert.Equal("abcd", pushToStart["token"]);
        Assert.False(pushToStart.ContainsKey("activityId"));

        var activity = ApnsLiveActivityTokenUpdate.Activity("activity-1", "ef01").ToJson();
        Assert.Equal("update", activity["kind"]);
        Assert.Equal("activity-1", activity["activityId"]);

        Assert.Equal("00ff10ab", ApnsLiveActivityTokenUpdate.EncodeHex(new byte[] { 0x00, 0xff, 0x10, 0xab }));
    }

    [Fact]
    public async Task LiveActivityProxyPublishesReachApns()
    {
        // Requires SOCKUDO_LIVE_TESTS=1 and a push proxy such as
        // examples/apple-live-activities-backend (SOCKUDO_PUSH_PROXY_URL, default
        // http://127.0.0.1:8787/push) in front of a Sockudo built with push-apns.
        if (Environment.GetEnvironmentVariable("SOCKUDO_LIVE_TESTS") != "1")
        {
            return;
        }
        var proxy = Environment.GetEnvironmentVariable("SOCKUDO_PUSH_PROXY_URL") ?? "http://127.0.0.1:8787/push";
        using var http = new HttpClient();
        var push = new SockudoPushRegistration(new PushRegistrationOptions(Endpoint: proxy), http);

        using var channelResponse = await http.PostAsJsonAsync(
            $"{proxy}/liveActivities/channels", new { storagePolicy = "mostRecent" });
        channelResponse.EnsureSuccessStatusCode();
        var channelId = (await channelResponse.Content.ReadFromJsonAsync<JsonElement>())
            .GetProperty("channelId").GetString()!;
        try
        {
            var runId = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString("x");
            var nowSecs = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var activityToken = ApnsLiveActivityTokenUpdate.EncodeHex(
                Enumerable.Range(0, 32).Select(index => (byte)((index * 11 + 5) & 0xff)).ToArray());

            var direct = await push.PublishLiveActivityAsync(new ApnsLiveActivityPublishRequest(
                PublishId: $"dotnet-{runId}-update",
                Recipient: new ApnsLiveActivityTokenRecipient(activityToken),
                LiveActivity: new ApnsLiveActivityPayload(
                    ApnsLiveActivityEvent.Update,
                    nowSecs,
                    new Dictionary<string, object?> { ["status"] = "arriving", ["etaMinutes"] = 1 },
                    StaleDate: nowSecs + 120,
                    RelevanceScore: 0.9)));
            Assert.Equal($"dotnet-{runId}-update", direct["publishId"]);
            Assert.Equal(1L, direct["expectedRecipients"]);

            var broadcast = await push.PublishLiveActivityAsync(new ApnsLiveActivityPublishRequest(
                PublishId: $"dotnet-{runId}-broadcast",
                Recipient: new ApnsLiveActivityBroadcastRecipient(channelId, ApnsChannelStoragePolicy.MostRecent),
                ExpiresAtMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1800 * 1000,
                LiveActivity: new ApnsLiveActivityPayload(
                    ApnsLiveActivityEvent.End,
                    nowSecs,
                    new Dictionary<string, object?> { ["home"] = 3, ["away"] = 1 },
                    DismissalDate: nowSecs + 600,
                    Priority: ApnsLiveActivityPriority.LowPower)));
            Assert.Equal(1L, broadcast["expectedRecipients"]);

            async Task<Dictionary<string, object?>> Settled(string publishId)
            {
                var deadline = DateTime.UtcNow.AddSeconds(20);
                while (true)
                {
                    var status = await push.GetPublishStatusAsync(publishId);
                    var counters = Assert.IsType<Dictionary<string, object?>>(status["counters"]);
                    var done = (long)counters["succeeded"]! + (long)counters["failed"]! > 0;
                    if (done || DateTime.UtcNow > deadline)
                    {
                        return status;
                    }
                    await Task.Delay(300);
                }
            }

            var directStatus = await Settled($"dotnet-{runId}-update");
            Assert.Equal(1L, Assert.IsType<Dictionary<string, object?>>(directStatus["counters"])["succeeded"]);
            var broadcastStatus = await Settled($"dotnet-{runId}-broadcast");
            Assert.Equal(1L, Assert.IsType<Dictionary<string, object?>>(broadcastStatus["counters"])["succeeded"]);

            var mock = Environment.GetEnvironmentVariable("SOCKUDO_APNS_MOCK_URL");
            if (mock is not null)
            {
                var seen = (await http.GetFromJsonAsync<JsonElement>($"{mock}/_mock/requests")).GetProperty("requests");
                var directRequest = seen.EnumerateArray().First(r => r.GetProperty("path").GetString()!.EndsWith(activityToken));
                Assert.Equal("liveactivity", directRequest.GetProperty("headers").GetProperty("apns-push-type").GetString());
                Assert.Equal("5", directRequest.GetProperty("headers").GetProperty("apns-priority").GetString());
                Assert.Equal("update", directRequest.GetProperty("body").GetProperty("aps").GetProperty("event").GetString());
                var broadcastRequest = seen.EnumerateArray().First(r =>
                    r.GetProperty("headers").TryGetProperty("apns-channel-id", out var id) && id.GetString() == channelId);
                Assert.Equal("1", broadcastRequest.GetProperty("headers").GetProperty("apns-priority").GetString());
                Assert.True(long.Parse(broadcastRequest.GetProperty("headers").GetProperty("apns-expiration").GetString()!) > nowSecs);
            }
        }
        finally
        {
            using var delete = await http.DeleteAsync($"{proxy}/liveActivities/channels/{Uri.EscapeDataString(channelId)}");
        }
    }
}
