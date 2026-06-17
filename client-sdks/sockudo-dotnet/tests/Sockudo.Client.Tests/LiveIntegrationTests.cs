using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Xunit;

namespace Sockudo.Client.Tests;

public sealed class LiveIntegrationTests
{
    private readonly HttpClient _httpClient = new();

    [Fact]
    public async Task LocalSockudoConnectsAndReceivesPublishedEvent()
    {
        if (!LiveTestsEnabled())
        {
            return;
        }

        var connected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = new TaskCompletionSource<Dictionary<string, object?>>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var client = new SockudoClient(
            "app-key",
            new SockudoOptions(
                Cluster: "local",
                ForceTls: false,
                EnabledTransports: new[] { SockudoTransport.Ws },
                WsHost: "127.0.0.1",
                WsPort: 6001,
                WssPort: 6001,
                WireFormat: SockudoWireFormat.Json
            )
        );

        var channel = client.Subscribe("public-updates");
        client.Bind("connected", (_, _) => connected.TrySetResult());
        channel.Bind("sockudo:subscription_succeeded", (_, _) => subscribed.TrySetResult());
        channel.Bind("integration-event", (data, _) =>
        {
            if (data is Dictionary<string, object?> payload)
            {
                received.TrySetResult(payload);
            }
        });

        await client.ConnectAsync();
        await WaitForAsync(connected.Task);
        await WaitForAsync(subscribed.Task);

        await PublishToLocalSockudoAsync(
            channel: "public-updates",
            eventName: "integration-event",
            payload: new Dictionary<string, object?>
            {
                ["message"] = "hello from dotnet",
                ["item_id"] = "dotnet-client",
                ["padding"] = new string('x', 140),
            }
        );

        var payload = await WaitForAsync(received.Task);
        Assert.Equal("hello from dotnet", payload["message"]);
    }

    [Fact]
    public async Task LiveV2HeartbeatUsesControlFramesOnIdle()
    {
        if (!LiveTestsEnabled())
        {
            return;
        }

        using var socket = new ClientWebSocket();
        await socket.ConnectAsync(LiveSockudoUri(protocolVersion: 2), CancellationToken.None);

        var handshake = ProtocolCodec.DecodeEvent(
            await ReceiveMessageAsync(socket, timeout: TimeSpan.FromSeconds(3)),
            SockudoWireFormat.Json
        );
        Assert.Equal("sockudo:connection_established", handshake.Event);

        await Assert.ThrowsAsync<TimeoutException>(async () =>
        {
            var unexpected = await ReceiveMessageAsync(socket, timeout: TimeSpan.FromSeconds(8));
            var @event = ProtocolCodec.DecodeEvent(unexpected, SockudoWireFormat.Json);
            throw new Xunit.Sdk.XunitException($"Expected no protocol heartbeat messages on idle V2 connection, got {@event.Event}");
        });
    }

    [Fact]
    public async Task LiveV2FallbackPongHasNoMetadata()
    {
        if (!LiveTestsEnabled())
        {
            return;
        }

        using var socket = new ClientWebSocket();
        await socket.ConnectAsync(LiveSockudoUri(protocolVersion: 2), CancellationToken.None);

        var handshake = ProtocolCodec.DecodeEvent(
            await ReceiveMessageAsync(socket, timeout: TimeSpan.FromSeconds(3)),
            SockudoWireFormat.Json
        );
        Assert.Equal("sockudo:connection_established", handshake.Event);

        await SendJsonAsync(socket, new Dictionary<string, object?>
        {
            ["event"] = "sockudo:ping",
            ["data"] = new Dictionary<string, object?>(),
        });

        var pong = ProtocolCodec.DecodeEvent(
            await ReceiveMessageAsync(socket, timeout: TimeSpan.FromSeconds(3)),
            SockudoWireFormat.Json
        );

        Assert.Equal("sockudo:pong", pong.Event);
        Assert.Null(pong.MessageId);
        Assert.Null(pong.Serial);
        Assert.Null(pong.StreamId);
    }

    [Fact]
    public async Task LiveV1HeartbeatStillUsesProtocolPing()
    {
        if (!LiveTestsEnabled())
        {
            return;
        }

        using var socket = new ClientWebSocket();
        await socket.ConnectAsync(LiveSockudoUri(protocolVersion: 7), CancellationToken.None);

        var handshake = ProtocolCodec.DecodeEvent(
            await ReceiveMessageAsync(socket, timeout: TimeSpan.FromSeconds(3)),
            SockudoWireFormat.Json
        );
        Assert.Equal("pusher:connection_established", handshake.Event);

        var ping = ProtocolCodec.DecodeEvent(
            await ReceiveMessageAsync(socket, timeout: TimeSpan.FromSeconds(6)),
            SockudoWireFormat.Json
        );
        Assert.Equal("pusher:ping", ping.Event);

        await SendJsonAsync(socket, new Dictionary<string, object?>
        {
            ["event"] = "pusher:pong",
            ["data"] = new Dictionary<string, object?>(),
        });

        await Assert.ThrowsAsync<TimeoutException>(async () =>
            await ReceiveMessageAsync(socket, timeout: TimeSpan.FromSeconds(1.5)));
    }

    private static bool LiveTestsEnabled() =>
        Environment.GetEnvironmentVariable("SOCKUDO_LIVE_TESTS") == "1";

    private static Uri LiveSockudoUri(int protocolVersion)
    {
        var builder = new UriBuilder
        {
            Scheme = "ws",
            Host = "127.0.0.1",
            Port = 6001,
            Path = "/app/app-key",
        };

        var query = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["protocol"] = protocolVersion,
            ["client"] = "dotnet-live",
            ["version"] = "1.0.0",
        };

        if (protocolVersion == 2)
        {
            query["format"] = "json";
        }

        builder.Query = string.Join(
            "&",
            query.OrderBy(pair => pair.Key, StringComparer.Ordinal)
                .Select(pair => $"{Uri.EscapeDataString(pair.Key)}={Uri.EscapeDataString($"{pair.Value}")}")
        );
        return builder.Uri;
    }

    private static async Task SendJsonAsync(ClientWebSocket socket, Dictionary<string, object?> payload)
    {
        var json = JsonSerializer.Serialize(payload);
        var bytes = Encoding.UTF8.GetBytes(json);
        await socket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
    }

    private static async Task<string> ReceiveMessageAsync(ClientWebSocket socket, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(timeout);

        try
        {
            var result = await ReceiveMessageInternalAsync(socket, cts.Token);
            return result;
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException("Timed out waiting for websocket message");
        }
    }

    private static async Task<string> ReceiveMessageInternalAsync(ClientWebSocket socket, CancellationToken cancellationToken)
    {
        var buffer = new byte[8192];
        using var stream = new MemoryStream();

        while (true)
        {
            var result = await socket.ReceiveAsync(buffer, cancellationToken);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                throw new InvalidOperationException("Socket closed while waiting for message");
            }

            if (result.Count > 0)
            {
                stream.Write(buffer, 0, result.Count);
            }

            if (result.EndOfMessage)
            {
                return Encoding.UTF8.GetString(stream.ToArray());
            }
        }
    }

    private async Task PublishToLocalSockudoAsync(string channel, string eventName, Dictionary<string, object?> payload)
    {
        const string path = "/apps/app-id/events";
        var body = JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["name"] = eventName,
            ["channels"] = new[] { channel },
            ["data"] = JsonSerializer.Serialize(payload),
        });

        var bodyMd5 = Convert.ToHexString(MD5.HashData(Encoding.UTF8.GetBytes(body))).ToLowerInvariant();
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
        var parameters = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["auth_key"] = "app-key",
            ["auth_timestamp"] = timestamp,
            ["auth_version"] = "1.0",
            ["body_md5"] = bodyMd5,
        };

        var canonicalQuery = string.Join(
            "&",
            parameters.OrderBy(pair => pair.Key, StringComparer.Ordinal)
                .Select(pair => $"{pair.Key}={pair.Value}")
        );

        var signature = HmacSha256Hex($"POST\n{path}\n{canonicalQuery}", "app-secret");
        var url = $"http://127.0.0.1:6001{path}?{canonicalQuery}&auth_signature={signature}";

        using var request = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json"),
        };

        using var response = await _httpClient.SendAsync(request, CancellationToken.None);
        Assert.Contains(response.StatusCode, new[] { System.Net.HttpStatusCode.OK, System.Net.HttpStatusCode.Accepted });
    }

    private static string HmacSha256Hex(string value, string secret)
    {
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(secret));
        return Convert.ToHexString(hmac.ComputeHash(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
    }

    private static async Task WaitForAsync(Task task, TimeSpan? timeout = null)
    {
        var delay = Task.Delay(timeout ?? TimeSpan.FromSeconds(8), CancellationToken.None);
        var completed = await Task.WhenAny(task, delay);
        if (completed != task)
        {
            throw new TimeoutException("Timed out waiting for task completion");
        }

        await task;
    }

    private static async Task<T> WaitForAsync<T>(Task<T> task, TimeSpan? timeout = null)
    {
        var delay = Task.Delay(timeout ?? TimeSpan.FromSeconds(8), CancellationToken.None);
        var completed = await Task.WhenAny(task, delay);
        if (completed != task)
        {
            throw new TimeoutException("Timed out waiting for task completion");
        }

        return await task;
    }
}
