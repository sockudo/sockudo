using System.Net.WebSockets;
using System.Reflection;
using System.Text;
using Xunit;

namespace Sockudo.Client.Tests;

public sealed class ReconnectionTests
{
    [Fact]
    public void ReconnectionOptionsAndStateHaveParityDefaults()
    {
        var options = new SockudoOptions(Cluster: "local");
        var unlimited = new SockudoOptions(Cluster: "local", MaxReconnectAttempts: null);

        Assert.Equal("reconnecting", ConnectionState.Reconnecting.ToString().ToLowerInvariant());
        Assert.Equal(6, options.MaxReconnectAttempts);
        Assert.Equal(120.0, options.MaxReconnectGapInSeconds);
        Assert.Equal(0.0, options.ReconnectJitter);
        Assert.Null(unlimited.MaxReconnectAttempts);
    }

    [Fact]
    public void CloseActionsUseQuadraticCappedDelay()
    {
        var client = TestClient(maxReconnectGapInSeconds: 5.0);
        SetReconnectAttempts(client, 3);

        Assert.Equal("Backoff", InvokeCloseAction(4100)?.ToString());
        Assert.Equal("Retry", InvokeCloseAction(4200)?.ToString());
        Assert.Equal("TlsOnly", InvokeCloseAction(4000)?.ToString());
        Assert.Equal("Refused", InvokeCloseAction(4300)?.ToString());
        Assert.Equal(TimeSpan.FromSeconds(5), InvokeReconnectDelay(client, null));
        Assert.Equal(TimeSpan.Zero, InvokeReconnectDelay(client, InvokeCloseAction(4200)));
        Assert.Equal(TimeSpan.Zero, InvokeReconnectDelay(client, InvokeCloseAction(4000)));
    }

    [Fact]
    public async Task RetryEmitsReconnectingAndStopsAtConfiguredLimit()
    {
        await using var client = TestClient(maxReconnectAttempts: 1);
        var reconnecting = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.Bind("state_change", (data, _) =>
        {
            if (((StateChange)data!).Current == "reconnecting")
            {
                reconnecting.TrySetResult(true);
            }
        });

        await InvokeScheduleRetryAsync(client, TimeSpan.Zero);
        await reconnecting.Task.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal(ConnectionState.Reconnecting, client.ConnectionState);

        await InvokeScheduleRetryAsync(client, TimeSpan.Zero);

        Assert.Equal(ConnectionState.Disconnected, client.ConnectionState);
    }

    [Fact]
    public async Task SuccessfulHandshakeAndDisconnectResetReconnectAttempts()
    {
        await using var client = TestClient();
        SetReconnectAttempts(client, 4);

        await InvokeRawMessageAsync(
            client,
            """{"event":"sockudo:connection_established","data":{"socket_id":"1.1","activity_timeout":120}}"""
        );

        Assert.Equal(0, GetReconnectAttempts(client));

        SetReconnectAttempts(client, 5);
        await client.DisconnectAsync();

        Assert.Equal(0, GetReconnectAttempts(client));
    }

    [Fact]
    public void JitterRandomizesDelayWithinConfiguredFraction()
    {
        var client = TestClient(maxReconnectAttempts: null, reconnectJitter: 0.5);
        SetReconnectAttempts(client, 3);

        // Half of the 9s delay is randomized away, so it lands in [4.5, 9].
        var seen = new HashSet<double>();
        for (var i = 0; i < 200; i++)
        {
            var delay = InvokeReconnectDelay(client, null).TotalSeconds;
            Assert.InRange(delay, 4.5, 9.0);
            seen.Add(delay);
        }

        Assert.True(seen.Count > 1, "expected jittered delays to vary");
    }

    [Fact]
    public void FullJitterStaysWithinCapAndSkipsImmediateRetries()
    {
        var client = TestClient(maxReconnectAttempts: null, maxReconnectGapInSeconds: 5.0, reconnectJitter: 1.0);
        SetReconnectAttempts(client, 20);

        for (var i = 0; i < 200; i++)
        {
            Assert.InRange(InvokeReconnectDelay(client, null).TotalSeconds, 0.0, 5.0);
        }

        Assert.Equal(TimeSpan.Zero, InvokeReconnectDelay(client, InvokeCloseAction(4200)));
        Assert.Equal(TimeSpan.Zero, InvokeReconnectDelay(client, InvokeCloseAction(4000)));
    }

    [Fact]
    public void DelaysStayExactWhenJitterIsNotConfigured()
    {
        var client = TestClient(maxReconnectAttempts: null);
        SetReconnectAttempts(client, 3);

        Assert.Equal(TimeSpan.FromSeconds(9), InvokeReconnectDelay(client, null));
    }

    [Fact]
    public void JitterIsClamped()
    {
        Assert.Equal(1.0, new SockudoOptions(Cluster: "local", ReconnectJitter: 5.0).EffectiveReconnectJitter);
        Assert.Equal(0.0, new SockudoOptions(Cluster: "local", ReconnectJitter: -1.0).EffectiveReconnectJitter);
        Assert.Equal(0.0, new SockudoOptions(Cluster: "local", ReconnectJitter: double.NaN).EffectiveReconnectJitter);
        Assert.Equal(0.25, new SockudoOptions(Cluster: "local", ReconnectJitter: 0.25).EffectiveReconnectJitter);
    }

    private static SockudoClient TestClient(
        int? maxReconnectAttempts = 6,
        double maxReconnectGapInSeconds = 120.0,
        double reconnectJitter = 0.0
    ) => new(
        "app-key",
        new SockudoOptions(
            Cluster: "local",
            ForceTls: false,
            EnabledTransports: new[] { SockudoTransport.Ws },
            WsHost: "127.0.0.1",
            WsPort: 1,
            MaxReconnectAttempts: maxReconnectAttempts,
            MaxReconnectGapInSeconds: maxReconnectGapInSeconds,
            ReconnectJitter: reconnectJitter
        )
    );

    private static object? InvokeCloseAction(int code) =>
        Method("CloseActionFor").Invoke(null, new object?[] { code });

    private static TimeSpan InvokeReconnectDelay(SockudoClient client, object? action) =>
        (TimeSpan)Method("ReconnectDelay").Invoke(client, new[] { action })!;

    private static async Task InvokeScheduleRetryAsync(SockudoClient client, TimeSpan delay) =>
        await (Task)Method("ScheduleRetryAsync").Invoke(client, new object[] { delay })!;

    private static async Task InvokeRawMessageAsync(SockudoClient client, string raw) =>
        await (Task)Method("HandleRawMessageAsync").Invoke(
            client,
            new object[] { Encoding.UTF8.GetBytes(raw), WebSocketMessageType.Text }
        )!;

    private static int GetReconnectAttempts(SockudoClient client) =>
        (int)Field("_reconnectAttempts").GetValue(client)!;

    private static void SetReconnectAttempts(SockudoClient client, int attempts) =>
        Field("_reconnectAttempts").SetValue(client, attempts);

    private static MethodInfo Method(string name) =>
        typeof(SockudoClient).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        ?? throw new MissingMethodException(typeof(SockudoClient).FullName, name);

    private static FieldInfo Field(string name) =>
        typeof(SockudoClient).GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)
        ?? throw new MissingFieldException(typeof(SockudoClient).FullName, name);
}
