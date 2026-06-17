using Sodium;
using System.Text.Json;
using System.Text;
using System.Net.Http;
using VCDiff.Decoders;
using VCDiff.Encoders;
using VCDiff.Includes;
using VCDiff.Shared;
using Xunit;

namespace Sockudo.Client.Tests;

public sealed class ProtocolTests
{
    [Fact]
    public void EncodesWebSocketUrlWithV2FormatQuery()
    {
        var client = new SockudoClient(
            "app-key",
            new SockudoOptions(
                Cluster: "local",
                ForceTls: false,
                EnabledTransports: new[] { SockudoTransport.Ws },
                WsHost: "ws.example.com",
                WsPort: 6001,
                WssPort: 6002,
                WireFormat: SockudoWireFormat.MessagePack
            )
        );

        var url = client.SocketUrl(SockudoTransport.Ws);

        Assert.Contains("protocol=2", url);
        Assert.Contains("format=messagepack", url);
    }

    [Fact]
    public void RoundTripsMessagePack()
    {
        var payload = (byte[])ProtocolCodec.EncodeEnvelope(
            new Dictionary<string, object?>
            {
                ["event"] = "sockudo:test",
                ["channel"] = "chat:room-1",
                ["data"] = new Dictionary<string, object?> { ["hello"] = "world", ["count"] = 3 },
                ["stream_id"] = "stream-1",
                ["serial"] = 7,
                ["__delta_seq"] = 7,
                ["__conflation_key"] = "room",
            },
            SockudoWireFormat.MessagePack
        );

        var decoded = ProtocolCodec.DecodeEvent(payload, SockudoWireFormat.MessagePack);

        Assert.Equal("sockudo:test", decoded.Event);
        Assert.Equal("chat:room-1", decoded.Channel);
        var data = Assert.IsType<Dictionary<string, object?>>(decoded.Data);
        Assert.Equal("world", data["hello"]);
        Assert.Equal(3L, data["count"]);
        Assert.Equal("stream-1", decoded.StreamId);
        Assert.Equal(7, decoded.Serial);
        Assert.Equal(7, decoded.Sequence);
        Assert.Equal("room", decoded.ConflationKey);
    }

    [Fact]
    public void RoundTripsProtobuf()
    {
        var payload = (byte[])ProtocolCodec.EncodeEnvelope(
            new Dictionary<string, object?>
            {
                ["event"] = "sockudo:test",
                ["channel"] = "chat:room-1",
                ["data"] = new Dictionary<string, object?> { ["hello"] = "world" },
                ["stream_id"] = "stream-2",
                ["serial"] = 9,
                ["__delta_seq"] = 11,
                ["__conflation_key"] = "btc",
                ["extras"] = new Dictionary<string, object?>
                {
                    ["headers"] = new Dictionary<string, object> { ["region"] = "eu", ["ttl"] = 5, ["replay"] = true },
                    ["echo"] = false,
                },
            },
            SockudoWireFormat.Protobuf
        );

        var decoded = ProtocolCodec.DecodeEvent(payload, SockudoWireFormat.Protobuf);

        Assert.Equal("sockudo:test", decoded.Event);
        Assert.Equal("chat:room-1", decoded.Channel);
        var data = Assert.IsType<Dictionary<string, object?>>(decoded.Data);
        Assert.Equal("world", data["hello"]);
        Assert.Equal("stream-2", decoded.StreamId);
        Assert.Equal(9, decoded.Serial);
        Assert.Equal(11, decoded.Sequence);
        Assert.Equal("btc", decoded.ConflationKey);
        Assert.NotNull(decoded.Extras);
        Assert.Equal("eu", decoded.Extras!.Headers!["region"]);
        Assert.Equal(5.0, decoded.Extras.Headers["ttl"]);
        Assert.Equal(true, decoded.Extras.Headers["replay"]);
        Assert.False(decoded.Extras.Echo ?? true);
    }

    [Fact]
    public void AppliesInsertOnlyFossilDelta()
    {
        Assert.Equal("hello", Encoding.UTF8.GetString(FossilDelta.Apply([], Encoding.UTF8.GetBytes("5\n5:hello3NPMmh;"))));
    }

    [Fact]
    public async Task DecryptsEncryptedChannelPayload()
    {
        var secret = SecretBox.GenerateKey();
        var client = new SockudoClient(
            "app-key",
            new SockudoOptions(
                Cluster: "local",
                ForceTls: false,
                ChannelAuthorization: new ChannelAuthorizationOptions(
                    CustomHandler: _ => Task.FromResult(
                        new ChannelAuthorizationData(
                            "token",
                            SharedSecret: Convert.ToBase64String(secret)
                        )
                    )
                )
            )
        );

        var channel = Assert.IsType<EncryptedChannel>(client.Subscribe("private-encrypted-room"));
        await channel.AuthorizeAsync("123.456");

        var nonce = SecretBox.GenerateNonce();
        var payload = JsonSerializer.Serialize(new Dictionary<string, object?> { ["hello"] = "world" });
        var ciphertext = SecretBox.Create(Encoding.UTF8.GetBytes(payload), nonce, secret);
        var decrypted = channel.Decrypt(new Dictionary<string, object?>
        {
            ["ciphertext"] = Convert.ToBase64String(ciphertext),
            ["nonce"] = Convert.ToBase64String(nonce),
        });

        var decoded = Assert.IsType<Dictionary<string, object?>>(decrypted);
        Assert.Equal("world", decoded["hello"]);
    }

    [Fact]
    public void AppliesXdelta3ViaVcdiffDecoder()
    {
        var original = "{\"data\":{\"price\":100,\"volume\":5}}";
        var updated = "{\"data\":{\"price\":101,\"volume\":7}}";

        byte[] deltaBytes;
        using (var source = new MemoryStream(Encoding.UTF8.GetBytes(original)))
        using (var target = new MemoryStream(Encoding.UTF8.GetBytes(updated)))
        using (var output = new MemoryStream())
        using (var encoder = new VcEncoder(source, target, output))
        {
            Assert.Equal(VCDiffResult.SUCCESS, encoder.Encode(false, ChecksumFormat.Xdelta3));
            deltaBytes = output.ToArray();
        }

        using var sourceStream = new MemoryStream(Encoding.UTF8.GetBytes(original));
        using var deltaStream = new MemoryStream(deltaBytes);
        using var result = new MemoryStream();
        using var decoder = new VcDecoder(sourceStream, deltaStream, result);
        decoder.Decode(out _);

        Assert.Equal(updated, Encoding.UTF8.GetString(result.ToArray()));
    }

    [Fact]
    public void PresenceHistoryParamsPreferNormalizedTimeBounds()
    {
        var payload = new PresenceHistoryParams(
            Direction: "newest_first",
            Limit: 50,
            Start: 1000,
            End: 2000
        ).ToPayload();

        Assert.Equal("newest_first", payload["direction"]);
        Assert.Equal(50, payload["limit"]);
        Assert.Equal(1000L, payload["start_time_ms"]);
        Assert.Equal(2000L, payload["end_time_ms"]);
        Assert.DoesNotContain("start", payload.Keys);
        Assert.DoesNotContain("end", payload.Keys);
    }

    [Fact]
    public async Task PresenceHistoryPageNextUsesNextCursor()
    {
        string? capturedCursor = null;
        var page = new PresenceHistoryPage(
            Array.Empty<PresenceHistoryItem>(),
            "newest_first",
            50,
            true,
            "cursor-2",
            new PresenceHistoryBounds(null, null, null, null),
            new PresenceHistoryContinuity(null, null, null, null, null, 0, 0, false, true, false),
            cursor =>
            {
                capturedCursor = cursor;
                return Task.FromResult(
                    new PresenceHistoryPage(
                        Array.Empty<PresenceHistoryItem>(),
                        "newest_first",
                        50,
                        false,
                        null,
                        new PresenceHistoryBounds(null, null, null, null),
                        new PresenceHistoryContinuity(null, null, null, null, null, 0, 0, false, true, false)
                    )
                );
            }
        );

        Assert.True(page.HasNext());
        await page.NextAsync();
        Assert.Equal("cursor-2", capturedCursor);
    }

    [Fact]
    public void AnnotationRequestPayloadUsesProxyShape()
    {
        var payload = new PublishAnnotationRequest(
            Type: "reactions:distinct.v1",
            Name: "like",
            Count: 2,
            Data: new Dictionary<string, object?> { ["emoji"] = "thumbs-up" },
            ClientId: "client-1",
            Extras: new Dictionary<string, object?> { ["source"] = "dotnet" },
            IdempotencyKey: "anno-1"
        ).ToPayload();

        Assert.Equal("reactions:distinct.v1", payload["type"]);
        Assert.Equal("like", payload["name"]);
        Assert.Equal(2, payload["count"]);
        Assert.Equal("thumbs-up", ((IDictionary<string, object?>)payload["data"]!)["emoji"]);
        Assert.Equal("client-1", payload["clientId"]);
        Assert.Equal("dotnet", ((IDictionary<string, object?>)payload["extras"]!)["source"]);
        Assert.Equal("anno-1", payload["idempotencyKey"]);
    }

    [Fact]
    public async Task AnnotationEventsPageNextUsesNextCursor()
    {
        string? capturedCursor = null;
        var page = new AnnotationEventsPage(
            Array.Empty<Dictionary<string, object?>>(),
            "oldest_first",
            10,
            true,
            "anno-cursor-2",
            cursor =>
            {
                capturedCursor = cursor;
                return Task.FromResult(
                    new AnnotationEventsPage(
                        Array.Empty<Dictionary<string, object?>>(),
                        "oldest_first",
                        10,
                        false,
                        null
                    )
                );
            }
        );

        Assert.True(page.HasNext());
        await page.NextAsync();
        Assert.Equal("anno-cursor-2", capturedCursor);
    }

    [Fact]
    public async Task PushProxyHelpersUseBackendEndpointAndAsyncPublishDefaults()
    {
        var requests = new List<HttpRequestMessage>();
        using var httpClient = new HttpClient(new RecordingHandler(async request =>
        {
            requests.Add(await CloneRequestAsync(request));
            if (request.RequestUri!.AbsolutePath.EndsWith("/publish", StringComparison.Ordinal))
            {
                return new HttpResponseMessage(System.Net.HttpStatusCode.Accepted)
                {
                    Content = new StringContent(
                        JsonSerializer.Serialize(new Dictionary<string, object?> { ["publish_id"] = "pub_123" }),
                        Encoding.UTF8,
                        "application/json"),
                };
            }

            return new HttpResponseMessage(System.Net.HttpStatusCode.OK)
            {
                Content = new StringContent(
                    JsonSerializer.Serialize(new Dictionary<string, object?> { ["items"] = Array.Empty<object>(), ["has_more"] = false }),
                    Encoding.UTF8,
                    "application/json"),
            };
        }));

        var client = new SockudoPushRegistration(
            new PushRegistrationOptions(
                Endpoint: "https://api.example.test/push/",
                Headers: new Dictionary<string, string> { ["Authorization"] = "Bearer session" }),
            httpClient);

        var publish = await client.PublishAsync(new Dictionary<string, object?>
        {
            ["recipients"] = new[]
            {
                new Dictionary<string, object?> { ["type"] = "channel", ["channel"] = "orders" },
            },
            ["payload"] = new Dictionary<string, object?> { ["title"] = "Order", ["body"] = "Updated" },
        });
        await client.UpdateDeviceRegistrationAsync(
            new Dictionary<string, object?>
            {
                ["id"] = "device-1",
                ["formFactor"] = "phone",
                ["platform"] = "android",
                ["timezone"] = "UTC",
                ["locale"] = "en",
                ["push"] = new Dictionary<string, object?>
                {
                    ["recipient"] = new Dictionary<string, object?>
                    {
                        ["transportType"] = "gcm",
                        ["registrationToken"] = "rotated",
                    },
                },
            },
            "identity");
        await client.ListChannelSubscriptionsAsync(new PushSubscriptionParams(DeviceId: "device-1", Limit: 10, Cursor: "c1"));

        Assert.Equal("pub_123", publish["publish_id"]);

        Assert.Equal("https://api.example.test/push/publish", requests[0].RequestUri!.ToString());
        Assert.Equal(HttpMethod.Post, requests[0].Method);
        using var publishBodyJson = JsonDocument.Parse(await requests[0].Content!.ReadAsStringAsync());
        Assert.False(publishBodyJson.RootElement.GetProperty("sync").GetBoolean());
        Assert.Equal("Bearer session", requests[0].Headers.GetValues("Authorization").Single());

        Assert.Equal(
            "identity",
            requests[1].Headers.GetValues("X-Sockudo-Device-Identity-Token").Single());

        Assert.Equal(
            "https://api.example.test/push/channelSubscriptions?deviceId=device-1&limit=10&cursor=c1",
            requests[2].RequestUri!.ToString());
    }

    private static async Task<HttpRequestMessage> CloneRequestAsync(HttpRequestMessage request)
    {
        var clone = new HttpRequestMessage(request.Method, request.RequestUri);
        foreach (var header in request.Headers)
        {
            clone.Headers.TryAddWithoutValidation(header.Key, header.Value);
        }

        if (request.Content is not null)
        {
            var content = await request.Content.ReadAsStringAsync();
            clone.Content = new StringContent(
                content,
                Encoding.UTF8,
                request.Content.Headers.ContentType?.MediaType ?? "application/json");
        }

        return clone;
    }

    private sealed class RecordingHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, Task<HttpResponseMessage>> _handler;

        public RecordingHandler(Func<HttpRequestMessage, Task<HttpResponseMessage>> handler)
        {
            _handler = handler;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            _handler(request);
    }
}
