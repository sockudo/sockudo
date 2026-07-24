using Sodium;
using System.Collections;
using System.Globalization;
using System.Net.WebSockets;
using System.Reflection;
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
                WireFormat: SockudoWireFormat.MessagePack,
                AppendMode: SockudoAppendMode.Full,
                AppendRollupWindow: 40,
                TokenAuthentication: new TokenAuthenticationOptions(Token: "static-token")
            )
        );

        var url = client.SocketUrl(SockudoTransport.Ws);

        Assert.Contains("protocol=2", url);
        Assert.Contains("format=messagepack", url);
        Assert.Contains("append_mode=full", url);
        Assert.Contains("append_rollup_window=40", url);
        Assert.Contains("token=static-token", url);
    }

    [Fact]
    public void V1UrlOmitsV2OnlyTokenAndAppendRollupParams()
    {
        var client = new SockudoClient(
            "app-key",
            new SockudoOptions(
                Cluster: "local",
                ProtocolVersion: 1,
                ForceTls: false,
                EnabledTransports: new[] { SockudoTransport.Ws },
                WsHost: "ws.example.com",
                WsPort: 6001,
                AppendRollupWindow: 40,
                TokenAuthentication: new TokenAuthenticationOptions(Token: "static-token")
            )
        );

        var url = client.SocketUrl(SockudoTransport.Ws);

        Assert.Contains("protocol=7", url);
        Assert.DoesNotContain("append_rollup_window", url);
        Assert.DoesNotContain("token=", url);
    }

    [Fact]
    public void AppendRollupWindowRejectsUnknownValuesLocally()
    {
        var client = new SockudoClient(
            "app-key",
            new SockudoOptions(
                Cluster: "local",
                ForceTls: false,
                EnabledTransports: new[] { SockudoTransport.Ws },
                WsHost: "ws.example.com",
                WsPort: 6001,
                AppendRollupWindow: 41
            )
        );

        Assert.Throws<ArgumentOutOfRangeException>(() => client.SocketUrl(SockudoTransport.Ws));
    }

    [Fact]
    public async Task RefreshAuthUsesTokenProviderAndCachesUrlToken()
    {
        var calls = 0;
        await using var client = new SockudoClient(
            "app-key",
            new SockudoOptions(
                Cluster: "local",
                ForceTls: false,
                EnabledTransports: new[] { SockudoTransport.Ws },
                WsHost: "ws.example.com",
                WsPort: 6001,
                TokenAuthentication: new TokenAuthenticationOptions(
                    TokenProvider: _ =>
                    {
                        calls += 1;
                        return Task.FromResult("fresh token");
                    })
            )
        );

        var sent = await client.RefreshAuthAsync();
        var url = client.SocketUrl(SockudoTransport.Ws);

        Assert.False(sent);
        Assert.Equal(1, calls);
        Assert.Contains("token=fresh%20token", url);
    }

    [Fact]
    public async Task JwtTokenProviderSchedulesProactiveRefreshAtEightyPercentLifetime()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var firstToken = TestJwt(iat: now - 10, exp: now + 1);
        var secondRefresh = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        await using var client = new SockudoClient(
            "app-key",
            new SockudoOptions(
                Cluster: "local",
                ForceTls: false,
                EnabledTransports: new[] { SockudoTransport.Ws },
                WsHost: "ws.example.com",
                WsPort: 6001,
                TokenAuthentication: new TokenAuthenticationOptions(
                    TokenProvider: _ =>
                    {
                        var call = Interlocked.Increment(ref calls);
                        if (call == 1)
                        {
                            return Task.FromResult(firstToken);
                        }

                        secondRefresh.TrySetResult();
                        return Task.FromResult("opaque-second-token");
                    })
            )
        );

        await client.RefreshAuthAsync();
        await secondRefresh.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await Task.Delay(TimeSpan.FromMilliseconds(200));

        Assert.Equal(2, Volatile.Read(ref calls));
        Assert.Contains("token=opaque-second-token", client.SocketUrl(SockudoTransport.Ws));
    }

    [Fact]
    public async Task JwtTokenProviderWithoutIatSchedulesFromNow()
    {
        var firstToken = TestJwt(iat: null, exp: DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 1);
        var secondRefresh = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        await using var client = new SockudoClient(
            "app-key",
            new SockudoOptions(
                Cluster: "local",
                ForceTls: false,
                EnabledTransports: new[] { SockudoTransport.Ws },
                WsHost: "ws.example.com",
                WsPort: 6001,
                TokenAuthentication: new TokenAuthenticationOptions(
                    TokenProvider: _ =>
                    {
                        var call = Interlocked.Increment(ref calls);
                        if (call == 1)
                        {
                            return Task.FromResult(firstToken);
                        }

                        secondRefresh.TrySetResult();
                        return Task.FromResult("opaque-second-token");
                    })
            )
        );

        await client.RefreshAuthAsync();
        await secondRefresh.Task.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal(2, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task OpaqueTokenProviderDoesNotScheduleProactiveRefresh()
    {
        var calls = 0;
        await using var client = new SockudoClient(
            "app-key",
            new SockudoOptions(
                Cluster: "local",
                ForceTls: false,
                EnabledTransports: new[] { SockudoTransport.Ws },
                WsHost: "ws.example.com",
                WsPort: 6001,
                TokenAuthentication: new TokenAuthenticationOptions(
                    TokenProvider: _ =>
                    {
                        Interlocked.Increment(ref calls);
                        return Task.FromResult("opaque-token");
                    })
            )
        );

        await client.RefreshAuthAsync();
        await Task.Delay(TimeSpan.FromMilliseconds(250));

        Assert.Equal(1, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task DisconnectCancelsScheduledTokenRefresh()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var firstToken = TestJwt(iat: now - 2, exp: now + 4);
        var calls = 0;
        await using var client = new SockudoClient(
            "app-key",
            new SockudoOptions(
                Cluster: "local",
                ForceTls: false,
                EnabledTransports: new[] { SockudoTransport.Ws },
                WsHost: "ws.example.com",
                WsPort: 6001,
                TokenAuthentication: new TokenAuthenticationOptions(
                    TokenProvider: _ =>
                    {
                        Interlocked.Increment(ref calls);
                        return Task.FromResult(firstToken);
                    })
            )
        );

        await client.RefreshAuthAsync();
        await client.DisconnectAsync();
        await Task.Delay(TimeSpan.FromMilliseconds(3500));

        Assert.Equal(1, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task TokenAuthEventsEmitTypedResults()
    {
        await using var client = TestClient();
        var authSuccess = false;
        var errors = new List<object?>();

        client.Bind("sockudo:auth_success", (_, _) => authSuccess = true);
        client.Bind("error", (data, _) => errors.Add(data));

        await DispatchRawAsync(client, JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["event"] = "sockudo:auth_success",
            ["data"] = new Dictionary<string, object?>
            {
                ["client_id"] = "client-1",
                ["jti"] = "token-1",
                ["exp"] = 123,
            },
        }));
        await DispatchRawAsync(client, JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["event"] = "sockudo:token_expired",
            ["data"] = new Dictionary<string, object?>
            {
                ["code"] = 40142,
                ["reason"] = "expired",
            },
        }));
        await DispatchRawAsync(client, JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["event"] = "sockudo:token_expired",
            ["data"] = new Dictionary<string, object?>
            {
                ["code"] = 40160,
                ["reason"] = "revoked",
            },
        }));

        Assert.True(authSuccess);
        Assert.IsType<TokenExpiredException>(errors[0]);
        Assert.IsType<TokenRevokedException>(errors[1]);
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
        Assert.Equal(7L, decoded.Serial);
        Assert.Equal(7, decoded.Sequence);
        Assert.Equal("room", decoded.ConflationKey);
    }

    [Theory]
    [InlineData(SockudoWireFormat.MessagePack)]
    [InlineData(SockudoWireFormat.Protobuf)]
    public void RoundTripsNativeBinaryData(SockudoWireFormat wireFormat)
    {
        var data = new byte[] { 0, 1, 2, 255 };
        var payload = ProtocolCodec.EncodeEnvelope(
            new Dictionary<string, object?>
            {
                ["event"] = "sockudo:binary",
                ["channel"] = "binary:room-1",
                ["data"] = data,
            },
            wireFormat
        );

        var decoded = ProtocolCodec.DecodeEvent(payload, wireFormat);

        Assert.Equal(data, Assert.IsType<byte[]>(decoded.Data));
    }

    [Fact]
    public void SubscriptionExpressionsSupportTheSourceShorthand()
    {
        var client = new SockudoClient(
            "app-key",
            new SockudoOptions(Cluster: "local", ForceTls: false)
        );

        var channel = client.Subscribe(
            "orders",
            new SubscriptionOptions(
                Events: new[] { "order.updated" },
                Expression: "data.total >= `100`"
            )
        );

        Assert.Equal(new[] { "order.updated" }, channel.EventsFilter);
        Assert.Equal("data.total >= `100`", channel.ExpressionFilter?.Source);
        Assert.Equal("jmespath", channel.ExpressionFilter?.Language);
    }

    [Fact]
    public async Task ResumeSuccessAppliesAuthoritativeChannelPosition()
    {
        var client = new SockudoClient(
            "app-key",
            new SockudoOptions(Cluster: "local", ForceTls: false, ConnectionRecovery: true)
        );
        var raw = JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["event"] = "sockudo:resume_success",
            ["data"] = JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["recovered"] = new[]
                {
                    new Dictionary<string, object?>
                    {
                        ["channel"] = "orders",
                        ["source"] = "durable",
                        ["replayed"] = 0,
                        ["position"] = new Dictionary<string, object?>
                        {
                            ["stream_id"] = "stream-2",
                            ["serial"] = 42,
                            ["last_message_id"] = "message-42",
                        },
                    },
                },
                ["failed"] = Array.Empty<object>(),
            }),
        });
        var handler = typeof(SockudoClient).GetMethod(
            "HandleRawMessageAsync",
            BindingFlags.Instance | BindingFlags.NonPublic
        )!;

        await (Task)handler.Invoke(
            client,
            new object[] { Encoding.UTF8.GetBytes(raw), WebSocketMessageType.Text }
        )!;

        var positionsField = typeof(SockudoClient).GetField(
            "_channelPositions",
            BindingFlags.Instance | BindingFlags.NonPublic
        )!;
        var positions = Assert.IsAssignableFrom<IDictionary>(positionsField.GetValue(client));
        var position = positions["orders"];
        Assert.NotNull(position);
        var recoveredPosition = position!;
        Assert.Equal(42L, recoveredPosition.GetType().GetProperty("Serial")!.GetValue(recoveredPosition));
        Assert.Equal("stream-2", recoveredPosition.GetType().GetProperty("StreamId")!.GetValue(recoveredPosition));
        Assert.Equal("message-42", recoveredPosition.GetType().GetProperty("LastMessageId")!.GetValue(recoveredPosition));
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
        Assert.Equal(9L, decoded.Serial);
        Assert.Equal(11, decoded.Sequence);
        Assert.Equal("btc", decoded.ConflationKey);
        Assert.NotNull(decoded.Extras);
        Assert.Equal("eu", decoded.Extras!.Headers!["region"]);
        Assert.Equal(5.0, decoded.Extras.Headers["ttl"]);
        Assert.Equal(true, decoded.Extras.Headers["replay"]);
        Assert.False(decoded.Extras.Echo ?? true);
    }

    [Theory]
    [InlineData(2147483648L)]
    [InlineData(9007199254740993L)]
    public void PreservesWideJsonSerials(long serial)
    {
        var raw = "{\"event\":\"sockudo:test\",\"channel\":\"private-ai-forward\",\"data\":{},\"stream_id\":\"wide-stream\",\"serial\":"
            + serial.ToString(CultureInfo.InvariantCulture)
            + "}";

        var decoded = ProtocolCodec.DecodeEvent(raw, SockudoWireFormat.Json);

        Assert.Equal(serial, decoded.Serial);
        Assert.Equal(serial.ToString(CultureInfo.InvariantCulture), decoded.SerialText);
    }

    [Fact]
    public void ParsesStringSerialsWithoutDoublePrecisionLoss()
    {
        var decoded = ProtocolCodec.DecodeEvent(
            """{"event":"sockudo:test","channel":"private-ai-forward","data":{},"serial":"9007199254740993"}""",
            SockudoWireFormat.Json);

        Assert.Equal(9007199254740993L, decoded.Serial);
        Assert.Equal("9007199254740993", decoded.SerialText);
    }

    [Fact]
    public void RoundTripsWideSerialsThroughMessagePackAndProtobuf()
    {
        const long wideSerial = 9007199254740993L;
        var envelope = new Dictionary<string, object?>
        {
            ["event"] = "sockudo:test",
            ["channel"] = "private-ai-forward",
            ["data"] = "content",
            ["serial"] = wideSerial,
        };

        var messagePack = ProtocolCodec.DecodeEvent(
            ProtocolCodec.EncodeEnvelope(envelope, SockudoWireFormat.MessagePack),
            SockudoWireFormat.MessagePack);
        var protobuf = ProtocolCodec.DecodeEvent(
            ProtocolCodec.EncodeEnvelope(envelope, SockudoWireFormat.Protobuf),
            SockudoWireFormat.Protobuf);

        Assert.Equal(wideSerial, messagePack.Serial);
        Assert.Equal(wideSerial, protobuf.Serial);
    }

    [Fact]
    public void PreservesUnknownJsonExtras()
    {
        var decoded = ProtocolCodec.DecodeEvent(FixtureText("unknown-ai-extras.json"), SockudoWireFormat.Json);

        var raw = decoded.Extras?.Raw;
        Assert.NotNull(raw);
        var ai = Assert.IsType<Dictionary<string, object?>>(raw!["ai"]);
        var transport = Assert.IsType<Dictionary<string, object?>>(ai["transport"]);

        Assert.Equal("turn-1", transport["turn-id"]);
        Assert.Equal("streaming", transport["status"]);
        Assert.True(Assert.IsType<bool>(raw["futureExtrasField"]));
    }

    [Fact]
    public void PreservesUnknownMessagePackExtras()
    {
        var payload = ProtocolCodec.EncodeEnvelope(
            new Dictionary<string, object?>
            {
                ["event"] = "ai-output",
                ["channel"] = "private-ai-forward",
                ["data"] = "content",
                ["extras"] = new Dictionary<string, object?>
                {
                    ["ai"] = new Dictionary<string, object?>
                    {
                        ["transport"] = new Dictionary<string, object?>
                        {
                            ["turn-id"] = "turn-1",
                            ["status"] = "future-status",
                        },
                    },
                    ["futureExtrasField"] = true,
                },
            },
            SockudoWireFormat.MessagePack);

        var decoded = ProtocolCodec.DecodeEvent(payload, SockudoWireFormat.MessagePack);

        var raw = decoded.Extras?.Raw;
        Assert.NotNull(raw);
        var ai = Assert.IsType<Dictionary<string, object?>>(raw!["ai"]);
        var transport = Assert.IsType<Dictionary<string, object?>>(ai["transport"]);
        Assert.Equal("future-status", transport["status"]);
        Assert.True(Assert.IsType<bool>(raw["futureExtrasField"]));
    }

    [Theory]
    [InlineData("future-v2-frame.json", "sockudo:future_event")]
    [InlineData("future-versioned-action.json", "sockudo:message.future")]
    [InlineData("unknown-ai-extras.json", "ai-output")]
    public async Task ForwardCompatRealtimeFixturesDispatchWithoutErrors(string fixtureName, string expectedEvent)
    {
        await using var client = TestClient();
        client.Subscribe("private-ai-forward");
        var errors = new List<object?>();
        var seen = new List<string>();

        client.Bind("error", (data, _) => errors.Add(data));
        client.BindGlobal((eventName, _) => seen.Add(eventName));

        await DispatchRawAsync(client, FixtureText(fixtureName));

        Assert.Empty(errors);
        Assert.Contains(expectedEvent, seen);
    }

    [Fact]
    public async Task ChannelDispatchDeliversUnknownAiEvent()
    {
        await using var client = TestClient();
        var channel = client.Subscribe("private-ai-forward");
        object? received = null;

        channel.Bind("ai-output", (data, _) => received = data);

        await DispatchRawAsync(client, FixtureText("unknown-ai-extras.json"));

        Assert.Equal("content", received);
    }

    [Fact]
    public async Task PresenceIgnoresUnknownAndMalformedMemberFramesWithoutCorruptingMembers()
    {
        await using var client = TestClient();
        var channel = Assert.IsType<PresenceChannel>(client.Subscribe("presence-ai-forward"));
        var errors = new List<object?>();
        client.Bind("error", (data, _) => errors.Add(data));

        await DispatchRawAsync(client, JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["event"] = "sockudo_internal:subscription_succeeded",
            ["channel"] = "presence-ai-forward",
            ["data"] = new Dictionary<string, object?>
            {
                ["presence"] = new Dictionary<string, object?>
                {
                    ["hash"] = new Dictionary<string, object?>
                    {
                        ["user-1"] = new Dictionary<string, object?> { ["name"] = "Ada" },
                    },
                    ["count"] = 1,
                },
            },
        }));

        Assert.Empty(errors);
        Assert.Equal(1, channel.Members.Count);
        Assert.NotNull(channel.Members.Member("user-1"));

        await DispatchRawAsync(client, JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["event"] = "sockudo_internal:member_updated",
            ["channel"] = "presence-ai-forward",
            ["data"] = new Dictionary<string, object?> { ["user_id"] = "user-1", ["future"] = true },
        }));
        await DispatchRawAsync(client, JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["event"] = "sockudo_internal:member_added",
            ["channel"] = "presence-ai-forward",
            ["data"] = new Dictionary<string, object?> { ["member"] = new Dictionary<string, object?> { ["id"] = "user-2" } },
        }));
        await DispatchRawAsync(client, JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["event"] = "sockudo_internal:subscription_succeeded",
            ["channel"] = "presence-ai-forward",
            ["data"] = new Dictionary<string, object?> { ["presence"] = new Dictionary<string, object?> { ["count"] = 0 } },
        }));

        Assert.Empty(errors);
        Assert.Equal(1, channel.Members.Count);
        Assert.NotNull(channel.Members.Member("user-1"));
        Assert.Null(channel.Members.Member("user-2"));
    }

    [Fact]
    public async Task PresenceUpdateAppliesMemberDataAndEmitsPlatformEvent()
    {
        await using var client = TestClient();
        var channel = Assert.IsType<PresenceChannel>(client.Subscribe("presence-ai-forward"));
        object? updateEvent = null;

        channel.Bind("sockudo:presence_update", (data, _) => updateEvent = data);

        await DispatchRawAsync(client, JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["event"] = "sockudo_internal:subscription_succeeded",
            ["channel"] = "presence-ai-forward",
            ["data"] = new Dictionary<string, object?>
            {
                ["attach_serial"] = 42,
                ["presence"] = new Dictionary<string, object?>
                {
                    ["hash"] = new Dictionary<string, object?>
                    {
                        ["user-1"] = new Dictionary<string, object?> { ["status"] = "idle" },
                    },
                    ["count"] = 1,
                },
            },
        }));
        await DispatchRawAsync(client, JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["event"] = "sockudo_internal:presence_update",
            ["channel"] = "presence-ai-forward",
            ["data"] = new Dictionary<string, object?>
            {
                ["user_id"] = "user-1",
                ["user_info"] = new Dictionary<string, object?> { ["status"] = "thinking" },
            },
        }));

        Assert.Equal(42, channel.AttachSerial);
        var emitted = Assert.IsType<PresenceMember>(updateEvent);
        Assert.Equal("user-1", emitted.Id);
        var emittedInfo = Assert.IsType<Dictionary<string, object?>>(emitted.Info);
        Assert.Equal("thinking", emittedInfo["status"]);

        var stored = Assert.IsType<Dictionary<string, object?>>(channel.Members.Member("user-1")!.Info);
        Assert.Equal("thinking", stored["status"]);
        Assert.Equal(1, channel.Members.Count);
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
    public async Task ChannelHistoryProxyIncludesUntilAttach()
    {
        var requests = new List<HttpRequestMessage>();
        using var httpClient = new HttpClient(new RecordingHandler(async request =>
        {
            requests.Add(await CloneRequestAsync(request));
            return new HttpResponseMessage(System.Net.HttpStatusCode.OK)
            {
                Content = new StringContent(
                    JsonSerializer.Serialize(new Dictionary<string, object?>
                    {
                        ["items"] = Array.Empty<object>(),
                        ["direction"] = "newest_first",
                        ["limit"] = 50,
                        ["has_more"] = false,
                        ["bounds"] = new Dictionary<string, object?>(),
                        ["continuity"] = new Dictionary<string, object?>(),
                    }),
                    Encoding.UTF8,
                    "application/json"),
            };
        }));
        await using var client = new SockudoClient(
            "app-key",
            new SockudoOptions(
                Cluster: "local",
                ForceTls: false,
                VersionedMessages: new VersionedMessagesOptions(
                    Endpoint: "https://api.example.test/versioned",
                    Headers: new Dictionary<string, string> { ["Authorization"] = "Bearer session" })
            ),
            httpClient);

        var channel = client.Subscribe("private-history");
        await channel.ChannelHistoryAsync(new ChannelHistoryParams(
            Direction: "newest_first",
            Limit: 50,
            EndSerial: 99,
            UntilAttach: true));

        Assert.Single(requests);
        Assert.Equal("Bearer session", requests[0].Headers.GetValues("Authorization").Single());
        using var body = JsonDocument.Parse(await requests[0].Content!.ReadAsStringAsync());
        Assert.Equal("channel_history", body.RootElement.GetProperty("action").GetString());
        Assert.Equal("private-history", body.RootElement.GetProperty("channel").GetString());
        var parameters = body.RootElement.GetProperty("params");
        Assert.True(parameters.GetProperty("until_attach").GetBoolean());
        Assert.Equal(99, parameters.GetProperty("end_serial").GetInt64());
    }

    [Fact]
    public async Task VersionedMutationProxyHelpersReturnTypedAcks()
    {
        var requests = new List<HttpRequestMessage>();
        using var httpClient = new HttpClient(new RecordingHandler(async request =>
        {
            var clone = await CloneRequestAsync(request);
            requests.Add(clone);
            using var requestBody = JsonDocument.Parse(await clone.Content!.ReadAsStringAsync());
            var action = requestBody.RootElement.GetProperty("action").GetString();
            object responsePayload = action == "publish_create"
                ? new Dictionary<string, object?>
                {
                    ["channels"] = new Dictionary<string, object?>
                    {
                        ["chat"] = new Dictionary<string, object?>
                        {
                            ["message_serial"] = "msg:1",
                            ["version_serial"] = "v1",
                            ["history_serial"] = 7,
                            ["delivery_serial"] = "18446744073709551615",
                        },
                    },
                }
                : new Dictionary<string, object?>
                {
                    ["channel"] = "chat",
                    ["message_serial"] = "msg:1",
                    ["action"] = action switch
                    {
                        "message_append" => "message.append",
                        "message_update" => "message.update",
                        "message_delete" => "message.delete",
                        _ => "message.create",
                    },
                    ["accepted"] = true,
                    ["version_serial"] = $"v{requests.Count + 1}",
                    ["history_serial"] = 8 + requests.Count,
                    ["delivery_serial"] = 9 + requests.Count,
                    ["status"] = "applied",
                };

            return new HttpResponseMessage(System.Net.HttpStatusCode.OK)
            {
                Content = new StringContent(
                    JsonSerializer.Serialize(responsePayload),
                    Encoding.UTF8,
                    "application/json"),
            };
        }));
        await using var client = new SockudoClient(
            "app-key",
            new SockudoOptions(
                Cluster: "local",
                ForceTls: false,
                VersionedMessages: new VersionedMessagesOptions(
                    Endpoint: "https://api.example.test/versioned"))
            ,
            httpClient);
        var channel = client.Subscribe("chat");

        var create = await channel.CreateVersionedMessageAsync(
            "ai-output",
            new Dictionary<string, object?> { ["text"] = "hello" },
            new VersionedMessageCreateOptions(MessageId: "mid-1", ClientId: "client-1"));
        var append = await channel.AppendVersionedMessageAsync(
            "msg:1",
            " world",
            new VersionedMessageMutationOptions(OpId: "op-append"));
        var update = await channel.UpdateVersionedMessageAsync(
            "msg:1",
            new VersionedMessageMutationOptions(
                Name: "ai-output.updated",
                Data: new Dictionary<string, object?> { ["text"] = "updated" },
                ClearFields: new[] { "extras" },
                OpId: "op-update"));
        var delete = await channel.DeleteVersionedMessageAsync(
            "msg:1",
            new VersionedMessageMutationOptions(OpId: "op-delete"));

        Assert.Equal(MutableMessageAction.Create, create.Action);
        Assert.Equal("msg:1", create.MessageSerial);
        Assert.Equal("18446744073709551615", create.DeliverySerialText);
        Assert.Null(create.DeliverySerial);
        Assert.Equal(MutableMessageAction.Append, append.Action);
        Assert.Equal(MutableMessageAction.Update, update.Action);
        Assert.Equal(MutableMessageAction.Delete, delete.Action);
        Assert.All(new[] { create, append, update, delete }, ack => Assert.True(ack.Accepted));

        Assert.Equal(4, requests.Count);
        var actions = new List<string?>();
        foreach (var request in requests)
        {
            using var body = JsonDocument.Parse(await request.Content!.ReadAsStringAsync());
            actions.Add(body.RootElement.GetProperty("action").GetString());
        }
        Assert.Equal(new[] { "publish_create", "message_append", "message_update", "message_delete" }, actions);

        using var createBody = JsonDocument.Parse(await requests[0].Content!.ReadAsStringAsync());
        Assert.Equal("ai-output", createBody.RootElement.GetProperty("name").GetString());
        Assert.Equal("mid-1", createBody.RootElement.GetProperty("messageId").GetString());
        Assert.Equal("client-1", createBody.RootElement.GetProperty("clientId").GetString());

        using var updateBody = JsonDocument.Parse(await requests[2].Content!.ReadAsStringAsync());
        Assert.Equal("ai-output.updated", updateBody.RootElement.GetProperty("name").GetString());
        Assert.Equal("extras", updateBody.RootElement.GetProperty("clearFields")[0].GetString());
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

    private static SockudoClient TestClient() =>
        new(
            "app-key",
            new SockudoOptions(
                Cluster: "local",
                ForceTls: false,
                EnabledTransports: new[] { SockudoTransport.Ws }
            )
        );

    private static async Task DispatchRawAsync(SockudoClient client, string json)
    {
        var method = typeof(SockudoClient).GetMethod("HandleRawMessageAsync", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        var task = Assert.IsAssignableFrom<Task>(method.Invoke(
            client,
            new object[] { Encoding.UTF8.GetBytes(json), WebSocketMessageType.Text }));
        await task;
    }

    private static string FixtureText(string fileName) => File.ReadAllText(FixturePath(fileName));

    private static string TestJwt(long? iat, long exp)
    {
        var payload = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["exp"] = exp,
        };
        if (iat is not null)
        {
            payload["iat"] = iat.Value;
        }

        return string.Join(
            ".",
            Base64Url(JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["alg"] = "none",
                ["typ"] = "JWT",
            })),
            Base64Url(JsonSerializer.Serialize(payload)),
            "signature");
    }

    private static string Base64Url(string value) =>
        Convert.ToBase64String(Encoding.UTF8.GetBytes(value))
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');

    private static string FixturePath(string fileName)
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory);
             directory is not null;
             directory = directory.Parent)
        {
            var candidate = Path.Combine(
                directory.FullName,
                "tests",
                "ai-conformance",
                "fixtures",
                "forward-compat",
                fileName);
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        throw new FileNotFoundException($"Unable to locate forward-compat fixture '{fileName}'.");
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
