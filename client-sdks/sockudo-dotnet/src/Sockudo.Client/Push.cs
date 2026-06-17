using System.Net.Http.Json;

namespace Sockudo.Client;

public sealed record PushRegistrationOptions(
    string Endpoint,
    IDictionary<string, string>? Headers = null,
    Func<IDictionary<string, string>>? HeadersProvider = null
);

public sealed record PushCursorParams(
    int? Limit = null,
    string? Cursor = null
)
{
    internal IReadOnlyList<KeyValuePair<string, string>> ToQueryParameters()
    {
        var parameters = new List<KeyValuePair<string, string>>();
        if (Limit is not null)
        {
            parameters.Add(new("limit", Limit.Value.ToString()));
        }
        if (Cursor is not null)
        {
            parameters.Add(new("cursor", Cursor));
        }
        return parameters;
    }
}

public sealed record PushSubscriptionParams(
    string? Channel = null,
    string? DeviceId = null,
    int? Limit = null,
    string? Cursor = null
)
{
    internal IReadOnlyList<KeyValuePair<string, string>> ToQueryParameters()
    {
        var parameters = new List<KeyValuePair<string, string>>();
        if (Channel is not null)
        {
            parameters.Add(new("channel", Channel));
        }
        if (DeviceId is not null)
        {
            parameters.Add(new("deviceId", DeviceId));
        }
        if (Limit is not null)
        {
            parameters.Add(new("limit", Limit.Value.ToString()));
        }
        if (Cursor is not null)
        {
            parameters.Add(new("cursor", Cursor));
        }
        return parameters;
    }
}

public sealed class SockudoPushRegistration
{
    private readonly HttpClient _httpClient;
    private readonly string _endpoint;

    public SockudoPushRegistration(PushRegistrationOptions options, HttpClient? httpClient = null)
    {
        Options = options;
        _httpClient = httpClient ?? new HttpClient();
        _endpoint = options.Endpoint.TrimEnd('/');
    }

    public PushRegistrationOptions Options { get; }

    public Task<Dictionary<string, object?>> ActivateDeviceAsync(
        Dictionary<string, object?> device,
        CancellationToken cancellationToken = default) =>
        RequestObjectAsync(HttpMethod.Post, "/deviceRegistrations", body: device, cancellationToken: cancellationToken);

    public Task<Dictionary<string, object?>> UpdateDeviceRegistrationAsync(
        Dictionary<string, object?> device,
        string deviceIdentityToken,
        CancellationToken cancellationToken = default) =>
        RequestObjectAsync(
            HttpMethod.Post,
            "/deviceRegistrations",
            body: device,
            requestHeaders: new Dictionary<string, string> { ["X-Sockudo-Device-Identity-Token"] = deviceIdentityToken },
            cancellationToken: cancellationToken);

    public Task<Dictionary<string, object?>> ListDeviceRegistrationsAsync(
        PushCursorParams? parameters = null,
        CancellationToken cancellationToken = default) =>
        RequestObjectAsync(
            HttpMethod.Get,
            "/deviceRegistrations",
            queryParameters: (parameters ?? new PushCursorParams()).ToQueryParameters(),
            cancellationToken: cancellationToken);

    public Task<Dictionary<string, object?>> GetDeviceRegistrationAsync(
        string deviceId,
        CancellationToken cancellationToken = default) =>
        RequestObjectAsync(
            HttpMethod.Get,
            $"/deviceRegistrations/{Uri.EscapeDataString(deviceId)}",
            cancellationToken: cancellationToken);

    public Task DeleteDeviceRegistrationAsync(
        string deviceId,
        CancellationToken cancellationToken = default) =>
        RequestVoidAsync(
            HttpMethod.Delete,
            $"/deviceRegistrations/{Uri.EscapeDataString(deviceId)}",
            cancellationToken: cancellationToken);

    public Task<Dictionary<string, object?>> UpsertChannelSubscriptionAsync(
        Dictionary<string, object?> subscription,
        CancellationToken cancellationToken = default) =>
        RequestObjectAsync(HttpMethod.Post, "/channelSubscriptions", body: subscription, cancellationToken: cancellationToken);

    public Task<Dictionary<string, object?>> ListChannelSubscriptionsAsync(
        PushSubscriptionParams? parameters = null,
        CancellationToken cancellationToken = default) =>
        RequestObjectAsync(
            HttpMethod.Get,
            "/channelSubscriptions",
            queryParameters: (parameters ?? new PushSubscriptionParams()).ToQueryParameters(),
            cancellationToken: cancellationToken);

    public Task DeleteChannelSubscriptionsAsync(
        PushSubscriptionParams parameters,
        CancellationToken cancellationToken = default) =>
        RequestVoidAsync(
            HttpMethod.Delete,
            "/channelSubscriptions",
            queryParameters: parameters.ToQueryParameters(),
            cancellationToken: cancellationToken);

    public Task<Dictionary<string, object?>> PublishAsync(
        Dictionary<string, object?> request,
        CancellationToken cancellationToken = default)
    {
        var payload = new Dictionary<string, object?>(request, StringComparer.Ordinal)
        {
            ["sync"] = false,
        };
        return RequestObjectAsync(HttpMethod.Post, "/publish", body: payload, cancellationToken: cancellationToken);
    }

    public Task<object?> PublishBatchAsync(
        IReadOnlyList<Dictionary<string, object?>> requests,
        CancellationToken cancellationToken = default)
    {
        var payload = requests
            .Select(request => new Dictionary<string, object?>(request, StringComparer.Ordinal)
            {
                ["sync"] = false,
            })
            .Cast<object?>()
            .ToArray();
        return RequestValueAsync(HttpMethod.Post, "/batch/publish", body: payload, cancellationToken: cancellationToken);
    }

    public Task<Dictionary<string, object?>> SchedulePublishAsync(
        Dictionary<string, object?> request,
        CancellationToken cancellationToken = default) =>
        PublishAsync(request, cancellationToken);

    public Task<Dictionary<string, object?>> GetPublishStatusAsync(
        string publishId,
        CancellationToken cancellationToken = default) =>
        RequestObjectAsync(
            HttpMethod.Get,
            $"/publish/{Uri.EscapeDataString(publishId)}/status",
            cancellationToken: cancellationToken);

    public Task CancelScheduledPublishAsync(
        string publishId,
        CancellationToken cancellationToken = default) =>
        RequestVoidAsync(
            HttpMethod.Delete,
            $"/scheduled/{Uri.EscapeDataString(publishId)}",
            cancellationToken: cancellationToken);

    public Task<Dictionary<string, object?>> PostDeliveryStatusAsync(
        Dictionary<string, object?> @event,
        CancellationToken cancellationToken = default) =>
        RequestObjectAsync(HttpMethod.Post, "/deliveryStatus", body: @event, cancellationToken: cancellationToken);

    private async Task<Dictionary<string, object?>> RequestObjectAsync(
        HttpMethod method,
        string path,
        object? body = null,
        IReadOnlyList<KeyValuePair<string, string>>? queryParameters = null,
        IDictionary<string, string>? requestHeaders = null,
        CancellationToken cancellationToken = default)
    {
        var value = await RequestValueAsync(method, path, body, queryParameters, requestHeaders, cancellationToken).ConfigureAwait(false);
        return value as Dictionary<string, object?> ?? new Dictionary<string, object?>();
    }

    private Task RequestVoidAsync(
        HttpMethod method,
        string path,
        object? body = null,
        IReadOnlyList<KeyValuePair<string, string>>? queryParameters = null,
        IDictionary<string, string>? requestHeaders = null,
        CancellationToken cancellationToken = default) =>
        RequestVoidCoreAsync(method, path, body, queryParameters, requestHeaders, cancellationToken);

    private async Task RequestVoidCoreAsync(
        HttpMethod method,
        string path,
        object? body,
        IReadOnlyList<KeyValuePair<string, string>>? queryParameters,
        IDictionary<string, string>? requestHeaders,
        CancellationToken cancellationToken)
    {
        await RequestValueAsync(method, path, body, queryParameters, requestHeaders, cancellationToken).ConfigureAwait(false);
    }

    private async Task<object?> RequestValueAsync(
        HttpMethod method,
        string path,
        object? body = null,
        IReadOnlyList<KeyValuePair<string, string>>? queryParameters = null,
        IDictionary<string, string>? requestHeaders = null,
        CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(method, BuildUrl(path, queryParameters));
        if (body is not null)
        {
            request.Content = JsonContent.Create(body, options: JsonSupport.SerializerOptions);
        }

        if (Options.Headers is not null)
        {
            foreach (var header in Options.Headers)
            {
                request.Headers.TryAddWithoutValidation(header.Key, header.Value);
            }
        }
        if (Options.HeadersProvider is not null)
        {
            foreach (var header in Options.HeadersProvider())
            {
                request.Headers.TryAddWithoutValidation(header.Key, header.Value);
            }
        }
        if (requestHeaders is not null)
        {
            foreach (var header in requestHeaders)
            {
                request.Headers.TryAddWithoutValidation(header.Key, header.Value);
            }
        }

        using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        var content = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new SockudoException($"Sockudo push request failed with HTTP {(int)response.StatusCode}");
        }

        if (response.StatusCode == System.Net.HttpStatusCode.NoContent || string.IsNullOrWhiteSpace(content))
        {
            return new Dictionary<string, object?>();
        }

        return JsonSupport.Decode(content);
    }

    private string BuildUrl(string path, IReadOnlyList<KeyValuePair<string, string>>? queryParameters)
    {
        if (queryParameters is null || queryParameters.Count == 0)
        {
            return $"{_endpoint}{path}";
        }

        var query = string.Join(
            "&",
            queryParameters.Select(parameter =>
                $"{Uri.EscapeDataString(parameter.Key)}={Uri.EscapeDataString(parameter.Value)}"));
        return $"{_endpoint}{path}?{query}";
    }
}
