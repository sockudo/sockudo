package io.sockudo.rest;

import io.sockudo.rest.data.Result;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.util.HttpConstants;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

/**
 * A library for interacting with the Sockudo HTTP API asynchronously.
 * <p>
 * See the project README for an overview
 * <p>
 * Essentially:
 * <pre>
 * // Init
 * SockudoAsync sockudo = new SockudoAsync(APP_ID, KEY, SECRET);
 *
 * // Publish
 * CompletableFuture&lt;Result&gt; futureTriggerResult = sockudo.trigger("my-channel", "my-eventname", myPojoForSerialisation);
 * triggerResult.thenAccept(triggerResult -&gt; {
 *   if (triggerResult.getStatus() == Status.SUCCESS) {
 *     // request was successful
 *   } else {
 *     // something went wrong with the request
 *   }
 * });
 *
 * // Query
 * CompletableFuture&lt;Result&gt; futureChannelListResult = sockudo.get("/channels");
 * futureChannelListResult.thenAccept(triggerResult -&gt; {
 *   if (triggerResult.getStatus() == Status.SUCCESS) {
 *     String channelListAsJson = channelListResult.getMessage();
 *     // etc.
 *   } else {
 *     // something went wrong with the request
 *   }
 * });
 * </pre>
 *
 * See {@link Sockudo} for the synchronous implementation.
 */
public class SockudoAsync extends SockudoAbstract<CompletableFuture<Result>> implements AutoCloseable {

    private AsyncHttpClient client;

    /**
     * Construct an instance of the Sockudo object through which you may interact with the Sockudo API.
     * <p>
     * The parameters to use are found on your dashboard and are specific per App.
     * <p>
     *
     * @param appId  The ID of the App you will to interact with.
     * @param key    The App Key, the same key you give to websocket clients to identify your app when they connect to Sockudo.
     * @param secret The App Secret. Used to sign requests to the API, this should be treated as sensitive and not distributed.
     */
    public SockudoAsync(final String appId, final String key, final String secret) {
        super(appId, key, secret);
        configureHttpClient(config());
    }

    /**
     * Construct an instance of the Sockudo object through which you may interact with the Sockudo API.
     * <p>
     * The parameters to use are found on your dashboard and are specific per App.
     * <p>
     *
     * @param appId  The ID of the App you will to interact with.
     * @param key    The App Key, the same key you give to websocket clients to identify your app when they connect to Sockudo.
     * @param secret The App Secret. Used to sign requests to the API, this should be treated as sensitive and not distributed.
     * @param encryptionMasterKeyBase64 32 byte key, base64 encoded. This key, along with the channel name, are used to derive per-channel encryption keys.
     */
    public SockudoAsync(final String appId, final String key, final String secret, final String encryptionMasterKeyBase64) {
        super(appId, key, secret, encryptionMasterKeyBase64);
        configureHttpClient(config());
    }

    public SockudoAsync(final String url) {
        super(url);
        configureHttpClient(config());
    }

    /*
     * CONFIG
     */

    /**
     * Configure the AsyncHttpClient instance which will be used for making calls to the Sockudo API.
     * <p>
     * This method allows almost complete control over all aspects of the HTTP client, including
     * <ul>
     * <li>proxy host</li>
     * <li>connection pooling and reuse strategies</li>
     * <li>automatic retry and backoff strategies</li>
     * </ul>
     * <p>
     * e.g.
     * <pre>
     * sockudo.configureHttpClient(
     *     config()
     *         .setProxyServer(proxyServer("127.0.0.1", 38080))
     *         .setMaxRequestRetry(5)
     * );
     * </pre>
     *
     * @param builder an {@link DefaultAsyncHttpClientConfig.Builder} with which to configure
     *                the internal HTTP client
     */
    public void configureHttpClient(final DefaultAsyncHttpClientConfig.Builder builder) {
        try {
            close();
        } catch (final Exception e) {
            // Not a lot useful we can do here
        }

        this.client = asyncHttpClient(builder);
    }

    /*
     * REST
     */

    @Override
    protected CompletableFuture<Result> doGet(final URI uri) {
        final Request request = new RequestBuilder(HttpConstants.Methods.GET)
            .setUrl(uri.toString())
            .build();

        return httpCall(request);
    }

    @Override
    protected CompletableFuture<Result> doGet(final URI uri, final Map<String, String> headers) {
        final RequestBuilder builder = new RequestBuilder(HttpConstants.Methods.GET)
                .setUrl(uri.toString());
        applyHeaders(builder, headers);
        return httpCall(builder.build());
    }

    @Override
    protected CompletableFuture<Result> doDelete(final URI uri) {
        final Request request = new RequestBuilder(HttpConstants.Methods.DELETE)
            .setUrl(uri.toString())
            .build();

        return httpCall(request);
    }

    @Override
    protected CompletableFuture<Result> doDelete(final URI uri, final Map<String, String> headers) {
        final RequestBuilder builder = new RequestBuilder(HttpConstants.Methods.DELETE)
                .setUrl(uri.toString());
        applyHeaders(builder, headers);
        return httpCall(builder.build());
    }

    @Override
    protected CompletableFuture<Result> doPost(final URI uri, final String body) {
        final Request request = new RequestBuilder(HttpConstants.Methods.POST)
                .setUrl(uri.toString())
                .setBody(body)
                .addHeader("Content-Type", "application/json")
                .build();

        return httpCall(request);
    }

    @Override
    protected CompletableFuture<Result> doPost(final URI uri, final String body, final Map<String, String> headers) {
        final RequestBuilder builder = new RequestBuilder(HttpConstants.Methods.POST)
                .setUrl(uri.toString())
                .setBody(body)
                .addHeader("Content-Type", "application/json");

        applyHeaders(builder, headers);

        return httpCall(builder.build());
    }

    private void applyHeaders(final RequestBuilder builder, final Map<String, String> headers) {
        if (headers == null) {
            return;
        }
        for (final Map.Entry<String, String> header : headers.entrySet()) {
            builder.addHeader(header.getKey(), header.getValue());
        }
    }

    private static final int MAX_RETRY_ATTEMPTS = 3;

    CompletableFuture<Result> httpCall(final Request request) {
        return httpCallWithRetry(request, 1);
    }

    private CompletableFuture<Result> httpCallWithRetry(final Request request, final int attempt) {
        return client
                .prepareRequest(request)
                .execute()
                .toCompletableFuture()
                .thenApply(response -> Result.fromHttpCode(response.getStatusCode(), response.getResponseBody(UTF_8)))
                .exceptionally(Result::fromThrowable)
                .thenCompose(result -> {
                    if (!result.getStatus().shouldRetry() || attempt >= MAX_RETRY_ATTEMPTS) {
                        return CompletableFuture.completedFuture(result);
                    }
                    final long delay = 100L * (1 << (attempt - 1));
                    return CompletableFuture.supplyAsync(() -> {
                        try { Thread.sleep(delay); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                        return null;
                    }).thenCompose(ignored -> httpCallWithRetry(request, attempt + 1));
                });
    }

    @Override
    public void close() throws Exception {
        if (client != null && !client.isClosed()) {
            client.close();
        }
    }

}
