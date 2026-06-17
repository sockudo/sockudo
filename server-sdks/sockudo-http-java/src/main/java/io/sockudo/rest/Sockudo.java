package io.sockudo.rest;

import io.sockudo.rest.data.Result;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * A library for interacting with the Sockudo HTTP API.
 * <p>
 * See the project README for an overview
 * <p>
 * Essentially:
 * <pre>
 * // Init
 * Sockudo sockudo = new Sockudo(APP_ID, KEY, SECRET);
 * // Publish
 * Result triggerResult = sockudo.trigger("my-channel", "my-eventname", myPojoForSerialisation);
 * if (triggerResult.getStatus() != Status.SUCCESS) {
 *   if (triggerResult.getStatus().shouldRetry()) {
 *     // Temporary, let's schedule a retry
 *   }
 *   else {
 *     // Something is wrong with our request
 *   }
 * }
 *
 * // Query
 * Result channelListResult = sockudo.get("/channels");
 * if (channelListResult.getStatus() == Status.SUCCESS) {
 *   String channelListAsJson = channelListResult.getMessage();
 *   // etc
 * }
 * </pre>
 *
 * See {@link SockudoAsync} for the asynchronous implementation.
 */
public class Sockudo extends SockudoAbstract<Result> implements AutoCloseable {

    private int requestTimeout = 4000; // milliseconds

    private CloseableHttpClient client;

    /**
     * Construct an instance of the Sockudo object through which you may interact with the Sockudo API.
     * <p>
     * The parameters to use are found on your dashboard and are specific per App.
     * <p>
     * @param appId The ID of the App you will to interact with.
     * @param key The App Key, the same key you give to websocket clients to identify your app when they connect to Sockudo.
     * @param secret The App Secret. Used to sign requests to the API, this should be treated as sensitive and not distributed.
     */
    public Sockudo(final String appId, final String key, final String secret) {
        super(appId, key, secret);
        configureHttpClient(defaultHttpClientBuilder());
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
    public Sockudo(final String appId, final String key, final String secret, final String encryptionMasterKeyBase64) {
        super(appId, key, secret, encryptionMasterKeyBase64);

        configureHttpClient(defaultHttpClientBuilder());
    }

    public Sockudo(final String url) {
        super(url);
        configureHttpClient(defaultHttpClientBuilder());
    }

    /*
     * CONFIG
     */

    /**
     * Default: 4000
     *
     * @param requestTimeout the request timeout in milliseconds
     */
    public void setRequestTimeout(final int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    /**
     * Returns an HttpClientBuilder with the settings used by default applied. You may apply
     * further configuration (for example an HTTP proxy), override existing configuration
     * (for example, the connection manager which handles connection pooling for reuse) and
     * then call {@link #configureHttpClient(HttpClientBuilder)} to have this configuration
     * applied to all subsequent calls.
     *
     * @see #configureHttpClient(HttpClientBuilder)
     *
     * @return an {@link org.apache.http.impl.client.HttpClientBuilder} with the default settings applied
     */
    public static HttpClientBuilder defaultHttpClientBuilder() {
        return HttpClientBuilder.create()
                .setConnectionManager(new PoolingHttpClientConnectionManager())
                .setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())
                .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                .disableRedirectHandling();
    }

    /**
     * Configure the HttpClient instance which will be used for making calls to the Sockudo API.
     * <p>
     * This method allows almost complete control over all aspects of the HTTP client, including
     * <ul>
     * <li>proxy host</li>
     * <li>connection pooling and reuse strategies</li>
     * <li>automatic retry and backoff strategies</li>
     * </ul>
     * It is <strong>strongly</strong> recommended that you take the value of {@link #defaultHttpClientBuilder()}
     * as a base, apply your custom config to that and then pass the builder in here, to ensure
     * that sensible defaults for configuration areas you are not setting explicitly are retained.
     * <p>
     * e.g.
     * <pre>
     * sockudo.configureHttpClient(
     *     Sockudo.defaultHttpClientBuilder()
     *           .setProxy(new HttpHost("proxy.example.com"))
     *           .disableAutomaticRetries()
     * );
     * </pre>
     *
     * @see #defaultHttpClientBuilder()
     *
     * @param builder an {@link org.apache.http.impl.client.HttpClientBuilder} with which to configure
     * the internal HTTP client
     */
    public void configureHttpClient(final HttpClientBuilder builder) {
        try {
            close();
        } catch (final Exception e) {
            // Not a lot useful we can do here
        }

        this.client = builder.build();
    }

    /*
     * REST
     */

    @Override
    protected Result doGet(final URI uri) {
        return httpCall(new HttpGet(uri));
    }

    @Override
    protected Result doGet(final URI uri, final Map<String, String> headers) {
        final HttpGet request = new HttpGet(uri);
        applyHeaders(request, headers);
        return httpCall(request);
    }

    @Override
    protected Result doDelete(final URI uri) {
        return httpCall(new HttpDelete(uri));
    }

    @Override
    protected Result doDelete(final URI uri, final Map<String, String> headers) {
        final HttpDelete request = new HttpDelete(uri);
        applyHeaders(request, headers);
        return httpCall(request);
    }

    @Override
    protected Result doPost(final URI uri, final String body) {
        final StringEntity bodyEntity = new StringEntity(body, "UTF-8");
        bodyEntity.setContentType("application/json");

        final HttpPost request = new HttpPost(uri);
        request.setEntity(bodyEntity);

        return httpCall(request);
    }

    @Override
    protected Result doPost(final URI uri, final String body, final Map<String, String> headers) {
        final StringEntity bodyEntity = new StringEntity(body, "UTF-8");
        bodyEntity.setContentType("application/json");

        final HttpPost request = new HttpPost(uri);
        request.setEntity(bodyEntity);

        applyHeaders(request, headers);

        return httpCall(request);
    }

    private void applyHeaders(final HttpRequestBase request, final Map<String, String> headers) {
        if (headers == null) {
            return;
        }
        for (final Map.Entry<String, String> header : headers.entrySet()) {
            request.setHeader(header.getKey(), header.getValue());
        }
    }

    private static final int MAX_RETRY_ATTEMPTS = 3;

    Result httpCall(final HttpRequestBase request) {
        final RequestConfig config = RequestConfig.custom()
                .setSocketTimeout(requestTimeout)
                .setConnectionRequestTimeout(requestTimeout)
                .setConnectTimeout(requestTimeout)
                .build();
        request.setConfig(config);

        Result lastResult = null;
        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                final HttpResponse response = client.execute(request);

                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                response.getEntity().writeTo(baos);
                final String responseBody = new String(baos.toByteArray(), "UTF-8");

                lastResult = Result.fromHttpCode(response.getStatusLine().getStatusCode(), responseBody);
            } catch (final IOException e) {
                lastResult = Result.fromException(e);
            }

            if (!lastResult.getStatus().shouldRetry() || attempt == MAX_RETRY_ATTEMPTS) {
                return lastResult;
            }

            try {
                Thread.sleep(100L * (1 << (attempt - 1)));
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return lastResult;
            }
        }

        return lastResult;
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
        }
    }

}
