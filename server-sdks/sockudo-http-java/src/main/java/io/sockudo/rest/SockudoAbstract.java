package io.sockudo.rest;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.sockudo.rest.crypto.CryptoUtil;
import io.sockudo.rest.data.*;
import io.sockudo.rest.marshaller.DataMarshaller;
import io.sockudo.rest.marshaller.DefaultDataMarshaller;
import io.sockudo.rest.util.Prerequisites;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parent class for Sockudo clients, deals with anything that isn't IO related.
 *
 * @param <T> The return type of the IO calls.
 *
 * See {@link Sockudo} for the synchronous implementation, {@link SockudoAsync} for the asynchronous implementation.
 */
public abstract class SockudoAbstract<T> {
    private static final String PUSH_ADMIN_CAPABILITY = "push-admin";
    private static final String PUSH_SUBSCRIBE_CAPABILITY = "push-subscribe";
    protected static final Gson BODY_SERIALISER = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .disableHtmlEscaping()
            .create();

    private static final Pattern HEROKU_URL = Pattern.compile("(https?)://(.+):(.+)@(.+:?.*)/apps/(.+)");
    private static final String ENCRYPTED_CHANNEL_PREFIX = "private-encrypted-";

    protected final String appId;
    protected final String key;
    protected final String secret;

    protected String host = "api.sockudo.io";
    protected String scheme = "http";

    private final String baseId;
    private final AtomicLong publishSerial = new AtomicLong(0);
    private boolean autoIdempotencyKey = true;

    private static String generateBaseId() {
        byte[] bytes = new byte[12];
        new java.security.SecureRandom().nextBytes(bytes);
        return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private DataMarshaller dataMarshaller;
    private CryptoUtil crypto;
    private final boolean hasValidEncryptionMasterKey;

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
    public SockudoAbstract(final String appId, final String key, final String secret) {
        Prerequisites.nonEmpty("appId", appId);
        Prerequisites.nonEmpty("key", key);
        Prerequisites.nonEmpty("secret", secret);
        Prerequisites.isValidSha256Key("secret", secret);

        this.appId = appId;
        this.key = key;
        this.secret = secret;
        this.hasValidEncryptionMasterKey = false;
        this.baseId = generateBaseId();

        configureDataMarshaller();
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
    public SockudoAbstract(final String appId, final String key, final String secret, final String encryptionMasterKeyBase64) {
        Prerequisites.nonEmpty("appId", appId);
        Prerequisites.nonEmpty("key", key);
        Prerequisites.nonEmpty("secret", secret);
        Prerequisites.isValidSha256Key("secret", secret);
        Prerequisites.nonEmpty("encryptionMasterKeyBase64", encryptionMasterKeyBase64);

        this.appId = appId;
        this.key = key;
        this.secret = secret;
        this.baseId = generateBaseId();

        this.crypto = new CryptoUtil(encryptionMasterKeyBase64);
        this.hasValidEncryptionMasterKey = true;

        configureDataMarshaller();
    }

    public SockudoAbstract(final String url) {
        Prerequisites.nonNull("url", url);

        final Matcher m = HEROKU_URL.matcher(url);
        if (m.matches()) {
            this.scheme = m.group(1);
            this.key = m.group(2);
            this.secret = m.group(3);
            this.host = m.group(4);
            this.appId = m.group(5);
            this.hasValidEncryptionMasterKey = false;
        } else {
            throw new IllegalArgumentException("URL '" + url + "' does not match pattern '<scheme>://<key>:<secret>@<host>[:<port>]/apps/<appId>'");
        }

        this.baseId = generateBaseId();
        Prerequisites.isValidSha256Key("secret", secret);
        configureDataMarshaller();
    }

    private void configureDataMarshaller() {
        this.dataMarshaller = new DefaultDataMarshaller();
    }

    protected void setCryptoUtil(CryptoUtil crypto) {
        this.crypto = crypto;
    }

    /*
     * CONFIG
     */

    /**
     * For testing or specifying an alternative cluster. See also {@link #setCluster(String)} for the latter.
     * <p>
     * Default: api.sockudo.io
     *
     * @param host the API endpoint host
     */
    public void setHost(final String host) {
        Prerequisites.nonNull("host", host);

        this.host = host;
    }

    /**
     * For Specifying an alternative cluster.
     * <p>
     * See also {@link #setHost(String)} for targetting an arbitrary endpoint.
     *
     * @param cluster the Sockudo cluster to target
     */
    public void setCluster(final String cluster) {
        Prerequisites.nonNull("cluster", cluster);

        this.host = "api-" + cluster + ".sockudo.io";
    }

    /**
     * Set whether to use a secure connection to the API (SSL).
     * <p>
     * Authentication is secure even without this option, requests cannot be faked or replayed with access
     * to their plain text, a secure connection is only required if the requests or responses contain
     * sensitive information.
     * <p>
     * Default: false
     *
     * @param encrypted whether to use SSL to contact the API
     */
    public void setEncrypted(final boolean encrypted) {
        this.scheme = encrypted ? "https" : "http";
    }

    /**
     * Set the Gson instance used to marshal Objects passed to {@link #trigger(List, String, Object)}
     * Set the marshaller used to serialize Objects passed to {@link #trigger(List, String, Object)}
     * and friends.
     * By default, the library marshals the objects provided to JSON using the Gson library
     * (see https://code.google.com/p/google-gson/ for more details). By providing an instance
     * here, you may exert control over the marshalling, for example choosing how Java property
     * names are mapped on to the field names in the JSON representation, allowing you to match
     * the expected scheme on the client side.
     * We added the {@link #setDataMarshaller(DataMarshaller)} method to allow specification
     * of other marshalling libraries. This method was kept around to maintain backwards
     * compatibility.
     * @param gson a GSON instance configured to your liking
     */
    public void setGsonSerialiser(final Gson gson) {
        setDataMarshaller(new DefaultDataMarshaller(gson));
    }

    /**
     * Set a custom marshaller used to serialize Objects passed to {@link #trigger(List, String, Object)}
     * and friends.
     * <p>
     * By default, the library marshals the objects provided to JSON using the Gson library
     * (see https://code.google.com/p/google-gson/ for more details). By providing an instance
     * here, you may exert control over the marshalling, for example choosing how Java property
     * names are mapped on to the field names in the JSON representation, allowing you to match
     * the expected scheme on the client side.
     *
     * @param marshaller a DataMarshaller instance configured to your liking
     */
    public void setDataMarshaller(final DataMarshaller marshaller) {
        this.dataMarshaller = marshaller;
    }

    /**
     * Enable or disable automatic deterministic idempotency key generation.
     * When enabled (default), each {@code trigger()} / batch trigger call that lacks
     * an explicit idempotency key receives one derived from the client's session ID
     * and a monotonically increasing publish serial.
     *
     * @param autoIdempotencyKey whether to auto-generate idempotency keys
     */
    public void setAutoIdempotencyKey(final boolean autoIdempotencyKey) {
        this.autoIdempotencyKey = autoIdempotencyKey;
    }

    /**
     * @return whether automatic idempotency key generation is enabled
     */
    public boolean isAutoIdempotencyKey() {
        return autoIdempotencyKey;
    }

    /**
     * @return the base ID used for deterministic idempotency key generation
     */
    public String getBaseId() {
        return baseId;
    }

    /**
     * @return the current publish serial (the next serial that will be used)
     */
    public long getPublishSerial() {
        return publishSerial.get();
    }

    /**
     * Generate a random idempotency key (16-char base64url string from 12 random bytes).
     *
     * @return a compact random string suitable for use as an idempotency key
     */
    public static String generateIdempotencyKey() {
        return generateBaseId();
    }

    /**
     * This method provides an override point if the default Gson based serialisation is absolutely
     * unsuitable for your use case, even with customisation of the Gson instance doing the serialisation.
     * <p>
     * For example, in the simplest case, you might already have your data pre-serialised and simply want
     * to elide the default serialisation:
     * <pre>
     * Sockudo sockudo = new Sockudo(appId, key, secret) {
     *     protected String serialise(final Object data) {
     *         return (String)data;
     *     }
     * };
     *
     * sockudo.trigger("my-channel", "my-event", "{\"my-data\":\"my-value\"}");
     * </pre>
     *
     * @param data an unserialised event payload
     * @return a serialised event payload
     */
    protected String serialise(final Object data) {
        return dataMarshaller.marshal(data);
    }

    /*
     * REST
     */

    /**
     * Publish a message to a single channel.
     * <p>
     * The message data should be a POJO, which will be serialised to JSON for submission.
     * Use {@link #setDataMarshaller(DataMarshaller)} to control the serialisation
     * <p>
     * Note that if you do not wish to create classes specifically for the purpose of specifying
     * the message payload, use Map&lt;String, Object&gt;. These maps will nest just fine.
     *
     * @param channel   the channel name on which to trigger the event
     * @param eventName the name given to the event
     * @param data      an object which will be serialised to create the event body
     * @return a {@link Result} object encapsulating the success state and response to the request
     */
    public T trigger(final String channel, final String eventName, final Object data) {
        return trigger(channel, eventName, data, null);
    }

    /**
     * Publish identical messages to multiple channels.
     *
     * @param channels  the channel names on which to trigger the event
     * @param eventName the name given to the event
     * @param data      an object which will be serialised to create the event body
     * @return a {@link Result} object encapsulating the success state and response to the request
     */
    public T trigger(final List<String> channels, final String eventName, final Object data) {
        return trigger(channels, eventName, data, null);
    }

    /**
     * Publish a message to a single channel, excluding the specified socketId from receiving the message.
     *
     * @param channel   the channel name on which to trigger the event
     * @param eventName the name given to the event
     * @param data      an object which will be serialised to create the event body
     * @param socketId  a socket id which should be excluded from receiving the event
     * @return a {@link Result} object encapsulating the success state and response to the request
     */
    public T trigger(final String channel, final String eventName, final Object data, final String socketId) {
        return trigger(Collections.singletonList(channel), eventName, data, socketId);
    }

    /**
     * Publish identical messages to multiple channels, excluding the specified socketId from receiving the message.
     *
     * @param channels  the channel names on which to trigger the event
     * @param eventName the name given to the event
     * @param data      an object which will be serialised to create the event body
     * @param socketId  a socket id which should be excluded from receiving the event
     * @return a {@link Result} object encapsulating the success state and response to the request
     */
    public T trigger(final List<String> channels, final String eventName, final Object data, final String socketId) {
        return trigger(channels, eventName, data, socketId, null);
    }

    /**
     * Publish a message to a single channel with an idempotency key.
     *
     * @param channel        the channel name on which to trigger the event
     * @param eventName      the name given to the event
     * @param data           an object which will be serialised to create the event body
     * @param socketId       a socket id which should be excluded from receiving the event, or null
     * @param idempotencyKey a unique key to ensure idempotent event delivery, or null
     * @return a {@link Result} object encapsulating the success state and response to the request
     */
    public T trigger(final String channel, final String eventName, final Object data, final String socketId, final String idempotencyKey) {
        return trigger(Collections.singletonList(channel), eventName, data, socketId, idempotencyKey);
    }

    /**
     * Publish identical messages to multiple channels with an idempotency key.
     * <p>
     * When an idempotency key is provided, it is included in the JSON request body as
     * {@code idempotency_key} and sent as the {@code X-Idempotency-Key} HTTP header.
     * Use {@link #generateIdempotencyKey()} to create a suitable key.
     *
     * @param channels       the channel names on which to trigger the event
     * @param eventName      the name given to the event
     * @param data           an object which will be serialised to create the event body
     * @param socketId       a socket id which should be excluded from receiving the event, or null
     * @param idempotencyKey a unique key to ensure idempotent event delivery, or null
     * @return a {@link Result} object encapsulating the success state and response to the request
     */
    public T trigger(final List<String> channels, final String eventName, final Object data, final String socketId, final String idempotencyKey) {
        Prerequisites.nonNull("channels", channels);
        Prerequisites.nonNull("eventName", eventName);
        Prerequisites.nonNull("data", data);
        Prerequisites.maxLength("channels", 100, channels);
        Prerequisites.noNullMembers("channels", channels);
        Prerequisites.areValidChannels(channels);
        Prerequisites.isValidSocketId(socketId);

        final String effectiveKey;
        if (idempotencyKey != null) {
            effectiveKey = idempotencyKey;
        } else if (autoIdempotencyKey) {
            effectiveKey = baseId + ":" + publishSerial.getAndIncrement();
        } else {
            effectiveKey = null;
        }

        final String eventBody;
        final String encryptedChannel = channels.stream()
            .filter(this::isEncryptedChannel)
            .findFirst()
            .orElse("");

        if (encryptedChannel.isEmpty()) {
            eventBody = serialise(data);
        } else {
            requireEncryptionMasterKey();

            if (channels.size() > 1) {
                throw SockudoException.cannotTriggerMultipleChannelsWithEncryption();
            }

            eventBody = encryptPayload(encryptedChannel, serialise(data));
        }

        final String body = BODY_SERIALISER.toJson(new TriggerData(channels, eventName, eventBody, socketId, effectiveKey));

        if (effectiveKey != null) {
            final Map<String, String> headers = new HashMap<>();
            headers.put("X-Idempotency-Key", effectiveKey);
            return post("/events", body, headers);
        } else {
            return post("/events", body);
        }
    }


    /**
     * Publish a batch of different events with a single API call.
     * <p>
     * The batch is limited to 10 events on our multi-tenant clusters.
     * <p>
     * Each {@link Event} in the batch may carry its own idempotency key
     * (see {@link Event#getIdempotencyKey()}), which will be included in the
     * per-event JSON body as {@code idempotency_key}.
     *
     * @param batch a list of events to publish
     * @return a {@link Result} object encapsulating the success state and response to the request
     */
    public T trigger(final List<Event> batch) {
        final long serial = autoIdempotencyKey ? publishSerial.getAndIncrement() : publishSerial.get();
        final List<Event> eventsWithSerialisedBodies = new ArrayList<Event>(batch.size());

        for (int i = 0; i < batch.size(); i++) {
            final Event e = batch.get(i);
            final String eventData;

            if (isEncryptedChannel(e.getChannel())) {
                requireEncryptionMasterKey();

                eventData = encryptPayload(e.getChannel(), serialise(e.getData()));
            } else {
                eventData = serialise(e.getData());
            }

            final String effectiveKey;
            if (e.getIdempotencyKey() != null) {
                effectiveKey = e.getIdempotencyKey();
            } else if (autoIdempotencyKey) {
                effectiveKey = baseId + ":" + serial + ":" + i;
            } else {
                effectiveKey = null;
            }

            eventsWithSerialisedBodies.add(
                    new Event(
                            e.getChannel(),
                            e.getName(),
                            eventData,
                            e.getSocketId(),
                            effectiveKey,
                            e.getExtras()
                    )
            );
        }

        final String body = BODY_SERIALISER.toJson(new EventBatch(eventsWithSerialisedBodies));

        return post("/batch_events", body);
    }

    /**
     * Make a generic HTTP call to the Sockudo API.
     * <p>
     * See the REST API docs/rest_api
     * <p>
     * NOTE: the path specified here is relative to that of your app. For example, to access
     * the channel list for your app, simply pass "/channels". Do not include the "/apps/[appId]"
     * at the beginning of the path.
     *
     * @param path the path (e.g. /channels) to query
     * @return a {@link Result} object encapsulating the success state and response to the request
     */
    public T get(final String path) {
        return get(path, Collections.<String, String>emptyMap());
    }

    /**
     * Make a generic HTTP call to the Sockudo API.
     * <p>
     * See the REST API docs/rest_api
     * <p>
     * Parameters should be a map of query parameters for the HTTP call, and may be null
     * if none are required.
     * <p>
     * NOTE: the path specified here is relative to that of your app. For example, to access
     * the channel list for your app, simply pass "/channels". Do not include the "/apps/[appId]"
     * at the beginning of the path.
     *
     * @param path       the path (e.g. /channels) to query
     * @param parameters query parameters to submit with the request
     * @return a {@link Result} object encapsulating the success state and response to the request
     */
    public T get(final String path, final Map<String, String> parameters) {
        return get(path, parameters, Collections.<String, String>emptyMap());
    }

    public T get(final String path, final Map<String, String> parameters, final Map<String, String> headers) {
        final String fullPath = "/apps/" + appId + path;
        final URI uri = SignatureUtil.uri("GET", scheme, host, fullPath, null, key, secret, parameters);

        return doGet(uri, headers);
    }

    /**
     * Fetch durable history for a specific channel.
     *
     * @param channelName the channel to query
     * @param parameters history query parameters such as limit, direction, cursor,
     *                   start_serial, end_serial, start_time_ms, end_time_ms
     * @return a {@link Result} object encapsulating the success state and response
     */
    public T getChannelHistory(final String channelName, final Map<String, String> parameters) {
        Prerequisites.nonEmpty("channelName", channelName);
        Prerequisites.isValidChannel(channelName);
        return get("/channels/" + channelName + "/history", parameters);
    }

    /**
     * Fetch durable history for a specific channel without additional query parameters.
     *
     * @param channelName the channel to query
     * @return a {@link Result} object encapsulating the success state and response
     */
    public T getChannelHistory(final String channelName) {
        return getChannelHistory(channelName, Collections.<String, String>emptyMap());
    }

    /**
     * Fetch the latest visible version of a mutable message.
     *
     * @param channelName the channel to query
     * @param messageSerial the stable serial of the target message
     * @return a {@link Result} object encapsulating the success state and response
     */
    public T getMessage(final String channelName, final String messageSerial) {
        Prerequisites.nonEmpty("channelName", channelName);
        Prerequisites.isValidChannel(channelName);
        Prerequisites.nonEmpty("messageSerial", messageSerial);
        return get("/channels/" + channelName + "/messages/" + messageSerial, Collections.<String, String>emptyMap());
    }

    /**
     * Fetch preserved versions of a mutable message.
     *
     * @param channelName the channel to query
     * @param messageSerial the stable serial of the target message
     * @param parameters versions query parameters such as limit, direction and cursor
     * @return a {@link Result} object encapsulating the success state and response
     */
    public T getMessageVersions(final String channelName, final String messageSerial, final Map<String, String> parameters) {
        Prerequisites.nonEmpty("channelName", channelName);
        Prerequisites.isValidChannel(channelName);
        Prerequisites.nonEmpty("messageSerial", messageSerial);
        return get("/channels/" + channelName + "/messages/" + messageSerial + "/versions", parameters);
    }

    /**
     * Fetch preserved versions of a mutable message without additional query parameters.
     */
    public T getMessageVersions(final String channelName, final String messageSerial) {
        return getMessageVersions(channelName, messageSerial, Collections.<String, String>emptyMap());
    }

    /**
     * Apply a mutable-message update.
     *
     * @param channelName the channel to query
     * @param messageSerial the stable serial of the target message
     * @param body the request payload
     * @return a {@link Result} object encapsulating the success state and response
     */
    public T updateMessage(final String channelName, final String messageSerial, final String body) {
        Prerequisites.nonEmpty("channelName", channelName);
        Prerequisites.isValidChannel(channelName);
        Prerequisites.nonEmpty("messageSerial", messageSerial);
        return post("/channels/" + channelName + "/messages/" + messageSerial + "/update", body);
    }

    /**
     * Apply a mutable-message delete.
     *
     * @param channelName the channel to query
     * @param messageSerial the stable serial of the target message
     * @param body the request payload
     * @return a {@link Result} object encapsulating the success state and response
     */
    public T deleteMessage(final String channelName, final String messageSerial, final String body) {
        Prerequisites.nonEmpty("channelName", channelName);
        Prerequisites.isValidChannel(channelName);
        Prerequisites.nonEmpty("messageSerial", messageSerial);
        return post("/channels/" + channelName + "/messages/" + messageSerial + "/delete", body);
    }

    /**
     * Apply a mutable-message append.
     *
     * @param channelName the channel to query
     * @param messageSerial the stable serial of the target message
     * @param body the request payload
     * @return a {@link Result} object encapsulating the success state and response
     */
    public T appendMessage(final String channelName, final String messageSerial, final String body) {
        Prerequisites.nonEmpty("channelName", channelName);
        Prerequisites.isValidChannel(channelName);
        Prerequisites.nonEmpty("messageSerial", messageSerial);
        return post("/channels/" + channelName + "/messages/" + messageSerial + "/append", body);
    }

    /**
     * Publish an annotation for a versioned message.
     */
    public T publishAnnotation(final String channelName, final String messageSerial, final String body) {
        Prerequisites.nonEmpty("channelName", channelName);
        Prerequisites.isValidChannel(channelName);
        Prerequisites.nonEmpty("messageSerial", messageSerial);
        return post("/channels/" + channelName + "/messages/" + messageSerial + "/annotations", body);
    }

    /**
     * Delete an annotation from a versioned message.
     */
    public T deleteAnnotation(final String channelName, final String messageSerial, final String annotationSerial, final Map<String, String> parameters) {
        Prerequisites.nonEmpty("channelName", channelName);
        Prerequisites.isValidChannel(channelName);
        Prerequisites.nonEmpty("messageSerial", messageSerial);
        Prerequisites.nonEmpty("annotationSerial", annotationSerial);
        return delete("/channels/" + channelName + "/messages/" + messageSerial + "/annotations/" + annotationSerial, parameters);
    }

    public T deleteAnnotation(final String channelName, final String messageSerial, final String annotationSerial) {
        return deleteAnnotation(channelName, messageSerial, annotationSerial, Collections.<String, String>emptyMap());
    }

    /**
     * List raw annotation events for a versioned message.
     */
    public T listAnnotations(final String channelName, final String messageSerial, final Map<String, String> parameters) {
        Prerequisites.nonEmpty("channelName", channelName);
        Prerequisites.isValidChannel(channelName);
        Prerequisites.nonEmpty("messageSerial", messageSerial);
        return get("/channels/" + channelName + "/messages/" + messageSerial + "/annotations", parameters);
    }

    public T listAnnotations(final String channelName, final String messageSerial) {
        return listAnnotations(channelName, messageSerial, Collections.<String, String>emptyMap());
    }

    /**
     * Fetch presence history for a specific presence channel.
     *
     * @param channelName the presence channel to query
     * @param parameters presence history query parameters such as limit, direction, cursor,
     *                   start_serial, end_serial, start_time_ms, end_time_ms
     * @return a {@link Result} object encapsulating the success state and response
     */
    public T getChannelPresenceHistory(final String channelName, final Map<String, String> parameters) {
        Prerequisites.nonEmpty("channelName", channelName);
        Prerequisites.isValidChannel(channelName);
        return get("/channels/" + channelName + "/presence/history", parameters);
    }

    /**
     * Fetch presence history for a specific presence channel without additional query parameters.
     *
     * @param channelName the presence channel to query
     * @return a {@link Result} object encapsulating the success state and response
     */
    public T getChannelPresenceHistory(final String channelName) {
        return getChannelPresenceHistory(channelName, Collections.<String, String>emptyMap());
    }

    /**
     * Fetch a reconstructed presence snapshot for a specific presence channel.
     *
     * @param channelName the presence channel to query
     * @param parameters snapshot query parameters such as at_time_ms or at_serial
     * @return a {@link Result} object encapsulating the success state and response
     */
    public T getChannelPresenceSnapshot(final String channelName, final Map<String, String> parameters) {
        Prerequisites.nonEmpty("channelName", channelName);
        Prerequisites.isValidChannel(channelName);
        return get("/channels/" + channelName + "/presence/history/snapshot", parameters);
    }

    /**
     * Fetch a reconstructed presence snapshot for a specific presence channel without additional query parameters.
     *
     * @param channelName the presence channel to query
     * @return a {@link Result} object encapsulating the success state and response
     */
    public T getChannelPresenceSnapshot(final String channelName) {
        return getChannelPresenceSnapshot(channelName, Collections.<String, String>emptyMap());
    }

    /**
     * Terminate all active WebSocket connections for a user.
     * <p>
     * Every socket currently authenticated as the given user will be disconnected.
     *
     * @param userId the user ID whose connections should be terminated
     * @return a {@link Result} object encapsulating the success state and response to the request
     */
    public T terminateUserConnections(final String userId) {
        final String path = "/users/" + userId + "/terminate_connections";
        return post(path, "{}");
    }

    /**
     * Force all active WebSocket connections for a user to reconnect.
     * <p>
     * Every socket currently authenticated as the given user will be closed with code {@code 4200},
     * prompting clients to reconnect.
     *
     * @param userId the user ID whose connections should be force-reconnected
     * @return a {@link Result} object encapsulating the success state and response to the request
     */
    public T forceReconnectUser(final String userId) {
        final String path = "/users/" + userId + "/force_reconnect";
        return post(path, "{}");
    }

    protected abstract T doGet(final URI uri);

    protected T doGet(final URI uri, final Map<String, String> headers) {
        return doGet(uri);
    }

    public T delete(final String path, final Map<String, String> parameters) {
        return delete(path, parameters, Collections.<String, String>emptyMap());
    }

    public T delete(final String path, final Map<String, String> parameters, final Map<String, String> headers) {
        final String fullPath = "/apps/" + appId + path;
        final URI uri = SignatureUtil.uri("DELETE", scheme, host, fullPath, null, key, secret, parameters);

        return doDelete(uri, headers);
    }

    protected abstract T doDelete(final URI uri);

    protected T doDelete(final URI uri, final Map<String, String> headers) {
        return doDelete(uri);
    }

    /**
     * Make a generic HTTP call to the Sockudo API.
     * <p>
     * The body should be a UTF-8 encoded String
     * <p>
     * See the REST API docs/rest_api
     * <p>
     * NOTE: the path specified here is relative to that of your app. For example, to access
     * the channel list for your app, simply pass "/channels". Do not include the "/apps/[appId]"
     * at the beginning of the path.
     *
     * @param path the path (e.g. /channels) to submit
     * @param body the body to submit
     * @return a {@link Result} object encapsulating the success state and response to the request
     */
    public T post(final String path, final String body) {
        final String fullPath = "/apps/" + appId + path;
        final URI uri = SignatureUtil.uri("POST", scheme, host, fullPath, body, key, secret, Collections.<String, String>emptyMap());

        return doPost(uri, body);
    }

    /**
     * Make a generic HTTP POST to the Sockudo API with additional headers.
     *
     * @param path    the path (e.g. /events) to submit
     * @param body    the body to submit
     * @param headers additional HTTP headers to include in the request
     * @return a {@link Result} object encapsulating the success state and response to the request
     */
    public T post(final String path, final String body, final Map<String, String> headers) {
        final String fullPath = "/apps/" + appId + path;
        final URI uri = SignatureUtil.uri("POST", scheme, host, fullPath, body, key, secret, Collections.<String, String>emptyMap());

        return doPost(uri, body, headers);
    }

    protected abstract T doPost(final URI uri, final String body);

    protected T doPost(final URI uri, final String body, final Map<String, String> headers) {
        return doPost(uri, body);
    }

    public T activateDevice(final Map<String, Object> device) {
        return activateDevice(device, false);
    }

    public T activateDevice(final Map<String, Object> device, final boolean rotateDeviceIdentityToken) {
        Prerequisites.nonNull("device", device);

        final Map<String, String> headers = pushHeaders(PUSH_ADMIN_CAPABILITY, null);
        if (rotateDeviceIdentityToken) {
            headers.put("X-Sockudo-Rotate-Device-Identity-Token", "true");
        }
        return post(pushPath("/deviceRegistrations"), BODY_SERIALISER.toJson(device), headers);
    }

    public T createDeviceActivation(final Map<String, Object> device) {
        return activateDevice(device);
    }

    public T updateDeviceRegistration(final Map<String, Object> device, final String deviceIdentityToken) {
        Prerequisites.nonNull("device", device);
        Prerequisites.nonEmpty("deviceIdentityToken", deviceIdentityToken);
        return post(
                pushPath("/deviceRegistrations"),
                BODY_SERIALISER.toJson(device),
                pushHeaders(PUSH_SUBSCRIBE_CAPABILITY, deviceIdentityToken)
        );
    }

    public T listDeviceRegistrations() {
        return listDeviceRegistrations(Collections.<String, String>emptyMap());
    }

    public T listDeviceRegistrations(final Map<String, String> parameters) {
        return get(
                pushPath("/deviceRegistrations"),
                parameters,
                pushHeaders(PUSH_ADMIN_CAPABILITY, null)
        );
    }

    public T getDeviceRegistration(final String deviceId) {
        return getDeviceRegistration(deviceId, null);
    }

    public T getDeviceRegistration(final String deviceId, final String deviceIdentityToken) {
        Prerequisites.nonEmpty("deviceId", deviceId);
        final boolean subscribeScoped = deviceIdentityToken != null && !deviceIdentityToken.isEmpty();
        return get(
                pushPath("/deviceRegistrations/" + deviceId),
                Collections.<String, String>emptyMap(),
                pushHeaders(subscribeScoped ? PUSH_SUBSCRIBE_CAPABILITY : PUSH_ADMIN_CAPABILITY, deviceIdentityToken)
        );
    }

    public T deleteDeviceRegistration(final String deviceId) {
        return deleteDeviceRegistration(deviceId, null);
    }

    public T deleteDeviceRegistration(final String deviceId, final String deviceIdentityToken) {
        Prerequisites.nonEmpty("deviceId", deviceId);
        final boolean subscribeScoped = deviceIdentityToken != null && !deviceIdentityToken.isEmpty();
        return delete(
                pushPath("/deviceRegistrations/" + deviceId),
                Collections.<String, String>emptyMap(),
                pushHeaders(subscribeScoped ? PUSH_SUBSCRIBE_CAPABILITY : PUSH_ADMIN_CAPABILITY, deviceIdentityToken)
        );
    }

    public T removeDeviceRegistrationsByClient(final String clientId) {
        Prerequisites.nonEmpty("clientId", clientId);
        return delete(
                pushPath("/deviceRegistrations"),
                Collections.singletonMap("clientId", clientId),
                pushHeaders(PUSH_ADMIN_CAPABILITY, null)
        );
    }

    public T upsertChannelPushSubscription(final Map<String, Object> subscription) {
        return upsertChannelPushSubscription(subscription, null);
    }

    public T upsertChannelPushSubscription(final Map<String, Object> subscription, final String deviceIdentityToken) {
        Prerequisites.nonNull("subscription", subscription);
        final boolean subscribeScoped = deviceIdentityToken != null && !deviceIdentityToken.isEmpty();
        return post(
                pushPath("/channelSubscriptions"),
                BODY_SERIALISER.toJson(subscription),
                pushHeaders(subscribeScoped ? PUSH_SUBSCRIBE_CAPABILITY : PUSH_ADMIN_CAPABILITY, deviceIdentityToken)
        );
    }

    public T listChannelPushSubscriptions() {
        return listChannelPushSubscriptions(Collections.<String, String>emptyMap(), null);
    }

    public T listChannelPushSubscriptions(final Map<String, String> parameters) {
        return listChannelPushSubscriptions(parameters, null);
    }

    public T listChannelPushSubscriptions(final Map<String, String> parameters, final String deviceIdentityToken) {
        final boolean subscribeScoped = deviceIdentityToken != null && !deviceIdentityToken.isEmpty();
        return get(
                pushPath("/channelSubscriptions"),
                parameters,
                pushHeaders(subscribeScoped ? PUSH_SUBSCRIBE_CAPABILITY : PUSH_ADMIN_CAPABILITY, deviceIdentityToken)
        );
    }

    public T deleteChannelPushSubscriptions(final Map<String, String> parameters) {
        return deleteChannelPushSubscriptions(parameters, null);
    }

    public T deleteChannelPushSubscriptions(final Map<String, String> parameters, final String deviceIdentityToken) {
        final boolean subscribeScoped = deviceIdentityToken != null && !deviceIdentityToken.isEmpty();
        return delete(
                pushPath("/channelSubscriptions"),
                parameters,
                pushHeaders(subscribeScoped ? PUSH_SUBSCRIBE_CAPABILITY : PUSH_ADMIN_CAPABILITY, deviceIdentityToken)
        );
    }

    public T listChannelPushSubscriptionChannels() {
        return listChannelPushSubscriptionChannels(Collections.<String, String>emptyMap());
    }

    public T listChannelPushSubscriptionChannels(final Map<String, String> parameters) {
        return get(
                pushPath("/channelSubscriptions/channels"),
                parameters,
                pushHeaders(PUSH_ADMIN_CAPABILITY, null)
        );
    }

    public T listPushCredentials() {
        return listPushCredentials(Collections.<String, String>emptyMap());
    }

    public T listPushCredentials(final Map<String, String> parameters) {
        return get(
                pushPath("/credentials"),
                parameters,
                pushHeaders(PUSH_ADMIN_CAPABILITY, null)
        );
    }

    public T putPushCredential(final String provider, final Map<String, Object> credential) {
        Prerequisites.nonEmpty("provider", provider);
        Prerequisites.nonNull("credential", credential);
        return post(
                pushPath("/credentials/" + provider),
                BODY_SERIALISER.toJson(credential),
                pushHeaders(PUSH_ADMIN_CAPABILITY, null)
        );
    }

    public T publishPush(final Map<String, Object> request) {
        Prerequisites.nonNull("request", request);
        final Map<String, Object> payload = new HashMap<String, Object>(request);
        payload.put("sync", Boolean.FALSE);
        return post(
                pushPath("/publish"),
                BODY_SERIALISER.toJson(payload),
                pushHeaders(PUSH_ADMIN_CAPABILITY, null)
        );
    }

    public T publishPushDirect(final Map<String, Object> request) {
        return publishPush(request);
    }

    public T publishPushBatch(final List<Map<String, Object>> requests) {
        Prerequisites.nonNull("requests", requests);
        final List<Map<String, Object>> payload = new ArrayList<Map<String, Object>>(requests.size());
        for (final Map<String, Object> request : requests) {
            final Map<String, Object> item = new HashMap<String, Object>(request);
            item.put("sync", Boolean.FALSE);
            payload.add(item);
        }
        return post(
                pushPath("/batch/publish"),
                BODY_SERIALISER.toJson(payload),
                pushHeaders(PUSH_ADMIN_CAPABILITY, null)
        );
    }

    public T schedulePush(final Map<String, Object> request) {
        Prerequisites.nonNull("request", request);
        if (!request.containsKey("notBeforeMs")) {
            throw new IllegalArgumentException("scheduled push requires notBeforeMs");
        }
        return publishPush(request);
    }

    public T getPublishStatus(final String publishId) {
        Prerequisites.nonEmpty("publishId", publishId);
        return get(
                pushPath("/publish/" + publishId + "/status"),
                Collections.<String, String>emptyMap(),
                pushHeaders(PUSH_ADMIN_CAPABILITY, null)
        );
    }

    public T cancelScheduledPush(final String publishId) {
        Prerequisites.nonEmpty("publishId", publishId);
        return delete(
                pushPath("/scheduled/" + publishId),
                Collections.<String, String>emptyMap(),
                pushHeaders(PUSH_ADMIN_CAPABILITY, null)
        );
    }

    public T postPushDeliveryStatus(final Map<String, Object> event) {
        Prerequisites.nonNull("event", event);
        return post(
                pushPath("/deliveryStatus"),
                BODY_SERIALISER.toJson(event),
                pushHeaders(PUSH_ADMIN_CAPABILITY, null)
        );
    }

    private Map<String, String> pushHeaders(final String capability, final String deviceIdentityToken) {
        final Map<String, String> headers = new HashMap<String, String>();
        headers.put("X-Sockudo-Push-Capability", capability);
        if (deviceIdentityToken != null && !deviceIdentityToken.isEmpty()) {
            headers.put("X-Sockudo-Device-Identity-Token", deviceIdentityToken);
        }
        return headers;
    }

    private String pushPath(final String suffix) {
        return "/push" + suffix;
    }

    /**
     * If you wanted to send the HTTP API requests manually (e.g. using a different HTTP client), this method
     * will return a java.net.URI which includes all of the appropriate query parameters which sign the request.
     *
     * @param method the HTTP method, e.g. GET, POST
     * @param path   the HTTP path, e.g. /channels
     * @param body   the HTTP request body, if there is one (otherwise pass null)
     * @return a URI object which includes the necessary query params for request authentication
     */
    public URI signedUri(final String method, final String path, final String body) {
        return signedUri(method, path, body, Collections.<String, String>emptyMap());
    }

    /**
     * If you wanted to send the HTTP API requests manually (e.g. using a different HTTP client), this method
     * will return a java.net.URI which includes all of the appropriate query parameters which sign the request.
     * <p>
     * Note that any further query parameters you wish to be add must be specified here, as they form part of the signature.
     *
     * @param method     the HTTP method, e.g. GET, POST
     * @param path       the HTTP path, e.g. /channels
     * @param body       the HTTP request body, if there is one (otherwise pass null)
     * @param parameters HTTP query parameters to be included in the request
     * @return a URI object which includes the necessary query params for request authentication
     */
    public URI signedUri(final String method, final String path, final String body, final Map<String, String> parameters) {
        return SignatureUtil.uri(method, scheme, host, path, body, key, secret, parameters);
    }

    /*
     * CHANNEL AUTHENTICATION
     */

    /**
     * Generate authentication response to authorise a user on a private channel
     * <p>
     * The return value is the complete body which should be returned to a client requesting authorisation.
     *
     * @param socketId the socket id of the connection to authenticate
     * @param channel  the name of the channel which the socket id should be authorised to join
     * @return an authentication string, suitable for return to the requesting client
     */
    public String authenticate(final String socketId, final String channel) {
        Prerequisites.nonNull("socketId", socketId);
        Prerequisites.nonNull("channel", channel);
        Prerequisites.isValidChannel(channel);
        Prerequisites.isValidSocketId(socketId);

        if (channel.startsWith("presence-")) {
            throw new IllegalArgumentException("This method is for private channels, use authenticate(String, String, PresenceUser) to authenticate for a presence channel.");
        }
        if (!channel.startsWith("private-")) {
            throw new IllegalArgumentException("Authentication is only applicable to private and presence channels");
        }

        final String signature = SignatureUtil.sign(socketId + ":" + channel, secret);

        final AuthData authData = new AuthData(key, signature);

        if (isEncryptedChannel(channel)) {
            requireEncryptionMasterKey();

            authData.setSharedSecret(crypto.generateBase64EncodedSharedSecret(channel));
        }

        return BODY_SERIALISER.toJson(authData);
    }

    /**
     * Generate authentication response to authorise a user on a presence channel
     * <p>
     * The return value is the complete body which should be returned to a client requesting authorisation.
     *
     * @param socketId the socket id of the connection to authenticate
     * @param channel  the name of the channel which the socket id should be authorised to join
     * @param user     a {@link PresenceUser} object which represents the channel data to be associated with the user
     * @return an authentication string, suitable for return to the requesting client
     */
    public String authenticate(final String socketId, final String channel, final PresenceUser user) {
        Prerequisites.nonNull("socketId", socketId);
        Prerequisites.nonNull("channel", channel);
        Prerequisites.nonNull("user", user);
        Prerequisites.isValidChannel(channel);
        Prerequisites.isValidSocketId(socketId);

        if (channel.startsWith("private-")) {
            throw new IllegalArgumentException("This method is for presence channels, use authenticate(String, String) to authenticate for a private channel.");
        }
        if (!channel.startsWith("presence-")) {
            throw new IllegalArgumentException("Authentication is only applicable to private and presence channels");
        }

        final String channelData = BODY_SERIALISER.toJson(user);
        final String signature = SignatureUtil.sign(socketId + ":" + channel + ":" + channelData, secret);
        return BODY_SERIALISER.toJson(new AuthData(key, signature, channelData));
    }

    /*
     * WEBHOOK VALIDATION
     */

    /**
     * Check the signature on a webhook received from Sockudo
     *
     * @param xSockudoKeyHeader       the X-Pusher-Key header as received in the webhook request
     * @param xSockudoSignatureHeader the X-Pusher-Signature header as received in the webhook request
     * @param body                   the webhook body
     * @return enum representing the possible validities of the webhook request
     */
    public Validity validateWebhookSignature(final String xSockudoKeyHeader, final String xSockudoSignatureHeader, final String body) {
        if (!xSockudoKeyHeader.trim().equals(key)) {
            // We can't validate the signature, because it was signed with a different key to the one we were initialised with.
            return Validity.SIGNED_WITH_WRONG_KEY;
        }

        final String recalculatedSignature = SignatureUtil.sign(body, secret);
        return xSockudoSignatureHeader.trim().equals(recalculatedSignature) ? Validity.VALID : Validity.INVALID;
    }

    /**
     * Parse a webhook body after validating its signature.
     *
     * <p>This parser preserves the raw JSON for future webhook event names and
     * fields. Signature validation still uses the exact raw body string passed
     * to {@link #validateWebhookSignature(String, String, String)}.</p>
     *
     * @param body the raw webhook body
     * @return a tolerant webhook representation
     */
    public Webhook parseWebhook(final String body) {
        return Webhook.parse(body);
    }

    /**
     * Determine whether a trigger result represents a successful response.
     * Subclasses may override this for custom result types. The default
     * implementation checks if the result is a {@link Result} with SUCCESS status.
     *
     * @param result the result returned from a trigger call
     * @return true if the result indicates success
     */
    protected boolean isSuccessResult(final T result) {
        if (result instanceof Result) {
            return ((Result) result).getStatus() == Result.Status.SUCCESS;
        }
        return false;
    }

    private boolean isEncryptedChannel(final String channel) {
        return channel.startsWith(ENCRYPTED_CHANNEL_PREFIX);
    }

    private void requireEncryptionMasterKey()
    {
        if (hasValidEncryptionMasterKey) {
            return;
        }

        throw SockudoException.encryptionMasterKeyRequired();
    }

    private String encryptPayload(final String encryptedChannel, final String payload) {
        final EncryptedMessage encryptedMsg = crypto.encrypt(
            encryptedChannel,
            payload.getBytes(StandardCharsets.UTF_8)
        );

        return BODY_SERIALISER.toJson(encryptedMsg);
    }
}
