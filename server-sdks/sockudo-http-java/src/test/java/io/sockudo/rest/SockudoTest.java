package io.sockudo.rest;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.sockudo.rest.crypto.CryptoUtil;
import io.sockudo.rest.data.EncryptedMessage;
import io.sockudo.rest.data.Event;
import io.sockudo.rest.marshaller.DataMarshaller;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.imposters.ByteBuddyClassImposteriser;
import org.jmock.junit5.JUnit5Mockery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.sockudo.rest.util.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests which mock the HttpClient to check outgoing requests
 */
public class SockudoTest {

    static final String APP_ID = "00001";
    static final String KEY    = "157a2f3df564323a4a73";
    static final String SECRET = "3457a88be87f890dcd98";
    static final String VALID_MASTER_KEY = "VGhlIDMyIGNoYXJzIGxvbmcgZW5jcnlwdGlvbiBrZXk=";
    static final String INVALID_MASTER_KEY = "VGhlIDMyIGNoYXJzIGxvbmcgZW5jce";

    private final Mockery context = new JUnit5Mockery() {{
        setImposteriser(ByteBuddyClassImposteriser.INSTANCE);
    }};

    private CloseableHttpClient httpClient = context.mock(CloseableHttpClient.class);

    private final Sockudo p = new Sockudo(APP_ID, KEY, SECRET);

    @BeforeEach
    public void setup() {
        p.setAutoIdempotencyKey(false);
        configureHttpClient(p);
    }

    private void configureHttpClient(Sockudo p) {
        p.configureHttpClient(new HttpClientBuilder() {
            @Override
            public CloseableHttpClient build() {
                return httpClient;
            }
        });
    }

    /*
     * Serialisation tests
     */

    @SuppressWarnings("unused")
    private static class MyPojo {
        private String aString;
        private int aNumber;

        public MyPojo() {
            this.aString = "value";
            this.aNumber = 42;
        }

        public MyPojo(final String aString, final int aNumber) {
            this.aString = aString;
            this.aNumber = aNumber;
        }
    }

    @Test
    public void serialisePojo() throws IOException {
        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(field("data", "{\"aString\":\"value\",\"aNumber\":42}")));
        }});

        p.trigger("my-channel", "event", new MyPojo());
    }
    @Test

    public void customSerialisationGson() throws Exception {
        p.setGsonSerialiser(new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_DASHES).create());

        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(field("data", "{\"a-string\":\"value\",\"a-number\":42}")));
        }});

        p.trigger("my-channel", "event", new MyPojo());
    }

    @Test
    public void customSerialisationDataMarshaller() throws Exception {
        p.setDataMarshaller(new DataMarshaller() {
            private Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_DASHES).create();
            public String marshal(final Object data) {
                return gson.toJson(data);
            }
        });

        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(field("data", "{\"a-string\":\"value\",\"a-number\":42}")));
        }});

        p.trigger("my-channel", "event", new MyPojo());
    }

    @Test
    public void customSerialisationByExtension() throws Exception {
        Sockudo p = new Sockudo(APP_ID, KEY, SECRET) {
            @Override
            protected String serialise(Object data) {
                return (String)data;
            }
        };
        configureHttpClient(p);

        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(field("data", "this is my string data")));
        }});

        p.trigger("my-channel", "event", "this is my string data");
    }

    @Test
    public void batchEvents() throws IOException {
        final List<Map<String, Object>> res = new ArrayList<Map<String, Object>>() {{
          add(new HashMap<String, Object>() {{
            put("channel", "my-channel");
            put("name", "event-name");
            put("data", "{\"aString\":\"value1\",\"aNumber\":42}");
          }});

          add(new HashMap<String, Object>() {{
            put("channel", "my-channel");
            put("name", "event-name");
            put("data", "{\"aString\":\"value2\",\"aNumber\":43}");
            put("socket_id", "22.33");
          }});
        }};

        context.checking(new Expectations() {{
            oneOf(httpClient).execute(
                with(field("batch", res))
            );
        }});

        List<Event> batch = new ArrayList<>();
        batch.add(new Event("my-channel", "event-name", new MyPojo("value1", 42)));
        batch.add(new Event("my-channel", "event-name", new MyPojo("value2", 43), "22.33"));

        p.trigger(batch);
    }

    @Test
    public void mapShouldBeASuitableObjectForData() throws IOException {
        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(field("data", "{\"name\":\"value\"}")));
        }});

        p.trigger("my-channel", "event", Collections.singletonMap("name", "value"));
    }

    @Test
    public void multiLayerMapShouldSerialiseFully() throws IOException {
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("k1", "v1");
        Map<String, Object> level2 = new HashMap<String, Object>();
        level2.put("k3", "v3");
        List<String> level3 = new ArrayList<String>();
        level3.add("v4");
        level3.add("v5");
        level2.put("k4", level3);;
        data.put("k2", level2);

        final String expectedData = "{\"k1\":\"v1\",\"k2\":{\"k3\":\"v3\",\"k4\":[\"v4\",\"v5\"]}}";
        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(field("data", expectedData)));
        }});

        p.trigger("my-channel", "event", data);
    }

    @Test
    public void channelList() throws Exception {
        final List<String> channels = Arrays.asList(new String[] { "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten" });

        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(field("channels", channels)));
        }});

        p.trigger(channels, "event", Collections.singletonMap("name", "value"));
    }

    @Test
    public void channelListLimitOverLimit() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {

            final List<String> channels = Arrays.asList(new String[101]);
            p.trigger(channels, "event", Collections.singletonMap("name", "value"));
        });

    }

    @Test
    public void socketIdExclusion() throws Exception {
        final String socketId = "12345.6789";
        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(field("socket_id", socketId)));
        }});

        p.trigger("channel", "event", Collections.singletonMap("name", "value"), socketId);
    }

    @Test
    public void genericGet() throws Exception {
        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(path("/apps/" + APP_ID + "/channels")));
        }});

        p.get("/channels");
    }

    @Test
    public void reservedParameter() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            p.get("/channels", Collections.singletonMap("auth_timestamp", "anything"));
        });
    }

    @Test
    public void getChannelHistory() throws Exception {
        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(path("/apps/" + APP_ID + "/channels/history-room/history")));
        }});

        p.getChannelHistory("history-room");
    }

    @Test
    public void getChannelHistoryWithParams() throws Exception {
        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(path("/apps/" + APP_ID + "/channels/history-room/history")));
        }});

        p.getChannelHistory("history-room", Collections.singletonMap("direction", "newest_first"));
    }

    @Test
    public void listDeviceRegistrationsUsesCursorPagination() throws Exception {
        final Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("limit", "10");
        parameters.put("cursor", "c1");

        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(allOf(
                    path("/apps/" + APP_ID + "/push/deviceRegistrations"),
                    queryParam("limit", "10"),
                    queryParam("cursor", "c1"),
                    requestHeader("X-Sockudo-Push-Capability", "push-admin")
            )));
        }});

        p.listDeviceRegistrations(parameters);
    }

    @Test
    public void publishPushUsesAsyncDefaultAndAdminCapability() throws Exception {
        final Map<String, Object> payload = new HashMap<String, Object>();
        payload.put("recipients", Collections.singletonList(Collections.<String, Object>singletonMap("channel", "orders")));
        payload.put("payload", Collections.<String, Object>singletonMap("title", "Order"));
        payload.put("providerOverrides", Collections.singletonList(Collections.<String, Object>singletonMap("provider", "fcm")));

        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(allOf(
                    path("/apps/" + APP_ID + "/push/publish"),
                    fieldAndHeader("sync", false, "X-Sockudo-Push-Capability", "push-admin")
            )));
        }});

        p.publishPush(payload);
    }

    @Test
    public void schedulePushRequiresNotBeforeMs() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> p.schedulePush(Collections.<String, Object>emptyMap()));
    }

    @Test
    public void getChannelPresenceHistory() throws Exception {
        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(path("/apps/" + APP_ID + "/channels/presence-room/presence/history")));
        }});

        p.getChannelPresenceHistory("presence-room");
    }

    @Test
    public void getChannelPresenceHistoryWithParams() throws Exception {
        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(path("/apps/" + APP_ID + "/channels/presence-room/presence/history")));
        }});

        p.getChannelPresenceHistory("presence-room", Collections.singletonMap("direction", "newest_first"));
    }

    @Test
    public void getChannelPresenceSnapshot() throws Exception {
        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(path("/apps/" + APP_ID + "/channels/presence-room/presence/history/snapshot")));
        }});

        p.getChannelPresenceSnapshot("presence-room");
    }

    @Test
    public void getChannelPresenceSnapshotWithParams() throws Exception {
        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(path("/apps/" + APP_ID + "/channels/presence-room/presence/history/snapshot")));
        }});

        p.getChannelPresenceSnapshot("presence-room", Collections.singletonMap("at_serial", "4"));
    }

    @Test
    public void testTriggerOnEncryptedChannel() throws IOException {
        final Sockudo pe = new Sockudo(APP_ID, KEY, SECRET, VALID_MASTER_KEY);
        configureHttpClient(pe);

        CryptoUtil cryptoUtilMock = context.mock(CryptoUtil.class);
        pe.setCryptoUtil(cryptoUtilMock);

        final Map<String, String> testData = Collections.singletonMap("n", "1");

        final byte[] expectedMessage = "{\"n\":\"1\"}".getBytes(StandardCharsets.UTF_8);
        context.checking(new Expectations() {{
            oneOf(cryptoUtilMock).encrypt(with(same("private-encrypted-test")), with(equal(expectedMessage)));
            will(returnValue(new EncryptedMessage("1", "2")));
        }});

        final String expectedData = "{\"nonce\":\"1\",\"ciphertext\":\"2\"}";
        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(field("data", expectedData)));
        }});


        pe.trigger("private-encrypted-test", "test-event", testData);
    }

    @Test
    public void testTriggerBatchOnEncryptedChannel() throws IOException {
        final Sockudo pe = new Sockudo(APP_ID, KEY, SECRET, VALID_MASTER_KEY);
        pe.setAutoIdempotencyKey(false);
        configureHttpClient(pe);

        CryptoUtil cryptoUtilMock = context.mock(CryptoUtil.class);
        pe.setCryptoUtil(cryptoUtilMock);

        final byte[] expectedMessage = "{\"n\":\"1\"}".getBytes(StandardCharsets.UTF_8);
        context.checking(new Expectations() {{
            oneOf(cryptoUtilMock).encrypt(with(same("private-encrypted-test")), with(equal(expectedMessage)));
            will(returnValue(new EncryptedMessage("e1", "e2")));
        }});

        final List<Map<String, Object>> expectedData = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{
                put("channel", "private-encrypted-test");
                put("name", "event-name");
                put("data", "{\"nonce\":\"e1\",\"ciphertext\":\"e2\"}");
            }});

            add(new HashMap<String, Object>() {{
                put("channel", "my-channel");
                put("name", "event-name");
                put("data", "{\"n\":\"2\"}");
            }});
        }};

        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(field("batch", expectedData)));
        }});

        List<Event> testBatch = new ArrayList<Event>();
        testBatch.add(new Event("private-encrypted-test", "event-name", Collections.singletonMap("n", "1")));
        testBatch.add(new Event("my-channel", "event-name", Collections.singletonMap("n", "2")));

        pe.trigger(testBatch);
    }

    @Test
    public void testInstantiateSockudoWithInvalidMasterKey() {
        final Exception exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new Sockudo(APP_ID, KEY, SECRET, INVALID_MASTER_KEY);
        });

        assertThat(
            exception.getMessage(),
            containsString("encryptionMasterKeyBase64 must be a 32 byte key, base64 encoded")
        );
    }

    @Test
    public void testInstantiateSockudoWithEmptyMasterKey() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new Sockudo(APP_ID, KEY, SECRET, "");
        });

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new Sockudo(APP_ID, KEY, SECRET, null);
        });
    }

    @Test
    public void testTriggerOnEncryptedChannelWithoutMasterKey() {
        final Exception exception = Assertions.assertThrows(RuntimeException.class, () -> {
            p.trigger("private-encrypted-test", "test-event", "testData");
        });

        assertThat(
            exception.getMessage(),
            is(SockudoException.encryptionMasterKeyRequired().getMessage())
        );
    }

    @Test
    public void testTriggerBatchOnEncryptedChannelWithoutMasterKey() {
        List<Event> events = Arrays.asList(
            new Event("private-encrypted-test", "test_event", "test_data1"),
            new Event("private-encrypted-test", "test_event", "test_data2")
        );

        final Exception exception = Assertions.assertThrows(RuntimeException.class, () -> {
            p.trigger(events);
        });

        assertThat(
            exception.getMessage(),
            is(SockudoException.encryptionMasterKeyRequired().getMessage())
        );
    }

    @Test
    public void testTriggerOnMultipleChannelsWithEncryptedChannelIsNotSupported() throws IOException {
        final Sockudo pe = new Sockudo(APP_ID, KEY, SECRET, VALID_MASTER_KEY);
        configureHttpClient(pe);

        final Exception exception = Assertions.assertThrows(RuntimeException.class, () -> {
            pe.trigger(Arrays.asList("private-encrypted-test", "another-channel"), "test_data", "test_data");
        });

        assertThat(
            exception.getMessage(),
            is(SockudoException.cannotTriggerMultipleChannelsWithEncryption().getMessage())
        );
    }

    @Test
    public void testTriggerOnMultipleEncryptedChannelsIsNotSupported() throws IOException {
        final Sockudo pe = new Sockudo(APP_ID, KEY, SECRET, VALID_MASTER_KEY);
        configureHttpClient(pe);

        final Exception exception = Assertions.assertThrows(RuntimeException.class, () -> {
            pe.trigger(Arrays.asList("private-encrypted-test1", "private-encrypted-test2"), "test_data", "test_data");
        });

        assertThat(
            exception.getMessage(),
            is(SockudoException.cannotTriggerMultipleChannelsWithEncryption().getMessage())
        );
    }

    /*
     * Idempotency key tests
     */

    @Test
    public void triggerWithIdempotencyKeySetsBodyFieldAndHeader() throws IOException {
        final String idempotencyKey = "my-unique-key-123";
        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(fieldAndHeader("idempotency_key", idempotencyKey, "X-Idempotency-Key", idempotencyKey)));
        }});

        p.trigger("my-channel", "event", Collections.singletonMap("name", "value"), null, idempotencyKey);
    }

    @Test
    public void triggerWithIdempotencyKeyOnMultipleChannels() throws IOException {
        final String idempotencyKey = "multi-channel-key";
        final List<String> channels = Arrays.asList("channel-1", "channel-2");
        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(fieldAndHeader("idempotency_key", idempotencyKey, "X-Idempotency-Key", idempotencyKey)));
        }});

        p.trigger(channels, "event", Collections.singletonMap("name", "value"), null, idempotencyKey);
    }

    @Test
    public void triggerWithoutIdempotencyKeyDoesNotIncludeField() throws IOException {
        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(field("data", "{\"name\":\"value\"}")));
        }});

        p.trigger("my-channel", "event", Collections.singletonMap("name", "value"), null, null);
    }

    @Test
    public void triggerWithSocketIdAndIdempotencyKey() throws IOException {
        final String socketId = "12345.6789";
        final String idempotencyKey = "socket-idem-key";
        context.checking(new Expectations() {{
            oneOf(httpClient).execute(with(fieldAndHeader("idempotency_key", idempotencyKey, "X-Idempotency-Key", idempotencyKey)));
        }});

        p.trigger("channel", "event", Collections.singletonMap("name", "value"), socketId, idempotencyKey);
    }

    @Test
    public void batchEventsWithIdempotencyKeys() throws IOException {
        final List<Map<String, Object>> res = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{
                put("channel", "my-channel");
                put("name", "event-name");
                put("data", "{\"aString\":\"value1\",\"aNumber\":42}");
                put("idempotency_key", "key-1");
            }});

            add(new HashMap<String, Object>() {{
                put("channel", "my-channel");
                put("name", "event-name");
                put("data", "{\"aString\":\"value2\",\"aNumber\":43}");
                put("idempotency_key", "key-2");
            }});
        }};

        context.checking(new Expectations() {{
            oneOf(httpClient).execute(
                with(field("batch", res))
            );
        }});

        List<Event> batch = new ArrayList<>();
        batch.add(new Event("my-channel", "event-name", new MyPojo("value1", 42), null, "key-1"));
        batch.add(new Event("my-channel", "event-name", new MyPojo("value2", 43), null, "key-2"));

        p.trigger(batch);
    }

    @Test
    public void generateIdempotencyKeyReturnsCompactToken() {
        String key1 = SockudoAbstract.generateIdempotencyKey();
        String key2 = SockudoAbstract.generateIdempotencyKey();

        assertThat(key1, is(not(nullValue())));
        assertThat(key2, is(not(nullValue())));
        assertThat(key1.length(), is(16));
        assertThat(key1, is(not(key2)));
    }
}
