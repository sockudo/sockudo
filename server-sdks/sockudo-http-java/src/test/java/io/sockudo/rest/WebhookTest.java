package io.sockudo.rest;

import com.google.gson.JsonElement;
import io.sockudo.rest.data.Result;
import io.sockudo.rest.data.Validity;
import io.sockudo.rest.data.Webhook;
import io.sockudo.rest.data.WebhookEvent;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class WebhookTest {
    private static final String APP_ID = "00001";
    private static final String KEY = "157a2f3df564323a4a73";
    private static final String SECRET = "3457a88be87f890dcd98";

    private final Sockudo sockudo = new Sockudo(APP_ID, KEY, SECRET);

    @Test
    public void parsesSharedFutureWebhookFixture() throws IOException {
        final Webhook webhook = sockudo.parseWebhook(readForwardCompatFixture());

        assertThat(webhook.getTimeMs(), is(1710000000000L));
        assertThat(webhook.getEvents().size(), is(3));

        final WebhookEvent memberUpdated = webhook.getEvents().get(0);
        assertThat(memberUpdated.getName(), is("member_updated"));
        assertThat(memberUpdated.getChannel(), is("presence-ai-forward"));
        assertThat(memberUpdated.getUserId(), is("user-1"));
        assertThat(memberUpdated.getFieldAsString("future_field"), is("must-pass-through"));

        final WebhookEvent aiTurnStarted = webhook.getEvents().get(1);
        assertThat(aiTurnStarted.getName(), is("ai_turn_started"));
        assertThat(aiTurnStarted.getFieldAsString("turn_id"), is("turn-1"));

        final WebhookEvent messageVersionCreated = webhook.getEvents().get(2);
        assertThat(messageVersionCreated.getName(), is("message_version_created"));
        assertThat(messageVersionCreated.getMessageSerial(), is("msg-1"));
        assertThat(messageVersionCreated.getVersionSerial(), is("ver-1"));
    }

    @Test
    public void preservesNestedAiWebhookData() {
        final Webhook webhook = sockudo.parseWebhook("{"
                + "\"events\":[{"
                + "\"name\":\"ai_turn_ended\","
                + "\"channel\":\"private-ai\","
                + "\"data\":{"
                + "\"usage\":{\"input_tokens\":10,\"output_tokens\":20},"
                + "\"tool_calls\":[{\"name\":\"search\",\"ok\":true}],"
                + "\"cached\":false,"
                + "\"optional\":null"
                + "},"
                + "\"future\":{\"nested\":[1,2,3]}"
                + "}]}");

        final WebhookEvent event = webhook.getEvents().get(0);
        final JsonElement data = event.getData();
        assertThat(data.isJsonObject(), is(true));
        assertThat(data.getAsJsonObject().get("usage").getAsJsonObject().get("input_tokens").getAsInt(), is(10));
        assertThat(data.getAsJsonObject().get("tool_calls").getAsJsonArray().get(0).getAsJsonObject().get("ok").getAsBoolean(), is(true));
        assertThat(data.getAsJsonObject().get("cached").getAsBoolean(), is(false));
        assertThat(data.getAsJsonObject().get("optional").isJsonNull(), is(true));
        assertThat(event.get("future").getAsJsonObject().get("nested").getAsJsonArray().size(), is(3));
    }

    @Test
    public void preservesUnknownEventNamesAsGenericEvents() {
        final Webhook webhook = sockudo.parseWebhook("{"
                + "\"events\":[{"
                + "\"name\":\"future_event_name\","
                + "\"channel\":\"private-future\","
                + "\"delivery_serial\":9007199254740993,"
                + "\"data\":{\"enabled\":true}"
                + "}]}");

        final WebhookEvent event = webhook.getEvents().get(0);
        assertThat(event.getName(), is("future_event_name"));
        assertThat(event.getChannel(), is("private-future"));
        assertThat(event.getDeliverySerial(), is("9007199254740993"));
        assertThat(event.getData().getAsJsonObject().get("enabled").getAsBoolean(), is(true));
    }

    @Test
    public void keepsTypedAccessForExistingMemberEvents() {
        final Webhook webhook = sockudo.parseWebhook("{"
                + "\"events\":["
                + "{\"name\":\"member_added\",\"channel\":\"presence-room\",\"user_id\":\"user-a\"},"
                + "{\"name\":\"member_removed\",\"channel\":\"presence-room\",\"user_id\":\"user-b\",\"socket_id\":\"1.2\"}"
                + "]}");

        assertThat(webhook.getEvents().get(0).getName(), is("member_added"));
        assertThat(webhook.getEvents().get(0).getChannel(), is("presence-room"));
        assertThat(webhook.getEvents().get(0).getUserId(), is("user-a"));
        assertThat(webhook.getEvents().get(0).getSocketId(), nullValue());
        assertThat(webhook.getEvents().get(1).getName(), is("member_removed"));
        assertThat(webhook.getEvents().get(1).getUserId(), is("user-b"));
        assertThat(webhook.getEvents().get(1).getSocketId(), is("1.2"));
    }

    @Test
    public void validatesSignatureAgainstRawBodyBeforeParsing() {
        final String body = "{\"events\":[{\"name\":\"member_added\",\"channel\":\"presence-room\",\"user_id\":\"user-a\"}]}";
        final String signature = SignatureUtil.sign(body, SECRET);

        assertThat(sockudo.validateWebhookSignature(KEY, signature, body), is(Validity.VALID));
        assertThat(sockudo.parseWebhook(body).getEvents().get(0).getUserId(), is("user-a"));
    }

    @Test
    public void resultResponsesRemainRawAndIgnoreAdditiveFields() {
        final String response = "{\"channels\":{},\"serials\":{\"history_serial\":\"9007199254740993\"},\"ai_stats\":{\"turns\":1}}";
        final Result result = Result.fromHttpCode(200, response);

        assertThat(result.getStatus(), is(Result.Status.SUCCESS));
        assertThat(result.getMessage(), is(response));
    }

    private static String readForwardCompatFixture() throws IOException {
        final List<Path> candidates = Arrays.asList(
                Paths.get("..", "..", "tests", "ai-conformance", "fixtures", "forward-compat", "future-webhook-events.json"),
                Paths.get("tests", "ai-conformance", "fixtures", "forward-compat", "future-webhook-events.json")
        );
        for (Path candidate : candidates) {
            if (Files.exists(candidate)) {
                return new String(Files.readAllBytes(candidate), StandardCharsets.UTF_8);
            }
        }
        throw new IOException("future-webhook-events.json fixture not found from " + Paths.get("").toAbsolutePath());
    }
}
