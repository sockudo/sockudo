package sockudo

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/stretchr/testify.v1/assert"
)

func setUpClient() Client {
	return Client{AppID: "id", Key: "key", Secret: "secret"}
}

func TestClientWebhookValidation(t *testing.T) {
	client := setUpClient()
	header := make(http.Header)
	header["X-Pusher-Key"] = []string{"key"}
	header["X-Pusher-Signature"] = []string{"2677ad3e7c090b2fa2c0fb13020d66d5420879b8316eb356a2d60fb9073bc778"}
	body := []byte(`{"hello":"world"}`)
	webhook, err := client.Webhook(header, body)
	assert.NotNil(t, webhook)
	assert.Nil(t, err)
}

func TestWebhookImproperKeyCase(t *testing.T) {
	client := setUpClient()
	badHeader := make(http.Header)
	badHeader["X-Pusher-Key"] = []string{"narr you're going down!"}
	badHeader["X-Pusher-Signature"] = []string{"2677ad3e7c090b2fa2c0fb13020d66d5420879b8316eb356a2d60fb9073bc778"}
	badBody := []byte(`{"hello":"world"}`)

	badWebhook, err := client.Webhook(badHeader, badBody)
	assert.Nil(t, badWebhook)
	assert.Error(t, err)
}

func TestWebhookImproperSignatureCase(t *testing.T) {
	client := setUpClient()
	badHeader := make(http.Header)
	badHeader["X-Pusher-Key"] = []string{"key"}
	badHeader["X-Pusher-Signature"] = []string{"2677ad3e7c090i'mgonnagetyaeb356a2d60fb9073bc778"}
	badBody := []byte(`{"hello":"world"}`)

	badWebhook, err := client.Webhook(badHeader, badBody)
	assert.Nil(t, badWebhook)
	assert.Error(t, err)
}

func TestWebhookNoSignature(t *testing.T) {
	client := setUpClient()
	badHeader := make(http.Header)
	badHeader["X-Pusher-Key"] = []string{"key"}
	badBody := []byte(`{"hello":"world"}`)

	badWebhook, err := client.Webhook(badHeader, badBody)
	assert.Nil(t, badWebhook)
	assert.Error(t, err)
}

func TestWebhookUnmarshalling(t *testing.T) {
	body := []byte(`{"time_ms":1427233518933,"events":[{"name":"client_event","channel":"private-channel","event":"client-yolo","data":"{\"yolo\":\"woot\"}","socket_id":"44610.7511910"}]}`)
	result, err := unmarshalledWebhook(body)
	assert.NoError(t, err)
	assert.Equal(t, 1427233518933, result.TimeMs)
	assert.Equal(t, 1, len(result.Events))
	assert.Equal(t, "client_event", result.Events[0].Name)
	assert.Equal(t, "private-channel", result.Events[0].Channel)
	assert.Equal(t, "client-yolo", result.Events[0].Event)
	assert.Equal(t, "{\"yolo\":\"woot\"}", result.Events[0].Data)
	assert.Equal(t, "44610.7511910", result.Events[0].SocketID)
	assert.Equal(t, []byte(`"{\"yolo\":\"woot\"}"`), []byte(result.Events[0].RawData))
}

func TestWebhookForwardCompatFixture(t *testing.T) {
	body, readErr := os.ReadFile(filepath.Join("..", "..", "tests", "ai-conformance", "fixtures", "forward-compat", "future-webhook-events.json"))
	assert.NoError(t, readErr)

	result, err := unmarshalledWebhook(body)

	assert.NoError(t, err)
	assert.Equal(t, 1710000000000, result.TimeMs)
	assert.Equal(t, 3, len(result.Events))
	assert.Equal(t, "member_updated", result.Events[0].Name)
	assert.Equal(t, "presence-ai-forward", result.Events[0].Channel)
	assert.Equal(t, "user-1", result.Events[0].UserID)
	assert.Equal(t, []byte(`"must-pass-through"`), []byte(result.Events[0].Fields["future_field"]))
	assert.Equal(t, "ai_turn_started", result.Events[1].Name)
	assert.Equal(t, []byte(`"turn-1"`), []byte(result.Events[1].Fields["turn_id"]))
	assert.Equal(t, "message_version_created", result.Events[2].Name)
}

func TestWebhookPreservesNestedFutureEventValues(t *testing.T) {
	body := []byte(`{"time_ms":1710000000000,"events":[{"name":"ai_turn_started","channel":"private-ai-forward","data":{"turn_id":"turn-1","tokens":["hello","world"],"done":false,"nullable":null},"future_field":{"nested":true}}]}`)
	result, err := unmarshalledWebhook(body)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(result.Events))
	assert.Equal(t, "ai_turn_started", result.Events[0].Name)
	assert.Equal(t, `{"turn_id":"turn-1","tokens":["hello","world"],"done":false,"nullable":null}`, result.Events[0].Data)
	assert.Equal(t, []byte(`{"turn_id":"turn-1","tokens":["hello","world"],"done":false,"nullable":null}`), []byte(result.Events[0].RawData))
	assert.Equal(t, []byte(`{"nested":true}`), []byte(result.Events[0].Fields["future_field"]))
}
