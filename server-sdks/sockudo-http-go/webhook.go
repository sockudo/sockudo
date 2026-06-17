package sockudo

import (
	"encoding/json"
)

// Webhook is the parsed form of a valid webhook received by the server.
type Webhook struct {
	TimeMs int            `json:"time_ms"` // the timestamp of the request
	Events []WebhookEvent `json:"events"`  // the events associated with the webhook
}

// WebhookEvent is the parsed form of a valid webhook event received by the
// server.
type WebhookEvent struct {
	Name     string `json:"name"`                // the type of the event
	Channel  string `json:"channel"`             // the channel on which it was sent
	Event    string `json:"event,omitempty"`     // the name of the event
	Data     string `json:"data,omitempty"`      // the data associated with the event
	SocketID string `json:"socket_id,omitempty"` // the socket_id of the sending socket
	UserID   string `json:"user_id,omitempty"`   // the user_id of a member who has joined or vacated a presence-channel
}

func unmarshalledWebhook(requestBody []byte) (*Webhook, error) {
	webhook := &Webhook{}
	err := json.Unmarshal(requestBody, &webhook)
	if err != nil {
		return nil, err
	}
	return webhook, nil
}
