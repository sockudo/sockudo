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
	Name     string                     `json:"name"`                // the type of the event
	Channel  string                     `json:"channel"`             // the channel on which it was sent
	Event    string                     `json:"event,omitempty"`     // the name of the event
	Data     string                     `json:"data,omitempty"`      // the data associated with the event
	SocketID string                     `json:"socket_id,omitempty"` // the socket_id of the sending socket
	UserID   string                     `json:"user_id,omitempty"`   // the user_id of a member who has joined or vacated a presence-channel
	RawData  json.RawMessage            `json:"-"`                   // the original JSON value for data, if present
	Fields   map[string]json.RawMessage `json:"-"`                   // the original JSON object for forward-compatible event fields
}

// UnmarshalJSON preserves future webhook fields and nested payload values while
// keeping the legacy string Data projection populated.
func (event *WebhookEvent) UnmarshalJSON(payload []byte) error {
	type webhookEventAlias struct {
		Name     string          `json:"name"`
		Channel  string          `json:"channel"`
		Event    string          `json:"event,omitempty"`
		Data     json.RawMessage `json:"data,omitempty"`
		SocketID string          `json:"socket_id,omitempty"`
		UserID   string          `json:"user_id,omitempty"`
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return err
	}

	var decoded webhookEventAlias
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return err
	}

	event.Name = decoded.Name
	event.Channel = decoded.Channel
	event.Event = decoded.Event
	event.SocketID = decoded.SocketID
	event.UserID = decoded.UserID
	event.Fields = fields
	event.RawData = decoded.Data
	event.Data = ""

	if len(decoded.Data) > 0 && string(decoded.Data) != "null" {
		var dataString string
		if err := json.Unmarshal(decoded.Data, &dataString); err == nil {
			event.Data = dataString
		} else {
			event.Data = string(decoded.Data)
		}
	}

	return nil
}

func unmarshalledWebhook(requestBody []byte) (*Webhook, error) {
	webhook := &Webhook{}
	err := json.Unmarshal(requestBody, &webhook)
	if err != nil {
		return nil, err
	}
	return webhook, nil
}
