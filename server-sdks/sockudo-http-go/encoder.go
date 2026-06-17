package sockudo

import (
	"encoding/json"
	"fmt"
)

// defaultMaxEventPayloadSizeKB indicates the max size allowed for the data content
// (payload) of each event, unless an override is present in the client
const defaultMaxEventPayloadSizeKB = 10

type batchEvent struct {
	Channel        string         `json:"channel"`
	Name           string         `json:"name"`
	Data           string         `json:"data"`
	SocketID       *string        `json:"socket_id,omitempty"`
	Info           *string        `json:"info,omitempty"`
	IdempotencyKey *string        `json:"idempotency_key,omitempty"`
	Extras         *MessageExtras `json:"extras,omitempty"`
}
type batchPayload struct {
	Batch []batchEvent `json:"batch"`
}

func encodeTriggerBody(
	channels []string,
	event string,
	data interface{},
	params map[string]string,
	encryptionKey []byte,
	overrideMaxMessagePayloadKB int,
	extras *MessageExtras,
) ([]byte, error) {
	dataBytes, err := encodeEventData(data)
	if err != nil {
		return nil, err
	}
	var payloadData string
	if isEncryptedChannel(channels[0]) {
		payloadData = encrypt(channels[0], dataBytes, encryptionKey)
	} else {
		payloadData = string(dataBytes)
	}

	eventExceedsMaximumSize := false
	if overrideMaxMessagePayloadKB == 0 {
		eventExceedsMaximumSize = len(payloadData) > defaultMaxEventPayloadSizeKB*1024
	} else {
		eventExceedsMaximumSize = len(payloadData) > overrideMaxMessagePayloadKB*1024
	}
	if eventExceedsMaximumSize {
		return nil, fmt.Errorf("Event payload exceeded maximum size (%d bytes is too much)", len(payloadData))
	}
	eventPayload := map[string]interface{}{
		"name":     event,
		"channels": channels,
		"data":     payloadData,
	}
	for k, v := range params {
		if _, ok := eventPayload[k]; ok {
			return nil, fmt.Errorf("Paramater %s specified multiple times", k)
		}
		eventPayload[k] = v
	}
	if extras != nil {
		eventPayload["extras"] = extras
	}
	return json.Marshal(eventPayload)
}

func encodeTriggerBatchBody(
	batch []Event,
	encryptionKey []byte,
	overrideMaxMessagePayloadKB int,
) ([]byte, error) {
	batchEvents := make([]batchEvent, len(batch))
	for idx, e := range batch {
		var stringifyedDataBytes string
		dataBytes, err := encodeEventData(e.Data)
		if err != nil {
			return nil, err
		}
		if isEncryptedChannel(e.Channel) {
			stringifyedDataBytes = encrypt(e.Channel, dataBytes, encryptionKey)
		} else {
			stringifyedDataBytes = string(dataBytes)
		}
		eventExceedsMaximumSize := false
		if overrideMaxMessagePayloadKB == 0 {
			eventExceedsMaximumSize = len(stringifyedDataBytes) > defaultMaxEventPayloadSizeKB*1024
		} else {
			eventExceedsMaximumSize = len(stringifyedDataBytes) > overrideMaxMessagePayloadKB*1024
		}
		if eventExceedsMaximumSize {
			return nil, fmt.Errorf("Data of the event #%d in batch, exceeded maximum size (%d bytes is too much)", idx, len(stringifyedDataBytes))
		}
		newBatchEvent := batchEvent{
			Channel:        e.Channel,
			Name:           e.Name,
			Data:           stringifyedDataBytes,
			SocketID:       e.SocketID,
			Info:           e.Info,
			IdempotencyKey: e.IdempotencyKey,
			Extras:         e.Extras,
		}
		batchEvents[idx] = newBatchEvent
	}
	return json.Marshal(&batchPayload{batchEvents})
}

func encodeEventData(data interface{}) ([]byte, error) {
	var dataBytes []byte
	var err error

	switch d := data.(type) {
	case []byte:
		dataBytes = d
	case string:
		dataBytes = []byte(d)
	default:
		dataBytes, err = json.Marshal(data)
		if err != nil {
			return nil, err
		}
	}
	return dataBytes, nil
}
