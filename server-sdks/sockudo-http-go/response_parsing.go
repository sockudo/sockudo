package sockudo

import (
	"encoding/json"
	"strconv"
)

// Channel represents the information about a channel from the Sockudo API.
type Channel struct {
	Name              string
	Occupied          bool `json:"occupied,omitempty"`
	UserCount         int  `json:"user_count,omitempty"`
	SubscriptionCount int  `json:"subscription_count,omitempty"`
}

// ChannelsList represents a list of channels received by the Sockudo API.
type ChannelsList struct {
	Channels map[string]ChannelListItem `json:"channels"`
}

// ChannelListItem represents an item within ChannelsList
type ChannelListItem struct {
	UserCount int `json:"user_count"`
}

type TriggerChannelsList struct {
	Channels map[string]TriggerChannelListItem `json:"channels"`
}

type TriggerChannelListItem struct {
	UserCount         *int `json:"user_count,omitempty"`
	SubscriptionCount *int `json:"subscription_count,omitempty"`
}

type TriggerBatchChannelsList struct {
	Batch []TriggerBatchChannelListItem `json:"batch"`
}

type TriggerBatchChannelListItem struct {
	UserCount         *int `json:"user_count,omitempty"`
	SubscriptionCount *int `json:"subscription_count,omitempty"`
}

// Users represents a list of users in a presence-channel
type Users struct {
	List []User `json:"users"`
}

type HistoryPage struct {
	Items      []HistoryItem     `json:"items"`
	Direction  string            `json:"direction"`
	Limit      int               `json:"limit"`
	HasMore    bool              `json:"has_more"`
	NextCursor *string           `json:"next_cursor"`
	Bounds     HistoryBounds     `json:"bounds"`
	Continuity HistoryContinuity `json:"continuity"`
}

type HistoryItem struct {
	StreamID        string                 `json:"stream_id"`
	Serial          int64                  `json:"serial"`
	PublishedAtMS   int64                  `json:"published_at_ms"`
	MessageID       *string                `json:"message_id"`
	EventName       *string                `json:"event_name"`
	OperationKind   string                 `json:"operation_kind"`
	PayloadSizeByte int                    `json:"payload_size_bytes"`
	Message         map[string]interface{} `json:"message"`
}

type HistoryBounds struct {
	StartSerial *int64 `json:"start_serial"`
	EndSerial   *int64 `json:"end_serial"`
	StartTimeMS *int64 `json:"start_time_ms"`
	EndTimeMS   *int64 `json:"end_time_ms"`
}

type HistoryContinuity struct {
	StreamID                   *string `json:"stream_id"`
	OldestAvailableSerial      *int64  `json:"oldest_available_serial"`
	NewestAvailableSerial      *int64  `json:"newest_available_serial"`
	OldestAvailablePublishedMS *int64  `json:"oldest_available_published_at_ms"`
	NewestAvailablePublishedMS *int64  `json:"newest_available_published_at_ms"`
	RetainedMessages           int64   `json:"retained_messages"`
	RetainedBytes              int64   `json:"retained_bytes"`
	Complete                   bool    `json:"complete"`
	TruncatedByRetention       bool    `json:"truncated_by_retention"`
}

// User represents a user and contains their ID.
type User struct {
	ID string `json:"id"`
}

/*
MemberData represents what to assign to a channel member, consisting of a
`UserID` and any custom `UserInfo`.
*/
type MemberData struct {
	UserID   string            `json:"user_id"`
	UserInfo map[string]string `json:"user_info,omitempty"`
}

func unmarshalledTriggerChannelsList(response []byte) (*TriggerChannelsList, error) {
	channels := &TriggerChannelsList{}
	err := json.Unmarshal(response, channels)

	if err != nil {
		return nil, err
	}

	return channels, nil
}

func unmarshalledTriggerBatchChannelsList(response []byte) (*TriggerBatchChannelsList, error) {
	channels := &TriggerBatchChannelsList{}
	err := json.Unmarshal(response, channels)

	if err != nil {
		return nil, err
	}

	return channels, nil
}

func unmarshalledChannelsList(response []byte) (*ChannelsList, error) {
	channels := &ChannelsList{}
	err := json.Unmarshal(response, channels)

	if err != nil {
		return nil, err
	}

	return channels, nil
}

func unmarshalledChannel(response []byte, name string) (*Channel, error) {
	channel := &Channel{Name: name}
	err := json.Unmarshal(response, channel)

	if err != nil {
		return nil, err
	}

	return channel, nil
}

func unmarshalledChannelUsers(response []byte) (*Users, error) {
	users := &Users{}
	err := json.Unmarshal(response, users)

	if err != nil {
		return nil, err
	}

	return users, nil
}

func unmarshalledHistory(response []byte) (*HistoryPage, error) {
	page := &HistoryPage{}
	err := json.Unmarshal(response, page)
	if err != nil {
		return nil, err
	}
	return page, nil
}

// PresenceHistoryPage represents a page of presence history events.
type PresenceHistoryPage struct {
	Items      []PresenceHistoryItem     `json:"items"`
	Direction  string                    `json:"direction"`
	Limit      int                       `json:"limit"`
	HasMore    bool                      `json:"has_more"`
	NextCursor *string                   `json:"next_cursor"`
	Bounds     HistoryBounds             `json:"bounds"`
	Continuity PresenceHistoryContinuity `json:"continuity"`
}

// PresenceHistoryItem represents a single presence transition event.
type PresenceHistoryItem struct {
	StreamID         string                 `json:"stream_id"`
	Serial           int64                  `json:"serial"`
	PublishedAtMS    int64                  `json:"published_at_ms"`
	Event            string                 `json:"event"`
	Cause            string                 `json:"cause"`
	UserID           string                 `json:"user_id"`
	ConnectionID     *string                `json:"connection_id"`
	DeadNodeID       *string                `json:"dead_node_id"`
	PayloadSizeBytes int                    `json:"payload_size_bytes"`
	PresenceEvent    map[string]interface{} `json:"presence_event"`
}

// PresenceHistoryContinuity contains retention and continuity metadata for presence history.
type PresenceHistoryContinuity struct {
	StreamID                   *string `json:"stream_id"`
	OldestAvailableSerial      *int64  `json:"oldest_available_serial"`
	NewestAvailableSerial      *int64  `json:"newest_available_serial"`
	OldestAvailablePublishedMS *int64  `json:"oldest_available_published_at_ms"`
	NewestAvailablePublishedMS *int64  `json:"newest_available_published_at_ms"`
	RetainedEvents             int64   `json:"retained_events"`
	RetainedBytes              int64   `json:"retained_bytes"`
	Degraded                   bool    `json:"degraded"`
	Complete                   bool    `json:"complete"`
	TruncatedByRetention       bool    `json:"truncated_by_retention"`
}

// PresenceSnapshotMember represents a member in a reconstructed presence snapshot.
type PresenceSnapshotMember struct {
	UserID          string `json:"user_id"`
	LastEvent       string `json:"last_event"`
	LastEventSerial int64  `json:"last_event_serial"`
	LastEventAtMS   int64  `json:"last_event_at_ms"`
}

// PresenceSnapshot represents reconstructed presence membership at a point in time.
type PresenceSnapshot struct {
	Channel        string                    `json:"channel"`
	Members        []PresenceSnapshotMember  `json:"members"`
	MemberCount    int                       `json:"member_count"`
	EventsReplayed int64                     `json:"events_replayed"`
	SnapshotSerial *int64                    `json:"snapshot_serial"`
	SnapshotTimeMS *int64                    `json:"snapshot_time_ms"`
	Continuity     PresenceHistoryContinuity `json:"continuity"`
}

func unmarshalledPresenceHistory(response []byte) (*PresenceHistoryPage, error) {
	page := &PresenceHistoryPage{}
	err := json.Unmarshal(response, page)
	if err != nil {
		return nil, err
	}
	return page, nil
}

func unmarshalledPresenceSnapshot(response []byte) (*PresenceSnapshot, error) {
	snapshot := &PresenceSnapshot{}
	err := json.Unmarshal(response, snapshot)
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

type MessageVersionsParams struct {
	Limit     *int
	Direction *string
	Cursor    *string
}

func (params MessageVersionsParams) toMap() map[string]string {
	m := make(map[string]string)
	if params.Limit != nil {
		m["limit"] = strconv.Itoa(*params.Limit)
	}
	if params.Direction != nil {
		m["direction"] = *params.Direction
	}
	if params.Cursor != nil {
		m["cursor"] = *params.Cursor
	}
	return m
}

type AnnotationEventsParams struct {
	Type       *string
	FromSerial *string
	Limit      *int
	SocketID   *string
}

func (params AnnotationEventsParams) toMap() map[string]string {
	m := make(map[string]string)
	if params.Type != nil {
		m["type"] = *params.Type
	}
	if params.FromSerial != nil {
		m["from_serial"] = *params.FromSerial
	}
	if params.Limit != nil {
		m["limit"] = strconv.Itoa(*params.Limit)
	}
	if params.SocketID != nil {
		m["socket_id"] = *params.SocketID
	}
	return m
}

type PublishAnnotationRequest struct {
	Type     string      `json:"type"`
	Name     *string     `json:"name,omitempty"`
	ClientID *string     `json:"clientId,omitempty"`
	SocketID *string     `json:"socketId,omitempty"`
	Count    *uint64     `json:"count,omitempty"`
	Data     interface{} `json:"data,omitempty"`
	Encoding *string     `json:"encoding,omitempty"`
}

type PublishAnnotationResponse struct {
	AnnotationSerial string `json:"annotationSerial"`
}

type DeleteAnnotationResponse struct {
	AnnotationSerial        string `json:"annotationSerial"`
	DeletedAnnotationSerial string `json:"deletedAnnotationSerial"`
}

type AnnotationEvent struct {
	Action        string      `json:"action"`
	ID            *string     `json:"id,omitempty"`
	Serial        string      `json:"serial"`
	MessageSerial string      `json:"messageSerial"`
	Type          string      `json:"type"`
	Name          *string     `json:"name,omitempty"`
	ClientID      *string     `json:"clientId,omitempty"`
	Count         *uint64     `json:"count,omitempty"`
	Data          interface{} `json:"data,omitempty"`
	Encoding      *string     `json:"encoding,omitempty"`
	Timestamp     *int64      `json:"timestamp,omitempty"`
}

type AnnotationEventsResponse struct {
	Channel       string            `json:"channel"`
	MessageSerial string            `json:"messageSerial"`
	Limit         int               `json:"limit"`
	HasMore       bool              `json:"hasMore"`
	NextCursor    *string           `json:"nextCursor"`
	Items         []AnnotationEvent `json:"items"`
}

type MutationResponse struct {
	Channel       string  `json:"channel"`
	MessageSerial string  `json:"message_serial"`
	Action        string  `json:"action"`
	Accepted      bool    `json:"accepted"`
	VersionSerial *string `json:"version_serial"`
	Status        string  `json:"status"`
}

type GetMessageResponse struct {
	Channel string                 `json:"channel"`
	Item    map[string]interface{} `json:"item"`
}

type MessageVersionsResponse struct {
	Channel    string                   `json:"channel"`
	Direction  string                   `json:"direction"`
	Limit      int                      `json:"limit"`
	HasMore    bool                     `json:"has_more"`
	NextCursor *string                  `json:"next_cursor"`
	Items      []map[string]interface{} `json:"items"`
}

func unmarshalledMessage(response []byte) (*GetMessageResponse, error) {
	page := &GetMessageResponse{}
	err := json.Unmarshal(response, page)
	if err != nil {
		return nil, err
	}
	return page, nil
}

func unmarshalledMessageVersions(response []byte) (*MessageVersionsResponse, error) {
	page := &MessageVersionsResponse{}
	err := json.Unmarshal(response, page)
	if err != nil {
		return nil, err
	}
	return page, nil
}

func unmarshalledMutationResponse(response []byte) (*MutationResponse, error) {
	payload := &MutationResponse{}
	err := json.Unmarshal(response, payload)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func unmarshalledPublishAnnotationResponse(response []byte) (*PublishAnnotationResponse, error) {
	payload := &PublishAnnotationResponse{}
	err := json.Unmarshal(response, payload)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func unmarshalledDeleteAnnotationResponse(response []byte) (*DeleteAnnotationResponse, error) {
	payload := &DeleteAnnotationResponse{}
	err := json.Unmarshal(response, payload)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func unmarshalledAnnotationEvents(response []byte) (*AnnotationEventsResponse, error) {
	payload := &AnnotationEventsResponse{}
	err := json.Unmarshal(response, payload)
	if err != nil {
		return nil, err
	}
	return payload, nil
}
