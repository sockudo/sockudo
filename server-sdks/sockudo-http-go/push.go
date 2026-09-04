package sockudo

import (
	"encoding/json"
	"fmt"
	"net/url"
)

type ApnsChannelStoragePolicy string

const (
	ApnsChannelNoStorage  ApnsChannelStoragePolicy = "noStorage"
	ApnsChannelMostRecent ApnsChannelStoragePolicy = "mostRecent"
)

type ApnsLiveActivityEvent string

const (
	ApnsLiveActivityStart  ApnsLiveActivityEvent = "start"
	ApnsLiveActivityUpdate ApnsLiveActivityEvent = "update"
	ApnsLiveActivityEnd    ApnsLiveActivityEvent = "end"
)

type ApnsLiveActivityPriority string

const (
	ApnsLiveActivityLowPower      ApnsLiveActivityPriority = "lowPower"
	ApnsLiveActivityConservePower ApnsLiveActivityPriority = "conservePower"
	ApnsLiveActivityImmediate     ApnsLiveActivityPriority = "immediate"
)

// ApnsLiveActivityPayload is rendered by Sockudo into Apple's ActivityKit
// wire format. All date fields are UNIX timestamps in seconds.
type ApnsLiveActivityPayload struct {
	Event            ApnsLiveActivityEvent    `json:"event"`
	Timestamp        uint64                   `json:"timestamp"`
	ContentState     map[string]interface{}   `json:"contentState"`
	AttributesType   string                   `json:"attributesType,omitempty"`
	Attributes       map[string]interface{}   `json:"attributes,omitempty"`
	Alert            map[string]interface{}   `json:"alert,omitempty"`
	StaleDate        *uint64                  `json:"staleDate,omitempty"`
	DismissalDate    *uint64                  `json:"dismissalDate,omitempty"`
	RelevanceScore   *float64                 `json:"relevanceScore,omitempty"`
	InputPushToken   bool                     `json:"inputPushToken,omitempty"`
	InputPushChannel string                   `json:"inputPushChannel,omitempty"`
	Priority         ApnsLiveActivityPriority `json:"priority,omitempty"`
}

type ApnsLiveActivityRecipient struct {
	TransportType string `json:"transportType"`
	ActivityToken string `json:"activityToken"`
}

func NewApnsLiveActivityRecipient(activityToken string) ApnsLiveActivityRecipient {
	return ApnsLiveActivityRecipient{
		TransportType: "apnsLiveActivity",
		ActivityToken: activityToken,
	}
}

type ApnsLiveActivityBroadcastRecipient struct {
	TransportType string                   `json:"transportType"`
	ChannelID     string                   `json:"channelId"`
	StoragePolicy ApnsChannelStoragePolicy `json:"storagePolicy"`
}

func NewApnsLiveActivityBroadcastRecipient(channelID string, storagePolicy ApnsChannelStoragePolicy) ApnsLiveActivityBroadcastRecipient {
	return ApnsLiveActivityBroadcastRecipient{
		TransportType: "apnsLiveActivityBroadcast",
		ChannelID:     channelID,
		StoragePolicy: storagePolicy,
	}
}

type ApnsBroadcastChannel struct {
	ChannelID     string                   `json:"channelId"`
	StoragePolicy ApnsChannelStoragePolicy `json:"storagePolicy"`
}

type ApnsBroadcastChannelList struct {
	Channels []string `json:"channels"`
}

type PushCursorParams struct {
	Limit  *int
	Cursor *string
}

func (params PushCursorParams) toMap() map[string]string {
	m := make(map[string]string)
	if params.Limit != nil {
		m["limit"] = fmt.Sprintf("%d", *params.Limit)
	}
	if params.Cursor != nil {
		m["cursor"] = *params.Cursor
	}
	return m
}

type PushSubscriptionParams struct {
	Limit    *int
	Cursor   *string
	Channel  *string
	DeviceID *string
}

func (params PushSubscriptionParams) toMap() map[string]string {
	m := PushCursorParams{Limit: params.Limit, Cursor: params.Cursor}.toMap()
	if params.Channel != nil {
		m["channel"] = *params.Channel
	}
	if params.DeviceID != nil {
		m["deviceId"] = *params.DeviceID
	}
	return m
}

type PushRecipient map[string]interface{}
type PushDeviceDetails map[string]interface{}
type PushChannelSubscription map[string]interface{}
type PushPublishRequest map[string]interface{}
type PushDeliveryStatusEvent map[string]interface{}

func pushHeaders(capability string, deviceIdentityToken string) map[string]string {
	if capability == "" {
		capability = "push-admin"
	}
	headers := map[string]string{"X-Sockudo-Push-Capability": capability}
	if deviceIdentityToken != "" {
		headers["X-Sockudo-Device-Identity-Token"] = deviceIdentityToken
	}
	return headers
}

func (c *Client) pushRequest(method, path string, body interface{}, params map[string]string, extraHeaders map[string]string) ([]byte, error) {
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}

	requestURL, err := createRequestURL(method, c.Host, fmt.Sprintf("/apps/%s/push%s", c.AppID, path), c.Key, c.Secret, authTimestamp(), c.Secure, payload, params, c.Cluster)
	if err != nil {
		return nil, err
	}
	return c.requestWithExtraHeaders(method, requestURL, payload, extraHeaders)
}

func (c *Client) ActivateDevice(device PushDeviceDetails) ([]byte, error) {
	return c.pushRequest("POST", "/deviceRegistrations", device, nil, pushHeaders("push-admin", ""))
}

func (c *Client) CreateDeviceActivation(device PushDeviceDetails) ([]byte, error) {
	return c.ActivateDevice(device)
}

func (c *Client) UpdateDeviceRegistration(device PushDeviceDetails, deviceIdentityToken string) ([]byte, error) {
	return c.pushRequest("POST", "/deviceRegistrations", device, nil, pushHeaders("push-subscribe", deviceIdentityToken))
}

func (c *Client) ListDeviceRegistrations(params PushCursorParams) ([]byte, error) {
	return c.pushRequest("GET", "/deviceRegistrations", nil, params.toMap(), pushHeaders("push-admin", ""))
}

func (c *Client) GetDeviceRegistration(deviceID string, deviceIdentityToken string) ([]byte, error) {
	capability := "push-admin"
	if deviceIdentityToken != "" {
		capability = "push-subscribe"
	}
	return c.pushRequest("GET", "/deviceRegistrations/"+deviceID, nil, nil, pushHeaders(capability, deviceIdentityToken))
}

func (c *Client) DeleteDeviceRegistration(deviceID string, deviceIdentityToken string) ([]byte, error) {
	capability := "push-admin"
	if deviceIdentityToken != "" {
		capability = "push-subscribe"
	}
	return c.pushRequest("DELETE", "/deviceRegistrations/"+deviceID, nil, nil, pushHeaders(capability, deviceIdentityToken))
}

func (c *Client) RemoveDeviceRegistrationsByClient(clientID string) ([]byte, error) {
	return c.pushRequest("DELETE", "/deviceRegistrations", nil, map[string]string{"clientId": clientID}, pushHeaders("push-admin", ""))
}

func (c *Client) UpsertChannelPushSubscription(subscription PushChannelSubscription, deviceIdentityToken string) ([]byte, error) {
	capability := "push-admin"
	if deviceIdentityToken != "" {
		capability = "push-subscribe"
	}
	return c.pushRequest("POST", "/channelSubscriptions", subscription, nil, pushHeaders(capability, deviceIdentityToken))
}

func (c *Client) ListChannelPushSubscriptions(params PushSubscriptionParams, deviceIdentityToken string) ([]byte, error) {
	capability := "push-admin"
	if deviceIdentityToken != "" {
		capability = "push-subscribe"
	}
	return c.pushRequest("GET", "/channelSubscriptions", nil, params.toMap(), pushHeaders(capability, deviceIdentityToken))
}

func (c *Client) DeleteChannelPushSubscriptions(params PushSubscriptionParams, deviceIdentityToken string) ([]byte, error) {
	capability := "push-admin"
	if deviceIdentityToken != "" {
		capability = "push-subscribe"
	}
	return c.pushRequest("DELETE", "/channelSubscriptions", nil, params.toMap(), pushHeaders(capability, deviceIdentityToken))
}

func (c *Client) ListChannelPushSubscriptionChannels(params PushCursorParams) ([]byte, error) {
	return c.pushRequest("GET", "/channelSubscriptions/channels", nil, params.toMap(), pushHeaders("push-admin", ""))
}

func (c *Client) ListPushCredentials(params PushCursorParams) ([]byte, error) {
	return c.pushRequest("GET", "/credentials", nil, params.toMap(), pushHeaders("push-admin", ""))
}

func (c *Client) PutPushCredential(provider string, credential map[string]interface{}) ([]byte, error) {
	return c.pushRequest("POST", "/credentials/"+provider, credential, nil, pushHeaders("push-admin", ""))
}

func (c *Client) PublishPush(request PushPublishRequest) ([]byte, error) {
	request["sync"] = false
	return c.pushRequest("POST", "/publish", request, nil, pushHeaders("push-admin", ""))
}

// CreateApnsLiveActivityChannel creates an APNs broadcast channel through Sockudo.
func (c *Client) CreateApnsLiveActivityChannel(storagePolicy ApnsChannelStoragePolicy) ([]byte, error) {
	return c.pushRequest(
		"POST",
		"/liveActivities/channels",
		map[string]interface{}{"storagePolicy": storagePolicy},
		nil,
		pushHeaders("push-admin", ""),
	)
}

func (c *Client) GetApnsLiveActivityChannel(channelID string) ([]byte, error) {
	return c.pushRequest("GET", "/liveActivities/channels/"+url.PathEscape(channelID), nil, nil, pushHeaders("push-admin", ""))
}

func (c *Client) ListApnsLiveActivityChannels() ([]byte, error) {
	return c.pushRequest("GET", "/liveActivities/channels", nil, nil, pushHeaders("push-admin", ""))
}

func (c *Client) DeleteApnsLiveActivityChannel(channelID string) ([]byte, error) {
	return c.pushRequest("DELETE", "/liveActivities/channels/"+url.PathEscape(channelID), nil, nil, pushHeaders("push-admin", ""))
}

func (c *Client) PublishPushDirect(request PushPublishRequest) ([]byte, error) {
	return c.PublishPush(request)
}

func (c *Client) PublishPushBatch(requests []PushPublishRequest) ([]byte, error) {
	for i := range requests {
		requests[i]["sync"] = false
	}
	return c.pushRequest("POST", "/batch/publish", requests, nil, pushHeaders("push-admin", ""))
}

func (c *Client) SchedulePush(request PushPublishRequest) ([]byte, error) {
	if _, ok := request["notBeforeMs"]; !ok {
		return nil, fmt.Errorf("scheduled push requires notBeforeMs")
	}
	return c.PublishPush(request)
}

func (c *Client) GetPublishStatus(publishID string) ([]byte, error) {
	return c.pushRequest("GET", "/publish/"+publishID+"/status", nil, nil, pushHeaders("push-admin", ""))
}

func (c *Client) CancelScheduledPush(publishID string) ([]byte, error) {
	return c.pushRequest("DELETE", "/scheduled/"+publishID, nil, nil, pushHeaders("push-admin", ""))
}

func (c *Client) PostPushDeliveryStatus(event PushDeliveryStatusEvent) ([]byte, error) {
	return c.pushRequest("POST", "/deliveryStatus", event, nil, pushHeaders("push-admin", ""))
}
