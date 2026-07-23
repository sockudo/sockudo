package sockudo

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

var sockudoPathRegex = regexp.MustCompile("^/apps/([0-9]+)$")
var maxTriggerableChannels = 100

func generateBaseID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func (c *Client) ensureBaseID() {
	if c.idempotencyBaseID == "" {
		c.idempotencyBaseID = generateBaseID()
	}
}

const maxRetries = 3

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	if strings.HasPrefix(errMsg, "Status Code: 5") {
		return true
	}
	if strings.HasPrefix(errMsg, "Status Code: ") {
		return false
	}
	return true
}

func (c *Client) autoIdempotencyEnabled() bool {
	if c.AutoIdempotencyKey == nil {
		return true
	}
	return *c.AutoIdempotencyKey
}

const (
	libraryVersion = "2.1.0"
	libraryName    = "sockudo-http-go"
)

/*
Client to the HTTP API of Sockudo.

There easiest way to configure the library is by creating a `Sockudo` instance:

	client := sockudo.Client{
		AppID: "your_app_id",
		Key: "your_app_key",
		Secret: "your_app_secret",
	}

To ensure requests occur over HTTPS, set the `Secure` property of a
`sockudo.Client` to `true`.

	client.Secure = true // false by default

If you wish to set a time-limit for each HTTP request, set the `Timeout`
property to an instance of `time.Duration`, for example:

	client.Timeout = time.Second * 3 // 5 seconds by default

Changing the `sockudo.Client`'s `Host` property will make sure requests are sent
to your specified host.

	client.Host = "foo.bar.com" // by default this is "localhost".
*/
type Client struct {
	AppID                        string
	Key                          string
	Secret                       string
	Host                         string // host or host:port pair
	Secure                       bool   // true for HTTPS
	Cluster                      string
	HTTPClient                   *http.Client
	EncryptionMasterKey          string  // deprecated
	EncryptionMasterKeyBase64    string  // for E2E
	OverrideMaxMessagePayloadKB  int     // set the agreed Sockudo message limit increase
	AutoIdempotencyKey           *bool   // when true (default), auto-generate idempotency keys on trigger
	validatedEncryptionMasterKey *[]byte // parsed key for use
	idempotencyBaseID            string
	publishSerial                uint64
}

/*
ClientFromURL allows client instantiation from a specially-crafted Sockudo URL.

	c := sockudo.ClientFromURL("http://key:secret@localhost/apps/app_id")
*/
func ClientFromURL(serverURL string) (*Client, error) {
	url2, err := url.Parse(serverURL)
	if err != nil {
		return nil, err
	}

	c := Client{
		Host: url2.Host,
	}

	matches := sockudoPathRegex.FindStringSubmatch(url2.Path)
	if len(matches) == 0 {
		return nil, errors.New("No app ID found")
	}
	c.AppID = matches[1]

	if url2.User == nil {
		return nil, errors.New("Missing <key>:<secret>")
	}
	c.Key = url2.User.Username()
	var isSet bool
	c.Secret, isSet = url2.User.Password()
	if !isSet {
		return nil, errors.New("Missing <secret>")
	}

	if url2.Scheme == "https" {
		c.Secure = true
	}

	return &c, nil
}

/*
ClientFromEnv allows instantiation of a client from an environment variable.
This is particularly relevant if you are using Sockudo as a Heroku add-on,
which stores credentials in a `"PUSHER_URL"` environment variable. For example:

	client := sockudo.ClientFromEnv("PUSHER_URL")
*/
func ClientFromEnv(key string) (*Client, error) {
	url := os.Getenv(key)
	return ClientFromURL(url)
}

/*
Returns the underlying HTTP client.
Useful to set custom properties to it.
*/
func (c *Client) requestClient() *http.Client {
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: time.Second * 5}
	}

	return c.HTTPClient
}

func (c *Client) request(method, url string, body []byte) ([]byte, error) {
	return request(c.requestClient(), method, url, body)
}

func (c *Client) requestWithExtraHeaders(method, url string, body []byte, extraHeaders map[string]string) ([]byte, error) {
	return requestWithHeaders(c.requestClient(), method, url, body, extraHeaders)
}

/*
Trigger triggers an event to the Sockudo API.
It is possible to trigger an event on one or more channels. Channel names can
contain only characters which are alphanumeric, `_` or `-“ and have
to be at most 200 characters long. Event name can be at most 200 characters long too.

Pass in the channel's name, the event's name, and a data payload. The data payload must
be marshallable into JSON.

	data := map[string]string{"hello": "world"}
	client.Trigger("greeting_channel", "say_hello", data)
*/
func (c *Client) Trigger(channel string, eventName string, data interface{}) error {
	_, err := c.validateChannelsAndTrigger([]string{channel}, eventName, data, TriggerParams{})
	return err
}

/*
ChannelsParams are any parameters than can be sent with a
TriggerWithParams or TriggerMultiWithParams requests.
*/
type TriggerParams struct {
	// SocketID excludes a recipient whose connection has the `socket_id`
	// specified here. You can read more here:
	// http://sockudo.com/docs/duplicates.
	SocketID *string
	// Info is comma-separated vales of `"user_count"`, for
	// presence-channels, and `"subscription_count"`, for all-channels.
	// Note that the subscription count is not allowed by default. Please
	// contact us at http://support.sockudo.com if you wish to enable this.
	// Pass in `nil` if you do not wish to specify any query attributes.
	// This is part of an [experimental feature](https://sockudo.com/docs/lab#experimental-program).
	Info *string
	// IdempotencyKey allows you to ensure that a trigger request is only
	// processed once. When set, the key is included in the JSON body and
	// sent as an X-Idempotency-Key HTTP header.
	IdempotencyKey *string
	// Extras contains V2 extras for publish events.
	Extras *MessageExtras
}

func (params TriggerParams) toMap() map[string]string {
	m := make(map[string]string)
	if params.SocketID != nil {
		m["socket_id"] = *params.SocketID
	}
	if params.Info != nil {
		m["info"] = *params.Info
	}
	if params.IdempotencyKey != nil {
		m["idempotency_key"] = *params.IdempotencyKey
	}
	return m
}

func (params TriggerParams) extraHeaders() map[string]string {
	if params.IdempotencyKey != nil {
		return map[string]string{
			"X-Idempotency-Key": *params.IdempotencyKey,
		}
	}
	return nil
}

/*
TriggerWithParams is the same as `client.Trigger`, except it allows additional
parameters to be passed in. See:
https://sockudo.com/docs/channels/library_auth_reference/rest-api#request
for a complete list.

	data := map[string]string{"hello": "world"}
	socketID := "1234.12"
	attributes := "user_count"
	params := sockudo.TriggerParams{SocketID: &socketID, Info: &attributes}
	channels, err := client.Trigger("greeting_channel", "say_hello", data, params)

	//channels=> &{Channels:map[presence-chatroom:{UserCount:4} presence-notifications:{UserCount:31}]}
*/
func (c *Client) TriggerWithParams(
	channel string,
	eventName string,
	data interface{},
	params TriggerParams,
) (*TriggerChannelsList, error) {
	return c.validateChannelsAndTrigger([]string{channel}, eventName, data, params)
}

/*
TriggerMulti is the same as `client.Trigger`, except one passes in a slice of
`channels` as the first parameter. The maximum length of channels is 100.

	client.TriggerMulti([]string{"a_channel", "another_channel"}, "event", data)
*/
func (c *Client) TriggerMulti(channels []string, eventName string, data interface{}) error {
	_, err := c.validateChannelsAndTrigger(channels, eventName, data, TriggerParams{})
	return err
}

/*
TriggerMultiWithParams is the same as `client.TriggerMulti`, except it
allows additional parameters to be specified in the same way as
`client.TriggerWithParams`.
*/
func (c *Client) TriggerMultiWithParams(
	channels []string,
	eventName string,
	data interface{},
	params TriggerParams,
) (*TriggerChannelsList, error) {
	return c.validateChannelsAndTrigger(channels, eventName, data, params)
}

/*
TriggerExclusive triggers an event excluding a recipient whose connection has
the `socket_id` you specify here from receiving the event.
You can read more here: http://sockudo.com/docs/duplicates.

	client.TriggerExclusive("a_channel", "event", data, "123.12")

Deprecated: use TriggerWithParams instead.
*/
func (c *Client) TriggerExclusive(channel string, eventName string, data interface{}, socketID string) error {
	params := TriggerParams{SocketID: &socketID}
	_, err := c.validateChannelsAndTrigger([]string{channel}, eventName, data, params)
	return err
}

/*
TriggerMultiExclusive triggers an event to multiple channels excluding a
recipient whose connection has the `socket_id` you specify here from receiving
the event on any of the channels.

	client.TriggerMultiExclusive([]string{"a_channel", "another_channel"}, "event", data, "123.12")

Deprecated: use TriggerMultiWithParams instead.
*/
func (c *Client) TriggerMultiExclusive(channels []string, eventName string, data interface{}, socketID string) error {
	params := TriggerParams{SocketID: &socketID}
	_, err := c.validateChannelsAndTrigger(channels, eventName, data, params)
	return err
}

/*
SendToUser triggers an event to a specific user.
Pass in the user id, the event's name, and a data payload. The data payload must
be marshallable into JSON.

	data := map[string]string{"hello": "world"}
	client.SendToUser("user123", "say_hello", data)
*/
func (c *Client) SendToUser(userId string, eventName string, data interface{}) error {
	if !validUserId(userId) {
		return fmt.Errorf("User id '%s' is invalid", userId)
	}
	_, err := c.trigger([]string{"#server-to-user-" + userId}, eventName, data, TriggerParams{})
	return err
}

// TerminateUserConnections disconnects every active socket for the given user.
func (c *Client) TerminateUserConnections(userId string) error {
	if !validUserId(userId) {
		return fmt.Errorf("User id '%s' is invalid", userId)
	}
	path := fmt.Sprintf("/apps/%s/users/%s/terminate_connections", c.AppID, userId)
	u, err := createRequestURL("POST", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, []byte("{}"), nil, c.Cluster)
	if err != nil {
		return err
	}
	var reqErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		_, reqErr = c.request("POST", u, []byte("{}"))
		if reqErr == nil || !isRetryableError(reqErr) {
			break
		}
	}
	return reqErr
}

// ForceReconnectUser closes all active connections for a user with code 4200,
// prompting clients to reconnect.
func (c *Client) ForceReconnectUser(userId string) error {
	if !validUserId(userId) {
		return fmt.Errorf("User id '%s' is invalid", userId)
	}
	path := fmt.Sprintf("/apps/%s/users/%s/force_reconnect", c.AppID, userId)
	u, err := createRequestURL("POST", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, []byte("{}"), nil, c.Cluster)
	if err != nil {
		return err
	}
	var reqErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		_, reqErr = c.request("POST", u, []byte("{}"))
		if reqErr == nil || !isRetryableError(reqErr) {
			break
		}
	}
	return reqErr
}

func (c *Client) validateChannelsAndTrigger(channels []string, eventName string, data interface{}, params TriggerParams) (*TriggerChannelsList, error) {
	if len(channels) > maxTriggerableChannels {
		return nil, fmt.Errorf("You cannot trigger on more than %d channels at once", maxTriggerableChannels)
	}
	if !channelsAreValid(channels) {
		return nil, errors.New("At least one of your channels' names are invalid")
	}
	return c.trigger(channels, eventName, data, params)
}

func (c *Client) trigger(channels []string, eventName string, data interface{}, params TriggerParams) (*TriggerChannelsList, error) {
	hasEncryptedChannel := false
	for _, channel := range channels {
		if isEncryptedChannel(channel) {
			hasEncryptedChannel = true
		}
	}
	if hasEncryptedChannel && len(channels) > 1 {
		// For rationale, see limitations of end-to-end encryption in the README
		return nil, errors.New("You cannot trigger to multiple channels when using encrypted channels")
	}
	masterKey, keyErr := c.encryptionMasterKey()
	if hasEncryptedChannel && keyErr != nil {
		return nil, keyErr
	}

	if err := validateSocketID(params.SocketID); err != nil {
		return nil, err
	}

	serial := atomic.AddUint64(&c.publishSerial, 1) - 1
	if c.autoIdempotencyEnabled() && params.IdempotencyKey == nil {
		c.ensureBaseID()
		key := fmt.Sprintf("%s:%d", c.idempotencyBaseID, serial)
		params.IdempotencyKey = &key
	}

	payload, err := encodeTriggerBody(channels, eventName, data, params.toMap(), masterKey, c.OverrideMaxMessagePayloadKB, params.Extras)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/apps/%s/events", c.AppID)
	triggerURL, err := createRequestURL("POST", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, payload, nil, c.Cluster)
	if err != nil {
		return nil, err
	}

	var response []byte
	for attempt := 0; attempt < maxRetries; attempt++ {
		response, err = c.requestWithExtraHeaders("POST", triggerURL, payload, params.extraHeaders())
		if err == nil || !isRetryableError(err) {
			break
		}
	}
	if err != nil {
		return nil, err
	}

	return unmarshalledTriggerChannelsList(response)
}

// MessageExtras contains V2 extras for publish events.
type MessageExtras struct {
	Headers        map[string]interface{} `json:"headers,omitempty"`
	Ephemeral      *bool                  `json:"ephemeral,omitempty"`
	IdempotencyKey *string                `json:"idempotency_key,omitempty"`
	Echo           *bool                  `json:"echo,omitempty"`
}

/*
Event stores all the data for one Event that can be triggered.
*/
type Event struct {
	Channel  string
	Name     string
	Data     interface{}
	SocketID *string
	// Info is part of an [experimental feature](https://sockudo.com/docs/lab#experimental-program).
	Info *string
	// IdempotencyKey ensures this event in the batch is only processed once.
	IdempotencyKey *string
	// Extras contains V2 extras for publish events.
	Extras *MessageExtras
}

/*
TriggerBatch triggers multiple events on multiple channels in a single call:

	info := "subscription_count"
	socketID := "1234.12"
	client.TriggerBatch([]sockudo.Event{
		{ Channel: "donut-1", Name: "ev1", Data: "d1", SocketID: socketID, Info: &info },
		{ Channel: "private-encrypted-secretdonut", Name: "ev2", Data: "d2", SocketID: socketID, Info: &info },
	})
*/
func (c *Client) TriggerBatch(batch []Event) (*TriggerBatchChannelsList, error) {
	hasEncryptedChannel := false
	// validate every channel name and every sockedID (if present) in batch
	for _, event := range batch {
		if !validChannel(event.Channel) {
			return nil, fmt.Errorf("The channel named %s has a non-valid name", event.Channel)
		}
		if err := validateSocketID(event.SocketID); err != nil {
			return nil, err
		}
		if isEncryptedChannel(event.Channel) {
			hasEncryptedChannel = true
		}
	}
	masterKey, keyErr := c.encryptionMasterKey()
	if hasEncryptedChannel && keyErr != nil {
		return nil, keyErr
	}

	serial := atomic.AddUint64(&c.publishSerial, 1) - 1
	if c.autoIdempotencyEnabled() {
		c.ensureBaseID()
		for i := range batch {
			if batch[i].IdempotencyKey == nil {
				key := fmt.Sprintf("%s:%d:%d", c.idempotencyBaseID, serial, i)
				batch[i].IdempotencyKey = &key
			}
		}
	}

	payload, err := encodeTriggerBatchBody(batch, masterKey, c.OverrideMaxMessagePayloadKB)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/apps/%s/batch_events", c.AppID)
	triggerURL, err := createRequestURL("POST", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, payload, nil, c.Cluster)
	if err != nil {
		return nil, err
	}

	var response []byte
	for attempt := 0; attempt < maxRetries; attempt++ {
		response, err = c.request("POST", triggerURL, payload)
		if err == nil || !isRetryableError(err) {
			break
		}
	}
	if err != nil {
		return nil, err
	}

	return unmarshalledTriggerBatchChannelsList(response)
}

/*
ChannelsParams are any parameters than can be sent with a Channels request.
*/
type ChannelsParams struct {
	// FilterByPrefix will filter the returned channels.
	FilterByPrefix *string
	// Info should be specified with a value of "user_count" to get number
	// of users subscribed to a presence-channel. Pass in `nil` if you do
	// not wish to specify any query attributes.
	Info *string
}

func (params ChannelsParams) toMap() map[string]string {
	m := make(map[string]string)
	if params.FilterByPrefix != nil {
		m["filter_by_prefix"] = *params.FilterByPrefix
	}
	if params.Info != nil {
		m["info"] = *params.Info
	}
	return m
}

/*
Channels returns a list of all the channels in an application.

	prefixFilter := "presence-"
	attributes := "user_count"
	params := sockudo.ChannelsParams{FilterByPrefix: &prefixFilter, Info: &attributes}
	channels, err := client.Channels(params)

	//channels=> &{Channels:map[presence-chatroom:{UserCount:4} presence-notifications:{UserCount:31}  ]}
*/
func (c *Client) Channels(params ChannelsParams) (*ChannelsList, error) {
	path := fmt.Sprintf("/apps/%s/channels", c.AppID)
	u, err := createRequestURL("GET", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, nil, params.toMap(), c.Cluster)
	if err != nil {
		return nil, err
	}
	response, err := c.request("GET", u, nil)
	if err != nil {
		return nil, err
	}
	return unmarshalledChannelsList(response)
}

/*
ChannelParams are any parameters than can be sent with a Channel request.
*/
type ChannelParams struct {
	// Info is comma-separated vales of `"user_count"`, for
	// presence-channels, and `"subscription_count"`, for all-channels.
	// Note that the subscription count is not allowed by default. Please
	// contact us at http://support.sockudo.com if you wish to enable this.
	// Pass in `nil` if you do not wish to specify any query attributes.
	Info *string
}

func (params ChannelParams) toMap() map[string]string {
	m := make(map[string]string)
	if params.Info != nil {
		m["info"] = *params.Info
	}
	return m
}

type HistoryParams struct {
	Limit       *int
	Direction   *string
	Cursor      *string
	StartSerial *int64
	EndSerial   *int64
	StartTimeMS *int64
	EndTimeMS   *int64
}

func (params HistoryParams) toMap() map[string]string {
	m := make(map[string]string)
	if params.Limit != nil {
		m["limit"] = fmt.Sprintf("%d", *params.Limit)
	}
	if params.Direction != nil {
		m["direction"] = *params.Direction
	}
	if params.Cursor != nil {
		m["cursor"] = *params.Cursor
	}
	if params.StartSerial != nil {
		m["start_serial"] = fmt.Sprintf("%d", *params.StartSerial)
	}
	if params.EndSerial != nil {
		m["end_serial"] = fmt.Sprintf("%d", *params.EndSerial)
	}
	if params.StartTimeMS != nil {
		m["start_time_ms"] = fmt.Sprintf("%d", *params.StartTimeMS)
	}
	if params.EndTimeMS != nil {
		m["end_time_ms"] = fmt.Sprintf("%d", *params.EndTimeMS)
	}
	return m
}

/*
Channel allows you to get the state of a single channel.

	attributes := "user_count,subscription_count"
	params := sockudo.ChannelParams{Info: &attributes}
	channel, err := client.Channel("presence-chatroom", params)

	//channel=> &{Name:presence-chatroom Occupied:true UserCount:42 SubscriptionCount:42}
*/
func (c *Client) Channel(name string, params ChannelParams) (*Channel, error) {
	path := fmt.Sprintf("/apps/%s/channels/%s", c.AppID, name)
	u, err := createRequestURL("GET", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, nil, params.toMap(), c.Cluster)
	if err != nil {
		return nil, err
	}
	response, err := c.request("GET", u, nil)
	if err != nil {
		return nil, err
	}
	return unmarshalledChannel(response, name)
}

/*
GetChannelUsers returns a list of users in a presence-channel by passing to this
method the channel name.

	users, err := client.GetChannelUsers("presence-chatroom")

	//users=> &{List:[{ID:13} {ID:90}]}
*/
func (c *Client) GetChannelUsers(name string) (*Users, error) {
	path := fmt.Sprintf("/apps/%s/channels/%s/users", c.AppID, name)
	u, err := createRequestURL("GET", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, nil, nil, c.Cluster)
	if err != nil {
		return nil, err
	}
	response, err := c.request("GET", u, nil)
	if err != nil {
		return nil, err
	}
	return unmarshalledChannelUsers(response)
}

/*
ChannelHistory returns durable history for a single channel.

	limit := 50
	direction := "newest_first"
	page, err := client.ChannelHistory("presence-chatroom", HistoryParams{Limit: &limit, Direction: &direction})
*/
func (c *Client) ChannelHistory(name string, params HistoryParams) (*HistoryPage, error) {
	path := fmt.Sprintf("/apps/%s/channels/%s/history", c.AppID, name)
	u, err := createRequestURL("GET", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, nil, params.toMap(), c.Cluster)
	if err != nil {
		return nil, err
	}
	response, err := c.request("GET", u, nil)
	if err != nil {
		return nil, err
	}
	return unmarshalledHistory(response)
}

func (c *Client) GetMessage(channelName string, messageSerial string) (*GetMessageResponse, error) {
	if !validChannel(channelName) {
		return nil, fmt.Errorf("Channel name '%s' is invalid", channelName)
	}
	path := fmt.Sprintf("/apps/%s/channels/%s/messages/%s", c.AppID, channelName, messageSerial)
	u, err := createRequestURL("GET", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, nil, nil, c.Cluster)
	if err != nil {
		return nil, err
	}
	response, err := c.request("GET", u, nil)
	if err != nil {
		return nil, err
	}
	return unmarshalledMessage(response)
}

func (c *Client) GetMessageVersions(channelName string, messageSerial string, params MessageVersionsParams) (*MessageVersionsResponse, error) {
	if !validChannel(channelName) {
		return nil, fmt.Errorf("Channel name '%s' is invalid", channelName)
	}
	path := fmt.Sprintf("/apps/%s/channels/%s/messages/%s/versions", c.AppID, channelName, messageSerial)
	u, err := createRequestURL("GET", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, nil, params.toMap(), c.Cluster)
	if err != nil {
		return nil, err
	}
	response, err := c.request("GET", u, nil)
	if err != nil {
		return nil, err
	}
	return unmarshalledMessageVersions(response)
}

func (c *Client) UpdateMessage(channelName string, messageSerial string, payload interface{}) (*MutationResponse, error) {
	if !validChannel(channelName) {
		return nil, fmt.Errorf("Channel name '%s' is invalid", channelName)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/apps/%s/channels/%s/messages/%s/update", c.AppID, channelName, messageSerial)
	u, err := createRequestURL("POST", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, body, nil, c.Cluster)
	if err != nil {
		return nil, err
	}
	response, err := c.request("POST", u, body)
	if err != nil {
		return nil, err
	}
	return unmarshalledMutationResponse(response)
}

func (c *Client) DeleteMessage(channelName string, messageSerial string, payload interface{}) (*MutationResponse, error) {
	if !validChannel(channelName) {
		return nil, fmt.Errorf("Channel name '%s' is invalid", channelName)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/apps/%s/channels/%s/messages/%s/delete", c.AppID, channelName, messageSerial)
	u, err := createRequestURL("POST", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, body, nil, c.Cluster)
	if err != nil {
		return nil, err
	}
	response, err := c.request("POST", u, body)
	if err != nil {
		return nil, err
	}
	return unmarshalledMutationResponse(response)
}

func (c *Client) AppendMessage(channelName string, messageSerial string, payload interface{}) (*MutationResponse, error) {
	if !validChannel(channelName) {
		return nil, fmt.Errorf("Channel name '%s' is invalid", channelName)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/apps/%s/channels/%s/messages/%s/append", c.AppID, channelName, messageSerial)
	u, err := createRequestURL("POST", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, body, nil, c.Cluster)
	if err != nil {
		return nil, err
	}
	response, err := c.request("POST", u, body)
	if err != nil {
		return nil, err
	}
	return unmarshalledMutationResponse(response)
}

func (c *Client) PublishAnnotation(channelName string, messageSerial string, payload PublishAnnotationRequest) (*PublishAnnotationResponse, error) {
	if !validChannel(channelName) {
		return nil, fmt.Errorf("Channel name '%s' is invalid", channelName)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/apps/%s/channels/%s/messages/%s/annotations", c.AppID, channelName, messageSerial)
	u, err := createRequestURL("POST", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, body, nil, c.Cluster)
	if err != nil {
		return nil, err
	}
	response, err := c.request("POST", u, body)
	if err != nil {
		return nil, err
	}
	return unmarshalledPublishAnnotationResponse(response)
}

func (c *Client) DeleteAnnotation(channelName string, messageSerial string, annotationSerial string, socketID *string) (*DeleteAnnotationResponse, error) {
	if !validChannel(channelName) {
		return nil, fmt.Errorf("Channel name '%s' is invalid", channelName)
	}
	params := map[string]string{}
	if socketID != nil {
		params["socket_id"] = *socketID
	}
	path := fmt.Sprintf("/apps/%s/channels/%s/messages/%s/annotations/%s", c.AppID, channelName, messageSerial, annotationSerial)
	u, err := createRequestURL("DELETE", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, nil, params, c.Cluster)
	if err != nil {
		return nil, err
	}
	response, err := c.request("DELETE", u, nil)
	if err != nil {
		return nil, err
	}
	return unmarshalledDeleteAnnotationResponse(response)
}

func (c *Client) ListAnnotations(channelName string, messageSerial string, params AnnotationEventsParams) (*AnnotationEventsResponse, error) {
	if !validChannel(channelName) {
		return nil, fmt.Errorf("Channel name '%s' is invalid", channelName)
	}
	path := fmt.Sprintf("/apps/%s/channels/%s/messages/%s/annotations", c.AppID, channelName, messageSerial)
	u, err := createRequestURL("GET", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, nil, params.toMap(), c.Cluster)
	if err != nil {
		return nil, err
	}
	response, err := c.request("GET", u, nil)
	if err != nil {
		return nil, err
	}
	return unmarshalledAnnotationEvents(response)
}

// PresenceHistoryParams are parameters for querying presence history.
type PresenceHistoryParams struct {
	Limit       *int
	Direction   *string
	Cursor      *string
	StartSerial *int64
	EndSerial   *int64
	StartTimeMS *int64
	EndTimeMS   *int64
	// Ably-compatible alias for StartTimeMS
	Start *int64
	// Ably-compatible alias for EndTimeMS
	End *int64
}

func (params PresenceHistoryParams) toMap() map[string]string {
	m := make(map[string]string)
	if params.Limit != nil {
		m["limit"] = fmt.Sprintf("%d", *params.Limit)
	}
	if params.Direction != nil {
		m["direction"] = *params.Direction
	}
	if params.Cursor != nil {
		m["cursor"] = *params.Cursor
	}
	if params.StartSerial != nil {
		m["start_serial"] = fmt.Sprintf("%d", *params.StartSerial)
	}
	if params.EndSerial != nil {
		m["end_serial"] = fmt.Sprintf("%d", *params.EndSerial)
	}
	startTime := params.StartTimeMS
	if startTime == nil {
		startTime = params.Start
	}
	if startTime != nil {
		m["start_time_ms"] = fmt.Sprintf("%d", *startTime)
	}
	endTime := params.EndTimeMS
	if endTime == nil {
		endTime = params.End
	}
	if endTime != nil {
		m["end_time_ms"] = fmt.Sprintf("%d", *endTime)
	}
	return m
}

// PresenceSnapshotParams are parameters for reconstructing presence membership at a point in time.
type PresenceSnapshotParams struct {
	AtTimeMS *int64
	// Ably-compatible alias for AtTimeMS
	At       *int64
	AtSerial *int64
}

func (params PresenceSnapshotParams) toMap() map[string]string {
	m := make(map[string]string)
	atTime := params.AtTimeMS
	if atTime == nil {
		atTime = params.At
	}
	if atTime != nil {
		m["at_time_ms"] = fmt.Sprintf("%d", *atTime)
	}
	if params.AtSerial != nil {
		m["at_serial"] = fmt.Sprintf("%d", *params.AtSerial)
	}
	return m
}

/*
ChannelPresenceHistory returns presence history for a presence channel.

	limit := 50
	direction := "newest_first"
	page, err := client.ChannelPresenceHistory("presence-chatroom", PresenceHistoryParams{Limit: &limit, Direction: &direction})
*/
func (c *Client) ChannelPresenceHistory(name string, params PresenceHistoryParams) (*PresenceHistoryPage, error) {
	if !strings.HasPrefix(name, "presence-") {
		return nil, errors.New("presence history is only available for presence channels")
	}
	path := fmt.Sprintf("/apps/%s/channels/%s/presence/history", c.AppID, name)
	u, err := createRequestURL("GET", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, nil, params.toMap(), c.Cluster)
	if err != nil {
		return nil, err
	}
	response, err := c.request("GET", u, nil)
	if err != nil {
		return nil, err
	}
	return unmarshalledPresenceHistory(response)
}

/*
ChannelPresenceSnapshot reconstructs effective presence membership at a point in time.

	serial := int64(42)
	snapshot, err := client.ChannelPresenceSnapshot("presence-chatroom", PresenceSnapshotParams{AtSerial: &serial})
*/
func (c *Client) ChannelPresenceSnapshot(name string, params PresenceSnapshotParams) (*PresenceSnapshot, error) {
	if !strings.HasPrefix(name, "presence-") {
		return nil, errors.New("presence snapshot is only available for presence channels")
	}
	path := fmt.Sprintf("/apps/%s/channels/%s/presence/history/snapshot", c.AppID, name)
	u, err := createRequestURL("GET", c.Host, path, c.Key, c.Secret, authTimestamp(), c.Secure, nil, params.toMap(), c.Cluster)
	if err != nil {
		return nil, err
	}
	response, err := c.request("GET", u, nil)
	if err != nil {
		return nil, err
	}
	return unmarshalledPresenceSnapshot(response)
}

/*
AuthenticateUser allows you to authenticate a user's connection.
It returns an authentication signature to send back to the client
and authenticate them. In order to identify a user, this method acceps a map containing
arbitrary user data. It must contain at least an id field with the user's id as a string.

For more information see our docs: http://sockudo.com/docs/authenticating_users.

This is an example of authenticating a user, using the built-in
Golang HTTP library to start a server.

In order to authenticate a client, one must read the response into type `[]byte`
and pass it in. This will return a signature in the form of a `[]byte` for you
to send back to the client.

	func sockudoUserAuth(res http.ResponseWriter, req *http.Request) {

		params, _ := ioutil.ReadAll(req.Body)
		userData := map[string]interface{} { "id": "1234", "twitter": "jamiepatel" }
		response, err := client.AuthenticateUser(params, userData)
		if err != nil {
			panic(err)
		}

		fmt.Fprintf(res, string(response))
	}

	func main() {
		http.HandleFunc("/sockudo/user-auth", sockudoUserAuth)
		http.ListenAndServe(":5000", nil)
	}
*/
func (c *Client) AuthenticateUser(params []byte, userData map[string]interface{}) (response []byte, err error) {
	socketID, err := parseUserAuthenticationRequestParams(params)
	if err != nil {
		return
	}

	if err = validateSocketID(&socketID); err != nil {
		return
	}

	if err = validateUserData(userData); err != nil {
		return
	}

	var jsonUserData string
	if jsonUserData, err = jsonMarshalToString(userData); err != nil {
		return
	}
	stringToSign := strings.Join([]string{socketID, "user", jsonUserData}, "::")

	_response := createAuthMap(c.Key, c.Secret, stringToSign, "")
	_response["user_data"] = jsonUserData

	response, err = json.Marshal(_response)
	return
}

/*
AuthorizePrivateChannel allows you to authorize a users subscription to a
private channel. It returns an authorization signature to send back to the client
and authorize them.

For more information see our docs: http://sockudo.com/docs/authorizing_users.

This is an example of authorizing a private-channel, using the built-in
Golang HTTP library to start a server.

In order to authorize a client, one must read the response into type `[]byte`
and pass it in. This will return a signature in the form of a `[]byte` for you
to send back to the client.

	func sockudoAuth(res http.ResponseWriter, req *http.Request) {

		params, _ := ioutil.ReadAll(req.Body)
		response, err := client.AuthorizePrivateChannel(params)
		if err != nil {
			panic(err)
		}

		fmt.Fprintf(res, string(response))
	}

	func main() {
		http.HandleFunc("/sockudo/auth", sockudoAuth)
		http.ListenAndServe(":5000", nil)
	}
*/
func (c *Client) AuthorizePrivateChannel(params []byte) (response []byte, err error) {
	return c.authorizeChannel(params, nil)
}

/*
AuthenticatePrivateChannel allows you to authorize a users subscription to a
private channel. It returns an authorization signature to send back to the client
and authorize them.

Deprecated: use AuthorizePrivateChannel instead.
*/
func (c *Client) AuthenticatePrivateChannel(params []byte) (response []byte, err error) {
	return c.authorizeChannel(params, nil)
}

/*
AuthorizePresenceChannel allows you to authorize a users subscription to a
presence channel. It returns an authorization signature to send back to the client
and authorize them. In order to identify a user, clients are sent a user_id and,
optionally, custom data.

In this library, one does this by passing a `sockudo.MemberData` instance.

	params, _ := ioutil.ReadAll(req.Body)

	presenceData := sockudo.MemberData{
		UserID: "1",
		UserInfo: map[string]string{
			"twitter": "jamiepatel",
		},
	}

	response, err := client.AuthorizePresenceChannel(params, presenceData)
	if err != nil {
		panic(err)
	}

	fmt.Fprintf(res, response)
*/
func (c *Client) AuthorizePresenceChannel(params []byte, member MemberData) (response []byte, err error) {
	return c.authorizeChannel(params, &member)
}

/*
AuthenticatePresenceChannel allows you to authorize a users subscription to a
presence channel. It returns an authorization signature to send back to the client
and authorize them. In order to identify a user, clients are sent a user_id and,
optionally, custom data.

Deprecated: use AuthorizePresenceChannel instead.
*/
func (c *Client) AuthenticatePresenceChannel(params []byte, member MemberData) (response []byte, err error) {
	return c.authorizeChannel(params, &member)
}

func (c *Client) authorizeChannel(params []byte, member *MemberData) (response []byte, err error) {
	channelName, socketID, err := parseChannelAuthorizationRequestParams(params)
	if err != nil {
		return
	}

	if err = validateSocketID(&socketID); err != nil {
		return
	}

	stringToSign := strings.Join([]string{socketID, channelName}, ":")

	var jsonUserData string

	if member != nil {
		var _jsonUserData []byte
		_jsonUserData, err = json.Marshal(member)
		if err != nil {
			return
		}

		jsonUserData = string(_jsonUserData)
		stringToSign = strings.Join([]string{stringToSign, jsonUserData}, ":")
	}

	var _response map[string]string

	if isEncryptedChannel(channelName) {
		masterKey, err := c.encryptionMasterKey()
		if err != nil {
			return nil, err
		}
		sharedSecret := generateSharedSecret(channelName, masterKey)
		sharedSecretB64 := base64.StdEncoding.EncodeToString(sharedSecret[:])
		_response = createAuthMap(c.Key, c.Secret, stringToSign, sharedSecretB64)
	} else {
		_response = createAuthMap(c.Key, c.Secret, stringToSign, "")
	}

	if member != nil {
		_response["channel_data"] = jsonUserData
	}

	response, err = json.Marshal(_response)
	return
}

/*
Webhook allows you to check that a Webhook you receive is indeed from Sockudo, by
checking the token and authentication signature in the header of the request. On
your dashboard at http://app.sockudo.com, you can set up webhooks to POST a
payload to your server after certain events. Such events include channels being
occupied or vacated, members being added or removed in presence-channels, or
after client-originated events. For more information see
https://sockudo.com/docs/webhooks.

If the webhook is valid, a `*sockudo.Webhook* will be returned, and the `err`
value will be nil. If it is invalid, the first return value will be nil, and an
error will be passed.

	func sockudoWebhook(res http.ResponseWriter, req *http.Request) {

		body, _ := ioutil.ReadAll(req.Body)
		webhook, err := client.Webhook(req.Header, body)
		if err != nil {
			fmt.Println("Webhook is invalid :(")
		} else {
			fmt.Printf("%+v\n", webhook.Events)
		}

	}
*/
func (c *Client) Webhook(header http.Header, body []byte) (*Webhook, error) {
	for _, token := range header["X-Pusher-Key"] {
		if token == c.Key && checkSignature(header.Get("X-Pusher-Signature"), c.Secret, body) {
			unmarshalledWebhooks, err := unmarshalledWebhook(body)
			if err != nil {
				return nil, err
			}

			hasEncryptedChannel := false
			for _, event := range unmarshalledWebhooks.Events {
				if isEncryptedChannel(event.Channel) {
					hasEncryptedChannel = true
				}
			}
			masterKey, keyErr := c.encryptionMasterKey()
			if hasEncryptedChannel && keyErr != nil {
				return nil, keyErr
			}

			return decryptEvents(*unmarshalledWebhooks, masterKey)
		}
	}
	return nil, errors.New("Invalid webhook")
}

func (c *Client) encryptionMasterKey() ([]byte, error) {
	if c.validatedEncryptionMasterKey != nil {
		return *(c.validatedEncryptionMasterKey), nil
	}

	if c.EncryptionMasterKey != "" && c.EncryptionMasterKeyBase64 != "" {
		return nil, errors.New("Do not specify both EncryptionMasterKey and EncryptionMasterKeyBase64. EncryptionMasterKey is deprecated, specify only EncryptionMasterKeyBase64")
	}

	if c.EncryptionMasterKey != "" {
		if len(c.EncryptionMasterKey) != 32 {
			return nil, errors.New("EncryptionMasterKey must be 32 bytes. It is also deprecated, use EncryptionMasterKeyBase64")
		}

		keyBytes := []byte(c.EncryptionMasterKey)
		c.validatedEncryptionMasterKey = &keyBytes
		return keyBytes, nil
	}

	if c.EncryptionMasterKeyBase64 != "" {
		keyBytes, err := base64.StdEncoding.DecodeString(c.EncryptionMasterKeyBase64)
		if err != nil {
			return nil, errors.New("EncryptionMasterKeyBase64 must be valid base64")
		}
		if len(keyBytes) != 32 {
			return nil, errors.New("EncryptionMasterKeyBase64 must encode 32 bytes")
		}

		c.validatedEncryptionMasterKey = &keyBytes
		return keyBytes, nil
	}

	return nil, errors.New("No master encryption key supplied")
}
