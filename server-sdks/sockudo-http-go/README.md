# sockudo-http-go

Official Go server SDK for [Sockudo](https://github.com/sockudo/sockudo) — a fast, self-hosted WebSocket server with full Pusher HTTP API compatibility.

## Supported Platforms

- **Go 1.21 or greater**

## Table of Contents

- [Installation](#installation)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Triggering Events](#triggering-events)
- [Idempotent Publishing](#idempotent-publishing)
- [Authenticating Users](#authenticating-users)
- [Authorizing Channels](#authorizing-channels)
- [Application State](#application-state)
- [Webhook Validation](#webhook-validation)
- [End-to-End Encryption](#end-to-end-encryption)
- [Google App Engine](#google-app-engine)
- [Testing](#testing)
- [Pusher SDK Compatibility](#pusher-sdk-compatibility)

## Installation

Install the v2 Go module. Go resolves this package through the module path and the `v2.0.0` Git tag:

```sh
go get github.com/sockudo/sockudo/server-sdks/sockudo-http-go/v2
```

## Getting Started

```go
package main

import (
    sockudo "github.com/sockudo/sockudo/server-sdks/sockudo-http-go/v2"
)

func main() {
    client := sockudo.Client{
        AppID:  "your-app-id",
        Key:    "your-app-key",
        Secret: "your-app-secret",
        Host:   "127.0.0.1",
        Port:   "6001",
    }

    data := map[string]string{"message": "hello world"}

    err := client.Trigger("my-channel", "my-event", data)
    if err != nil {
        panic(err)
    }
}
```

## Configuration

### Direct struct initialization

```go
client := sockudo.Client{
    AppID:  "your-app-id",
    Key:    "your-app-key",
    Secret: "your-app-secret",
    Host:   "127.0.0.1",
    Port:   "6001",
    Secure: false,
}
```

### From URL

```go
client := sockudo.ClientFromURL("http://key:secret@127.0.0.1:6001/apps/app-id")
```

### From Environment Variable

```go
// Reads SOCKUDO_URL, expected format: http://key:secret@host:port/apps/app_id
client := sockudo.ClientFromEnv("SOCKUDO_URL")
```

### HTTPS

```go
client.Secure = true
```

### Custom HTTP Client (timeouts, keep-alive, etc.)

```go
import "net/http"
import "time"

client.HTTPClient = &http.Client{Timeout: time.Second * 5}
```

If no HTTP client is set, a default is created with a 5-second timeout.

## Triggering Events

### Single Channel

```go
data := map[string]string{"hello": "world"}
err := client.Trigger("my-channel", "my-event", data)
```

### With Parameters (excluding a socket, requesting channel info)

```go
socketID := "1234.12"
params := sockudo.TriggerParams{SocketID: &socketID}
channels, err := client.TriggerWithParams("my-channel", "my-event", data, params)
```

### Multiple Channels

```go
err := client.TriggerMulti([]string{"channel-1", "channel-2"}, "my-event", data)
```

### Batch Events

```go
batch := []sockudo.Event{
    {Channel: "channel-1", Name: "event-1", Data: "hello"},
    {Channel: "channel-2", Name: "event-2", Data: "world"},
}
response, err := client.TriggerBatch(batch)
```

The `Event` struct:

```go
type Event struct {
    Channel  string
    Name     string
    Data     interface{}
    SocketID *string
}
```

## Idempotent Publishing

Pass an `IdempotencyKey` in `TriggerParams` to safely retry publishes without causing duplicate deliveries:

```go
key := "order-shipped-order-789"
params := sockudo.TriggerParams{IdempotencyKey: &key}
err := client.TriggerWithParams("my-channel", "my-event", data, params)
```

The server deduplicates publishes with the same key within the configured window.

## Authenticating Users

Use `AuthenticateUser` to generate an authentication response for user connections. The `userData` map must contain at least an `"id"` key.

```go
func sockudoUserAuth(res http.ResponseWriter, req *http.Request) {
    params, _ := io.ReadAll(req.Body)
    userData := map[string]interface{}{
        "id":   "user-123",
        "name": "Jane Doe",
    }
    response, err := client.AuthenticateUser(params, userData)
    if err != nil {
        http.Error(res, "unauthorized", http.StatusUnauthorized)
        return
    }
    fmt.Fprint(res, string(response))
}

func main() {
    http.HandleFunc("/sockudo/user-auth", sockudoUserAuth)
    http.ListenAndServe(":5000", nil)
}
```

## Authorizing Channels

### Private Channel

```go
func sockudoAuth(res http.ResponseWriter, req *http.Request) {
    params, _ := io.ReadAll(req.Body)
    response, err := client.AuthorizePrivateChannel(params)
    if err != nil {
        http.Error(res, "unauthorized", http.StatusUnauthorized)
        return
    }
    fmt.Fprint(res, string(response))
}

func main() {
    http.HandleFunc("/sockudo/auth", sockudoAuth)
    http.ListenAndServe(":5000", nil)
}
```

### Presence Channel

```go
params, _ := io.ReadAll(req.Body)

member := sockudo.MemberData{
    UserID: "user-123",
    UserInfo: map[string]string{
        "name": "Jane Doe",
    },
}

response, err := client.AuthorizePresenceChannel(params, member)
if err != nil {
    panic(err)
}

fmt.Fprint(res, string(response))
```

The `MemberData` struct:

```go
type MemberData struct {
    UserID   string
    UserInfo map[string]string
}
```

## Application State

### List Channels

```go
prefix := "presence-"
attribute := "user_count"
params := sockudo.ChannelsParams{FilterByPrefix: &prefix, Info: &attribute}
channels, err := client.Channels(params)

// channels => &{Channels:map[presence-room:{UserCount:4}]}
```

### Get a Single Channel

```go
attribute := "user_count,subscription_count"
params := sockudo.ChannelParams{Info: &attribute}
channel, err := client.Channel("presence-room", params)

// channel => &{Name:presence-room Occupied:true UserCount:42 SubscriptionCount:42}
```

### Get Users in a Presence Channel

```go
users, err := client.GetChannelUsers("presence-room")

// users => &{List:[{ID:13} {ID:90}]}
```

## Webhook Validation

```go
func sockudoWebhook(res http.ResponseWriter, req *http.Request) {
    body, _ := io.ReadAll(req.Body)
    webhook, err := client.Webhook(req.Header, body)
    if err != nil {
        fmt.Println("Invalid webhook:", err)
        http.Error(res, "invalid", http.StatusUnauthorized)
        return
    }

    for _, event := range webhook.Events {
        fmt.Printf("event: %s, channel: %s\n", event.Name, event.Channel)
    }

    res.WriteHeader(http.StatusOK)
}
```

The `Webhook` struct:

```go
type Webhook struct {
    TimeMs int
    Events []WebhookEvent
}

type WebhookEvent struct {
    Name     string
    Channel  string
    Event    string
    Data     string
    SocketID string
}
```

## End-to-End Encryption

Sockudo supports end-to-end encryption for `private-encrypted-` prefixed channels. Only your server and connected clients can read the messages.

1. Generate a 32-byte master key:

   ```bash
   openssl rand -base64 32
   ```

2. Pass it when constructing the client:

   ```go
   client := sockudo.Client{
       AppID:                    "your-app-id",
       Key:                      "your-app-key",
       Secret:                   "your-app-secret",
       Host:                     "127.0.0.1",
       Port:                     "6001",
       EncryptionMasterKeyBase64: "<output from command above>",
   }
   ```

3. Use channels prefixed with `private-encrypted-`.

## Google App Engine

This library is compatible with Google App Engine's `urlfetch` library. Pass the `urlfetch.Client` as the HTTP client:

```go
package main

import (
    "appengine"
    "appengine/urlfetch"
    "fmt"
    "net/http"
    sockudo "github.com/sockudo/sockudo/server-sdks/sockudo-http-go/v2"
)

func init() {
    http.HandleFunc("/", handler)
}

func handler(w http.ResponseWriter, r *http.Request) {
    c := appengine.NewContext(r)

    client := sockudo.Client{
        AppID:      "your-app-id",
        Key:        "your-app-key",
        Secret:     "your-app-secret",
        Host:       "127.0.0.1",
        Port:       "6001",
        HTTPClient: urlfetch.Client(c),
    }

    client.Trigger("my-channel", "my-event", map[string]string{"message": "hello"})

    fmt.Fprint(w, "OK")
}
```

## Testing

```sh
go test ./...
```

## Channel History

```go
limit := 50
direction := "newest_first"
page, err := client.ChannelHistory("my-channel", sockudo.HistoryParams{
	Limit: &limit,
	Direction: &direction,
})

cursor := "opaque-cursor-from-previous-page"
nextPage, err := client.ChannelHistory("my-channel", sockudo.HistoryParams{
	Cursor: &cursor,
})
```

## Pusher SDK Compatibility

Sockudo implements the full Pusher HTTP API. If you prefer to use the official `github.com/pusher/pusher-http-go/v5` package or are migrating from Pusher, point it at your Sockudo instance:

```go
import pusher "github.com/pusher/pusher-http-go/v5"

client := pusher.Client{
    AppID:  "your-app-id",
    Key:    "your-app-key",
    Secret: "your-app-secret",
    Host:   "127.0.0.1",
    Port:   "6001",
}
```

All standard Pusher SDK calls work against a self-hosted Sockudo server without modification.

## License

MIT
