package sockudo

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type pushRoundTripper struct {
	t        *testing.T
	requests []*http.Request
	bodies   []map[string]interface{}
}

func (rt *pushRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.requests = append(rt.requests, req)
	body := map[string]interface{}{}
	if req.Body != nil {
		raw, _ := io.ReadAll(req.Body)
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &body); err != nil {
				rt.t.Fatal(err)
			}
		}
	}
	rt.bodies = append(rt.bodies, body)
	return &http.Response{
		StatusCode: 202,
		Body:       io.NopCloser(strings.NewReader(`{"publish_id":"pub_123","status":"queued"}`)),
		Header:     make(http.Header),
	}, nil
}

func TestPushPublishDefaultsToAsyncAdmission(t *testing.T) {
	rt := &pushRoundTripper{t: t}
	client := &Client{
		AppID:      "10000",
		Key:        "aaaa",
		Secret:     "tofu",
		Host:       "localhost",
		HTTPClient: &http.Client{Transport: rt},
	}

	_, err := client.PublishPush(PushPublishRequest{
		"recipients": []interface{}{map[string]interface{}{"type": "channel", "channel": "orders"}},
		"payload":    map[string]interface{}{"title": "Order", "body": "Updated"},
	})
	if err != nil {
		t.Fatal(err)
	}

	req := rt.requests[0]
	if req.Method != "POST" {
		t.Fatalf("method = %s", req.Method)
	}
	if req.URL.Path != "/apps/10000/push/publish" {
		t.Fatalf("path = %s", req.URL.Path)
	}
	if req.Header.Get("X-Sockudo-Push-Capability") != "push-admin" {
		t.Fatalf("missing push-admin header")
	}
	if rt.bodies[0]["sync"] != false {
		t.Fatalf("push publish should force sync=false, got %#v", rt.bodies[0]["sync"])
	}
	if req.URL.Query().Get("body_md5") == "" {
		t.Fatalf("signed push POST should include body_md5")
	}
}

func TestPushSubscribeUsesDeviceIdentityTokenAndCursorPagination(t *testing.T) {
	rt := &pushRoundTripper{t: t}
	client := &Client{
		AppID:      "10000",
		Key:        "aaaa",
		Secret:     "tofu",
		Host:       "localhost",
		HTTPClient: &http.Client{Transport: rt},
	}
	limit := 10
	cursor := "c1"
	deviceID := "device-1"

	_, err := client.ListChannelPushSubscriptions(PushSubscriptionParams{
		Limit:    &limit,
		Cursor:   &cursor,
		DeviceID: &deviceID,
	}, "identity")
	if err != nil {
		t.Fatal(err)
	}

	req := rt.requests[0]
	if req.URL.Path != "/apps/10000/push/channelSubscriptions" {
		t.Fatalf("path = %s", req.URL.Path)
	}
	if req.URL.Query().Get("cursor") != "c1" || req.URL.Query().Get("limit") != "10" {
		t.Fatalf("cursor pagination query missing: %s", req.URL.RawQuery)
	}
	if req.Header.Get("X-Sockudo-Push-Capability") != "push-subscribe" {
		t.Fatalf("missing push-subscribe header")
	}
	if req.Header.Get("X-Sockudo-Device-Identity-Token") != "identity" {
		t.Fatalf("missing device identity token")
	}
}
