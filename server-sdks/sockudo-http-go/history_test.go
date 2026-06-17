package sockudo

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestChannelHistoryRequestURLAndResponse(t *testing.T) {
	testJSON := []byte(`{"items":[],"direction":"newest_first","limit":50,"has_more":true,"next_cursor":"abc","bounds":{"start_serial":10},"continuity":{"stream_id":"stream-1","retained_messages":2,"retained_bytes":100,"complete":false,"truncated_by_retention":false}}`)
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		_, _ = res.Write(testJSON)
	}))
	defer server.Close()

	client := &Client{
		AppID:  "1",
		Key:    "key",
		Secret: "secret",
		Host:   strings.TrimPrefix(server.URL, "http://"),
		Secure: false,
	}
	limit := 50
	direction := "newest_first"
	startSerial := int64(10)
	page, err := client.ChannelHistory("history-room", HistoryParams{
		Limit:       &limit,
		Direction:   &direction,
		StartSerial: &startSerial,
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if page == nil {
		t.Fatal("expected history page, got nil")
	}
	if page.Direction != "newest_first" {
		t.Fatalf("expected newest_first direction, got %s", page.Direction)
	}
	if page.Limit != 50 {
		t.Fatalf("expected limit 50, got %d", page.Limit)
	}
	if page.NextCursor == nil || *page.NextCursor != "abc" {
		t.Fatalf("expected next cursor abc, got %#v", page.NextCursor)
	}
	if page.Bounds.StartSerial == nil || *page.Bounds.StartSerial != int64(10) {
		t.Fatalf("expected start serial 10, got %#v", page.Bounds.StartSerial)
	}
	if page.Continuity.StreamID == nil || *page.Continuity.StreamID != "stream-1" {
		t.Fatalf("expected stream id stream-1, got %#v", page.Continuity.StreamID)
	}
}
