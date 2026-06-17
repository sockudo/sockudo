package sockudo

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetMessageRequestURLAndResponse(t *testing.T) {
	testJSON := []byte(`{"channel":"chat:room-1","item":{"message_serial":"msg:1","action":"update","data":"hello brave"}}`)
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
	page, err := client.GetMessage("chat:room-1", "msg:1")

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if page == nil || page.Item["message_serial"] != "msg:1" {
		t.Fatalf("expected message_serial msg:1, got %#v", page)
	}
}

func TestGetMessageVersionsRequestURLAndResponse(t *testing.T) {
	testJSON := []byte(`{"channel":"chat:room-1","direction":"oldest_first","limit":10,"has_more":false,"items":[{"message_serial":"msg:1","action":"update","data":"hello brave"}]}`)
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
	limit := 10
	direction := "oldest_first"
	page, err := client.GetMessageVersions("chat:room-1", "msg:1", MessageVersionsParams{
		Limit:     &limit,
		Direction: &direction,
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if page == nil || page.Limit != 10 || len(page.Items) != 1 {
		t.Fatalf("unexpected versions page: %#v", page)
	}
}

func TestUpdateDeleteAppendMessageResponses(t *testing.T) {
	testJSON := []byte(`{"channel":"chat:room-1","message_serial":"msg:1","action":"update","accepted":true,"status":"applied","version_serial":"ver:2"}`)
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

	update, err := client.UpdateMessage("chat:room-1", "msg:1", map[string]interface{}{
		"data":        "hello brave",
		"description": "replace base",
	})
	if err != nil || update.Action != "update" {
		t.Fatalf("unexpected update result: %#v %v", update, err)
	}

	deleteRes, err := client.DeleteMessage("chat:room-1", "msg:1", map[string]interface{}{
		"clear_fields": []string{"data", "extras"},
	})
	if err != nil || deleteRes.Action != "update" {
		t.Fatalf("unexpected delete result: %#v %v", deleteRes, err)
	}

	appendRes, err := client.AppendMessage("chat:room-1", "msg:1", map[string]interface{}{
		"data": " world",
	})
	if err != nil || appendRes.Action != "update" {
		t.Fatalf("unexpected append result: %#v %v", appendRes, err)
	}
}
