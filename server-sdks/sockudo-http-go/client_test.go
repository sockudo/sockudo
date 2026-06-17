package sockudo

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"gopkg.in/stretchr/testify.v1/assert"
)

func boolPtr(v bool) *bool { return &v }

func TestSendToUserSuccessCase(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		fmt.Fprintf(res, "{}")
		assert.Equal(t, "POST", req.Method)

		expectedBody := map[string]interface{}{"name": "test", "channels": []interface{}{"#server-to-user-123456"}, "data": "yolo"}
		bodyDecoder := json.NewDecoder(req.Body)
		var actualBody map[string]interface{}
		err := bodyDecoder.Decode(&actualBody)
		assert.NoError(t, err)
		assert.Equal(t, expectedBody, actualBody)

		assert.Equal(t, "application/json", req.Header["Content-Type"][0])
		lib := fmt.Sprintf("%s %s", libraryName, libraryVersion)
		assert.Equal(t, lib, req.Header["X-Pusher-Library"][0])
		assert.NoError(t, err)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host, AutoIdempotencyKey: boolPtr(false)}
	err := client.SendToUser("123456", "test", "yolo")
	assert.NoError(t, err)
}

func TestSendToUserRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		t.Fatal("No request should reach the API")
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host}
	err := client.SendToUser("", "test", "yolo")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "User id '' is invalid")
}

func TestTriggerSuccessCase(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		fmt.Fprintf(res, "{}")
		assert.Equal(t, "POST", req.Method)

		expectedBody := map[string]interface{}{"name": "test", "channels": []interface{}{"test_channel"}, "data": "yolo"}
		bodyDecoder := json.NewDecoder(req.Body)
		var actualBody map[string]interface{}
		err := bodyDecoder.Decode(&actualBody)
		assert.NoError(t, err)
		assert.Equal(t, expectedBody, actualBody)

		assert.Equal(t, "application/json", req.Header["Content-Type"][0])
		lib := fmt.Sprintf("%s %s", libraryName, libraryVersion)
		assert.Equal(t, lib, req.Header["X-Pusher-Library"][0])
		assert.NoError(t, err)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host, AutoIdempotencyKey: boolPtr(false)}
	err := client.Trigger("test_channel", "test", "yolo")
	assert.NoError(t, err)
}

func TestTriggerWithStructSuccessCase(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		fmt.Fprintf(res, "{}")
		assert.Equal(t, "POST", req.Method)

		expectedBody := map[string]interface{}{"name": "test", "channels": []interface{}{"test_channel"}, "data": `{"Key":"value"}`}
		bodyDecoder := json.NewDecoder(req.Body)
		var actualBody map[string]interface{}
		err := bodyDecoder.Decode(&actualBody)
		assert.NoError(t, err)
		assert.Equal(t, expectedBody, actualBody)

		assert.Equal(t, "application/json", req.Header["Content-Type"][0])
		lib := fmt.Sprintf("%s %s", libraryName, libraryVersion)
		assert.Equal(t, lib, req.Header["X-Pusher-Library"][0])
		assert.NoError(t, err)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host, AutoIdempotencyKey: boolPtr(false)}
	err := client.Trigger("test_channel", "test", struct{ Key string }{Key: "value"})
	assert.NoError(t, err)
}

// Tests that when the "info" param is not specified, we get a nil Channels map in the returned TriggerChannelsList
func TestTriggerWithParamsSuccessCase(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		testJSON := "{}"
		fmt.Fprint(res, testJSON)
		assert.Equal(t, "POST", req.Method)

		expectedBody := map[string]interface{}{"name": "test", "channels": []interface{}{"test_channel"}, "data": "yolo"}
		bodyDecoder := json.NewDecoder(req.Body)
		var actualBody map[string]interface{}
		err := bodyDecoder.Decode(&actualBody)
		assert.NoError(t, err)
		assert.Equal(t, expectedBody, actualBody)

		assert.Equal(t, "application/json", req.Header["Content-Type"][0])
		lib := fmt.Sprintf("%s %s", libraryName, libraryVersion)
		assert.Equal(t, lib, req.Header["X-Pusher-Library"][0])
		assert.NoError(t, err)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host, AutoIdempotencyKey: boolPtr(false)}
	// Empty parameters
	channels, err := client.TriggerWithParams("test_channel", "test", "yolo", TriggerParams{})
	assert.NoError(t, err)

	expected := &TriggerChannelsList{
		Channels: nil,
	}
	assert.Equal(t, expected, channels)
}

func TestTriggerWithParamsInfoSuccessCase(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		testJSON := "{\"channels\":{\"test_channel\":{\"subscription_count\":1}}}"
		fmt.Fprint(res, testJSON)
		assert.Equal(t, "POST", req.Method)

		expectedBody := map[string]interface{}{"name": "test", "channels": []interface{}{"test_channel"}, "data": "yolo", "info": "subscription_count"}
		bodyDecoder := json.NewDecoder(req.Body)
		var actualBody map[string]interface{}
		err := bodyDecoder.Decode(&actualBody)
		assert.NoError(t, err)
		assert.Equal(t, expectedBody, actualBody)

		assert.Equal(t, "application/json", req.Header["Content-Type"][0])
		lib := fmt.Sprintf("%s %s", libraryName, libraryVersion)
		assert.Equal(t, lib, req.Header["X-Pusher-Library"][0])
		assert.NoError(t, err)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host, AutoIdempotencyKey: boolPtr(false)}
	attributes := "subscription_count"
	channels, err := client.TriggerWithParams("test_channel", "test", "yolo", TriggerParams{Info: &attributes})
	assert.NoError(t, err)

	expectedSubscriptionCount := 1
	expected := &TriggerChannelsList{
		Channels: map[string]TriggerChannelListItem{
			"test_channel": {SubscriptionCount: &expectedSubscriptionCount},
		},
	}
	assert.Equal(t, expected, channels)
}

func TestTriggerWithIdempotencyKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		fmt.Fprintf(res, "{}")
		assert.Equal(t, "POST", req.Method)

		expectedBody := map[string]interface{}{
			"name":            "test",
			"channels":        []interface{}{"test_channel"},
			"data":            "yolo",
			"idempotency_key": "abc-123",
		}
		bodyDecoder := json.NewDecoder(req.Body)
		var actualBody map[string]interface{}
		err := bodyDecoder.Decode(&actualBody)
		assert.NoError(t, err)
		assert.Equal(t, expectedBody, actualBody)

		assert.Equal(t, "abc-123", req.Header.Get("X-Idempotency-Key"))
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host}
	idempotencyKey := "abc-123"
	_, err := client.TriggerWithParams("test_channel", "test", "yolo", TriggerParams{IdempotencyKey: &idempotencyKey})
	assert.NoError(t, err)
}

func TestTriggerWithoutIdempotencyKeyNoHeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		fmt.Fprintf(res, "{}")

		assert.Empty(t, req.Header.Get("X-Idempotency-Key"))

		bodyDecoder := json.NewDecoder(req.Body)
		var actualBody map[string]interface{}
		err := bodyDecoder.Decode(&actualBody)
		assert.NoError(t, err)
		_, hasKey := actualBody["idempotency_key"]
		assert.False(t, hasKey, "idempotency_key should not be in body when not set")
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host, AutoIdempotencyKey: boolPtr(false)}
	_, err := client.TriggerWithParams("test_channel", "test", "yolo", TriggerParams{})
	assert.NoError(t, err)
}

func TestTriggerMultiSuccessCase(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		fmt.Fprintf(res, "{}")
		assert.Equal(t, "POST", req.Method)

		expectedBody := map[string]interface{}{"name": "test", "channels": []interface{}{"test_channel", "other_channel"}, "data": "yolo"}
		bodyDecoder := json.NewDecoder(req.Body)
		var actualBody map[string]interface{}
		err := bodyDecoder.Decode(&actualBody)
		assert.NoError(t, err)
		assert.Equal(t, expectedBody, actualBody)

		assert.Equal(t, "application/json", req.Header["Content-Type"][0])
		assert.NoError(t, err)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host, AutoIdempotencyKey: boolPtr(false)}
	err := client.TriggerMulti([]string{"test_channel", "other_channel"}, "test", "yolo")
	assert.NoError(t, err)
}

func TestTriggerMultiEncryptedRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		t.Fatal("No request should reach the API")
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{
		AppID:                     "id",
		Key:                       "key",
		Secret:                    "secret",
		Host:                      u.Host,
		EncryptionMasterKeyBase64: "ZUhQVldIZzduRkdZVkJzS2pPRkRYV1JyaWJJUjJiMGI=",
	}
	err := client.TriggerMulti([]string{"test_channel", "private-encrypted-other_channel"}, "test", "yolo")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "multiple channels")
	assert.Contains(t, err.Error(), "encrypted channels")
}

func TestTriggerMultiWithParamsInfoSuccessCase(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		testJSON := "{\"channels\":{\"presence-test_channel\":{\"subscription_count\":2,\"user_count\":1},\"test_channel\":{\"subscription_count\":3}}}"
		fmt.Fprint(res, testJSON)
		assert.Equal(t, "POST", req.Method)

		expectedBody := map[string]interface{}{"name": "test", "channels": []interface{}{"presence-test_channel", "test_channel"}, "data": "yolo", "info": "user_count,subscription_count"}
		bodyDecoder := json.NewDecoder(req.Body)
		var actualBody map[string]interface{}
		err := bodyDecoder.Decode(&actualBody)
		assert.NoError(t, err)
		assert.Equal(t, expectedBody, actualBody)

		assert.Equal(t, "application/json", req.Header["Content-Type"][0])
		lib := fmt.Sprintf("%s %s", libraryName, libraryVersion)
		assert.Equal(t, lib, req.Header["X-Pusher-Library"][0])
		assert.NoError(t, err)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host, AutoIdempotencyKey: boolPtr(false)}
	attributes := "user_count,subscription_count"
	channels, err := client.TriggerMultiWithParams([]string{"presence-test_channel", "test_channel"}, "test", "yolo", TriggerParams{Info: &attributes})
	assert.NoError(t, err)

	presenceExpectedUserCount := 1
	presenceExpectedSubscriptionCount := 2
	expectedSubscriptionCount := 3
	expected := &TriggerChannelsList{
		Channels: map[string]TriggerChannelListItem{
			"presence-test_channel": {UserCount: &presenceExpectedUserCount, SubscriptionCount: &presenceExpectedSubscriptionCount},
			"test_channel":          {SubscriptionCount: &expectedSubscriptionCount},
		},
	}
	assert.Equal(t, expected, channels)
}

func TestGetChannelsSuccessCase(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		testJSON := "{\"channels\":{\"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-5cbTiUiPNGI\":{\"user_count\":1},\"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-PbZ5E1pP8uF\":{\"user_count\":1},\"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-oz6iqpSxMwG\":{\"user_count\":1}}}"

		fmt.Fprint(res, testJSON)
		assert.Equal(t, "GET", req.Method)

	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host}
	channels, err := client.Channels(ChannelsParams{})
	assert.NoError(t, err)

	expected := &ChannelsList{
		Channels: map[string]ChannelListItem{
			"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-5cbTiUiPNGI": ChannelListItem{UserCount: 1},
			"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-PbZ5E1pP8uF": ChannelListItem{UserCount: 1},
			"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-oz6iqpSxMwG": ChannelListItem{UserCount: 1},
		},
	}
	assert.Equal(t, expected, channels)
}

func TestGetChannelSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		testJSON := "{\"user_count\":1,\"occupied\":true,\"subscription_count\":1}"
		fmt.Fprint(res, testJSON)

		assert.Equal(t, "GET", req.Method)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host}
	channel, err := client.Channel("test_channel", ChannelParams{})
	assert.NoError(t, err)

	expected := &Channel{
		Name:              "test_channel",
		Occupied:          true,
		UserCount:         1,
		SubscriptionCount: 1,
	}
	assert.Equal(t, expected, channel)
}

func TestGetChannelUserSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		testJSON := "{\"users\":[{\"id\":\"red\"},{\"id\":\"blue\"}]}"
		fmt.Fprint(res, testJSON)

		assert.Equal(t, "GET", req.Method)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host}
	users, err := client.GetChannelUsers("test_channel")
	assert.NoError(t, err)

	expected := &Users{
		List: []User{User{ID: "red"}, User{ID: "blue"}},
	}
	assert.Equal(t, expected, users)
}

func TestTriggerWithSocketID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		expectedBody := map[string]interface{}{"name": "test", "channels": []interface{}{"test_channel"}, "data": "yolo", "socket_id": "1234.12"}
		bodyDecoder := json.NewDecoder(req.Body)
		var actualBody map[string]interface{}
		err := bodyDecoder.Decode(&actualBody)
		assert.NoError(t, err)
		assert.Equal(t, expectedBody, actualBody)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host, AutoIdempotencyKey: boolPtr(false)}
	_ = client.TriggerExclusive("test_channel", "test", "yolo", "1234.12")
}

func TestTriggerSocketIDValidation(t *testing.T) {
	client := Client{AppID: "id", Key: "key", Secret: "secret"}
	err := client.TriggerExclusive("test_channel", "test", "yolo", "1234.12:lalala")
	assert.Error(t, err)
}

func TestTriggerBatchSuccess(t *testing.T) {
	expectedBody := `{"batch":[{"channel":"test_channel","name":"test","data":"yolo1"},{"channel":"test_channel","name":"test","data":"yolo2"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		fmt.Fprintf(res, "{}")
		assert.Equal(t, "POST", req.Method)

		actualBody, err := ioutil.ReadAll(req.Body)
		assert.Equal(t, expectedBody, string(actualBody))
		assert.Equal(t, "application/json", req.Header["Content-Type"][0])
		assert.Equal(t, "/apps/appid/batch_events", req.URL.Path)
		assert.NoError(t, err)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "appid", Key: "key", Secret: "secret", Host: u.Host, AutoIdempotencyKey: boolPtr(false)}
	response, err := client.TriggerBatch([]Event{
		{Channel: "test_channel", Name: "test", Data: "yolo1"},
		{Channel: "test_channel", Name: "test", Data: "yolo2"},
	})

	assert.NoError(t, err)
	assert.Equal(t, &TriggerBatchChannelsList{}, response)
}

func TestTriggerBatchWithIdempotencyKey(t *testing.T) {
	expectedBody := `{"batch":[{"channel":"test_channel","name":"test","data":"yolo1","idempotency_key":"key-1"},{"channel":"test_channel","name":"test","data":"yolo2","idempotency_key":"key-2"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		fmt.Fprintf(res, "{}")
		assert.Equal(t, "POST", req.Method)

		actualBody, err := ioutil.ReadAll(req.Body)
		assert.Equal(t, expectedBody, string(actualBody))
		assert.NoError(t, err)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "appid", Key: "key", Secret: "secret", Host: u.Host}
	key1 := "key-1"
	key2 := "key-2"
	response, err := client.TriggerBatch([]Event{
		{Channel: "test_channel", Name: "test", Data: "yolo1", IdempotencyKey: &key1},
		{Channel: "test_channel", Name: "test", Data: "yolo2", IdempotencyKey: &key2},
	})

	assert.NoError(t, err)
	assert.Equal(t, &TriggerBatchChannelsList{}, response)
}

func TestTriggerBatchInfoSuccess(t *testing.T) {
	expectedBody := `{"batch":[{"channel":"presence-test_channel","name":"test","data":"yolo1","info":"user_count,subscription_count"},{"channel":"test_channel","name":"test","data":"yolo2","info":"subscription_count"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		testJSON := "{\"batch\":[{\"subscription_count\":2,\"user_count\":1},{\"subscription_count\":3}]}"
		fmt.Fprint(res, testJSON)
		assert.Equal(t, "POST", req.Method)

		actualBody, err := ioutil.ReadAll(req.Body)
		assert.Equal(t, expectedBody, string(actualBody))
		assert.Equal(t, "application/json", req.Header["Content-Type"][0])
		assert.Equal(t, "/apps/appid/batch_events", req.URL.Path)
		assert.NoError(t, err)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "appid", Key: "key", Secret: "secret", Host: u.Host, AutoIdempotencyKey: boolPtr(false)}
	presenceChannelInfo := "user_count,subscription_count"
	channelInfo := "subscription_count"
	channels, err := client.TriggerBatch([]Event{
		{Channel: "presence-test_channel", Name: "test", Data: "yolo1", Info: &presenceChannelInfo},
		{Channel: "test_channel", Name: "test", Data: "yolo2", Info: &channelInfo},
	})

	assert.NoError(t, err)

	presenceExpectedUserCount := 1
	presenceExpectedSubscriptionCount := 2
	expectedSubscriptionCount := 3
	expected := &TriggerBatchChannelsList{
		Batch: []TriggerBatchChannelListItem{
			{UserCount: &presenceExpectedUserCount, SubscriptionCount: &presenceExpectedSubscriptionCount},
			{SubscriptionCount: &expectedSubscriptionCount},
		},
	}
	assert.Equal(t, expected, channels)
}

func TestTriggerBatchWithEncryptionMasterKeyNoEncryptedChanSuccess(t *testing.T) {
	expectedBody := `{"batch":[{"channel":"test_channel","name":"test","data":"yolo1"},{"channel":"test_channel","name":"test","data":"yolo2"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		fmt.Fprintf(res, "{}")
		assert.Equal(t, "POST", req.Method)

		actualBody, err := ioutil.ReadAll(req.Body)
		assert.Equal(t, expectedBody, string(actualBody))
		assert.Equal(t, "application/json", req.Header["Content-Type"][0])
		assert.Equal(t, "/apps/appid/batch_events", req.URL.Path)
		assert.NoError(t, err)
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	client := Client{AppID: "appid", Key: "key", Secret: "secret", EncryptionMasterKeyBase64: "ZUhQVldIZzduRkdZVkJzS2pPRkRYV1JyaWJJUjJiMGI=", Host: u.Host, AutoIdempotencyKey: boolPtr(false)}
	response, err := client.TriggerBatch([]Event{
		{Channel: "test_channel", Name: "test", Data: "yolo1"},
		{Channel: "test_channel", Name: "test", Data: "yolo2"},
	})

	assert.NoError(t, err)
	assert.Equal(t, &TriggerBatchChannelsList{}, response)
}

func TestTriggerBatchNoEncryptionMasterKeyWithEncryptedChanFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		t.Fatal("No request should have reached the API")
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "appid", Key: "key", Secret: "secret", Host: u.Host}
	_, err := client.TriggerBatch([]Event{
		{Channel: "test_channel", Name: "test", Data: "yolo1"},
		{Channel: "private-encrypted-test_channel", Name: "test", Data: "yolo2"},
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "master encryption key")
}

func TestTriggerWithEncryptedChanSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		fmt.Fprintf(res, "{}")
		assert.Equal(t, "POST", req.Method)

		actualBody, err := ioutil.ReadAll(req.Body)
		assert.Contains(t, string(actualBody), "ciphertext")
		assert.Contains(t, string(actualBody), "nonce")
		assert.Equal(t, "application/json", req.Header["Content-Type"][0])
		assert.Equal(t, "/apps/appid/events", req.URL.Path)
		assert.NoError(t, err)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "appid", Key: "key", Secret: "secret", EncryptionMasterKeyBase64: "ZUhQVldIZzduRkdZVkJzS2pPRkRYV1JyaWJJUjJiMGI=", Host: u.Host}
	err := client.Trigger("private-encrypted-test_channel", "test", "yolo1")
	assert.NoError(t, err)
}

func TestTriggerBatchWithEncryptedChanSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		fmt.Fprintf(res, "{}")
		assert.Equal(t, "POST", req.Method)

		_, err := ioutil.ReadAll(req.Body)
		assert.Equal(t, "application/json", req.Header["Content-Type"][0])
		assert.Equal(t, "/apps/appid/batch_events", req.URL.Path)
		assert.NoError(t, err)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "appid", Key: "key", Secret: "secret", EncryptionMasterKeyBase64: "ZUhQVldIZzduRkdZVkJzS2pPRkRYV1JyaWJJUjJiMGI=", Host: u.Host}
	response, err := client.TriggerBatch([]Event{
		{Channel: "test_channel", Name: "test", Data: "yolo1"},
		{Channel: "private-encrypted-test_channel", Name: "test", Data: "yolo2"},
	})
	assert.NoError(t, err)
	assert.Equal(t, &TriggerBatchChannelsList{}, response)
}

func TestTriggerInvalidMasterKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		t.Fatal("No HTTP request should have been made")
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)

	// too short (deprecated)
	client := Client{
		AppID:               "appid",
		Key:                 "key",
		Secret:              "secret",
		Host:                u.Host,
		EncryptionMasterKey: "this is 31 bytes 12345678901234",
	}
	err := client.Trigger("private-encrypted-test_channel", "test", "yolo1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "32 bytes")

	// too long (deprecated)
	client = Client{
		AppID:               "appid",
		Key:                 "key",
		Secret:              "secret",
		Host:                u.Host,
		EncryptionMasterKey: "this is 33 bytes 1234567890123456",
	}
	err = client.Trigger("private-encrypted-test_channel", "test", "yolo1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "32 bytes")

	// both provided
	client = Client{
		AppID:                     "appid",
		Key:                       "key",
		Secret:                    "secret",
		Host:                      u.Host,
		EncryptionMasterKey:       "this is 32 bytes 123456789012345",
		EncryptionMasterKeyBase64: "dGhpcyBpcyAzMiBieXRlcyAxMjM0NTY3ODkwMTIzNDU=",
	}
	err = client.Trigger("private-encrypted-test_channel", "test", "yolo1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "both")

	// too short
	client = Client{
		AppID:                     "appid",
		Key:                       "key",
		Secret:                    "secret",
		Host:                      u.Host,
		EncryptionMasterKeyBase64: "dGhpcyBpcyAzMSBieXRlcyAxMjM0NTY3ODkwMTIzNA==",
	}
	err = client.Trigger("private-encrypted-test_channel", "test", "yolo1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "32 bytes")

	// too long
	client = Client{
		AppID:                     "appid",
		Key:                       "key",
		Secret:                    "secret",
		Host:                      u.Host,
		EncryptionMasterKeyBase64: "dGhpcyBpcyAzMiBieXRlcyAxMjM0NTY3ODkwMTIzNDU2",
	}
	err = client.Trigger("private-encrypted-test_channel", "test", "yolo1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "32 bytes")

	// invalid base64
	client = Client{
		AppID:                     "appid",
		Key:                       "key",
		Secret:                    "secret",
		Host:                      u.Host,
		EncryptionMasterKeyBase64: "dGhp!yBpcyAzMiBieXRlcy#xMjM0NTY3ODkwMTIzNDU=",
	}
	err = client.Trigger("private-encrypted-test_channel", "test", "yolo1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "valid base64")
}

func TestAuthenticateUser(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		t.Fatal("No HTTP request should have been made")
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)

	client := Client{
		AppID:  "appid",
		Key:    "key",
		Secret: "secret",
		Host:   u.Host,
	}

	var params []byte
	var userData map[string]interface{}

	params = []byte("socket_id=12345.12345")
	userData = map[string]interface{}{}
	_, err := client.AuthenticateUser(params, userData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Missing id in user data")

	params = []byte("not_socket_id=12345.12345")
	userData = map[string]interface{}{"id": "1234"}
	_, err = client.AuthenticateUser(params, userData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "socket_id not found")

	params = []byte("socket_id=12345.12345")
	userData = map[string]interface{}{"id": "1234"}
	var response []byte
	response, err = client.AuthenticateUser(params, userData)
	assert.NoError(t, err)
	assert.Equal(t, string(response), "{\"auth\":\"key:e4c63b82c1e1d0955901f6a29ca51b244155bafda93968bc5664010f5ba54a41\",\"user_data\":\"{\\\"id\\\":\\\"1234\\\"}\"}")
}

func TestAuthorizeInvalidMasterKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		t.Fatal("No HTTP request should have been made")
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)

	params := []byte("channel_name=private-encrypted-test_channel&socket_id=12345.12345")

	// too short (deprecated)
	client := Client{
		AppID:               "appid",
		Key:                 "key",
		Secret:              "secret",
		Host:                u.Host,
		EncryptionMasterKey: "this is 31 bytes 12345678901234",
	}
	_, err := client.AuthorizePrivateChannel(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "32 bytes")

	// too long (deprecated)
	client = Client{
		AppID:               "appid",
		Key:                 "key",
		Secret:              "secret",
		Host:                u.Host,
		EncryptionMasterKey: "this is 33 bytes 1234567890123456",
	}
	_, err = client.AuthorizePrivateChannel(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "32 bytes")

	// both provided
	client = Client{
		AppID:                     "appid",
		Key:                       "key",
		Secret:                    "secret",
		Host:                      u.Host,
		EncryptionMasterKey:       "this is 32 bytes 123456789012345",
		EncryptionMasterKeyBase64: "dGhpcyBpcyAzMiBieXRlcyAxMjM0NTY3ODkwMTIzNDU=",
	}
	_, err = client.AuthorizePrivateChannel(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "both")

	// too short
	client = Client{
		AppID:                     "appid",
		Key:                       "key",
		Secret:                    "secret",
		Host:                      u.Host,
		EncryptionMasterKeyBase64: "dGhpcyBpcyAzMSBieXRlcyAxMjM0NTY3ODkwMTIzNA==",
	}
	_, err = client.AuthorizePrivateChannel(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "32 bytes")

	// too long
	client = Client{
		AppID:                     "appid",
		Key:                       "key",
		Secret:                    "secret",
		Host:                      u.Host,
		EncryptionMasterKeyBase64: "dGhpcyBpcyAzMiBieXRlcyAxMjM0NTY3ODkwMTIzNDU2",
	}
	_, err = client.AuthorizePrivateChannel(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "32 bytes")

	// invalid base64
	client = Client{
		AppID:                     "appid",
		Key:                       "key",
		Secret:                    "secret",
		Host:                      u.Host,
		EncryptionMasterKeyBase64: "dGhp!yBpcyAzMiBieXRlcy#xMjM0NTY3ODkwMTIzNDU=",
	}
	_, err = client.AuthorizePrivateChannel(params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "valid base64")
}

func TestErrorResponseHandler(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(400)
		fmt.Fprintf(res, "Cannot retrieve the user count unless the channel is a presence channel")

	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host}
	attributes := "user_count,subscription_count"
	channel, err := client.Channel("this_is_not_a_presence_channel", ChannelParams{Info: &attributes})

	assert.Error(t, err)
	assert.EqualError(t, err, "Status Code: 400 - Cannot retrieve the user count unless the channel is a presence channel")
	assert.Nil(t, channel)
}

func TestRequestTimeouts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		time.Sleep(time.Second * 1)
		// res.WriteHeader(200)
		fmt.Fprintf(res, "{}")
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host, HTTPClient: &http.Client{Timeout: time.Millisecond * 100}}
	err := client.Trigger("test_channel", "test", "yolo")

	assert.Error(t, err)
}

func TestChannelLengthValidation(t *testing.T) {
	channels := []string{
		"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11",
		"12", "13", "14", "15", "16", "17", "18", "19", "20", "21",
		"22", "23", "24", "25", "26", "27", "28", "29", "30", "31",
		"32", "33", "34", "35", "36", "37", "38", "39", "40", "41",
		"42", "43", "44", "45", "46", "47", "48", "49", "50", "51",
		"52", "53", "54", "55", "56", "57", "58", "59", "60", "61",
		"62", "63", "64", "65", "66", "67", "68", "69", "70", "71",
		"72", "73", "74", "75", "76", "77", "78", "79", "80", "81",
		"82", "83", "84", "85", "86", "87", "88", "89", "90", "91",
		"92", "93", "94", "95", "96", "97", "98", "99", "100", "101",
	}

	client := Client{AppID: "id", Key: "key", Secret: "secret"}
	err := client.TriggerMulti(channels, "yolo", "woot")

	assert.EqualError(t, err, "You cannot trigger on more than 100 channels at once")
}

func TestChannelFormatValidation(t *testing.T) {
	channel1 := "w000^$$£@@@"
	var channel2 string
	for i := 0; i <= 202; i++ {
		channel2 += "a"
	}
	client := Client{AppID: "id", Key: "key", Secret: "secret"}
	err1 := client.Trigger(channel1, "yolo", "w00t")

	err2 := client.Trigger(channel2, "yolo", "not 19 forever")

	assert.EqualError(t, err1, "At least one of your channels' names are invalid")

	assert.EqualError(t, err2, "At least one of your channels' names are invalid")

}

func TestDataSizeValidation(t *testing.T) {
	client := Client{AppID: "id", Key: "key", Secret: "secret"}
	data := strings.Repeat("a", 20481)
	err := client.Trigger("channel", "event", data)

	assert.EqualError(t, err, "Event payload exceeded maximum size (20481 bytes is too much)")

	_, err = client.TriggerBatch([]Event{
		{Channel: "channel", Name: "event", Data: data},
	})
	assert.EqualError(t, err, "Data of the event #0 in batch, exceeded maximum size (20481 bytes is too much)")
}

func TestDataSizeOverridenValidation(t *testing.T) {
	client := Client{AppID: "id", Key: "key", Secret: "secret", OverrideMaxMessagePayloadKB: 80}
	data := strings.Repeat("a", 81920)
	err := client.Trigger("channel", "event", data)
	assert.NotContains(t, err.Error(), "\"Event payload exceeded maximum size (81920 bytes is too much)")
	_, err = client.TriggerBatch([]Event{
		{Channel: "channel", Name: "event", Data: data},
	})
	assert.NotContains(t, err.Error(), "Data of the event #0 in batch, exceeded maximum size (81920 bytes is too much)")

	data = strings.Repeat("a", 81921)
	err = client.Trigger("channel", "event", data)
	assert.EqualError(t, err, "Event payload exceeded maximum size (81921 bytes is too much)")

	_, err = client.TriggerBatch([]Event{
		{Channel: "channel", Name: "event", Data: data},
	})
	assert.EqualError(t, err, "Data of the event #0 in batch, exceeded maximum size (81921 bytes is too much)")
}

func TestInitialisationFromURL(t *testing.T) {
	url := "http://feaf18a411d3cb9216ee:fec81108d90e1898e17a@localhost/apps/104060"
	client, _ := ClientFromURL(url)
	expectedClient := &Client{Key: "feaf18a411d3cb9216ee", Secret: "fec81108d90e1898e17a", AppID: "104060", Host: "localhost"}
	assert.Equal(t, expectedClient, client)
}

func TestURLInitErrorNoSecret(t *testing.T) {
	url := "http://fec81108d90e1898e17a@localhost/apps"
	client, err := ClientFromURL(url)
	assert.Nil(t, client)
	assert.Error(t, err)
}

func TestURLInitHTTPS(t *testing.T) {
	url := "https://key:secret@localhost/apps/104060"
	client, _ := ClientFromURL(url)
	assert.True(t, client.Secure)
}

func TestURLInitErrorNoID(t *testing.T) {
	url := "http://fec81108d90e1898e17a@localhost/apps"
	client, err := ClientFromURL(url)
	assert.Nil(t, client)
	assert.Error(t, err)
}

func TestInitialisationFromENV(t *testing.T) {
	os.Setenv("SOCKUDO_URL", "http://feaf18a411d3cb9216ee:fec81108d90e1898e17a@localhost/apps/104060")
	client, _ := ClientFromEnv("SOCKUDO_URL")
	expectedClient := &Client{Key: "feaf18a411d3cb9216ee", Secret: "fec81108d90e1898e17a", AppID: "104060", Host: "localhost"}
	assert.Equal(t, expectedClient, client)
}
