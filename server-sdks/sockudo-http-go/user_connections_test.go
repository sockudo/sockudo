package sockudo

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"gopkg.in/stretchr/testify.v1/assert"
)

func TestTerminateUserConnectionsInvalidUserId(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		t.Fatal("No request should reach the API")
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host}
	err := client.TerminateUserConnections("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "User id '' is invalid")
}

func TestTerminateUserConnectionsSuccessCase(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		fmt.Fprintf(res, "{}")
		assert.Equal(t, "POST", req.Method)
		assert.True(t, strings.Contains(req.URL.Path, "/users/user-123/terminate_connections"))
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host}
	err := client.TerminateUserConnections("user-123")
	assert.NoError(t, err)
}

func TestForceReconnectUserInvalidUserId(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		t.Fatal("No request should reach the API")
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host}
	err := client.ForceReconnectUser("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "User id '' is invalid")
}

func TestForceReconnectUserSuccessCase(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		fmt.Fprintf(res, "{}")
		assert.Equal(t, "POST", req.Method)
		assert.True(t, strings.Contains(req.URL.Path, "/users/user-123/force_reconnect"))
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	client := Client{AppID: "id", Key: "key", Secret: "secret", Host: u.Host}
	err := client.ForceReconnectUser("user-123")
	assert.NoError(t, err)
}
