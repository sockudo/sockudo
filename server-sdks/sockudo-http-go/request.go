package sockudo

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
)

var headers = map[string]string{
	"Content-Type":     "application/json",
	"X-Pusher-Library": fmt.Sprintf("%s %s", libraryName, libraryVersion),
}

// change timeout to time.Duration
func request(client *http.Client, method, url string, body []byte) ([]byte, error) {
	return requestWithHeaders(client, method, url, body, nil)
}

func requestWithHeaders(client *http.Client, method, url string, body []byte, extraHeaders map[string]string) ([]byte, error) {
	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	for key, val := range headers {
		req.Header.Set(key, val)
	}
	for key, val := range extraHeaders {
		req.Header.Set(key, val)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return processResponse(resp)
}

func processResponse(response *http.Response) ([]byte, error) {
	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return responseBody, nil
	}
	message := fmt.Sprintf("Status Code: %s - %s", strconv.Itoa(response.StatusCode), string(responseBody))
	err = errors.New(message)
	return nil, err
}
