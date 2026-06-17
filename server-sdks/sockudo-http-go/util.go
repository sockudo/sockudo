package sockudo

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var channelValidationRegex = regexp.MustCompile("^[-a-zA-Z0-9_=@,.;:]+$")
var socketIDValidationRegex = regexp.MustCompile(`\A\d+\.\d+\z`)
var maxChannelNameSize = 200

func jsonMarshalToString(data interface{}) (result string, err error) {
	var _result []byte
	_result, err = json.Marshal(data)
	if err != nil {
		return
	}
	return string(_result), err
}

func authTimestamp() string {
	return strconv.FormatInt(time.Now().Unix(), 10)
}

func parseUserAuthenticationRequestParams(_params []byte) (socketID string, err error) {
	params, err := url.ParseQuery(string(_params))
	if err != nil {
		return
	}
	if _, ok := params["socket_id"]; !ok {
		return "", errors.New("socket_id not found")
	}
	return params["socket_id"][0], nil
}

func parseChannelAuthorizationRequestParams(_params []byte) (channelName string, socketID string, err error) {
	params, err := url.ParseQuery(string(_params))
	if err != nil {
		return
	}
	if _, ok := params["channel_name"]; !ok {
		return "", "", errors.New("channel_name not found")
	}
	if _, ok := params["socket_id"]; !ok {
		return "", "", errors.New("socket_id not found")
	}
	return params["channel_name"][0], params["socket_id"][0], nil
}

func validUserId(userId string) bool {
	length := len(userId)
	return length > 0 && length < maxChannelNameSize
}

func validChannel(channel string) bool {
	if len(channel) > maxChannelNameSize || !channelValidationRegex.MatchString(channel) {
		return false
	}
	return true
}

func channelsAreValid(channels []string) bool {
	for _, channel := range channels {
		if !validChannel(channel) {
			return false
		}
	}
	return true
}

func isEncryptedChannel(channel string) bool {
	return strings.HasPrefix(channel, "private-encrypted-")
}

func validateUserData(userData map[string]interface{}) (err error) {
	_id, ok := userData["id"]
	if !ok || _id == nil {
		return errors.New("Missing id in user data")
	}
	var id string
	id, ok = _id.(string)
	if !ok {
		return errors.New("id field in user data is not a string")
	}
	if !validUserId(id) {
		return fmt.Errorf("Invalid id in user data: '%s'", id)
	}
	return
}

func validateSocketID(socketID *string) (err error) {
	if (socketID == nil) || socketIDValidationRegex.MatchString(*socketID) {
		return
	}
	return errors.New("socket_id invalid")
}

// GenerateIdempotencyKey returns a new UUID v4 string suitable for use as an
// idempotency key in trigger requests.
func GenerateIdempotencyKey() string {
	var uuid [16]byte
	_, _ = rand.Read(uuid[:])
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}
