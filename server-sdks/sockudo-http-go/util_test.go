package sockudo

import (
	"regexp"
	"testing"

	"gopkg.in/stretchr/testify.v1/assert"
)

func TestParseUserAuthenticationRequestParamsNoSock(t *testing.T) {
	params := "abc=hello"
	_, result := parseUserAuthenticationRequestParams([]byte(params))
	assert.Error(t, result)
	assert.EqualError(t, result, "socket_id not found")
}

func TestInvalidUserAuthenticationParams(t *testing.T) {
	params := "%$@£$${}$£%|$^%$^|"
	_, result := parseUserAuthenticationRequestParams([]byte(params))
	assert.Error(t, result)
}

func TestUserAuthenticationParamsSuccess(t *testing.T) {
	params := "socket_id=123"
	socket_id, result := parseUserAuthenticationRequestParams([]byte(params))
	assert.Equal(t, socket_id, "123")
	assert.NoError(t, result)
}

func TestParseChannelAuthorizationRequestParamsNoSock(t *testing.T) {
	params := "channel_name=hello"
	_, _, result := parseChannelAuthorizationRequestParams([]byte(params))
	assert.Error(t, result)
	assert.EqualError(t, result, "socket_id not found")
}

func TestParseChannelAuthorizationRequestParamsNoChan(t *testing.T) {
	params := "socket_id=45.3"
	_, _, result := parseChannelAuthorizationRequestParams([]byte(params))
	assert.Error(t, result)
	assert.EqualError(t, result, "channel_name not found")
}

func TestInvalidChannelAuthorizationParams(t *testing.T) {
	params := "%$@£$${}$£%|$^%$^|"
	_, _, result := parseChannelAuthorizationRequestParams([]byte(params))
	assert.Error(t, result)
}

func TestValidateUserDataSuccess(t *testing.T) {
	m := map[string]interface{}{
		"id":    "12345",
		"email": "test@test.com",
	}
	err := validateUserData(m)
	assert.NoError(t, err)
}

func TestValidateUserDataNoId(t *testing.T) {
	m := map[string]interface{}{
		"email": "test@test.com",
	}
	err := validateUserData(m)
	assert.EqualError(t, err, "Missing id in user data")
}

func TestValidateUserDataIdIsNotString(t *testing.T) {
	m := map[string]interface{}{
		"id":    123,
		"email": "test@test.com",
	}
	err := validateUserData(m)
	assert.EqualError(t, err, "id field in user data is not a string")
}

func TestValidateUserDataInvalidId(t *testing.T) {
	m := map[string]interface{}{
		"id":    "",
		"email": "test@test.com",
	}
	err := validateUserData(m)
	assert.EqualError(t, err, "Invalid id in user data: ''")
}

func TestGenerateIdempotencyKeyFormat(t *testing.T) {
	key := GenerateIdempotencyKey()
	uuidRegex := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	assert.Regexp(t, uuidRegex, key)
}

func TestGenerateIdempotencyKeyUnique(t *testing.T) {
	key1 := GenerateIdempotencyKey()
	key2 := GenerateIdempotencyKey()
	assert.NotEqual(t, key1, key2)
}
