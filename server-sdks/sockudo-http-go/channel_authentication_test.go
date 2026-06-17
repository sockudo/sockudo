package sockudo

import (
	"testing"

	"gopkg.in/stretchr/testify.v1/assert"
)

func setUpAuthClient() Client {
	return Client{
		Key:    "278d425bdf160c739803",
		Secret: "7ad3773142a6692b25b8",
	}
}

func TestPrivateChannelAuthentication(t *testing.T) {
	client := setUpAuthClient()
	postParams := []byte("channel_name=private-foobar&socket_id=1234.1234")
	expected := `{"auth":"278d425bdf160c739803:58df8b0c36d6982b82c3ecf6b4662e34fe8c25bba48f5369f135bf843651c3a4"}`
	result, err := client.AuthenticatePrivateChannel(postParams)
	assert.Equal(t, expected, string(result))
	assert.NoError(t, err)
}

func TestPrivateChannelAuthenticationWrongParams(t *testing.T) {
	client := setUpAuthClient()
	postParams := []byte("hello=hi&two=3")
	_, err := client.AuthenticatePrivateChannel(postParams)
	assert.Error(t, err)
}

func TestPresenceChannelAuthentication(t *testing.T) {
	client := setUpAuthClient()
	postParams := []byte("channel_name=presence-foobar&socket_id=1234.1234")
	presenceData := MemberData{UserID: "10", UserInfo: map[string]string{"name": "Mr. Sockudo"}}
	expected := `{"auth":"278d425bdf160c739803:6aa46f081231a05c0d8a13377261ec8d23671be2d64764e0681fb44dd39a57ad","channel_data":"{\"user_id\":\"10\",\"user_info\":{\"name\":\"Mr. Sockudo\"}}"}`
	result, err := client.AuthenticatePresenceChannel(postParams, presenceData)
	assert.Equal(t, expected, string(result))
	assert.NoError(t, err)
}

func TestAuthSocketIDValidation(t *testing.T) {
	client := setUpAuthClient()
	postParams := []byte("channel_name=private-foobar&socket_id=12341234")
	result, err := client.AuthenticatePrivateChannel(postParams)
	assert.Nil(t, result)
	assert.Error(t, err)
}
