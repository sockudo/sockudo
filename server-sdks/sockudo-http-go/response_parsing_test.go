package sockudo

import (
	"testing"

	"gopkg.in/stretchr/testify.v1/assert"
)

func TestParsingTriggerChannelsList(t *testing.T) {
	testJSON := []byte(`{"channels":{"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-5cbTiUiPNGI":{},"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-PbZ5E1pP8uF":{"user_count":1},"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-oz6iqpSxMwG":{"user_count":2,"subscription_count":3}}}`)
	expectedUserCount1 := 1
	expectedUserCount2 := 2
	expectedSubscriptionCount2 := 3
	expected := &TriggerChannelsList{
		Channels: map[string]TriggerChannelListItem{
			"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-5cbTiUiPNGI": TriggerChannelListItem{},
			"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-PbZ5E1pP8uF": TriggerChannelListItem{UserCount: &expectedUserCount1},
			"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-oz6iqpSxMwG": TriggerChannelListItem{UserCount: &expectedUserCount2, SubscriptionCount: &expectedSubscriptionCount2},
		},
	}
	result, err := unmarshalledTriggerChannelsList(testJSON)
	assert.Equal(t, expected, result)
	assert.NoError(t, err)
}

func TestParsingChannelsList(t *testing.T) {
	testJSON := []byte(`{"channels":{"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-5cbTiUiPNGI":{"user_count":1},"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-PbZ5E1pP8uF":{"user_count":1},"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-oz6iqpSxMwG":{"user_count":1}}}`)
	expected := &ChannelsList{
		Channels: map[string]ChannelListItem{
			"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-5cbTiUiPNGI": ChannelListItem{UserCount: 1},
			"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-PbZ5E1pP8uF": ChannelListItem{UserCount: 1},
			"presence-session-d41a439c438a100756f5-4bf35003e819bb138249-oz6iqpSxMwG": ChannelListItem{UserCount: 1},
		},
	}
	result, err := unmarshalledChannelsList(testJSON)
	assert.Equal(t, expected, result)
	assert.NoError(t, err)
}

func TestParsingChannel(t *testing.T) {
	testJSON := []byte(`{"user_count":1,"occupied":true,"subscription_count":1}`)
	channelName := "test"
	expected := &Channel{
		Name:              channelName,
		Occupied:          true,
		UserCount:         1,
		SubscriptionCount: 1,
	}
	result, err := unmarshalledChannel(testJSON, channelName)
	assert.Equal(t, expected, result)
	assert.NoError(t, err)

}

func TestParsingChannelUsers(t *testing.T) {
	testJSON := []byte(`{"users":[{"id":"red"},{"id":"blue"}]}`)
	expected := &Users{
		List: []User{User{ID: "red"}, User{ID: "blue"}},
	}
	result, err := unmarshalledChannelUsers(testJSON)
	assert.Equal(t, expected, result)
	assert.NoError(t, err)

}

func TestParserError(t *testing.T) {
	testJSON := []byte("[];;[[p{{}}{{{}[][][]@£$@")
	_, err := unmarshalledChannelsList(testJSON)
	assert.Error(t, err)
}

func TestParsingMutationResponsePreservesU64Serials(t *testing.T) {
	testJSON := []byte(`{"channel":"private-ai-forward","message_serial":"msg-1","action":"update","accepted":true,"version_serial":"ver-1","history_serial":9007199254740993,"delivery_serial":9007199254740994,"status":"accepted","future_field":{"ignored":true}}`)

	result, err := unmarshalledMutationResponse(testJSON)

	assert.NoError(t, err)
	assert.Equal(t, "msg-1", result.MessageSerial)
	if assert.NotNil(t, result.HistorySerial) {
		assert.Equal(t, uint64(9007199254740993), *result.HistorySerial)
	}
	if assert.NotNil(t, result.DeliverySerial) {
		assert.Equal(t, uint64(9007199254740994), *result.DeliverySerial)
	}
}
