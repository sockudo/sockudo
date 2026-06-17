package sockudo

import (
	"testing"

	"gopkg.in/stretchr/testify.v1/assert"
)

func TestTriggerRequestUrl(t *testing.T) {
	expected := "http://localhost/apps/3/events?auth_key=278d425bdf160c739803&auth_signature=da454824c97ba181a32ccc17a72625ba02771f50b50e1e7430e47a1f3f457e6c&auth_timestamp=1353088179&auth_version=1.0&body_md5=ec365a775a4cd0599faeb73354201b6f"
	payload := []byte(`{"name":"foo","channels":["project-3"],"data":"{\"some\":\"data\"}"}`)
	result, _ := createRequestURL("POST", "", "/apps/3/events", "278d425bdf160c739803", "7ad3773142a6692b25b8", "1353088179", false, payload, nil, "")
	assert.Equal(t, expected, result)
}

func TestBuildClusterTriggerUrl(t *testing.T) {
	expected := "http://api-eu.sockudo.com/apps/3/events?auth_key=278d425bdf160c739803&auth_signature=da454824c97ba181a32ccc17a72625ba02771f50b50e1e7430e47a1f3f457e6c&auth_timestamp=1353088179&auth_version=1.0&body_md5=ec365a775a4cd0599faeb73354201b6f"
	payload := []byte(`{"name":"foo","channels":["project-3"],"data":"{\"some\":\"data\"}"}`)
	result, _ := createRequestURL("POST", "", "/apps/3/events", "278d425bdf160c739803", "7ad3773142a6692b25b8", "1353088179", false, payload, nil, "eu")
	assert.Equal(t, expected, result)
}

func TestBuildCustomHostTriggerUrl(t *testing.T) {
	expected := "http://my.server.com/apps/3/events?auth_key=278d425bdf160c739803&auth_signature=da454824c97ba181a32ccc17a72625ba02771f50b50e1e7430e47a1f3f457e6c&auth_timestamp=1353088179&auth_version=1.0&body_md5=ec365a775a4cd0599faeb73354201b6f"
	payload := []byte(`{"name":"foo","channels":["project-3"],"data":"{\"some\":\"data\"}"}`)
	result, _ := createRequestURL("POST", "my.server.com", "/apps/3/events", "278d425bdf160c739803", "7ad3773142a6692b25b8", "1353088179", false, payload, nil, "")
	assert.Equal(t, expected, result)
}

func TestTriggerSecureRequestUrl(t *testing.T) {
	expected := "https://localhost/apps/3/events?auth_key=278d425bdf160c739803&auth_signature=da454824c97ba181a32ccc17a72625ba02771f50b50e1e7430e47a1f3f457e6c&auth_timestamp=1353088179&auth_version=1.0&body_md5=ec365a775a4cd0599faeb73354201b6f"
	payload := []byte(`{"name":"foo","channels":["project-3"],"data":"{\"some\":\"data\"}"}`)
	result, _ := createRequestURL("POST", "", "/apps/3/events", "278d425bdf160c739803", "7ad3773142a6692b25b8", "1353088179", true, payload, nil, "")
	assert.Equal(t, expected, result)
}

func TestGetAllChannelsUrl(t *testing.T) {
	expected := "http://localhost/apps/102015/channels?auth_key=d41a439c438a100756f5&auth_signature=4d8a02edcc8a758b0162cd6da690a9a45fb8ae326a276dca1e06a0bc42796c11&auth_timestamp=1427034994&auth_version=1.0&filter_by_prefix=presence-&info=user_count"
	additionalQueries := map[string]string{"filter_by_prefix": "presence-", "info": "user_count"}
	result, _ := createRequestURL("GET", "", "/apps/102015/channels", "d41a439c438a100756f5", "4bf35003e819bb138249", "1427034994", false, nil, additionalQueries, "")
	assert.Equal(t, expected, result)
}

func TestGetAllChannelsWithOneAdditionalParamUrl(t *testing.T) {
	expected := "http://localhost/apps/102015/channels?auth_key=d41a439c438a100756f5&auth_signature=b540383af4582af5fbb5df7be5472d54bd0838c9c2021c7743062568839e6f97&auth_timestamp=1427036577&auth_version=1.0&filter_by_prefix=presence-"
	additionalQueries := map[string]string{"filter_by_prefix": "presence-"}
	result, _ := createRequestURL("GET", "", "/apps/102015/channels", "d41a439c438a100756f5", "4bf35003e819bb138249", "1427036577", false, nil, additionalQueries, "")
	assert.Equal(t, expected, result)
}

func TestGetAllChannelsWithNoParamsUrl(t *testing.T) {
	expected := "http://localhost/apps/102015/channels?auth_key=d41a439c438a100756f5&auth_signature=df89248f87f6e6d028925e0b04d60f316527a865992ace6936afa91281d8bef0&auth_timestamp=1427036787&auth_version=1.0"
	additionalQueries := map[string]string{}
	result, _ := createRequestURL("GET", "", "/apps/102015/channels", "d41a439c438a100756f5", "4bf35003e819bb138249", "1427036787", false, nil, additionalQueries, "")
	assert.Equal(t, expected, result)
}

func TestRequestURLSignsLowercaseQueryKeysButKeepsOriginalRequestKeys(t *testing.T) {
	additionalQueries := map[string]string{"deviceId": "device-1", "limit": "10"}
	result, _ := createRequestURL("GET", "", "/apps/102015/push/channelSubscriptions", "d41a439c438a100756f5", "4bf35003e819bb138249", "1427036787", false, nil, additionalQueries, "")

	expectedSignature := hmacSignature("GET\n/apps/102015/push/channelSubscriptions\nauth_key=d41a439c438a100756f5&auth_timestamp=1427036787&auth_version=1.0&deviceid=device-1&limit=10", "4bf35003e819bb138249")
	assert.Contains(t, result, "deviceId=device-1")
	assert.NotContains(t, result, "deviceid=device-1")
	assert.Contains(t, result, "auth_signature="+expectedSignature)
}

func TestGetChannelUrl(t *testing.T) {
	expected := "http://localhost/apps/102015/channels/presence-session-d41a439c438a100756f5-4bf35003e819bb138249-ROpCFmgFhXY?auth_key=d41a439c438a100756f5&auth_signature=f93ceb31f396aef336226efe512aaf339bd5e39c7c2c04b81cc8681dc16ee785&auth_timestamp=1427053326&auth_version=1.0&info=user_count,subscription_count"
	additionalQueries := map[string]string{"info": "user_count,subscription_count"}
	result, _ := createRequestURL("GET", "", "/apps/102015/channels/presence-session-d41a439c438a100756f5-4bf35003e819bb138249-ROpCFmgFhXY", "d41a439c438a100756f5", "4bf35003e819bb138249", "1427053326", false, nil, additionalQueries, "")
	assert.Equal(t, expected, result)
}

func TestGetUsersUrl(t *testing.T) {
	expected := "http://localhost/apps/102015/channels/presence-session-d41a439c438a100756f5-4bf35003e819bb138249-nYJLy67qh52/users?auth_key=d41a439c438a100756f5&auth_signature=207feaf4e8efeb24e5f148011704251bf90e2059a5f97a3eb52d06178b11feca&auth_timestamp=1427053709&auth_version=1.0"
	result, _ := createRequestURL("GET", "", "/apps/102015/channels/presence-session-d41a439c438a100756f5-4bf35003e819bb138249-nYJLy67qh52/users", "d41a439c438a100756f5", "4bf35003e819bb138249", "1427053709", false, nil, nil, "")
	assert.Equal(t, expected, result)
}

func TestBrokenUrl(t *testing.T) {
	result, err := createRequestURL("GET", "", "#%$)(!foo", "d41a439c438a100756f5", "4bf35003e819bb138249", "1427053709", false, nil, nil, "")
	assert.Error(t, err)
	assert.Equal(t, "", result)
}
