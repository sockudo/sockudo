package sockudo

import (
	"encoding/hex"
	"testing"

	"gopkg.in/stretchr/testify.v1/assert"
)

func TestHmacSignature(t *testing.T) {
	expected := "64e3f44166575febbc5de88c9476325ea7d4b3684752158d9fdb31fce34b980d"
	toSign := "Hello!"
	secret := "supersecret"
	hmac := hmacSignature(toSign, secret)
	assert.Equal(t, expected, hmac)
}

func TestHmacBytes(t *testing.T) {
	expectedHex := "64e3f44166575febbc5de88c9476325ea7d4b3684752158d9fdb31fce34b980d"
	expectedBytes, _ := hex.DecodeString(expectedHex)
	toSign := "Hello!"
	secret := "supersecret"
	hmacBytes := hmacBytes([]byte(toSign), []byte(secret))
	assert.Equal(t, expectedBytes, hmacBytes)
}

func TestCheckValidSignature(t *testing.T) {
	signature := "64e3f44166575febbc5de88c9476325ea7d4b3684752158d9fdb31fce34b980d"
	secret := "supersecret"
	body := "Hello!"
	validSignature := checkSignature(signature, secret, []byte(body))
	assert.Equal(t, true, validSignature)
}

func TestCheckInvalidSignature(t *testing.T) {
	signature := "no"
	secret := "supersecret"
	body := "Hello!"
	validSignature := checkSignature(signature, secret, []byte(body))
	assert.Equal(t, false, validSignature)
}

func TestCreateAuthMapNoE2E(t *testing.T) {
	signature := "64e3f44166575febbc5de88c9476325ea7d4b3684752158d9fdb31fce34b980d"
	key := "key"
	secret := "supersecret"
	stringToSign := "Hello!"
	sharedSecret := ""
	authMap := createAuthMap(key, secret, stringToSign, sharedSecret)
	// The [4:] here removes the prefix of key: from the string.
	assert.Equal(t, signature, authMap["auth"][4:])
	assert.Equal(t, "", authMap["shared_secret"])
}

func TestCreateAuthMapE2E(t *testing.T) {
	signature := "64e3f44166575febbc5de88c9476325ea7d4b3684752158d9fdb31fce34b980d"
	key := "key"
	secret := "supersecret"
	stringToSign := "Hello!"
	sharedSecret := "This is a string that is 32 chars"
	authMap := createAuthMap(key, secret, stringToSign, sharedSecret)
	// The [4:] here removes the prefix of key: from the string.
	assert.Equal(t, signature, authMap["auth"][4:])
	assert.Equal(t, sharedSecret, authMap["shared_secret"])
}

func TestMD5Signature(t *testing.T) {
	expected := "952d2c56d0485958336747bcdd98590d"
	actual := md5Signature([]byte("Hello!"))
	assert.Equal(t, expected, actual)
}

func TestEncrypt(t *testing.T) {
	channel := "private-encrypted-bla"
	body := []byte("Hello!")
	encryptionKey := []byte("This is a string that is 32 chars")
	cipherText := encrypt(channel, body, encryptionKey)
	assert.NotNil(t, cipherText)
	assert.NotEqual(t, body, cipherText)
}

func TestFormatMessage(t *testing.T) {
	nonce := "a"
	cipherText := "b"
	formatted := formatMessage(nonce, cipherText)
	assert.Equal(t, `{"nonce":"a","ciphertext":"b"}`, formatted)
}

func TestGenerateSharedSecret(t *testing.T) {
	channel := "private-encrypted-bla"
	encryptionKey := []byte("This is a string that is 32 chars")
	sharedSecret := generateSharedSecret(channel, encryptionKey)
	t.Log(hex.EncodeToString(sharedSecret[:]))
	expected := "004831f99d2a4e86723e893caded3a2897deeddbed9514fe9497dcddc52bd50b"
	assert.Equal(t, expected, hex.EncodeToString(sharedSecret[:]))
}

func TestDecryptValidKey(t *testing.T) {
	channel := "private-encrypted-bla"
	plaintext := "Hello!"
	cipherText := `{"nonce":"sjklahvpWWQgAjTx5FfYHCCxd2AmaL9T","ciphertext":"zoDEe8dA3nDXKsybAWce/hXGW4szJw=="}`
	encryptionKey := []byte("This is a string that is 32 chars")

	encryptedWebhookData := &Webhook{
		TimeMs: 1,
		Events: []WebhookEvent{
			WebhookEvent{
				Name:     "event",
				Channel:  channel,
				Event:    "event",
				Data:     cipherText,
				SocketID: "44610.7511910",
			},
		},
	}

	expectedWebhookData := &Webhook{
		TimeMs: 1,
		Events: []WebhookEvent{
			WebhookEvent{
				Name:     "event",
				Channel:  channel,
				Event:    "event",
				Data:     plaintext,
				SocketID: "44610.7511910",
			},
		},
	}
	decryptedWebhooks, _ := decryptEvents(*encryptedWebhookData, encryptionKey)
	assert.Equal(t, expectedWebhookData, decryptedWebhooks)
}

func TestDecryptInvalidKey(t *testing.T) {
	channel := "private-encrypted-bla"
	cipherText := `{"nonce":"sjklahvpWWQgAjTx5FfYHCCxd2AmaL9T","ciphertext":"zoDEe8dA3nDXKsybAWce/hXGW4szJw=="}`
	encryptionKey := []byte("This is an invalid key 32 chars!!")

	encryptedWebhookData := &Webhook{
		TimeMs: 1,
		Events: []WebhookEvent{
			WebhookEvent{
				Name:     "event",
				Channel:  channel,
				Event:    "event",
				Data:     cipherText,
				SocketID: "44610.7511910",
			},
		},
	}
	decryptedWebhooks, err := decryptEvents(*encryptedWebhookData, encryptionKey)
	assert.Equal(t, []WebhookEvent(nil), decryptedWebhooks.Events)
	assert.EqualError(t, err, "Failed to decrypt event, possibly wrong key?")
}
