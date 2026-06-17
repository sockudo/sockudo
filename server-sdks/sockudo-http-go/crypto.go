package sockudo

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"golang.org/x/crypto/nacl/secretbox"
)

// EncryptedMessage contains an encrypted message
type EncryptedMessage struct {
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
}

func hmacSignature(toSign, secret string) string {
	return hex.EncodeToString(hmacBytes([]byte(toSign), []byte(secret)))
}

func hmacBytes(toSign, secret []byte) []byte {
	_authSignature := hmac.New(sha256.New, secret)
	_authSignature.Write(toSign)
	return _authSignature.Sum(nil)
}

func checkSignature(result, secret string, body []byte) bool {
	expected := hmacBytes(body, []byte(secret))
	resultBytes, err := hex.DecodeString(result)
	if err != nil {
		return false
	}
	return hmac.Equal(expected, resultBytes)
}

func createAuthMap(key, secret, stringToSign string, sharedSecret string) map[string]string {
	authSignature := hmacSignature(stringToSign, secret)
	authString := strings.Join([]string{key, authSignature}, ":")
	if sharedSecret != "" {
		return map[string]string{"auth": authString, "shared_secret": sharedSecret}
	}
	return map[string]string{"auth": authString}
}

func md5Signature(body []byte) string {
	_bodyMD5 := md5.New()
	_bodyMD5.Write([]byte(body))
	return hex.EncodeToString(_bodyMD5.Sum(nil))
}

func encrypt(channel string, data []byte, encryptionKey []byte) string {
	sharedSecret := generateSharedSecret(channel, encryptionKey)
	nonce := generateNonce()
	nonceB64 := base64.StdEncoding.EncodeToString(nonce[:])
	cipherText := secretbox.Seal([]byte{}, data, &nonce, &sharedSecret)
	cipherTextB64 := base64.StdEncoding.EncodeToString(cipherText)
	return formatMessage(nonceB64, cipherTextB64)
}

func formatMessage(nonce string, cipherText string) string {
	encryptedMessage := &EncryptedMessage{
		Nonce:      nonce,
		Ciphertext: cipherText,
	}
	json, err := json.Marshal(encryptedMessage)
	if err != nil {
		panic(err)
	}

	return string(json)
}

func generateNonce() [24]byte {
	var nonce [24]byte
	//Trick ReadFull into thinking nonce is a slice
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		panic(err)
	}
	return nonce
}

func generateSharedSecret(channel string, encryptionKey []byte) [32]byte {
	return sha256.Sum256(append([]byte(channel), encryptionKey...))
}

func decryptEvents(webhookData Webhook, encryptionKey []byte) (*Webhook, error) {
	decryptedWebhooks := &Webhook{}
	decryptedWebhooks.TimeMs = webhookData.TimeMs
	for _, event := range webhookData.Events {
		if isEncryptedChannel(event.Channel) {
			var encryptedMessage EncryptedMessage
			if err := json.Unmarshal([]byte(event.Data), &encryptedMessage); err != nil {
				return decryptedWebhooks, err
			}
			cipherTextBytes, decodePayloadErr := base64.StdEncoding.DecodeString(encryptedMessage.Ciphertext)
			if decodePayloadErr != nil {
				return decryptedWebhooks, decodePayloadErr
			}
			nonceBytes, decodeNonceErr := base64.StdEncoding.DecodeString(encryptedMessage.Nonce)
			if decodeNonceErr != nil {
				return decryptedWebhooks, decodeNonceErr
			}
			// Convert slice to fixed length array for secretbox
			var nonce [24]byte
			copy(nonce[:], []byte(nonceBytes[:]))

			sharedSecret := generateSharedSecret(event.Channel, encryptionKey)
			box := []byte(cipherTextBytes)
			decryptedBox, ok := secretbox.Open([]byte{}, box, &nonce, &sharedSecret)
			if !ok {
				return decryptedWebhooks, errors.New("Failed to decrypt event, possibly wrong key?")
			}
			event.Data = string(decryptedBox)
		}
		decryptedWebhooks.Events = append(decryptedWebhooks.Events, event)
	}
	return decryptedWebhooks, nil
}
