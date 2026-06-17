package sockudo

import (
	"net/url"
	"strings"
)

const authVersion = "1.0"

func unsignedParams(key, timestamp string, body []byte, parameters map[string]string) url.Values {
	params := url.Values{
		"auth_key":       {key},
		"auth_timestamp": {timestamp},
		"auth_version":   {authVersion},
	}

	if body != nil {
		params.Add("body_md5", md5Signature(body))
	}

	for key, values := range parameters {
		params.Add(key, values)
	}

	return params

}

func unescapeURL(_url url.Values) string {
	unesc, _ := url.QueryUnescape(_url.Encode())
	return unesc
}

func lowercaseParamKeys(values url.Values) url.Values {
	lowercased := url.Values{}
	for key, valueSet := range values {
		for _, value := range valueSet {
			lowercased.Add(strings.ToLower(key), value)
		}
	}
	return lowercased
}

func createRequestURL(method, host, path, key, secret, timestamp string, secure bool, body []byte, parameters map[string]string, cluster string) (string, error) {
	params := unsignedParams(key, timestamp, body, parameters)

	stringToSign := strings.Join([]string{method, path, unescapeURL(lowercaseParamKeys(params))}, "\n")

	authSignature := hmacSignature(stringToSign, secret)

	params.Add("auth_signature", authSignature)

	if host == "" {
		if cluster != "" {
			host = "api-" + cluster + ".sockudo.com"
		} else {
			host = "localhost"
		}
	}
	var base string
	if secure {
		base = "https://"
	} else {
		base = "http://"
	}
	base += host

	endpoint, err := url.ParseRequestURI(base + path)
	if err != nil {
		return "", err
	}
	endpoint.RawQuery = unescapeURL(params)

	return endpoint.String(), nil
}
