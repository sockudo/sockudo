//! `private-encrypted-*` payload decryption (NaCl secretbox).

use std::fmt;

use base64::Engine as _;
#[cfg(feature = "encryption")]
use serde::Deserialize;

use crate::error::{Error, Result};
use crate::event::Payload;

/// A 32-byte channel key. Zeroed on drop is not guaranteed by the cipher
/// crate, so the key is kept only as long as the channel is subscribed.
#[derive(Clone)]
#[cfg_attr(not(feature = "encryption"), allow(dead_code))]
pub(crate) struct SharedSecret([u8; 32]);

impl fmt::Debug for SharedSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SharedSecret(<redacted>)")
    }
}

impl SharedSecret {
    pub(crate) fn from_base64(encoded: &str) -> Result<Self> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded.trim())
            .map_err(|_| Error::auth(None, "shared_secret is not valid base64"))?;
        let key: [u8; 32] = bytes
            .try_into()
            .map_err(|_| Error::auth(None, "shared_secret must decode to 32 bytes"))?;
        Ok(Self(key))
    }
}

#[cfg(feature = "encryption")]
#[derive(Deserialize)]
struct Envelope {
    ciphertext: String,
    nonce: String,
}

/// Decrypt an encrypted-channel payload `{"ciphertext", "nonce"}` into the
/// plaintext JSON text.
#[cfg(feature = "encryption")]
pub(crate) fn decrypt(secret: &SharedSecret, payload: &Payload) -> Result<String> {
    use crypto_secretbox::XSalsa20Poly1305;
    use crypto_secretbox::aead::{Aead, KeyInit};

    let envelope: Envelope = payload
        .json()
        .map_err(|_| Error::Decryption("payload is not a {ciphertext, nonce} object".into()))?;
    let engine = base64::engine::general_purpose::STANDARD;
    let ciphertext = engine
        .decode(envelope.ciphertext.as_bytes())
        .map_err(|_| Error::Decryption("ciphertext is not valid base64".into()))?;
    let nonce = engine
        .decode(envelope.nonce.as_bytes())
        .map_err(|_| Error::Decryption("nonce is not valid base64".into()))?;
    let nonce: [u8; 24] = nonce
        .try_into()
        .map_err(|_| Error::Decryption("nonce must be 24 bytes".into()))?;
    let cipher = XSalsa20Poly1305::new_from_slice(&secret.0)
        .map_err(|_| Error::Decryption("invalid key length".into()))?;
    let plaintext = cipher
        .decrypt(&nonce.into(), ciphertext.as_slice())
        .map_err(|_| {
            Error::Decryption("authentication failed (wrong key or tampered data)".into())
        })?;
    String::from_utf8(plaintext).map_err(|_| Error::Decryption("plaintext is not UTF-8".into()))
}

#[cfg(not(feature = "encryption"))]
pub(crate) fn decrypt(_secret: &SharedSecret, _payload: &Payload) -> Result<String> {
    Err(Error::unsupported(
        "encrypted channels require the `encryption` feature",
    ))
}

#[cfg(all(test, feature = "encryption"))]
pub(crate) mod tests {
    use super::*;
    use crypto_secretbox::XSalsa20Poly1305;
    use crypto_secretbox::aead::{Aead, KeyInit};

    /// Encrypt like the server SDKs do (exposed for integration-style tests).
    pub(crate) fn encrypt(key: &[u8; 32], nonce: [u8; 24], plaintext: &str) -> String {
        let cipher = XSalsa20Poly1305::new_from_slice(key).unwrap();
        let ciphertext = cipher.encrypt(&nonce.into(), plaintext.as_bytes()).unwrap();
        let engine = base64::engine::general_purpose::STANDARD;
        sonic_rs::to_string(&sonic_rs::json!({
            "ciphertext": engine.encode(ciphertext),
            "nonce": engine.encode(nonce),
        }))
        .unwrap()
    }

    #[test]
    fn decrypts_and_rejects_tampering() {
        let key = [7u8; 32];
        let secret =
            SharedSecret::from_base64(&base64::engine::general_purpose::STANDARD.encode(key))
                .unwrap();
        let encrypted = encrypt(&key, [1u8; 24], r#"{"doc":"hello"}"#);
        assert_eq!(
            decrypt(&secret, &Payload::Text(encrypted.clone())).unwrap(),
            r#"{"doc":"hello"}"#
        );
        let wrong = SharedSecret([8u8; 32]);
        assert!(decrypt(&wrong, &Payload::Text(encrypted)).is_err());
        assert!(decrypt(&secret, &Payload::Text("{}".into())).is_err());
    }

    #[test]
    fn validates_shared_secret() {
        assert!(SharedSecret::from_base64("not base64!").is_err());
        assert!(SharedSecret::from_base64("AAAA").is_err());
        assert!(format!("{:?}", SharedSecret([0; 32])).contains("redacted"));
    }
}
