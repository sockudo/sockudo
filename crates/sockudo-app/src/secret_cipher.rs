//! Optional at-rest encryption of app secrets for the SQL app managers.
//!
//! When a key is configured, the app manager stores each secret in the
//! `secret_enc` column as `v1:<base64(nonce || ciphertext || tag)>`, sealed
//! with AES-256-GCM. The app id is bound as associated data, so a ciphertext
//! copied onto another app's row fails to decrypt. The plaintext `secret`
//! column is then written empty.
//!
//! The key comes from `SOCKUDO_APP_SECRET_ENCRYPTION_KEY` or from the file
//! named by `SOCKUDO_APP_SECRET_ENCRYPTION_KEY_FILE` (for secret stores that
//! mount files), as 32 bytes encoded in base64 or hex. Without a key, rows
//! with a plaintext `secret` keep working exactly as before.

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use rand::RngCore;
use sockudo_core::error::{Error, Result};
use std::fmt;

/// Environment variable holding the encoded 32-byte key.
pub const KEY_ENV: &str = "SOCKUDO_APP_SECRET_ENCRYPTION_KEY";
/// Environment variable naming a file that holds the encoded 32-byte key.
pub const KEY_FILE_ENV: &str = "SOCKUDO_APP_SECRET_ENCRYPTION_KEY_FILE";

const ENVELOPE_PREFIX: &str = "v1:";
const AAD_DOMAIN: &[u8] = b"sockudo-app-secret-v1:";
const NONCE_LEN: usize = 12;

/// AES-256-GCM cipher for app secrets.
#[derive(Clone)]
pub struct AppSecretCipher {
    cipher: Aes256Gcm,
}

impl fmt::Debug for AppSecretCipher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AppSecretCipher(..)")
    }
}

impl AppSecretCipher {
    /// Builds a cipher from a base64- or hex-encoded 32-byte key.
    pub fn from_encoded_key(encoded: &str) -> Result<Self> {
        let encoded = encoded.trim();
        let key = decode_key(encoded).ok_or_else(|| {
            Error::Configuration(format!(
                "app secret encryption key must be 32 bytes encoded as base64 or hex ({KEY_ENV} / {KEY_FILE_ENV})"
            ))
        })?;
        let cipher = Aes256Gcm::new_from_slice(&key)
            .map_err(|e| Error::Configuration(format!("invalid app secret encryption key: {e}")))?;
        Ok(Self { cipher })
    }

    /// Loads the key from [`KEY_ENV`] or [`KEY_FILE_ENV`]. Returns `Ok(None)`
    /// when neither is set, and an error when one is set but unusable, so a
    /// misconfigured key fails startup instead of silently disabling
    /// encryption.
    pub fn from_env() -> Result<Option<Self>> {
        let inline = std::env::var(KEY_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty());
        let file = std::env::var(KEY_FILE_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty());
        match (inline, file) {
            (Some(_), Some(_)) => Err(Error::Configuration(format!(
                "set only one of {KEY_ENV} and {KEY_FILE_ENV}"
            ))),
            (Some(key), None) => Self::from_encoded_key(&key).map(Some),
            (None, Some(path)) => {
                let key = std::fs::read_to_string(path.trim()).map_err(|e| {
                    Error::Configuration(format!("failed to read {KEY_FILE_ENV}: {e}"))
                })?;
                Self::from_encoded_key(&key).map(Some)
            }
            (None, None) => Ok(None),
        }
    }

    /// Seals `secret` for the app `app_id`.
    pub fn encrypt(&self, app_id: &str, secret: &str) -> Result<String> {
        let mut nonce_bytes = [0_u8; NONCE_LEN];
        rand::rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from(nonce_bytes);
        let aad = associated_data(app_id);
        let sealed = self
            .cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: secret.as_bytes(),
                    aad: &aad,
                },
            )
            .map_err(|_| Error::Internal("failed to encrypt app secret".to_string()))?;
        let mut envelope = Vec::with_capacity(NONCE_LEN + sealed.len());
        envelope.extend_from_slice(&nonce_bytes);
        envelope.extend_from_slice(&sealed);
        Ok(format!("{ENVELOPE_PREFIX}{}", STANDARD.encode(envelope)))
    }

    /// Opens a `secret_enc` value sealed for the app `app_id`.
    pub fn decrypt(&self, app_id: &str, envelope: &str) -> Result<String> {
        let encoded = envelope
            .trim()
            .strip_prefix(ENVELOPE_PREFIX)
            .ok_or_else(|| invalid_envelope(app_id, "unknown envelope version"))?;
        let raw = STANDARD
            .decode(encoded)
            .map_err(|_| invalid_envelope(app_id, "envelope is not valid base64"))?;
        let (nonce_bytes, sealed) = raw
            .split_at_checked(NONCE_LEN)
            .ok_or_else(|| invalid_envelope(app_id, "envelope is too short"))?;
        let nonce = Nonce::try_from(nonce_bytes)
            .map_err(|_| invalid_envelope(app_id, "envelope has an invalid nonce"))?;
        let aad = associated_data(app_id);
        let plaintext = self
            .cipher
            .decrypt(
                &nonce,
                Payload {
                    msg: sealed,
                    aad: &aad,
                },
            )
            .map_err(|_| {
                invalid_envelope(
                    app_id,
                    "authentication failed (wrong key, or ciphertext belongs to another app)",
                )
            })?;
        String::from_utf8(plaintext).map_err(|_| invalid_envelope(app_id, "secret is not UTF-8"))
    }
}

/// Resolves the secret of a stored row. An encrypted `secret_enc` takes
/// precedence over the plaintext column and requires a configured key; a
/// row without `secret_enc` keeps using its plaintext `secret`.
pub fn resolve_row_secret(
    cipher: Option<&AppSecretCipher>,
    app_id: &str,
    plaintext: String,
    secret_enc: Option<&str>,
) -> Result<String> {
    match secret_enc.map(str::trim).filter(|value| !value.is_empty()) {
        Some(envelope) => match cipher {
            Some(cipher) => cipher.decrypt(app_id, envelope),
            None => Err(Error::Configuration(format!(
                "app '{app_id}' has an encrypted secret but no {KEY_ENV} or {KEY_FILE_ENV} is configured"
            ))),
        },
        None => Ok(plaintext),
    }
}

/// Splits a secret into the `(secret, secret_enc)` column values to store.
pub fn seal_row_secret(
    cipher: Option<&AppSecretCipher>,
    app_id: &str,
    secret: &str,
) -> Result<(String, Option<String>)> {
    match cipher {
        Some(cipher) => Ok((String::new(), Some(cipher.encrypt(app_id, secret)?))),
        None => Ok((secret.to_string(), None)),
    }
}

fn associated_data(app_id: &str) -> Vec<u8> {
    let mut aad = Vec::with_capacity(AAD_DOMAIN.len() + app_id.len());
    aad.extend_from_slice(AAD_DOMAIN);
    aad.extend_from_slice(app_id.as_bytes());
    aad
}

fn invalid_envelope(app_id: &str, reason: &str) -> Error {
    Error::Internal(format!("cannot decrypt secret of app '{app_id}': {reason}"))
}

fn decode_key(encoded: &str) -> Option<[u8; 32]> {
    let bytes = if encoded.len() == 64 && encoded.bytes().all(|b| b.is_ascii_hexdigit()) {
        decode_hex(encoded)?
    } else {
        STANDARD.decode(encoded).ok()?
    };
    bytes.try_into().ok()
}

fn decode_hex(encoded: &str) -> Option<Vec<u8>> {
    (0..encoded.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(encoded.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY_B64: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
    const KEY_HEX: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    #[test]
    fn round_trips_and_uses_fresh_nonces() {
        let cipher = AppSecretCipher::from_encoded_key(KEY_B64).unwrap();
        let first = cipher.encrypt("app-1", "s3cret").unwrap();
        let second = cipher.encrypt("app-1", "s3cret").unwrap();
        assert!(first.starts_with("v1:"));
        assert_ne!(first, second);
        assert!(!first.contains("s3cret"));
        assert_eq!(cipher.decrypt("app-1", &first).unwrap(), "s3cret");
    }

    #[test]
    fn hex_and_base64_keys_are_equivalent() {
        let b64 = AppSecretCipher::from_encoded_key(KEY_B64).unwrap();
        let hex = AppSecretCipher::from_encoded_key(KEY_HEX).unwrap();
        let sealed = b64.encrypt("app-1", "s3cret").unwrap();
        assert_eq!(hex.decrypt("app-1", &sealed).unwrap(), "s3cret");
    }

    #[test]
    fn ciphertext_is_bound_to_its_app_id() {
        let cipher = AppSecretCipher::from_encoded_key(KEY_B64).unwrap();
        let sealed = cipher.encrypt("app-1", "s3cret").unwrap();
        let error = cipher.decrypt("app-2", &sealed).unwrap_err().to_string();
        assert!(error.contains("app-2"), "{error}");
        assert!(!error.contains("s3cret"));
    }

    #[test]
    fn wrong_key_and_malformed_envelopes_fail_closed() {
        let cipher = AppSecretCipher::from_encoded_key(KEY_B64).unwrap();
        let other = AppSecretCipher::from_encoded_key(&STANDARD.encode([7_u8; 32])).unwrap();
        let sealed = cipher.encrypt("app-1", "s3cret").unwrap();
        assert!(other.decrypt("app-1", &sealed).is_err());
        assert!(cipher.decrypt("app-1", "s3cret").is_err());
        assert!(cipher.decrypt("app-1", "v1:not base64!").is_err());
        assert!(cipher.decrypt("app-1", "v1:AAAA").is_err());
    }

    #[test]
    fn rejects_keys_of_the_wrong_size() {
        assert!(AppSecretCipher::from_encoded_key("").is_err());
        assert!(AppSecretCipher::from_encoded_key(&STANDARD.encode([1_u8; 16])).is_err());
        assert!(AppSecretCipher::from_encoded_key("passphrase").is_err());
    }

    #[test]
    fn row_helpers_keep_plaintext_rows_working() {
        let cipher = AppSecretCipher::from_encoded_key(KEY_B64).unwrap();

        // No key, no secret_enc: unchanged behavior.
        assert_eq!(
            resolve_row_secret(None, "app-1", "plain".into(), None).unwrap(),
            "plain"
        );
        // Key configured, row not migrated yet: plaintext still accepted.
        assert_eq!(
            resolve_row_secret(Some(&cipher), "app-1", "plain".into(), Some("")).unwrap(),
            "plain"
        );

        let (column, sealed) = seal_row_secret(Some(&cipher), "app-1", "s3cret").unwrap();
        assert!(column.is_empty());
        let sealed = sealed.unwrap();
        assert_eq!(
            resolve_row_secret(Some(&cipher), "app-1", column, Some(&sealed)).unwrap(),
            "s3cret"
        );
        // Encrypted row without a key fails closed.
        assert!(resolve_row_secret(None, "app-1", String::new(), Some(&sealed)).is_err());

        assert_eq!(
            seal_row_secret(None, "app-1", "s3cret").unwrap(),
            ("s3cret".to_string(), None)
        );
    }
}
