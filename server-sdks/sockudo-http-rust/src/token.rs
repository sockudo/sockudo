use crate::util;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::fmt;
use zeroize::Zeroize;

type HmacSha256 = Hmac<Sha256>;

/// Token for signing and verifying data against the app key and secret
#[derive(Clone)]
pub struct Token {
    pub key: String,
    secret: SecretString,
}

/// Wrapper for secret that ensures it's zeroed on drop
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
struct SecretString(String);

impl Token {
    /// Creates a new token with the given key and secret
    pub fn new(key: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            secret: SecretString(secret.into()),
        }
    }

    /// Signs the string using HMAC-SHA256
    pub fn sign(&self, data: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.secret.0.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(data.as_bytes());

        // Use hex formatting directly for better performance
        format!("{:x}", mac.finalize().into_bytes())
    }

    /// Verifies the signature against the data
    pub fn verify(&self, data: &str, signature: &str) -> bool {
        let expected = self.sign(data);
        util::secure_compare(&expected, signature)
    }

    /// Gets the secret as a string (for internal use only)
    pub(crate) fn secret_string(&self) -> String {
        self.secret.0.clone()
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Token")
            .field("key", &self.key)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_and_verify() {
        let token = Token::new("test_key", "test_secret");
        let data = "test_data";
        let signature = token.sign(data);

        assert!(token.verify(data, &signature));
        assert!(!token.verify("other_data", &signature));
        assert!(!token.verify(data, "wrong_signature"));
    }

    #[test]
    fn test_hmac_consistency() {
        let token = Token::new("key", "secret");
        let data = "some data to sign";

        let sig1 = token.sign(data);
        let sig2 = token.sign(data);

        assert_eq!(sig1, sig2, "HMAC should be deterministic");
    }

    #[test]
    fn test_debug_redaction() {
        let token = Token::new("public_key", "secret_key");
        let debug_str = format!("{:?}", token);

        assert!(debug_str.contains("public_key"));
        assert!(debug_str.contains("[REDACTED]"));
        assert!(!debug_str.contains("secret_key"));
    }
}
