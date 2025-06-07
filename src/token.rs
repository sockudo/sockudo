#![allow(dead_code)]

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Performs a timing-safe comparison of two strings
pub fn secure_compare(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut result = 0u8;
    for (x, y) in a.bytes().zip(b.bytes()) {
        result |= x ^ y;
    }
    result == 0
}

/// A token manager that can sign and verify data using HMAC-SHA256
pub struct Token {
    key: String,
    secret: String,
}

impl Token {
    /// Creates a new Token instance with the given key and secret
    ///
    /// # Arguments
    ///
    /// * `key` - The application key
    /// * `secret` - The application secret used for signing
    pub fn new(key: String, secret: String) -> Self {
        Token { key, secret }
    }

    /// Signs the input string using HMAC-SHA256 with the secret
    ///
    /// # Arguments
    ///
    /// * `input` - The string to be signed
    ///
    /// # Returns
    ///
    /// Returns the hexadecimal representation of the HMAC signature
    pub fn sign(&self, input: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes())
            .expect("HMAC can take key of any size");

        mac.update(input.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Verifies if the provided signature matches the computed signature for the input
    ///
    /// # Arguments
    ///
    /// * `input` - The original input string
    /// * `signature` - The signature to verify against
    ///
    /// # Returns
    ///
    /// Returns true if the signatures match, false otherwise
    pub fn verify(&self, input: &str, signature: &str) -> bool {
        // Create a new MAC instance and verify directly to avoid timing attacks
        let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes())
            .expect("HMAC can take key of any size");

        mac.update(input.as_bytes());

        // Try to verify using the MAC's built-in verify method first
        if let Ok(signature_bytes) = hex::decode(signature) {
            mac.verify_slice(&signature_bytes).is_ok()
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_sign() {
        let token = Token::new("test_key".to_string(), "test_secret".to_string());
        let input = "test_input";
        let signature = token.sign(input);

        // Verify the signature is a valid hex string
        assert!(hex::decode(&signature).is_ok());

        // Verify the signature can be verified
        assert!(token.verify(input, &signature));
    }

    #[test]
    fn test_token_verify_valid() {
        let token = Token::new("test_key".to_string(), "test_secret".to_string());
        let input = "test_input";
        let signature = token.sign(input);

        assert!(token.verify(input, &signature));
    }

    #[test]
    fn test_token_verify_invalid() {
        let token = Token::new("test_key".to_string(), "test_secret".to_string());
        let input = "test_input";
        let wrong_input = "wrong_input";
        let signature = token.sign(input);

        assert!(!token.verify(wrong_input, &signature));
        assert!(!token.verify(input, "invalid_hex"));
    }

    #[test]
    fn test_token_verify_different_secrets() {
        let token1 = Token::new("test_key".to_string(), "secret1".to_string());
        let token2 = Token::new("test_key".to_string(), "secret2".to_string());
        let input = "test_input";
        let signature = token1.sign(input);

        assert!(!token2.verify(input, &signature));
    }
}
