use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

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

pub struct Token {
    _key: String,
    secret: String,
}

impl Token {
    pub fn new(key: String, secret: String) -> Self {
        Token { _key: key, secret }
    }

    pub fn sign(&self, input: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes())
            .expect("HMAC can take key of any size");

        mac.update(input.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    pub fn verify(&self, input: &str, signature: &str) -> bool {
        let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes())
            .expect("HMAC can take key of any size");

        mac.update(input.as_bytes());

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

        assert!(hex::decode(&signature).is_ok());
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
