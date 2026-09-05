//! Actual push identity hash/verify CPU costs; never prints the token or hash.
use sockudo_push::{SecretString, hash_device_identity_token, verify_device_identity_token};
use std::{hint::black_box, time::Instant};
fn main() {
    let token = SecretString::new("synthetic-audit-token").unwrap();
    let stored = hash_device_identity_token(&token);
    for op in ["hash", "verify"] {
        let mut ns = Vec::new();
        for _ in 0..21 {
            let start = Instant::now();
            if op == "hash" {
                black_box(hash_device_identity_token(&token));
            } else {
                assert!(black_box(verify_device_identity_token(
                    token.expose_secret(),
                    &stored
                )));
            }
            ns.push(start.elapsed().as_nanos());
        }
        ns.sort_unstable();
        println!(
            "{op},samples=21,min_ns={},median_ns={},max_ns={}",
            ns[0], ns[10], ns[20]
        );
    }
}
