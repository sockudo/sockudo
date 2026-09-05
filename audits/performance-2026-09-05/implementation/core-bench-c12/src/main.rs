use sockudo_push::{SecretString, hash_device_identity_token, verify_device_identity_token};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::{Duration, Instant};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let token = SecretString::new("isolated synthetic device token").unwrap();
    let hash = hash_device_identity_token(&token);
    assert!(verify_device_identity_token(token.expose_secret(), &hash));
    println!("scenario,offered,completed,rejected,elapsed_us,timer_p50_us,timer_p95_us,timer_max_us");
    for offered in [8, 64, 192] {
        let done = Arc::new(AtomicBool::new(false));
        let timer_done = done.clone();
        let timer = tokio::spawn(async move {
            let mut lateness = Vec::new();
            let mut due = tokio::time::Instant::now() + Duration::from_millis(1);
            while !timer_done.load(Ordering::Acquire) {
                tokio::time::sleep_until(due).await;
                lateness.push(tokio::time::Instant::now().saturating_duration_since(due).as_micros() as u64);
                due = tokio::time::Instant::now() + Duration::from_millis(1);
            }
            lateness.sort_unstable();
            lateness
        });
        tokio::task::yield_now().await;
        let start = Instant::now();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..offered {
            let token = token.clone(); let hash = hash.clone();
            tasks.spawn(async move {
                #[cfg(feature="after")]
                {
                    match sockudo_push::verify_device_identity_token_async(token.expose_secret(), &hash).await {
                        Ok(valid) => { assert!(valid); true },
                        Err(sockudo_push::DeviceIdentityCryptoError::Overloaded) => false,
                        Err(error) => panic!("unexpected crypto result {error}"),
                    }
                }
                #[cfg(not(feature="after"))]
                { assert!(verify_device_identity_token(token.expose_secret(), &hash)); true }
            });
        }
        let mut completed = 0; let mut rejected = 0;
        while let Some(result) = tasks.join_next().await { if result.unwrap() { completed += 1; } else { rejected += 1; } }
        let elapsed = start.elapsed().as_micros();
        done.store(true, Ordering::Release);
        let samples = timer.await.unwrap();
        assert_eq!(completed + rejected, offered);
        if offered <= 64 { assert_eq!(completed, offered); }
        println!("device_verify,{offered},{completed},{rejected},{elapsed},{},{},{}", samples[samples.len()/2], samples[(samples.len()*95/100).min(samples.len()-1)], samples.last().unwrap());
    }
}
