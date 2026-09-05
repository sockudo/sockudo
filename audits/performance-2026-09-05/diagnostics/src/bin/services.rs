//! Isolated audit diagnostics. Include as a module and call run_services_diagnostics().
//! Dependencies: sockudo-app, sockudo-cache, sockudo-core, async-trait, tokio.
use async_trait::async_trait;
use sockudo_app::cached_app_manager::CachedAppManager;
use sockudo_cache::MemoryCacheManager;
use sockudo_core::app::{App, AppManager, AppPolicy};
use sockudo_core::cache::CacheManager;
use sockudo_core::error::Result;
use sockudo_core::options::{CacheSettings, MemoryCacheOptions};
use std::hint::black_box;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

struct SlowApps {
    calls: AtomicUsize,
    exists: bool,
}
#[async_trait]
impl AppManager for SlowApps {
    async fn init(&self) -> Result<()> {
        Ok(())
    }
    async fn create_app(&self, _: App) -> Result<()> {
        Ok(())
    }
    async fn update_app(&self, _: App) -> Result<()> {
        Ok(())
    }
    async fn delete_app(&self, _: &str) -> Result<()> {
        Ok(())
    }
    async fn get_apps(&self) -> Result<Vec<App>> {
        Ok(vec![])
    }
    async fn find_by_key(&self, _: &str) -> Result<Option<App>> {
        self.find_by_id("audit").await
    }
    async fn find_by_id(&self, _: &str) -> Result<Option<App>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(self.exists.then(|| {
            App::from_policy(
                "audit".into(),
                "audit-key".into(),
                "audit-secret".into(),
                true,
                AppPolicy::default(),
            )
        }))
    }
    async fn check_health(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct CacheMisses {
    gets: AtomicUsize,
    sets: AtomicUsize,
}
#[async_trait]
impl CacheManager for CacheMisses {
    async fn has(&self, _: &str) -> Result<bool> {
        Ok(false)
    }
    async fn get(&self, _: &str) -> Result<Option<String>> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }
    async fn set(&self, _: &str, _: &str, _: u64) -> Result<()> {
        self.sets.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(2)).await;
        Ok(())
    }
    async fn remove(&self, _: &str) -> Result<()> {
        Ok(())
    }
    async fn disconnect(&self) -> Result<()> {
        Ok(())
    }
    async fn ttl(&self, _: &str) -> Result<Option<Duration>> {
        Ok(None)
    }
}

pub async fn run_services_diagnostics() {
    println!(
        "service_diagnostic,cache=memory,ttl=0,page_limit=256,values=64_and_4096_bytes,repeats=3"
    );
    for value_bytes in [64, 4096] {
        for count in [1_000usize, 5_000, 10_000] {
            let cache = MemoryCacheManager::new(
                "audit".into(),
                MemoryCacheOptions {
                    ttl: 0,
                    cleanup_interval: 60,
                    max_capacity: count as u64 * 2,
                },
            );
            let value = "x".repeat(value_bytes);
            for i in 0..count {
                cache
                    .set(&format!("active:{i:08}"), &value, 0)
                    .await
                    .unwrap();
            }
            // Allow async cache upkeep to settle; counts are asserted, not assumed.
            tokio::time::sleep(Duration::from_millis(100)).await;
            for sample in 0..3 {
                let start = Instant::now();
                black_box(cache.scan_prefix_page("active:", None, 256).await.unwrap());
                let first_page_us = start.elapsed().as_micros();
                let start = Instant::now();
                let mut cursor = None;
                let mut seen = 0usize;
                let mut pages = 0usize;
                loop {
                    let page = cache
                        .scan_prefix_page("active:", cursor, 256)
                        .await
                        .unwrap();
                    seen += page.entries.len();
                    pages += 1;
                    black_box(&page.entries);
                    cursor = page.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                    assert!(pages < count, "cursor failed to terminate");
                }
                assert_eq!(seen, count, "cache scan must include all inserted entries");
                println!(
                    "cache_scan,n={count},bytes={value_bytes},sample={sample},pages={pages},first_page_us={first_page_us},full_scan_us={}",
                    start.elapsed().as_micros()
                );
            }
        }
    }
    for exists in [true, false] {
        for concurrency in [1usize, 32, 128] {
            for sample in 0..3 {
                let inner = Arc::new(SlowApps {
                    calls: AtomicUsize::new(0),
                    exists,
                });
                let cache = Arc::new(CacheMisses::default());
                let manager = Arc::new(CachedAppManager::new(
                    inner.clone(),
                    cache.clone(),
                    CacheSettings {
                        enabled: true,
                        ttl: 300,
                    },
                ));
                let barrier = Arc::new(tokio::sync::Barrier::new(concurrency));
                let start = Instant::now();
                let mut tasks = tokio::task::JoinSet::new();
                for _ in 0..concurrency {
                    let manager = manager.clone();
                    let barrier = barrier.clone();
                    tasks.spawn(async move {
                        barrier.wait().await;
                        assert_eq!(
                            manager.find_by_key("audit-key").await.unwrap().is_some(),
                            exists
                        );
                    });
                }
                while let Some(result) = tasks.join_next().await {
                    result.unwrap();
                }
                println!(
                    "app_cold_wave,exists={exists},concurrency={concurrency},sample={sample},wall_us={},backend_calls={},shared_gets={},shared_sets={}",
                    start.elapsed().as_micros(),
                    inner.calls.load(Ordering::Relaxed),
                    cache.gets.load(Ordering::Relaxed),
                    cache.sets.load(Ordering::Relaxed)
                );
                let before = inner.calls.load(Ordering::Relaxed);
                for _ in 0..4 {
                    black_box(manager.find_by_key("audit-key").await.unwrap());
                }
                println!(
                    "app_followup,exists={exists},extra_backend_calls={}",
                    inner.calls.load(Ordering::Relaxed) - before
                );
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    run_services_diagnostics().await;
}
