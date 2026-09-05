use crate::cache::CacheManager;
use std::sync::atomic::{AtomicU64,Ordering};
pub static CACHE_CALLS: AtomicU64=AtomicU64::new(0);
pub static READ_BYTES: AtomicU64=AtomicU64::new(0);
pub struct CountedCache(pub crate::redis_cache_manager::RedisCacheManager);
#[async_trait::async_trait]
impl CacheManager for CountedCache {
async fn has(&self, key: &str) -> crate::error::Result<bool> { CACHE_CALLS.fetch_add(1,Ordering::Relaxed);let result=self.0.has(key).await?;READ_BYTES.fetch_add((0) as u64,Ordering::Relaxed);Ok(result) }
async fn get(&self, key: &str) -> crate::error::Result<Option<String>> { CACHE_CALLS.fetch_add(1,Ordering::Relaxed);let result=self.0.get(key).await?;READ_BYTES.fetch_add((result.as_ref().map_or(0,|s|s.len())) as u64,Ordering::Relaxed);Ok(result) }
async fn set(&self, key: &str, value: &str, ttl_seconds: u64) -> crate::error::Result<()> { CACHE_CALLS.fetch_add(1,Ordering::Relaxed);let result=self.0.set(key,value,ttl_seconds).await?;READ_BYTES.fetch_add((0) as u64,Ordering::Relaxed);Ok(result) }
async fn remove(&self, key: &str) -> crate::error::Result<()> { CACHE_CALLS.fetch_add(1,Ordering::Relaxed);let result=self.0.remove(key).await?;READ_BYTES.fetch_add((0) as u64,Ordering::Relaxed);Ok(result) }
async fn disconnect(&self) -> crate::error::Result<()> { CACHE_CALLS.fetch_add(1,Ordering::Relaxed);let result=self.0.disconnect().await?;READ_BYTES.fetch_add((0) as u64,Ordering::Relaxed);Ok(result) }
async fn ttl(&self, key: &str) -> crate::error::Result<Option<std::time::Duration>> { CACHE_CALLS.fetch_add(1,Ordering::Relaxed);let result=self.0.ttl(key).await?;READ_BYTES.fetch_add((0) as u64,Ordering::Relaxed);Ok(result) }
async fn scan_prefix(&self, prefix: &str, limit: usize) -> crate::error::Result<Vec<(String,String)>> { CACHE_CALLS.fetch_add(1,Ordering::Relaxed);let result=self.0.scan_prefix(prefix,limit).await?;READ_BYTES.fetch_add((result.iter().map(|(k,v)|k.len()+v.len()).sum::<usize>()) as u64,Ordering::Relaxed);Ok(result) }
async fn scan_prefix_page(&self, prefix: &str, cursor: Option<String>, limit: usize) -> crate::error::Result<crate::cache::CacheScanPage> { CACHE_CALLS.fetch_add(1,Ordering::Relaxed);let result=self.0.scan_prefix_page(prefix,cursor,limit).await?;READ_BYTES.fetch_add((result.entries.iter().map(|(k,v)|k.len()+v.len()).sum::<usize>()) as u64,Ordering::Relaxed);Ok(result) }
async fn set_if_not_exists(&self, key: &str, value: &str, ttl_seconds: u64) -> crate::error::Result<bool> { CACHE_CALLS.fetch_add(1,Ordering::Relaxed);let result=self.0.set_if_not_exists(key,value,ttl_seconds).await?;READ_BYTES.fetch_add((0) as u64,Ordering::Relaxed);Ok(result) }
async fn compare_and_swap(&self, key: &str, expected: &str, value: &str, ttl_seconds: u64) -> crate::error::Result<bool> { CACHE_CALLS.fetch_add(1,Ordering::Relaxed);let result=self.0.compare_and_swap(key,expected,value,ttl_seconds).await?;READ_BYTES.fetch_add((0) as u64,Ordering::Relaxed);Ok(result) }
async fn compare_and_remove(&self, key: &str, expected: &str) -> crate::error::Result<bool> { CACHE_CALLS.fetch_add(1,Ordering::Relaxed);let result=self.0.compare_and_remove(key,expected).await?;READ_BYTES.fetch_add((0) as u64,Ordering::Relaxed);Ok(result) }
async fn increment_by(&self, key: &str, delta: i64, ttl_seconds: u64) -> crate::error::Result<i64> { CACHE_CALLS.fetch_add(1,Ordering::Relaxed);let result=self.0.increment_by(key,delta,ttl_seconds).await?;READ_BYTES.fetch_add((0) as u64,Ordering::Relaxed);Ok(result) }
}
