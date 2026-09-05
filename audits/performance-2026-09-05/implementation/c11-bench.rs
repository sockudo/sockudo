//! Identical probe copied into unchanged and C11-only SDK variants.
use sockudo_http::{Config, Sockudo};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Counting;
static TRACK: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
// SAFETY: allocations retain the original System allocator's contracts.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc(layout) };
        if !result.is_null() && TRACK.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        result
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, layout, size) };
        if !result.is_null() && TRACK.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(size as u64, Ordering::Relaxed);
        }
        result
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("payload_bytes,attempts,sample,elapsed_us,allocations,allocated_bytes,received_bytes,retry1_us,retry2_us");
    for size in [65536, 1048576] {
        for attempts in [1usize, 3] {
            for sample in 0..5 {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let port = listener.local_addr().unwrap().port();
                let server = tokio::spawn(async move {
                    let mut original = Vec::new();
                    let mut arrival = Vec::new();
                    let mut received = 0;
                    for attempt in 0..attempts {
                        let (mut socket, _) = listener.accept().await.unwrap();
                        let mut bytes = Vec::new();
                        let header_end = loop {
                            let mut chunk = [0; 8192];
                            let count = socket.read(&mut chunk).await.unwrap();
                            assert_ne!(count, 0);
                            bytes.extend_from_slice(&chunk[..count]);
                            if let Some(offset) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                                break offset + 4;
                            }
                        };
                        let headers = std::str::from_utf8(&bytes[..header_end]).unwrap();
                        let length: usize = headers.lines().find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length").then(|| value.trim().parse().unwrap())
                        }).unwrap();
                        let target = headers.lines().next().unwrap().split_whitespace().nth(1).unwrap().to_string();
                        while bytes.len() < header_end + length {
                            let mut chunk = [0; 8192];
                            let count = socket.read(&mut chunk).await.unwrap();
                            assert_ne!(count, 0);
                            bytes.extend_from_slice(&chunk[..count]);
                        }
                        let body = &bytes[header_end..];
                        assert_eq!(body.len(), length);
                        assert!(target.contains(&format!("body_md5={:x}", md5::compute(body))));
                        let decoded: sonic_rs::Value = sonic_rs::from_slice(body).unwrap();
                        use sonic_rs::JsonValueTrait;
                        assert_eq!(decoded["name"].as_str(), Some("probe"));
                        assert_eq!(decoded["data"].as_str().unwrap().len(), size);
                        if attempt == 0 { original.extend_from_slice(body); }
                        else { assert_eq!(body, original); }
                        received += body.len();
                        arrival.push(Instant::now());
                        let status = if attempt + 1 < attempts { "503 Service Unavailable" } else { "200 OK" };
                        socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}").as_bytes()).await.unwrap();
                    }
                    let gaps: Vec<_> = arrival.windows(2).map(|times| times[1].duration_since(times[0]).as_micros()).collect();
                    (received, gaps)
                });
                let client = Sockudo::new(Config::builder().app_id("audit").key("key").secret("secret")
                    .host("127.0.0.1").port(port).use_tls(false).max_retries(attempts as u32 - 1).build().unwrap()).unwrap();
                let data = "x".repeat(size);
                ALLOCS.store(0, Ordering::SeqCst);
                BYTES.store(0, Ordering::SeqCst);
                TRACK.store(true, Ordering::SeqCst);
                let start = Instant::now();
                let response = client.trigger_on_channels(&["room".into()], "probe", data, None).await.unwrap();
                assert_eq!(response.status(), 200);
                let (received, gaps) = server.await.unwrap();
                let elapsed = start.elapsed().as_micros();
                TRACK.store(false, Ordering::SeqCst);
                println!("{size},{attempts},{sample},{elapsed},{},{},{received},{},{}", ALLOCS.load(Ordering::SeqCst), BYTES.load(Ordering::SeqCst), gaps.first().unwrap_or(&0), gaps.get(1).unwrap_or(&0));
            }
        }
    }
}
