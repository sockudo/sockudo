use async_trait::async_trait;
use sockudo_push::dispatch::{CachedTokenProvider, FcmDispatcher, FcmServiceAccountTokenSource, ProviderHttpClient, ProviderHttpRequest, ProviderHttpResponse, PushDispatcher};
use sockudo_push::domain::{DeliveryBatch, DeliveryOutcome, ProviderFailureClass, PushProviderKind};
use std::{collections::BTreeMap, sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}}, time::{Duration, Instant}};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener};

// Only the local fixture transport is substituted. Token signing/exchange,
// token-cache admission and per-recipient FCM classification are production code.
struct FixtureHttp { client: reqwest::Client, origin: String }
#[async_trait]
impl ProviderHttpClient for FixtureHttp {
    async fn send(&self, request: ProviderHttpRequest) -> Result<ProviderHttpResponse, String> {
        assert!(request.url.starts_with(&format!("{}/",self.origin)));
        let mut builder = self.client.post(&request.url);
        for (name,value) in request.headers { builder=builder.header(name,value); }
        if let Some(auth)=request.authorization { builder=builder.header("authorization",auth.expose_secret()); }
        let response=builder.body(request.body).send().await.map_err(|error|error.to_string())?;
        Ok(ProviderHttpResponse {status:response.status().as_u16(),headers:BTreeMap::new(),body:response.bytes().await.map_err(|error|error.to_string())?.to_vec()})
    }
}
#[derive(Default)]
struct Counts { tokens: AtomicU64, deliveries: AtomicU64, request_bytes: AtomicU64, response_bytes: AtomicU64 }

fn batch() -> DeliveryBatch {
    let jobs=(0..64).map(|index|sonic_rs::json!({
        "appId":"app", "publishId":"publish", "provider":"fcm", "batchId":"batch",
        "deviceId":format!("device-{index}"), "recipient":{"transportType":"gcm","registrationToken":format!("fixture-{index}")},
        "payload":{"templateData":{},"title":"fixture"}, "attempt":1
    })).collect::<Vec<_>>();
    sonic_rs::from_str(&sonic_rs::json!({"appId":"app","publishId":"publish","provider":"fcm","batchId":"batch","jobs":jobs}).to_string()).unwrap()
}

#[tokio::main(flavor="current_thread")]
async fn main() {
    let expected=std::env::args().nth(1).unwrap().parse::<u64>().unwrap();
    let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin=format!("http://{}",listener.local_addr().unwrap());
    let outage=Arc::new(AtomicBool::new(true));
    let counts=Arc::new(Counts::default());
    let server_counts=counts.clone(); let server_outage=outage.clone();
    let server=tokio::spawn(async move {
        loop {
            let (mut stream,_)=listener.accept().await.unwrap();
            let counts=server_counts.clone();let outage=server_outage.clone();
            tokio::spawn(async move {
                let mut request=Vec::new();let mut bytes=[0u8;4096];
                loop {
                    let read=stream.read(&mut bytes).await.unwrap();assert!(read>0);request.extend_from_slice(&bytes[..read]);assert!(request.len()<16384);
                    if let Some(header_end)=request.windows(4).position(|item|item==b"\r\n\r\n") {
                        let headers=std::str::from_utf8(&request[..header_end]).unwrap();
                        let length=headers.lines().find_map(|line|line.to_ascii_lowercase().strip_prefix("content-length: ").map(|value|value.parse::<usize>().unwrap())).unwrap_or(0);
                        if request.len()>=header_end+4+length { break; }
                    }
                }
                counts.request_bytes.fetch_add(request.len() as u64,Ordering::SeqCst);
                let token=request.starts_with(b"POST /token ");
                let (status,body)=if token {
                    counts.tokens.fetch_add(1,Ordering::SeqCst);
                    assert!(request.windows(10).any(|window|window==b"assertion="));
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    if outage.load(Ordering::SeqCst) {("503 Service Unavailable",r#"{"error":"fixture unavailable"}"#)} else {("200 OK",r#"{"access_token":"fixture-access","expires_in":3600}"#)}
                } else {
                    assert!(request.starts_with(b"POST /v1/projects/project/messages:send "));
                    counts.deliveries.fetch_add(1,Ordering::SeqCst);
                    ("200 OK",r#"{"name":"fixture-message"}"#)
                };
                let response=format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
                counts.response_bytes.fetch_add(response.len() as u64,Ordering::SeqCst);
                stream.write_all(response.as_bytes()).await.unwrap();
            });
        }
    });
    let http=Arc::new(FixtureHttp {client:reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(3)).build().unwrap(),origin:origin.clone()});
    let jobs=batch();assert_eq!(jobs.provider,PushProviderKind::Fcm);
    for rep in 0..7 {
        outage.store(true,Ordering::SeqCst);
        for counter in [&counts.tokens,&counts.deliveries,&counts.request_bytes,&counts.response_bytes] {counter.store(0,Ordering::SeqCst);}
        let source=FcmServiceAccountTokenSource::new("fixture@example.invalid".into(),include_str!("fcm-test-key.pem").into(),None,format!("{origin}/token"),Some("project".into()),http.clone()).unwrap();
        let dispatcher=FcmDispatcher::new("project",CachedTokenProvider::new(Arc::new(source)),http.clone()).with_base_url(&origin);
        let start=Instant::now();let results=dispatcher.dispatch(jobs.clone()).await;let failed_us=start.elapsed().as_micros();
        assert_eq!(results.len(),64);
        assert!(results.iter().all(|result|result.outcome==DeliveryOutcome::Rejected&&result.error.as_ref().is_some_and(|error|error.failure_class==ProviderFailureClass::CredentialAuth)));
        let token_calls=counts.tokens.load(Ordering::SeqCst);assert_eq!(token_calls,expected);assert_eq!(counts.deliveries.load(Ordering::SeqCst),0);
        let request_bytes=counts.request_bytes.load(Ordering::SeqCst);let response_bytes=counts.response_bytes.load(Ordering::SeqCst);
        outage.store(false,Ordering::SeqCst);tokio::time::sleep(Duration::from_millis(260)).await;
        let start=Instant::now();let recovered=dispatcher.dispatch(jobs.clone()).await;let recovery_us=start.elapsed().as_micros();
        assert_eq!(recovered.len(),64);assert!(recovered.iter().all(|result|result.outcome==DeliveryOutcome::Accepted));
        assert_eq!(counts.tokens.load(Ordering::SeqCst),expected+1);assert_eq!(counts.deliveries.load(Ordering::SeqCst),64);
        println!("p3http,rep={rep},failed=64,recovered=64,token_calls={token_calls},failed_us={failed_us},recovery_us={recovery_us},request_bytes={request_bytes},response_bytes={response_bytes}");
    }
    server.abort();assert!(server.await.unwrap_err().is_cancelled());
}
