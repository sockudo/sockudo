use sockudo_push::*;
use sonic_rs::json;
use std::{sync::Arc, time::Instant};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
const APP_ID: &str = "app-1";
fn sample_payload() -> PushPayload {
    PushPayload {
        template_id: None,
        template_data: json!({ "data": { "headline": "benchmark" } }),
        title: Some("Benchmark".to_owned()),
        body: Some("Body".to_owned()),
        icon: None,
        sound: None,
        collapse_key: None,
    }
}
fn sample_intent(publish_id: String, targets: Vec<PublishTarget>) -> PublishIntent {
    PublishIntent {
        app_id: APP_ID.to_owned(),
        publish_id,
        targets,
        payload: sample_payload(),
        provider_overrides: vec![],
        not_before_ms: None,
        expires_at_ms: None,
    }
}
fn sample_device(device_id: &str) -> DeviceDetails {
    DeviceDetails {
        app_id: APP_ID.to_owned(),
        id: device_id.to_owned(),
        client_id: Some("client-1".to_owned()),
        form_factor: FormFactor::Phone,
        platform: Platform::Android,
        metadata: json!({}),
        device_secret: SecretString::new(format!("pbkdf2-sha256$120000$bench${device_id}"))
            .unwrap(),
        timezone: "UTC".to_owned(),
        locale: "en".to_owned(),
        last_active_at_ms: 1,
        push: DevicePushDetails {
            recipient: PushRecipient::Fcm {
                registration_token: SecretString::new(format!("token-{device_id}")).unwrap(),
            },
            state: DevicePushState::Active,
            failure_count: 0,
            error_reason: None,
        },
        push_rate_policy: None,
    }
}
fn sample_delivery_batch(publish_id: String, jobs: usize) -> DeliveryBatch {
    let payload = Arc::new(sample_payload());
    DeliveryBatch {
        app_id: APP_ID.to_owned(),
        publish_id: publish_id.clone(),
        provider: PushProviderKind::Fcm,
        batch_id: "bench-batch-1".to_owned(),
        jobs: (0..jobs)
            .map(|index| DeliveryJob {
                app_id: APP_ID.to_owned(),
                publish_id: publish_id.clone(),
                provider: PushProviderKind::Fcm,
                batch_id: "bench-batch-1".to_owned(),
                device_id: Some(format!("device-{index}")),
                recipient: PushRecipient::Fcm {
                    registration_token: SecretString::new(format!("token-{index}")).unwrap(),
                },
                payload: Arc::clone(&payload),
                rendered_payload: None,
                attempt: 1,
                first_attempt_at_ms: None,
                not_before_ms: None,
                expires_at_ms: None,
            })
            .collect(),
    }
}


struct CountingAllocator;
static ALLOCS: AtomicU64=AtomicU64::new(0);static BYTES: AtomicU64=AtomicU64::new(0);
#[global_allocator]static ALLOCATOR:CountingAllocator=CountingAllocator;
unsafe impl GlobalAlloc for CountingAllocator {
 unsafe fn alloc(&self,l:Layout)->*mut u8{ALLOCS.fetch_add(1,Ordering::Relaxed);BYTES.fetch_add(l.size() as u64,Ordering::Relaxed);unsafe{System.alloc(l)}}
 unsafe fn dealloc(&self,p:*mut u8,l:Layout){unsafe{System.dealloc(p,l)}}
 unsafe fn realloc(&self,p:*mut u8,l:Layout,n:usize)->*mut u8{ALLOCS.fetch_add(1,Ordering::Relaxed);BYTES.fetch_add(n as u64,Ordering::Relaxed);unsafe{System.realloc(p,l,n)}}
}
#[tokio::main(flavor="current_thread")]
async fn main(){
 let mode=std::env::args().nth(1).unwrap();
 if mode=="p7" {
  for future in [1_000,10_000] {for rep in 0..7 {
   let queue=MemoryPushQueue::new();let stage=PushQueueStage::DeliveryJobs(PushProviderKind::Fcm);
   let payload=PushQueuePayload::DeliveryBatch(Box::new(sample_delivery_batch("publish".to_owned(),1)));
   for n in 0..future {queue.retry_at(stage,format!("future-{n}"),payload.clone(),u64::MAX).await.unwrap();}
   let mut latency=Vec::with_capacity(100);
   ALLOCS.store(0,Ordering::Relaxed);BYTES.store(0,Ordering::Relaxed);
   let start=Instant::now();
   for n in 0..100 {
    let started=Instant::now();let id=queue.produce(stage,format!("ready-{n}"),payload.clone()).await.unwrap();
    let messages=queue.consume(stage,"healthy",1,30_000).await.unwrap();assert_eq!(messages.len(),1);assert_eq!(messages[0].message_id,id);
    queue.ack(messages.into_iter().next().unwrap().ack).await.unwrap();latency.push(started.elapsed().as_micros());
   }
   let us=start.elapsed().as_micros();let allocations=ALLOCS.load(Ordering::Relaxed);let allocated_bytes=BYTES.load(Ordering::Relaxed);latency.sort_unstable();
   let lag=queue.lag(stage).await.unwrap();assert_eq!(lag.delayed_depth,future);assert_eq!(lag.ready_depth,0);
   println!("p7future,future={future},rep={rep},returned=100,us={us},p50_us={},p95_us={},p99_us={},allocations={allocations},allocated_bytes={allocated_bytes}",latency[50],latency[95],latency[99]);
  }}
 } else {
  for apps in [1,128] {for updates in [1_000,10_000] {for rep in 0..7 {
   let recorder=metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();let handle=recorder.handle();
   let app_ids=(0..apps).map(|n|format!("app-{n}")).collect::<Vec<_>>();let metrics=PushMetrics::default();
   let (us,allocations,allocated_bytes)=::metrics::with_local_recorder(&recorder,||{
    for app in &app_ids {metrics.observe("sockudo_push_dispatch_duration_seconds",&[("provider","fcm"),("app",app)],0.001);}
    ALLOCS.store(0,Ordering::Relaxed);BYTES.store(0,Ordering::Relaxed);
    let start=Instant::now();for n in 0..updates {metrics.observe("sockudo_push_dispatch_duration_seconds",&[("provider","fcm"),("app",&app_ids[n%apps])],0.001);}
    (start.elapsed().as_micros(),ALLOCS.load(Ordering::Relaxed),BYTES.load(Ordering::Relaxed))
   });
   let snapshot=metrics.snapshot();assert_eq!(snapshot.len(),apps);assert_eq!(snapshot.values().map(|v|v.count).sum::<u64>(),(apps+updates)as u64);
   let rendered=handle.render();let total=rendered.lines().filter(|l|l.starts_with("sockudo_push_dispatch_duration_seconds_count{")).map(|line|line.rsplit_once(' ').unwrap().1.parse::<u64>().unwrap()).sum::<u64>();assert_eq!(total,(apps+updates)as u64);
   println!("p8prometheus,apps={apps},updates={updates},rep={rep},us={us},allocations={allocations},allocated_bytes={allocated_bytes},recorded={total}");
  }}}
 }
}
