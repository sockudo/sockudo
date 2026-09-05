use sockudo_core::{error::Error,options::{KafkaAdapterConfig,RabbitMqAdapterConfig,QueueReliabilityConfig},queue::QueueInterface,webhook_types::{JobData,JobPayload}};
use sonic_rs::json;
use std::{sync::{Arc,atomic::{AtomicUsize,Ordering}},time::{Duration,Instant},collections::BTreeMap};
fn job(app:usize,sequence:usize)->JobData{JobData{job_id:Some(format!("{app}-{sequence}")),app_id:app.to_string(),app_key:"synthetic".into(),app_secret:"synthetic".into(),payload:JobPayload{time_ms:1,events:vec![]},original_signature:sequence.to_string()}}
async fn kafka(){
 use rdkafka::{admin::{AdminClient,AdminOptions,NewTopic,TopicReplication},config::ClientConfig,client::DefaultClientContext};
 let brokers=std::env::var("SOCKUDO_KAFKA_TEST_BROKERS").unwrap();
 let admin:AdminClient<DefaultClientContext>=ClientConfig::new().set("bootstrap.servers",&brokers).create().unwrap();
 for repeat in 0..3{
  let prefix=format!("services-pair-{}-{repeat}",std::process::id());
  let topic=format!("{prefix}.queue.ordered");let epoch=format!("{topic}.epoch.test");
  let results=admin.create_topics(&[NewTopic::new(&topic,4,TopicReplication::Fixed(1)),NewTopic::new(&epoch,4,TopicReplication::Fixed(1))],&AdminOptions::new()).await.unwrap();for result in results{result.unwrap();}
  let config:KafkaAdapterConfig=sonic_rs::from_str(&json!({"brokers":[brokers],"prefix":prefix,"partitions":4,"topic_epoch":"test"}).to_string()).unwrap();
  let manager=sockudo_queue::KafkaQueueManager::new(config).await.unwrap();
  let active=Arc::new(AtomicUsize::new(0));let peak=Arc::new(AtomicUsize::new(0));let completed=Arc::new(AtomicUsize::new(0));let seen=Arc::new(tokio::sync::Mutex::new(BTreeMap::<String,Vec<usize>>::new()));
  let(a,p,c,s)=(active.clone(),peak.clone(),completed.clone(),seen.clone());
  manager.process_queue("ordered",Box::new(move|job|{let(a,p,c,s)=(a.clone(),p.clone(),c.clone(),s.clone());Box::pin(async move{let current=a.fetch_add(1,Ordering::SeqCst)+1;p.fetch_max(current,Ordering::SeqCst);tokio::time::sleep(Duration::from_millis(10)).await;s.lock().await.entry(job.app_id).or_default().push(job.original_signature.parse().unwrap());a.fetch_sub(1,Ordering::SeqCst);c.fetch_add(1,Ordering::SeqCst);Ok(())})})).await.unwrap();
  tokio::time::sleep(Duration::from_secs(3)).await;
  let started=Instant::now();let mut jobs=Vec::new();for sequence in 0..10{for app in 0..16{jobs.push(job(app,sequence));}}
  manager.add_batch_to_queue("ordered",jobs).await.unwrap();
  tokio::time::timeout(Duration::from_secs(30),async{while completed.load(Ordering::SeqCst)<160{tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();
  let elapsed=started.elapsed().as_micros();manager.disconnect().await.unwrap();let seen=seen.lock().await;assert_eq!(seen.len(),16);for sequences in seen.values(){assert_eq!(sequences,&(0..10).collect::<Vec<_>>());}
  println!("{}",json!({"case":"S9_kafka","repeat":repeat,"elapsed_us":elapsed,"delivered":160,"apps":16,"ordered_per_app":true,"peak_callbacks":peak.load(Ordering::SeqCst),"topic_partitions":4}));
  admin.delete_topics(&[&topic,&epoch,&format!("{topic}.dlq"),&format!("{epoch}.dlq")],&AdminOptions::new()).await.unwrap();
 }
}
async fn rabbit(){
 for repeat in 0..5 {
  let config=RabbitMqAdapterConfig{url:std::env::var("SOCKUDO_RABBITMQ_TEST_URL").unwrap(),prefix:format!("services-pair-{}-{repeat}",std::process::id()),..Default::default()};
  let manager=sockudo_queue::RabbitMqQueueManager::new_with_reliability(config,QueueReliabilityConfig{retry_base_delay_ms:100,retry_max_delay_ms:100,retry_jitter:0.0,max_attempts:4,..Default::default()}).await.unwrap();
  let calls=Arc::new(tokio::sync::Mutex::new(Vec::new()));let completed=Arc::new(AtomicUsize::new(0));let(c,d)=(calls.clone(),completed.clone());
  manager.process_queue("retry",Box::new(move|_|{let(c,d)=(c.clone(),d.clone());Box::pin(async move{let mut c=c.lock().await;c.push(Instant::now());if c.len()<4{Err(Error::Queue("synthetic dependency rejection".into()))}else{d.fetch_add(1,Ordering::SeqCst);Ok(())}})})).await.unwrap();
  let started=Instant::now();manager.add_to_queue("retry",job(0,0)).await.unwrap();
  tokio::time::timeout(Duration::from_secs(5),async{while completed.load(Ordering::SeqCst)<1{tokio::time::sleep(Duration::from_millis(1)).await;}}).await.unwrap();
  let elapsed=started.elapsed().as_micros();manager.disconnect().await.unwrap();let calls=calls.lock().await;assert_eq!(calls.len(),4);println!("{}",json!({"case":"S10_rabbit","repeat":repeat,"elapsed_us":elapsed,"attempts":4,"delivered":1,"retry_span_us":calls[3].duration_since(calls[0]).as_micros()}));
 }
}
#[tokio::main(flavor="multi_thread",worker_threads=4)]
async fn main(){match std::env::args().nth(1).as_deref(){Some("kafka")=>kafka().await,Some("rabbit")=>rabbit().await,_=>panic!("mode required")}}
