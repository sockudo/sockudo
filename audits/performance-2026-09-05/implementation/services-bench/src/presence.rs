use async_trait::async_trait;
use sockudo_core::{error::Result, history::*, presence_history::*};
use sonic_rs::json;
use std::{sync::{Arc, atomic::{AtomicUsize,Ordering}}, time::Instant};
struct Observed { inner: MemoryHistoryStore, reads: AtomicUsize, rows: AtomicUsize }
#[async_trait]
impl HistoryStore for Observed {
 async fn reserve_publish_position(&self,a:&str,c:&str)->Result<HistoryWriteReservation>{self.inner.reserve_publish_position(a,c).await}
 async fn append(&self,r:HistoryAppendRecord)->Result<()>{self.inner.append(r).await}
 async fn stream_inspection(&self,a:&str,c:&str)->Result<HistoryStreamInspection>{self.inner.stream_inspection(a,c).await}
 async fn read_page(&self,r:HistoryReadRequest)->Result<HistoryPage>{let p=self.inner.read_page(r).await?;self.reads.fetch_add(1,Ordering::Relaxed);self.rows.fetch_add(p.items.len(),Ordering::Relaxed);Ok(p)}
}
fn record(user:String,key:String,event:PresenceHistoryEventKind)->PresenceHistoryTransitionRecord{
 PresenceHistoryTransitionRecord {app_id:"audit".into(),channel:"presence-audit".into(),event_kind:event,cause:PresenceHistoryEventCause::Join,user_id:user,connection_id:None,user_info:None,dead_node_id:None,dedupe_key:key,published_at_ms:now_ms(),retention:PresenceHistoryRetentionPolicy{retention_window_seconds:3600,max_events_per_channel:Some(100_000),max_bytes_per_channel:None}}
}
#[tokio::main(flavor="current_thread")]
async fn main(){
 for retained in [100,1000,10_000] {for case in ["new","hot","sparse","sparse_warm"] {for repeat in 0..5 {
  let history=Arc::new(Observed{inner:MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()),reads:AtomicUsize::new(0),rows:AtomicUsize::new(0)});
  for n in 0..retained {
   let r=record(format!("user-{n}"),format!("seed-{n}"),PresenceHistoryEventKind::MemberAdded);
   let reservation=history.reserve_publish_position("audit","[presence-history]presence-audit").await.unwrap();
   let payload=json!({"published_at_ms":r.published_at_ms,"event":r.event_kind,"cause":r.cause,"user_id":r.user_id,"connection_id":null,"user_info":null,"dead_node_id":null,"dedupe_key":r.dedupe_key});
   history.append(HistoryAppendRecord{app_id:"audit".into(),channel:"[presence-history]presence-audit".into(),stream_id:reservation.stream_id,serial:reservation.serial,published_at_ms:r.published_at_ms,message_id:None,event_name:None,operation_kind:"append".into(),payload_bytes:sonic_rs::to_vec(&payload).unwrap().into(),retention:HistoryRetentionPolicy{retention_window_seconds:3600,max_messages_per_channel:Some(100_000),max_bytes_per_channel:None}}).await.unwrap();
  }
  let store=DurablePresenceHistoryStore::new(history.clone(),None);
  store.record_transition(record("warm".into(),"warm".into(),PresenceHistoryEventKind::MemberAdded)).await.unwrap();
  if case=="sparse_warm" {for n in 0..100 {store.record_transition(record(format!("user-{}",n*retained/100),format!("query-{n}"),PresenceHistoryEventKind::MemberAdded)).await.unwrap();}}
  let reads=history.reads.load(Ordering::Relaxed);let rows=history.rows.load(Ordering::Relaxed);let started=Instant::now();
  for n in 0..100 {
   let(user,event)=match case {"hot"=>("warm".into(),if n%2==0{PresenceHistoryEventKind::MemberRemoved}else{PresenceHistoryEventKind::MemberAdded}),"sparse"|"sparse_warm"=>(format!("user-{}",n*retained/100),PresenceHistoryEventKind::MemberAdded),_=>(format!("new-{n}"),PresenceHistoryEventKind::MemberAdded)};
   store.record_transition(record(user,format!("request-{n}"),event)).await.unwrap();
  }
  let elapsed=started.elapsed().as_micros();
  let final_count=history.stream_inspection("audit","[presence-history]presence-audit").await.unwrap().retained.retained_messages;
  assert_eq!(final_count,retained as u64+1+if case.starts_with("sparse"){0}else{100});
  println!("{}",json!({"case":case,"repeat":repeat,"retained":retained,"elapsed_us":elapsed,"reads":history.reads.load(Ordering::Relaxed)-reads,"rows":history.rows.load(Ordering::Relaxed)-rows,"final_records":final_count}));
 }}}
}
