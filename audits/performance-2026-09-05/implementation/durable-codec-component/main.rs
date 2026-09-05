// One shared codec component: unchanged full-record JSON versus current storage plan.
// Database round trips, allocator retention, and latest-cache storage are not inferred.
pub use sockudo_core::{error,message_envelope,versioned_messages};
pub use sockudo_core::version_store::StoredVersionRecord;
#[path="../durable-codec-production.rs"] mod encoding;
use encoding::EncodedVersionRecord;
use versioned_messages::*;
use sockudo_protocol::messages::MessageData;
use std::{collections::HashMap,time::Instant};
fn version(n:u64)->VersionMetadata { VersionMetadata{serial:VersionSerial::new(format!("v:{n:020}")).unwrap(),client_id:Some("actor".into()),timestamp_ms:n as i64,description:None,metadata:None} }
fn main(){
 let compact=std::env::var("COMPACT").unwrap()=="1";
 println!("CODEC_CSV,appends,stored_entry_bytes,snapshot_bytes,latest_bytes,cumulative_snapshot_write_bytes,cumulative_latest_write_bytes,encode_us,replay_us,verified");
 for count in [128u64,512,2000] {
  let fragment="é🙂".repeat(10)+"abcd";
  let mut previous=StoredVersionRecord{app_id:"audit".into(),channel:"room".into(),original_client_id:Some("actor".into()),envelope:None,message:VersionedMessage::new_create(MessageSerial::new("message").unwrap(),version(1),1,1,Some("event".into()),Some(MessageData::String(String::new())),None)};
  let mut reference=None; let mut snapshots=HashMap::new(); let mut entries=vec![sonic_rs::to_vec(&previous).unwrap()]; let mut latest=entries[0].clone();
  let mut snapshot_write_bytes=0usize; let mut latest_write_bytes=0usize;
  let start=Instant::now();
  for n in 2..=count+1 {
   let mut next=previous.clone(); next.message=previous.message.apply_append(version(n),n,MessageAppend{data_fragment:fragment.clone(),extras:None}).unwrap();
   if compact {
    let plan=EncodedVersionRecord::plan(&next,Some((&previous,reference.as_ref()))).unwrap();
    if let Some((key,data))=plan.snapshot { snapshot_write_bytes+=data.len(); snapshots.insert(key.snapshot_key.clone(),data); reference=Some(key); }
    entries.push(plan.entry_bytes);latest=plan.latest_bytes;
   } else { let bytes=sonic_rs::to_vec(&next).unwrap();latest=bytes.clone();entries.push(bytes); }
   latest_write_bytes+=latest.len();
   previous=next;
  }
  let encode_us=start.elapsed().as_micros();
  let stored:usize=entries.iter().map(Vec::len).sum();let snapshot_bytes:usize=snapshots.values().map(String::len).sum();
  let start=Instant::now();
  for (n,bytes) in entries.iter().enumerate() {
   let row:StoredVersionRecord=if compact {
    let row=EncodedVersionRecord::decode(bytes).unwrap();
    let text=row.text.as_ref().and_then(|r|snapshots.get(&r.snapshot_key)).map(String::as_str);
    row.materialize(text).unwrap()
   } else {sonic_rs::from_slice(bytes).unwrap()};
   assert_eq!(row.message.data,Some(MessageData::String(fragment.repeat(n))));
   assert_eq!(row.version_serial(),&version(n as u64+1).serial);
   assert_eq!(row.history_serial(),1);assert_eq!(row.delivery_serial(),n as u64+1);
   assert_eq!(row.message.append_fragment.as_deref(),(n>0).then_some(fragment.as_str()));
   assert_eq!(row.original_client_id.as_deref(),Some("actor"));
  }
  assert_eq!(sonic_rs::to_vec(&EncodedVersionRecord::decode(&latest).unwrap().materialize(None).unwrap()).unwrap(),sonic_rs::to_vec(&previous).unwrap());
  println!("CODEC_CSV,{count},{stored},{snapshot_bytes},{},{snapshot_write_bytes},{latest_write_bytes},{encode_us},{},{}",latest.len(),start.elapsed().as_micros(),entries.len());
 }
}
