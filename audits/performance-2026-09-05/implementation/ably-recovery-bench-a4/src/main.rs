#![allow(dead_code)]
mod protocol;
mod codec;
use protocol::*;
use std::{collections::VecDeque,sync::Arc,alloc::{GlobalAlloc,Layout,System},sync::atomic::{AtomicU64,Ordering},time::Instant};
struct CountingAllocator;
static ALLOCS: AtomicU64=AtomicU64::new(0);
static BYTES: AtomicU64=AtomicU64::new(0);
#[global_allocator] static ALLOCATOR:CountingAllocator=CountingAllocator;
unsafe impl GlobalAlloc for CountingAllocator {
 unsafe fn alloc(&self,l:Layout)->*mut u8 {ALLOCS.fetch_add(1,Ordering::Relaxed);BYTES.fetch_add(l.size() as u64,Ordering::Relaxed);unsafe{System.alloc(l)}}
 unsafe fn dealloc(&self,p:*mut u8,l:Layout){unsafe{System.dealloc(p,l)}}
 unsafe fn realloc(&self,p:*mut u8,l:Layout,n:usize)->*mut u8{ALLOCS.fetch_add(1,Ordering::Relaxed);BYTES.fetch_add(n as u64,Ordering::Relaxed);unsafe{System.realloc(p,l,n)}}
}
const ABLY_MODE_SUBSCRIBE:u64=1<<18;
const ABLY_ATTACH_GATE_MAX_MESSAGES:usize=4096;
const ABLY_ATTACH_GATE_MAX_BYTES:usize=64*1024*1024;
include!("gate.rs");
fn should_deliver_to_subscriber(publisher:Option<&str>,subscriber:&str,echo:bool,override_echo:Option<bool>)->bool {publisher!=Some(subscriber)||override_echo.unwrap_or(echo)}
fn main() {
 for bytes in [256,65_536] {for count in [16,128] {for subscribers in [1,16,64] {
  let mut tail=AblyRecoveryTail::default();
  for n in 0..count {
   let message:AblyProtocolMessage=sonic_rs::from_str(&format!(r#"{{"action":15,"channel":"audit","channelSerial":"stream:{n}","messages":[{{"id":"message-{n}","name":"event","data":"{}"}}]}}"#,"x".repeat(bytes))).unwrap();
   let wire_bytes=sonic_rs::to_vec(&message).unwrap().len();
   tail.push_with_size(message.into(),None,None,wire_bytes);
  }
  for rep in 0..7 {
   ALLOCS.store(0,Ordering::Relaxed);BYTES.store(0,Ordering::Relaxed);
   let started=Instant::now();
   let mut gates=(0..subscribers).map(|_|tail.gate_for_subscriber(0,"subscriber",true,ABLY_MODE_SUBSCRIBE)).collect::<Vec<_>>();
   let us=started.elapsed().as_micros();let allocations=ALLOCS.load(Ordering::Relaxed);let allocated_bytes=BYTES.load(Ordering::Relaxed);
   for gate in &gates {assert!(!gate.overflowed);assert_eq!(gate.messages.len(),count);assert_eq!(gate.bytes,tail.bytes);assert_eq!(gate.messages.last().unwrap().channel_serial.as_deref(),Some(format!("stream:{}",count-1).as_str()));}
   #[cfg(feature="current")]
   {Arc::make_mut(&mut gates[0].messages[0]).channel=Some("subscriber-specific".to_owned());}
   #[cfg(not(feature="current"))]
   {gates[0].messages[0].channel=Some("subscriber-specific".to_owned());}
   for gate in gates.iter().skip(1) {assert_eq!(gate.messages[0].channel.as_deref(),Some("audit"));}
   assert_eq!(tail.messages[0].message.channel.as_deref(),Some("audit"));
   println!("a4,bytes={bytes},messages={count},subscribers={subscribers},rep={rep},returned={},us={us},allocations={allocations},allocated_bytes={allocated_bytes}",count*subscribers);
  }
 }}}
}
