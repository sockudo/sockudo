//! Event binding: callbacks and async streams.

use std::collections::HashMap;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use futures_util::Stream;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::event::Event;

/// A callback bound to events.
///
/// Callbacks run on the connection task, in wire order. Keep them short and
/// non-blocking; hand heavy work to a task or use an [`EventStream`].
pub type EventCallback = Arc<dyn Fn(&Event) + Send + Sync>;

/// Handle returned by `bind*`, used to unbind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BindingId(u64);

static NEXT_BINDING: AtomicU64 = AtomicU64::new(1);

impl BindingId {
    pub(crate) fn next() -> Self {
        Self(NEXT_BINDING.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Clone)]
enum Listener {
    Callback(EventCallback),
    Stream {
        sender: mpsc::Sender<Arc<Event>>,
        dropped: Arc<AtomicU64>,
    },
}

type Listeners = Arc<[(BindingId, Listener)]>;

fn empty() -> Listeners {
    Arc::from(Vec::new())
}

struct Inner {
    by_event: HashMap<String, Listeners, ahash::RandomState>,
    global: Listeners,
}

/// Copy-on-write listener registry. Emitting clones two `Arc`s under the lock
/// and invokes listeners after releasing it, so callbacks may bind or unbind
/// re-entrantly.
pub(crate) struct Dispatcher {
    inner: Mutex<Inner>,
}

impl fmt::Debug for Dispatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.lock();
        f.debug_struct("Dispatcher")
            .field("events", &inner.by_event.len())
            .field("global", &inner.global.len())
            .finish()
    }
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Inner {
                by_event: HashMap::default(),
                global: empty(),
            }),
        }
    }
}

fn with_added(list: &Listeners, id: BindingId, listener: Listener) -> Listeners {
    let mut next: Vec<(BindingId, Listener)> = list.iter().cloned().collect();
    next.push((id, listener));
    Arc::from(next)
}

fn without(list: &Listeners, id: BindingId) -> Option<Listeners> {
    if !list.iter().any(|(existing, _)| *existing == id) {
        return None;
    }
    let next: Vec<(BindingId, Listener)> = list
        .iter()
        .filter(|(existing, _)| *existing != id)
        .cloned()
        .collect();
    Some(Arc::from(next))
}

impl Dispatcher {
    fn bind_listener(&self, event: Option<&str>, listener: Listener) -> BindingId {
        let id = BindingId::next();
        let mut inner = self.inner.lock();
        match event {
            Some(name) => {
                let slot = inner.by_event.entry(name.to_owned()).or_insert_with(empty);
                *slot = with_added(slot, id, listener);
            }
            None => inner.global = with_added(&inner.global, id, listener),
        }
        id
    }

    pub(crate) fn bind(&self, event: &str, callback: EventCallback) -> BindingId {
        self.bind_listener(Some(event), Listener::Callback(callback))
    }

    pub(crate) fn bind_global(&self, callback: EventCallback) -> BindingId {
        self.bind_listener(None, Listener::Callback(callback))
    }

    pub(crate) fn bind_stream(
        &self,
        event: Option<&str>,
        capacity: usize,
    ) -> (BindingId, mpsc::Receiver<Arc<Event>>, Arc<AtomicU64>) {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let dropped = Arc::new(AtomicU64::new(0));
        let id = self.bind_listener(
            event,
            Listener::Stream {
                sender,
                dropped: Arc::clone(&dropped),
            },
        );
        (id, receiver, dropped)
    }

    /// Remove one binding. Returns whether it existed.
    pub(crate) fn unbind(&self, id: BindingId) -> bool {
        let mut inner = self.inner.lock();
        if let Some(next) = without(&inner.global, id) {
            inner.global = next;
            return true;
        }
        let mut found = None;
        for (name, list) in &inner.by_event {
            if let Some(next) = without(list, id) {
                found = Some((name.clone(), next));
                break;
            }
        }
        match found {
            Some((name, next)) if next.is_empty() => {
                inner.by_event.remove(&name);
                true
            }
            Some((name, next)) => {
                inner.by_event.insert(name, next);
                true
            }
            None => false,
        }
    }

    /// Remove every binding for one event name.
    pub(crate) fn unbind_event(&self, event: &str) {
        self.inner.lock().by_event.remove(event);
    }

    /// Remove every binding, including global ones.
    pub(crate) fn unbind_all(&self) {
        let mut inner = self.inner.lock();
        inner.by_event.clear();
        inner.global = empty();
    }

    /// Deliver an event to global listeners, then to listeners of its name.
    pub(crate) fn emit(&self, event: &Arc<Event>) {
        let (global, specific) = {
            let inner = self.inner.lock();
            if inner.global.is_empty() && inner.by_event.is_empty() {
                return;
            }
            (
                Arc::clone(&inner.global),
                inner.by_event.get(event.event.as_str()).cloned(),
            )
        };
        for (_, listener) in global.iter() {
            deliver(listener, event);
        }
        if let Some(specific) = specific {
            for (_, listener) in specific.iter() {
                deliver(listener, event);
            }
        }
    }
}

fn deliver(listener: &Listener, event: &Arc<Event>) {
    match listener {
        Listener::Callback(callback) => {
            // A panicking handler must not take the connection task down.
            if catch_unwind(AssertUnwindSafe(|| callback(event))).is_err() {
                tracing::error!(event = %event.event, "event callback panicked");
            }
        }
        Listener::Stream { sender, dropped } => {
            if sender.try_send(Arc::clone(event)).is_err() {
                dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

type Unbinder = Box<dyn Fn(BindingId) + Send + Sync>;

/// An async stream of events, created by `stream()` / `stream_event()`.
///
/// The stream is bounded: when you fall behind by more than its capacity,
/// newer events are dropped and counted in [`EventStream::dropped`] rather
/// than buffered without limit. Dropping the stream unbinds it.
pub struct EventStream {
    id: BindingId,
    receiver: mpsc::Receiver<Arc<Event>>,
    dropped: Arc<AtomicU64>,
    unbind: Option<Unbinder>,
}

impl fmt::Debug for EventStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventStream")
            .field("id", &self.id)
            .field("dropped", &self.dropped())
            .finish()
    }
}

impl EventStream {
    pub(crate) fn new(
        id: BindingId,
        receiver: mpsc::Receiver<Arc<Event>>,
        dropped: Arc<AtomicU64>,
        unbind: Unbinder,
    ) -> Self {
        Self {
            id,
            receiver,
            dropped,
            unbind: Some(unbind),
        }
    }

    /// Receive the next event. Returns `None` once the stream is closed.
    pub async fn recv(&mut self) -> Option<Arc<Event>> {
        self.receiver.recv().await
    }

    /// Receive without waiting.
    pub fn try_recv(&mut self) -> Option<Arc<Event>> {
        self.receiver.try_recv().ok()
    }

    /// Number of events dropped because the stream was full.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// The binding id of this stream.
    pub fn binding_id(&self) -> BindingId {
        self.id
    }
}

impl Stream for EventStream {
    type Item = Arc<Event>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl Drop for EventStream {
    fn drop(&mut self) {
        if let Some(unbind) = self.unbind.take() {
            unbind(self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn event(name: &str) -> Arc<Event> {
        Arc::new(Event::new(name))
    }

    #[test]
    fn emits_global_then_specific_and_unbinds() {
        let dispatcher = Dispatcher::default();
        let order = Arc::new(Mutex::new(Vec::new()));
        let o = Arc::clone(&order);
        let specific = dispatcher.bind("a", Arc::new(move |_| o.lock().push("specific")));
        let o = Arc::clone(&order);
        let global = dispatcher.bind_global(Arc::new(move |_| o.lock().push("global")));
        dispatcher.emit(&event("a"));
        dispatcher.emit(&event("b"));
        assert_eq!(*order.lock(), vec!["global", "specific", "global"]);
        assert!(dispatcher.unbind(specific));
        assert!(dispatcher.unbind(global));
        assert!(!dispatcher.unbind(global));
        dispatcher.emit(&event("a"));
        assert_eq!(order.lock().len(), 3);
    }

    #[test]
    fn callbacks_may_rebind_reentrantly() {
        let dispatcher = Arc::new(Dispatcher::default());
        let count = Arc::new(AtomicUsize::new(0));
        let d = Arc::clone(&dispatcher);
        let c = Arc::clone(&count);
        dispatcher.bind(
            "x",
            Arc::new(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                d.unbind_event("x");
                let c2 = Arc::clone(&c);
                d.bind(
                    "x",
                    Arc::new(move |_| {
                        c2.fetch_add(10, Ordering::SeqCst);
                    }),
                );
            }),
        );
        dispatcher.emit(&event("x"));
        dispatcher.emit(&event("x"));
        assert_eq!(count.load(Ordering::SeqCst), 11);
    }

    #[test]
    fn panicking_callback_does_not_stop_delivery() {
        let dispatcher = Dispatcher::default();
        let hits = Arc::new(AtomicUsize::new(0));
        dispatcher.bind("x", Arc::new(|_| panic!("boom")));
        let h = Arc::clone(&hits);
        dispatcher.bind(
            "x",
            Arc::new(move |_| {
                h.fetch_add(1, Ordering::SeqCst);
            }),
        );
        dispatcher.emit(&event("x"));
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn streams_are_bounded_and_count_drops() {
        let dispatcher = Arc::new(Dispatcher::default());
        let (id, receiver, dropped) = dispatcher.bind_stream(Some("x"), 2);
        let d = Arc::clone(&dispatcher);
        let mut stream = EventStream::new(
            id,
            receiver,
            dropped,
            Box::new(move |id| {
                d.unbind(id);
            }),
        );
        for _ in 0..5 {
            dispatcher.emit(&event("x"));
        }
        assert_eq!(stream.recv().await.unwrap().event, "x");
        assert_eq!(stream.try_recv().unwrap().event, "x");
        assert!(stream.try_recv().is_none());
        assert_eq!(stream.dropped(), 3);
        drop(stream);
        assert!(dispatcher.inner.lock().by_event.is_empty());
    }
}
