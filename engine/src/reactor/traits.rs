//! Generic reactor traits — `Collector`, `Strategy`, `Executor`.
//!
//! These traits are intentionally generic over the event type `E` and action
//! type `A`. The concrete enums live in the `keeper` binary as `keeper::wire`.

use {
    anyhow::Result,
    std::{future::Future, pin::Pin},
    tokio_stream::Stream,
};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type CollectorStream<'a, E> = Pin<Box<dyn Stream<Item = E> + Send + 'a>>;

pub trait Collector<E>: Send + Sync {
    fn get_event_stream(&mut self) -> BoxFuture<'_, Result<CollectorStream<'_, E>>>;
}

pub trait Strategy<E, A>: Send + Sync {
    fn sync_state(&mut self) -> BoxFuture<'_, Result<()>>;
    fn process_event(&mut self, event: E) -> BoxFuture<'_, Vec<A>>;
}

pub trait Executor<A>: Send + Sync {
    fn execute(&mut self, action: A) -> BoxFuture<'_, Result<()>>;
}
