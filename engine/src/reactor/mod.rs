//! Generic Artemis-style reactor.

mod traits;
pub use traits::{BoxFuture, Collector, CollectorStream, Executor, Strategy};

use {
    metrics::counter,
    tokio::{
        sync::broadcast::{self, Sender, error::RecvError},
        task::JoinSet,
    },
    tokio_stream::StreamExt,
    tracing::{error, info, warn},
};

const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// The Artemis-style reactor. Spawns one task per every collector, strategy, and
/// executor and wires them together via two broadcast channels.
pub struct Engine<E, A> {
    collectors: Vec<Box<dyn Collector<E>>>,
    strategies: Vec<Box<dyn Strategy<E, A>>>,
    executors: Vec<Box<dyn Executor<A>>>,
    action_channel_capacity: usize,
    event_channel_capacity: usize,
}

impl<E, A> Default for Engine<E, A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E, A> Engine<E, A> {
    pub fn new() -> Self {
        Self {
            collectors: vec![],
            strategies: vec![],
            executors: vec![],
            action_channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            event_channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }

    pub fn with_event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity;

        self
    }

    pub fn with_action_channel_capacity(mut self, capacity: usize) -> Self {
        self.action_channel_capacity = capacity;

        self
    }
}

impl<E, A> Engine<E, A>
where
    E: Clone + Send + 'static + std::fmt::Debug,
    A: Clone + Send + 'static + std::fmt::Debug,
{
    /// The core run loop. Spawns one task per registered component and
    /// returns a `JoinSet` so the caller can await for shutdown.
    pub async fn run(self) -> anyhow::Result<JoinSet<()>> {
        let (event_sender, _): (Sender<E>, _) = broadcast::channel(self.event_channel_capacity);
        let (action_sender, _): (Sender<A>, _) = broadcast::channel(self.action_channel_capacity);

        let mut join_set = JoinSet::new();

        for mut executor in self.executors {
            let mut receiver = action_sender.subscribe();

            join_set.spawn(async move {
                info!("starting executor");

                loop {
                    match receiver.recv().await {
                        Ok(action) => {
                            if let Err(e) = executor.execute(action).await {
                                error!(?e, "error executing action");

                                // TODO: Add error fatality check and drop the task if fatal
                            }
                        }
                        Err(RecvError::Lagged(n)) => {
                            warn!(dropped = n, "executor lagged — actions were dropped");
                            counter!("engine_executor_lagged_actions_total").increment(n);
                        }
                        Err(RecvError::Closed) => {
                            info!("executor channel closed — exiting");

                            break;
                        }
                    }
                }
            });
        }

        for mut strategy in self.strategies {
            let mut event_receiver = event_sender.subscribe();
            let action_sender = action_sender.clone();

            info!("syncing strategy");
            strategy.sync_state().await?;

            join_set.spawn(async move {
                info!("starting strategy");
                loop {
                    match event_receiver.recv().await {
                        Ok(event) => {
                            for action in strategy.process_event(event).await {
                                if let Err(e) = action_sender.send(action) {
                                    error!(?e, "error sending action");
                                }
                            }
                        }
                        Err(RecvError::Lagged(n)) => {
                            warn!(dropped = n, "strategy lagged — events were dropped");
                            counter!("engine_strategy_lagged_events_total").increment(n);
                        }
                        Err(RecvError::Closed) => {
                            info!("strategy event channel closed — exiting");

                            break;
                        }
                    }
                }
            });
        }

        for mut collector in self.collectors {
            let event_sender = event_sender.clone();

            join_set.spawn(async move {
                info!("starting collector");
                let mut event_stream = match collector.get_event_stream().await {
                    Ok(s) => s,
                    Err(e) => {
                        error!(?e, "collector failed to start");

                        return;
                    }
                };

                while let Some(event) = event_stream.next().await {
                    if let Err(e) = event_sender.send(event) {
                        error!(?e, "error sending event");
                    }
                }
                info!("collector stream ended");
            });
        }

        Ok(join_set)
    }

    pub fn add_collector(&mut self, collector: Box<dyn Collector<E>>) {
        self.collectors.push(collector);
    }

    pub fn add_strategy(&mut self, strategy: Box<dyn Strategy<E, A>>) {
        self.strategies.push(strategy);
    }

    pub fn add_executor(&mut self, executor: Box<dyn Executor<A>>) {
        self.executors.push(executor);
    }
}
