//! Generic Artemis-style reactor. Knows nothing of lending or any chain.
//!
//! Code in this module MUST NOT reference `engine::lending`.

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

/// The Artemis-style reactor. Spawns one task per collector, strategy, and
/// executor and wires them together via two broadcast channels.
pub struct Engine<E, A> {
    collectors: Vec<Box<dyn Collector<E>>>,
    strategies: Vec<Box<dyn Strategy<E, A>>>,
    executors: Vec<Box<dyn Executor<A>>>,
    event_channel_capacity: usize,
    action_channel_capacity: usize,
}

impl<E, A> Engine<E, A> {
    pub fn new() -> Self {
        Self {
            collectors: vec![],
            strategies: vec![],
            executors: vec![],
            event_channel_capacity: 512,
            action_channel_capacity: 512,
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

impl<E, A> Default for Engine<E, A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E, A> Engine<E, A>
where
    E: Send + Clone + 'static + std::fmt::Debug,
    A: Send + Clone + 'static + std::fmt::Debug,
{
    pub fn add_collector(&mut self, collector: Box<dyn Collector<E>>) {
        self.collectors.push(collector);
    }

    pub fn add_strategy(&mut self, strategy: Box<dyn Strategy<E, A>>) {
        self.strategies.push(strategy);
    }

    pub fn add_executor(&mut self, executor: Box<dyn Executor<A>>) {
        self.executors.push(executor);
    }

    /// The core run loop. Spawns one task per registered component and
    /// returns a `JoinSet` so the caller can await shutdown.
    pub async fn run(self) -> anyhow::Result<JoinSet<()>> {
        let (event_sender, _): (Sender<E>, _) = broadcast::channel(self.event_channel_capacity);
        let (action_sender, _): (Sender<A>, _) =
            broadcast::channel(self.action_channel_capacity);

        let mut join_set = JoinSet::new();

        // Executors
        for executor in self.executors {
            let mut receiver = action_sender.subscribe();
            join_set.spawn(async move {
                info!("starting executor");
                loop {
                    match receiver.recv().await {
                        Ok(action) => {
                            if let Err(e) = executor.execute(action).await {
                                error!(?e, "error executing action");
                            }
                        }
                        Err(RecvError::Lagged(n)) => {
                            // Tokio's broadcast advances the cursor on Lagged so the
                            // next recv will succeed. We still want to know loudly
                            // when actions are dropped — silent loss is the bug we
                            // explicitly do not want.
                            warn!(dropped = n, "executor lagged — actions were dropped");
                            // Using the `metrics` facade: this is a no-op if no
                            // recorder is installed (e.g. tests), and surfaces as
                            // a Prometheus counter when the keeper installs its
                            // exporter.
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

        // Strategies
        for mut strategy in self.strategies {
            let mut event_receiver = event_sender.subscribe();
            let action_sender = action_sender.clone();

            info!("syncing strategy");
            if let Err(e) = strategy.sync_state().await {
                error!(?e, "error syncing strategy state");
            }

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

        // Collectors
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
}
