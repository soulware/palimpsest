// Fork job registry for the consolidated `fork-start` / `fork-attach`
// IPC flow.
//
// The coordinator-side orchestrator drives the four-stage fork flow
// (resolve-name → pull-chain → snapshot → fork_create) as a single
// background tokio task per in-flight fork. Job state lives entirely
// in memory: unlike imports there is no long-lived child process to
// outlive the coordinator, so a coordinator restart simply means the
// caller gets "no active fork" and re-runs `volume create --from`.
// `fork_create_op` already handles cleaning up the kind of partial
// `by_name/<name>` symlinks a mid-flight crash can leave behind.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use elide_coordinator::ipc::{ForkAttachEvent, IpcError};

/// Terminal state of a fork job. `Failed` carries the error that the
/// orchestrator surfaced; `attach_fork` translates it back into an
/// `Envelope::Err` for the wire.
#[derive(Clone, Debug)]
pub enum ForkJobState {
    Running,
    Done,
    Failed(IpcError),
}

/// In-memory record for one in-flight fork. The orchestrator pushes
/// `ForkAttachEvent` values into `events` as the flow progresses;
/// `attach_fork` polls and replays them to the subscriber.
pub struct ForkJob {
    /// Buffered progress events. The orchestrator only ever appends;
    /// `attach_fork` reads from a per-subscriber offset.
    events: Mutex<Vec<ForkAttachEvent>>,
    /// Current job state. The orchestrator flips it to `Done` /
    /// `Failed` exactly once at the end of the flow.
    state: RwLock<ForkJobState>,
}

impl ForkJob {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            state: RwLock::new(ForkJobState::Running),
        })
    }

    /// Append one event to the job's buffer. Cheap (single mutex lock,
    /// no I/O); safe to call from the orchestrator task.
    pub fn append(&self, event: ForkAttachEvent) {
        self.events
            .lock()
            .expect("fork job events poisoned")
            .push(event);
    }

    /// Mark the job terminal. Called once by the orchestrator.
    pub fn finish(&self, state: ForkJobState) {
        *self.state.write().expect("fork job state poisoned") = state;
    }

    /// Snapshot the events appended at or after `offset`. Used by
    /// `attach_fork` for its polling loop.
    pub fn read_from(&self, offset: usize) -> Vec<ForkAttachEvent> {
        self.events.lock().expect("fork job events poisoned")[offset..].to_vec()
    }

    pub fn state(&self) -> ForkJobState {
        self.state.read().expect("fork job state poisoned").clone()
    }
}

/// Registry of in-flight fork jobs keyed by the new fork's name. The
/// name uniquely identifies a fork in flight: `fork_create_op` rejects
/// a second concurrent attempt for the same `by_name/<name>` symlink,
/// so two `fork-start` calls for the same name cannot both be live.
pub type ForkRegistry = Arc<Mutex<HashMap<String, Arc<ForkJob>>>>;

pub fn new_registry() -> ForkRegistry {
    Arc::new(Mutex::new(HashMap::new()))
}
