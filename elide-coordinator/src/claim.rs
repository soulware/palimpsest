// Claim job registry for the consolidated `claim-start` /
// `claim-attach` IPC flow. Mirrors `fork::ForkRegistry` exactly,
// only differing in event type.
//
// Claim jobs are tracked separately from fork jobs because their
// progress events differ (the claim flow has an explicit
// `HandoffKeyResolved` step) and because the registry key is the
// volume name, not the fork name — the volume's bucket-side record
// uniquely identifies the in-flight claim.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use elide_coordinator::ipc::{ClaimAttachEvent, IpcError};

#[derive(Clone, Debug)]
pub enum ClaimJobState {
    Running,
    Done,
    Failed(IpcError),
}

pub struct ClaimJob {
    events: Mutex<Vec<ClaimAttachEvent>>,
    state: RwLock<ClaimJobState>,
}

impl ClaimJob {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            state: RwLock::new(ClaimJobState::Running),
        })
    }

    pub fn append(&self, event: ClaimAttachEvent) {
        self.events
            .lock()
            .expect("claim job events poisoned")
            .push(event);
    }

    pub fn finish(&self, state: ClaimJobState) {
        *self.state.write().expect("claim job state poisoned") = state;
    }

    pub fn read_from(&self, offset: usize) -> Vec<ClaimAttachEvent> {
        self.events.lock().expect("claim job events poisoned")[offset..].to_vec()
    }

    pub fn state(&self) -> ClaimJobState {
        self.state.read().expect("claim job state poisoned").clone()
    }
}

/// Registry of in-flight claim jobs keyed by volume name. Same
/// uniqueness story as `ForkRegistry`: the bucket-side `claim-start`
/// op already serialises concurrent claims for the same name (the
/// conditional PUT inside `mark_claimed` will lose), so two claim
/// jobs for the same name cannot both be in their post-claim phase.
pub type ClaimRegistry = Arc<Mutex<HashMap<String, Arc<ClaimJob>>>>;

pub fn new_registry() -> ClaimRegistry {
    Arc::new(Mutex::new(HashMap::new()))
}
