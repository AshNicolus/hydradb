//! Whether this node may serve: the one answer `/readyz` reports and the
//! heartbeat publisher acts on.
//!
//! Three conditions, and all are necessary:
//!
//! - **Lifecycle.** False until every listener is bound, false again from the
//!   first sign of shutdown. This is the condition `/readyz` has always
//!   reported.
//! - **Placement (decision 7 of
//!   the rendezvous placement design).** A node whose heartbeat
//!   LIST has been failing for longer than the grace window has **shed** its
//!   view of the fleet. It knows nothing about who is live, so it refuses every
//!   promotion with `NotCellWriter` and no owner hint — and it must withdraw
//!   its own heartbeat.
//! - **Storage.** False while the object store has not passed the create-only
//!   probe ([`hydradb::WriterGate`]). Such a node refuses every writer, so
//!   advertising it would route writes to a node that rejects them all — the
//!   same trap as a shed view, and the same answer: withdraw.
//!
//! The withdrawal is the part that is easy to leave out, and leaving it out is
//! the nastiest failure available in this design. In a partial failure — LIST
//! throttled while PUTs still land — a node that sheds but keeps publishing
//! stays the *computed owner for every one of its peers*. They route every
//! write to it; it refuses every one, because it has shed; and nothing times
//! out, because its heartbeat is still fresh. The cell is permanently
//! unwritable and every node's view of the fleet is self-consistent.
//! Withdrawing is what lets peers age this node out and take over.
//!
//! # Why both consumers read one value
//!
//! `/readyz` and the publisher could each derive readiness for themselves. They
//! must not. Decision 4 deleted the client-side `/readyz` fan-out precisely
//! because two liveness signals that can disagree are worse than one, and
//! presence of the heartbeat object *is* the readiness signal that replaced it
//! — so a node reporting ready while publishing nothing, or the reverse, would
//! reintroduce the inconsistency that deletion removed.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use hydradb::{PlacementView, WriterGate};
use hydradb_placement::liveness::HeartbeatAction;

/// The node's readiness, shared by the admin server and the heartbeat
/// publisher.
///
/// Cheap to clone: an `Arc` around the lifecycle flag plus a
/// [`PlacementView`] handle, which is itself a shared snapshot.
#[derive(Clone, Debug)]
pub struct NodeReadiness {
    lifecycle: Arc<AtomicBool>,
    placement: PlacementView,
    storage: WriterGate,
}

impl NodeReadiness {
    /// A node that is not yet ready. The lifecycle flag starts false and is
    /// raised by [`NodeReadiness::mark_ready`] once the listeners are up.
    pub fn new(placement: PlacementView) -> Self {
        Self {
            lifecycle: Arc::new(AtomicBool::new(false)),
            placement,
            storage: WriterGate::default(),
        }
    }

    /// Read storage readiness from `gate`, the same one the cluster checks
    /// before taking a writer, so this node is never advertised while it
    /// refuses writes.
    pub fn with_writer_gate(mut self, gate: WriterGate) -> Self {
        self.storage = gate;
        self
    }

    /// The listeners are bound and this node can serve.
    pub fn mark_ready(&self) {
        self.lifecycle.store(true, Ordering::Release);
    }

    /// Shutting down, or otherwise unable to serve.
    pub fn mark_unready(&self) {
        self.lifecycle.store(false, Ordering::Release);
    }

    /// What the publisher must do with this node's heartbeat object right now.
    ///
    /// [`HeartbeatAction::Publish`] only while all three conditions hold. Any one
    /// failing means DELETE, not "skip the PUT": presence of the object is the
    /// readiness signal, so a stale object left behind would keep advertising a
    /// node that is not serving until it aged out.
    pub fn heartbeat_action(&self) -> HeartbeatAction {
        if self.lifecycle.load(Ordering::Acquire) && self.storage.is_open() {
            self.placement.heartbeat_action()
        } else {
            HeartbeatAction::Withdraw
        }
    }

    /// Whether `/readyz` should answer 200 — exactly when the publisher would
    /// publish.
    pub fn is_ready(&self) -> bool {
        matches!(self.heartbeat_action(), HeartbeatAction::Publish)
    }
}
