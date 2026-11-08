//! Typed node-local lifecycle observations; absent or unknown values fail closed.

use crate::proto::{
    SandboxCleanup, SandboxCleanupState, SandboxInfo, SandboxLifecycleState, SandboxListElement,
    WaitResponse,
};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LifecycleObservationError {
    #[error("unknown or unspecified sandbox lifecycle value {0}")]
    UnknownValue(i32),
    #[error("sandbox lifecycle observation is missing")]
    Missing,
    #[error("unrecognized persisted sandbox lifecycle {0:?}")]
    UnknownLabel(String),
    #[error("unknown sandbox cleanup state value {0}")]
    UnknownCleanupState(i32),
}

impl SandboxLifecycleState {
    pub fn label(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Creating => "creating",
            Self::Created => "created",
            Self::Starting => "starting",
            Self::Started => "started",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Deleting => "deleting",
            Self::Pausing => "pausing",
            Self::Paused => "paused",
            Self::Resuming => "resuming",
            Self::Checkpointing => "checkpointing",
            Self::Suspending => "suspending",
            Self::Suspended => "suspended",
            Self::Restoring => "restoring",
            Self::Quarantined => "quarantined",
            Self::Failed => "failed",
        }
    }

    pub fn from_label(label: &str) -> Result<Self, LifecycleObservationError> {
        match label {
            "creating" => Ok(Self::Creating),
            "created" => Ok(Self::Created),
            "starting" => Ok(Self::Starting),
            "started" => Ok(Self::Started),
            "stopping" => Ok(Self::Stopping),
            "stopped" => Ok(Self::Stopped),
            "deleting" => Ok(Self::Deleting),
            "pausing" => Ok(Self::Pausing),
            "paused" => Ok(Self::Paused),
            "resuming" => Ok(Self::Resuming),
            "checkpointing" => Ok(Self::Checkpointing),
            "suspending" => Ok(Self::Suspending),
            "suspended" => Ok(Self::Suspended),
            "restoring" => Ok(Self::Restoring),
            "quarantined" => Ok(Self::Quarantined),
            "failed" => Ok(Self::Failed),
            other => Err(LifecycleObservationError::UnknownLabel(other.to_string())),
        }
    }
}

pub fn decode_lifecycle(
    value: Option<i32>,
) -> Result<SandboxLifecycleState, LifecycleObservationError> {
    match value {
        None => Err(LifecycleObservationError::Missing),
        Some(raw) => SandboxLifecycleState::try_from(raw)
            .ok()
            .filter(|state| *state != SandboxLifecycleState::Unspecified)
            .ok_or(LifecycleObservationError::UnknownValue(raw)),
    }
}

/// A cleanup that owns a sandbox row. A node publishes it in `cleanup.state`,
/// apart from the lifecycle the cleanup is over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObservedCleanup {
    /// A forced teardown owns the row and is owed.
    Required,
    /// A teardown owns the row and its last attempt failed; it is retried.
    Failed,
}

/// What a reader decides from: the lifecycle a node publishes and the cleanup
/// that owns the row, if any. It equals a lifecycle only when no cleanup owns
/// the row, so a cleanup-owned row passes no lifecycle check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObservedStatus {
    pub lifecycle: SandboxLifecycleState,
    pub cleanup: Option<ObservedCleanup>,
}

impl ObservedStatus {
    /// The lifecycle, unless a cleanup owns the row.
    pub fn unowned(self) -> Option<SandboxLifecycleState> {
        self.cleanup.is_none().then_some(self.lifecycle)
    }

    /// The status label node-ctl publishes to the control plane:
    /// `cleanup_required` or `cleanup_failed` for a cleanup-owned row,
    /// otherwise the lifecycle's label.
    pub fn label(self) -> &'static str {
        match self.cleanup {
            Some(ObservedCleanup::Required) => "cleanup_required",
            Some(ObservedCleanup::Failed) => "cleanup_failed",
            None => self.lifecycle.label(),
        }
    }
}

impl PartialEq<SandboxLifecycleState> for ObservedStatus {
    fn eq(&self, other: &SandboxLifecycleState) -> bool {
        self.unowned() == Some(*other)
    }
}

/// A cleanup state this build doesn't know fails closed.
fn decode_status(
    lifecycle: Option<i32>,
    cleanup: Option<&SandboxCleanup>,
) -> Result<ObservedStatus, LifecycleObservationError> {
    let lifecycle = decode_lifecycle(lifecycle)?;
    let raw = cleanup.map_or(SandboxCleanupState::Unspecified as i32, |cleanup| {
        cleanup.state
    });
    let cleanup = match SandboxCleanupState::try_from(raw) {
        Ok(SandboxCleanupState::Unspecified) => None,
        Ok(SandboxCleanupState::Required) => Some(ObservedCleanup::Required),
        Ok(SandboxCleanupState::Failed) => Some(ObservedCleanup::Failed),
        Err(_) => return Err(LifecycleObservationError::UnknownCleanupState(raw)),
    };
    Ok(ObservedStatus { lifecycle, cleanup })
}

impl SandboxInfo {
    pub fn observed_status(&self) -> Result<ObservedStatus, LifecycleObservationError> {
        decode_status(self.lifecycle, self.cleanup.as_ref())
    }
}

impl SandboxListElement {
    pub fn observed_status(&self) -> Result<ObservedStatus, LifecycleObservationError> {
        decode_status(self.lifecycle, self.cleanup.as_ref())
    }
}

impl WaitResponse {
    pub fn observed_status(&self) -> Result<ObservedStatus, LifecycleObservationError> {
        decode_status(self.lifecycle, self.cleanup.as_ref())
    }
}

#[cfg(test)]
mod tests;
