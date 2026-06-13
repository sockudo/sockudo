use crate::error::{Error, Result};
use crate::versioned_messages::{MessageSerial, VersionSerial, VersionedMessage};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VersionStoreDirection {
    NewestFirst,
    OldestFirst,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionStoreCursor {
    pub version: u8,
    pub version_serial: VersionSerial,
    pub direction: VersionStoreDirection,
}

#[derive(Debug, Clone)]
pub struct VersionStoreReadRequest {
    pub app_id: String,
    pub channel: String,
    pub message_serial: MessageSerial,
    pub direction: VersionStoreDirection,
    pub limit: usize,
    pub cursor: Option<VersionStoreCursor>,
}

impl VersionStoreReadRequest {
    pub fn validate(&self) -> Result<()> {
        if self.limit == 0 {
            return Err(Error::InvalidMessageFormat(
                "version-history limit must be greater than 0".to_string(),
            ));
        }

        if let Some(cursor) = self.cursor.as_ref() {
            if cursor.version != 1 {
                return Err(Error::InvalidMessageFormat(format!(
                    "unsupported version-history cursor version: {}",
                    cursor.version
                )));
            }
            if cursor.direction != self.direction {
                return Err(Error::InvalidMessageFormat(
                    "version-history cursor direction does not match request".to_string(),
                ));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct VersionStorePage {
    pub items: Vec<StoredVersionRecord>,
    pub next_cursor: Option<VersionStoreCursor>,
    pub has_more: bool,
}

#[derive(Debug, Clone)]
pub struct VersionWriteReservation {
    pub stream_id: String,
    pub delivery_serial: u64,
}

#[derive(Debug, Clone)]
pub struct VersionWriteReservationBlock {
    pub stream_id: String,
    pub start_delivery_serial: u64,
    pub len: u64,
}

impl VersionWriteReservationBlock {
    pub(super) fn validate(block_size: u64) -> Result<()> {
        if block_size == 0 {
            return Err(Error::InvalidMessageFormat(
                "version delivery reservation block size must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct VersionStreamState {
    pub stream_id: Option<String>,
    pub next_delivery_serial: Option<u64>,
    pub oldest_available_delivery_serial: Option<u64>,
    pub newest_available_delivery_serial: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct VersionReplayRequest {
    pub app_id: String,
    pub channel: String,
    pub after_delivery_serial: u64,
    pub limit: usize,
}

impl VersionReplayRequest {
    pub fn validate(&self) -> Result<()> {
        if self.limit == 0 {
            return Err(Error::InvalidMessageFormat(
                "replay limit must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredVersionRecord {
    pub app_id: String,
    pub channel: String,
    pub original_client_id: Option<String>,
    pub message: VersionedMessage,
}

impl StoredVersionRecord {
    pub fn message_serial(&self) -> &MessageSerial {
        &self.message.identity.message_serial
    }

    pub fn version_serial(&self) -> &VersionSerial {
        &self.message.version.serial
    }

    pub fn history_serial(&self) -> u64 {
        self.message.identity.history_serial
    }

    pub fn delivery_serial(&self) -> u64 {
        self.message.replay_position.delivery_serial
    }
}
