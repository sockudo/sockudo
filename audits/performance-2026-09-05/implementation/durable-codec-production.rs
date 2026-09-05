//! Versioned storage encoding. This is never a realtime or HTTP wire format.
use super::StoredVersionRecord;
use crate::error::{Error, Result};
use crate::message_envelope::MessageContent;
use crate::versioned_messages::MessageAction;
use serde::{Deserialize, Serialize};
use sockudo_protocol::messages::MessageData;

const CURRENT_PREFIX: &[u8] = b"SVR2\0";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionTextReference {
    pub snapshot_key: String,
    pub byte_len: usize,
    #[serde(default)]
    pub retain_for_receipts: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncodedVersionRecord {
    pub record: StoredVersionRecord,
    pub text: Option<VersionTextReference>,
    envelope_text: bool,
}

pub struct VersionStoragePlan {
    pub entry_bytes: Vec<u8>,
    pub latest_bytes: Vec<u8>,
    /// Must be committed atomically with the version CAS. A losing writer must
    /// never replace the winner's snapshot with a different string suffix.
    pub snapshot: Option<(VersionTextReference, String)>,
}

impl EncodedVersionRecord {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if let Some(json) = bytes.strip_prefix(CURRENT_PREFIX) {
            return sonic_rs::from_slice(json).map_err(Error::from);
        }
        if bytes.starts_with(b"SVR") {
            return Err(Error::InvalidMessageFormat(
                "unsupported version storage encoding".into(),
            ));
        }
        Ok(Self {
            record: sonic_rs::from_slice(bytes)?,
            text: None,
            envelope_text: false,
        })
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = CURRENT_PREFIX.to_vec();
        sonic_rs::to_writer(&mut bytes, self)?;
        Ok(bytes)
    }

    pub fn needs_snapshot(&self) -> bool {
        self.text.is_some() && self.record.message.data.is_none()
    }

    pub fn materialize(mut self, snapshot: Option<&str>) -> Result<StoredVersionRecord> {
        if self.needs_snapshot() {
            let reference = self.text.as_ref().expect("checked reference");
            let snapshot = snapshot
                .ok_or_else(|| Error::Internal("version text snapshot is missing".into()))?;
            let data = snapshot
                .get(..reference.byte_len)
                .ok_or_else(|| Error::Internal("version text snapshot prefix is invalid".into()))?
                .to_string();
            if self.envelope_text
                && let Some(envelope) = self.record.envelope.as_mut()
            {
                envelope.data = Some(MessageContent::Text(data.clone()));
            }
            self.record.message.data = Some(MessageData::String(data));
        }
        Ok(self.record)
    }

    pub fn plan(
        record: &StoredVersionRecord,
        predecessor: Option<(&StoredVersionRecord, Option<&VersionTextReference>)>,
    ) -> Result<VersionStoragePlan> {
        let Some(MessageData::String(data)) = record
            .message
            .data
            .as_ref()
            .filter(|_| record.message.action == MessageAction::Append)
        else {
            let bytes = sonic_rs::to_vec(record)?;
            return Ok(VersionStoragePlan {
                latest_bytes: bytes.clone(),
                entry_bytes: bytes,
                snapshot: None,
            });
        };
        let previous_reference = predecessor.and_then(|(previous, reference)| {
            let MessageData::String(previous_data) = previous.message.data.as_ref()? else {
                return None;
            };
            let fragment = record.message.append_fragment.as_deref()?;
            (previous.message_serial() == record.message_serial()
                && previous.version_serial() < record.version_serial()
                && data.len() == previous_data.len().saturating_add(fragment.len())
                && data.starts_with(previous_data)
                && data.ends_with(fragment))
            .then_some(reference)
            .flatten()
        });
        let reference = VersionTextReference {
            snapshot_key: previous_reference.map_or_else(
                || {
                    format!(
                        "{}:{}{}",
                        record.message_serial().as_str().len(),
                        record.message_serial().as_str(),
                        record.version_serial().as_str()
                    )
                },
                |reference| reference.snapshot_key.clone(),
            ),
            byte_len: data.len(),
            retain_for_receipts: previous_reference
                .is_some_and(|reference| reference.retain_for_receipts)
                || record
                    .envelope
                    .as_ref()
                    .is_some_and(|envelope| envelope.idempotency.is_some()),
        };
        let envelope_text = record.envelope.as_ref().is_some_and(
            |envelope| matches!(&envelope.data, Some(MessageContent::Text(text)) if text == data),
        );
        let mut encoded = Self {
            record: record.clone(),
            text: Some(reference.clone()),
            envelope_text,
        };
        let latest_bytes = encoded.encode()?;
        encoded.record.message.data = None;
        if envelope_text && let Some(envelope) = encoded.record.envelope.as_mut() {
            envelope.data = None;
        }
        Ok(VersionStoragePlan {
            entry_bytes: encoded.encode()?,
            latest_bytes,
            snapshot: Some((reference, data.clone())),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::versioned_messages::{
        MessageSerial, VersionMetadata, VersionSerial, VersionedMessage,
    };

    fn record() -> StoredVersionRecord {
        let mut message = VersionedMessage::new_create(
            MessageSerial::new("message").unwrap(),
            VersionMetadata {
                serial: VersionSerial::new("version:1").unwrap(),
                client_id: None,
                timestamp_ms: 1,
                description: None,
                metadata: None,
            },
            1,
            1,
            None,
            Some(MessageData::String("α世界".into())),
            None,
        );
        message.action = MessageAction::Append;
        message.append_fragment = Some("世界".into());
        StoredVersionRecord {
            app_id: "app".into(),
            channel: "room".into(),
            original_client_id: None,
            envelope: None,
            message,
        }
    }

    #[test]
    fn legacy_bytes_and_compact_historical_record_are_exact() {
        let record = record();
        let legacy = sonic_rs::to_vec(&record).unwrap();
        let old = EncodedVersionRecord::decode(&legacy)
            .unwrap()
            .materialize(None)
            .unwrap();
        assert_eq!(sonic_rs::to_vec(&old).unwrap(), legacy);
        let plan = EncodedVersionRecord::plan(&record, None).unwrap();
        assert!(sonic_rs::from_slice::<StoredVersionRecord>(&plan.entry_bytes).is_err());
        let encoded = EncodedVersionRecord::decode(&plan.entry_bytes).unwrap();
        assert!(encoded.needs_snapshot());
        assert!(encoded.clone().materialize(None).is_err());
        let reconstructed = encoded.materialize(Some("α世界 later data")).unwrap();
        assert_eq!(sonic_rs::to_vec(&reconstructed).unwrap(), legacy);
        let latest = EncodedVersionRecord::decode(&plan.latest_bytes).unwrap();
        assert!(!latest.needs_snapshot());
        assert_eq!(
            sonic_rs::to_vec(&latest.materialize(None).unwrap()).unwrap(),
            legacy
        );
    }

    #[test]
    fn corrupt_or_unknown_storage_never_yields_truncated_text() {
        assert!(EncodedVersionRecord::decode(b"SVR3\0{}").is_err());
        assert!(EncodedVersionRecord::decode(b"SVR2\0{}").is_err());
        let plan = EncodedVersionRecord::plan(&record(), None).unwrap();
        let mut encoded = EncodedVersionRecord::decode(&plan.entry_bytes).unwrap();
        assert!(encoded.clone().materialize(Some("α")).is_err());
        encoded.text.as_mut().unwrap().byte_len = 1;
        assert!(encoded.materialize(Some("α世界")).is_err());
    }

    #[test]
    fn mismatched_predecessor_creates_independent_snapshot() {
        let first = record();
        let plan = EncodedVersionRecord::plan(&first, None).unwrap();
        let reference = &plan.snapshot.as_ref().unwrap().0;
        let mut next = first.clone();
        next.message.version.serial = VersionSerial::new("version:2").unwrap();
        next.message.data = Some(MessageData::String("α世界!".into()));
        next.message.append_fragment = Some("!".into());
        let shared = EncodedVersionRecord::plan(&next, Some((&first, Some(reference)))).unwrap();
        assert_eq!(
            shared.snapshot.unwrap().0.snapshot_key,
            reference.snapshot_key
        );
        next.message.data = Some(MessageData::String("different!".into()));
        let independent =
            EncodedVersionRecord::plan(&next, Some((&first, Some(reference)))).unwrap();
        assert_ne!(
            independent.snapshot.unwrap().0.snapshot_key,
            reference.snapshot_key
        );
    }
}
