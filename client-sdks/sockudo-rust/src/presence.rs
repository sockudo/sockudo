//! Presence channel membership.

use std::collections::BTreeMap;

use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};

/// A presence channel member.
#[derive(Debug, Clone, PartialEq)]
pub struct Member {
    /// The member's user id.
    pub id: String,
    /// The `user_info` supplied by the auth endpoint (or `presence_update`).
    pub info: Value,
}

/// Current membership of a presence channel.
///
/// Membership reflects the server's view: it is replaced wholesale on every
/// `subscription_succeeded` (including after reconnects) and updated by
/// `member_added`, `member_removed` and `presence_update`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Members {
    members: BTreeMap<String, Value>,
    count: usize,
    my_id: Option<String>,
}

impl Members {
    /// Number of members (the server-reported count when available).
    pub fn count(&self) -> usize {
        self.count
    }

    /// Whether there are no members.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Look up a member.
    pub fn get(&self, id: &str) -> Option<Member> {
        self.members.get(id).map(|info| Member {
            id: id.to_owned(),
            info: info.clone(),
        })
    }

    /// Iterate members in user-id order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.members.iter().map(|(id, info)| (id.as_str(), info))
    }

    /// All members.
    pub fn to_vec(&self) -> Vec<Member> {
        self.iter()
            .map(|(id, info)| Member {
                id: id.to_owned(),
                info: info.clone(),
            })
            .collect()
    }

    /// This connection's user id.
    pub fn my_id(&self) -> Option<&str> {
        self.my_id.as_deref()
    }

    /// This connection's member entry.
    pub fn me(&self) -> Option<Member> {
        self.get(self.my_id.as_deref()?)
    }

    pub(crate) fn set_my_id(&mut self, id: Option<String>) {
        self.my_id = id;
    }

    pub(crate) fn apply_subscription(&mut self, payload: &Value) {
        let Some(presence) = payload.get("presence") else {
            return;
        };
        let mut members = BTreeMap::new();
        if let Some(hash) = presence.get("hash").and_then(|hash| hash.as_object()) {
            for (id, info) in hash.iter() {
                members.insert(id.to_owned(), info.clone());
            }
        } else if let Some(ids) = presence.get("ids").and_then(|ids| ids.as_array()) {
            for id in ids.iter().filter_map(|id| id.as_str()) {
                members.insert(id.to_owned(), Value::new_null());
            }
        }
        self.count = presence
            .get("count")
            .and_then(|count| count.as_u64())
            .map_or(members.len(), |count| count as usize);
        self.members = members;
    }

    pub(crate) fn add(&mut self, payload: &Value) -> Option<Member> {
        let id = member_id(payload)?;
        let info = payload.get("user_info").cloned().unwrap_or_default();
        if self.members.insert(id.clone(), info.clone()).is_none() {
            self.count += 1;
        }
        Some(Member { id, info })
    }

    pub(crate) fn remove(&mut self, payload: &Value) -> Option<Member> {
        let id = member_id(payload)?;
        let info = self.members.remove(&id)?;
        self.count = self.count.saturating_sub(1);
        Some(Member { id, info })
    }

    pub(crate) fn update(&mut self, payload: &Value) -> Option<Member> {
        let id = member_id(payload)?;
        let slot = self.members.get_mut(&id)?;
        *slot = payload.get("user_info").cloned().unwrap_or_default();
        Some(Member {
            id,
            info: slot.clone(),
        })
    }

    pub(crate) fn reset(&mut self) {
        self.members.clear();
        self.count = 0;
    }
}

/// `user_id` may be sent as a string or a number.
fn member_id(payload: &Value) -> Option<String> {
    let id = payload.get("user_id")?;
    if let Some(text) = id.as_str() {
        return Some(text.to_owned());
    }
    id.as_u64()
        .map(|n| n.to_string())
        .or_else(|| id.as_i64().map(|n| n.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs::json;

    #[test]
    fn tracks_membership() {
        let mut members = Members::default();
        members.set_my_id(Some("1".into()));
        members.apply_subscription(&json!({
            "presence": {"ids": ["1", "2"], "hash": {"1": {"name": "a"}, "2": null}, "count": 2}
        }));
        assert_eq!(members.count(), 2);
        assert_eq!(members.me().unwrap().info, json!({"name": "a"}));

        let added = members
            .add(&json!({"user_id": 3, "user_info": {"name": "c"}}))
            .unwrap();
        assert_eq!(added.id, "3");
        assert_eq!(members.count(), 3);
        // Re-adding an existing member (second connection) does not double count.
        members.add(&json!({"user_id": "3", "user_info": {"name": "c2"}}));
        assert_eq!(members.count(), 3);

        let updated = members
            .update(&json!({"user_id": "1", "user_info": {"status": "busy"}}))
            .unwrap();
        assert_eq!(updated.info, json!({"status": "busy"}));
        assert!(members.update(&json!({"user_id": "nope"})).is_none());

        assert_eq!(members.remove(&json!({"user_id": "2"})).unwrap().id, "2");
        assert!(members.remove(&json!({"user_id": "2"})).is_none());
        assert_eq!(members.count(), 2);
        assert_eq!(
            members.iter().map(|(id, _)| id).collect::<Vec<_>>(),
            vec!["1", "3"]
        );
        members.reset();
        assert!(members.is_empty());
        assert_eq!(members.my_id(), Some("1"));
    }
}
