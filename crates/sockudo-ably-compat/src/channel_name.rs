//! Parsed channel names for the opt-in Ably compatibility boundary.

use thiserror::Error;

/// Maximum UTF-8 wire length accepted by the Sockudo Ably facade.
pub(crate) const MAX_ABLY_CHANNEL_NAME_BYTES: usize = 16 * 1024;

/// An Ably channel name split into its requested and storage identities.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AblyChannelName {
    requested: Box<str>,
    base_start: usize,
}

impl AblyChannelName {
    pub(crate) fn parse(raw: String) -> Result<Self, AblyChannelNameError> {
        let invalid = |raw, kind| AblyChannelNameError { raw, kind };
        if raw.is_empty() {
            return Err(invalid(raw, AblyChannelNameErrorKind::Empty));
        }
        if raw.len() > MAX_ABLY_CHANNEL_NAME_BYTES {
            return Err(invalid(raw, AblyChannelNameErrorKind::Oversized));
        }
        if raw.chars().any(char::is_control) {
            return Err(invalid(raw, AblyChannelNameErrorKind::ControlCharacter));
        }

        let base_start = if raw.starts_with('[') {
            qualifier_end(&raw)
                .ok_or_else(|| invalid(raw.clone(), AblyChannelNameErrorKind::MalformedQualifier))?
        } else {
            0
        };
        let base = &raw[base_start..];
        if base.is_empty() || base.starts_with(['[', ':']) {
            return Err(invalid(raw, AblyChannelNameErrorKind::ReservedPrefix));
        }

        Ok(Self {
            requested: raw.into_boxed_str(),
            base_start,
        })
    }

    pub(crate) fn requested(&self) -> &str {
        &self.requested
    }

    pub(crate) fn base(&self) -> &str {
        &self.requested[self.base_start..]
    }

    pub(crate) fn qualifier(&self) -> Option<&str> {
        (self.base_start != 0).then(|| &self.requested[1..self.base_start - 1])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AblyChannelNameErrorKind {
    Empty,
    Oversized,
    ControlCharacter,
    MalformedQualifier,
    ReservedPrefix,
}

impl std::fmt::Display for AblyChannelNameErrorKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "channel name must not be empty",
            Self::Oversized => "channel name exceeds the compatibility limit",
            Self::ControlCharacter => "channel name contains a control character",
            Self::MalformedQualifier => "channel name has a malformed qualifier",
            Self::ReservedPrefix => "base channel name starts with a reserved prefix",
        })
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("invalid Ably channel name: {kind}")]
pub(crate) struct AblyChannelNameError {
    raw: String,
    kind: AblyChannelNameErrorKind,
}

fn qualifier_end(raw: &str) -> Option<usize> {
    let mut depth = 0_u32;
    let mut quote = None;
    let mut escaped = false;

    for (index, character) in raw.char_indices() {
        if let Some(delimiter) = quote {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == delimiter {
                quote = None;
            }
            continue;
        }

        match character {
            '\'' | '"' | '`' => quote = Some(character),
            '[' => depth = depth.checked_add(1)?,
            ']' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    if raw[1..index].trim().is_empty() {
                        return None;
                    }
                    return Some(index + character.len_utf8());
                }
            }
            _ => {}
        }
    }
    None
}

impl AblyChannelNameError {
    pub(crate) fn requested(&self) -> &str {
        &self.raw
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn accepts_ably_character_set_and_extracts_base_channel() {
        for raw in [
            "space after decode",
            "namespace:room:updates",
            r#"publish {\"transport\":\"web_socket\"}"#,
            "emoji-😅🎉🚀-日本語",
            "[?rewind=1]history room",
            r#"[filter=name == `\"filtered\"` && headers.number == `26095`]chan"#,
        ] {
            let parsed = AblyChannelName::parse(raw.to_string())
                .unwrap_or_else(|error| panic!("{raw:?} should be valid: {error}"));
            assert_eq!(parsed.requested(), raw);
        }

        let qualified = AblyChannelName::parse("[filter=headers.role == `admin`]rooms:all".into())
            .expect("qualified channel should parse");
        assert_eq!(
            qualified.requested(),
            "[filter=headers.role == `admin`]rooms:all"
        );
        assert_eq!(qualified.base(), "rooms:all");
        assert_eq!(
            qualified.qualifier(),
            Some("filter=headers.role == `admin`")
        );
    }

    #[test]
    fn rejects_invalid_channel_shapes() {
        for raw in [
            "",
            ":reserved",
            "line\nfeed",
            "carriage\rreturn",
            "nul\0byte",
            "[",
            "[]base",
            "[filter=missing-close",
            "[filter=ok]",
            "[filter=ok]:reserved",
            "[filter=ok][second]base",
        ] {
            assert!(
                AblyChannelName::parse(raw.to_string()).is_err(),
                "{raw:?} should be invalid"
            );
        }
    }

    #[test]
    fn enforces_bounded_utf8_wire_length() {
        let maximum = "a".repeat(MAX_ABLY_CHANNEL_NAME_BYTES);
        assert!(AblyChannelName::parse(maximum).is_ok());
        let oversized = "a".repeat(MAX_ABLY_CHANNEL_NAME_BYTES + 1);
        assert!(AblyChannelName::parse(oversized).is_err());

        let unicode_at_limit = "🚀".repeat(MAX_ABLY_CHANNEL_NAME_BYTES / '🚀'.len_utf8());
        assert_eq!(unicode_at_limit.len(), MAX_ABLY_CHANNEL_NAME_BYTES);
        assert!(AblyChannelName::parse(unicode_at_limit).is_ok());
    }

    fn percent_round_trip(raw: &str) -> String {
        let encoded = raw
            .as_bytes()
            .iter()
            .map(|byte| format!("%{byte:02X}"))
            .collect::<String>();
        let decoded = encoded
            .as_bytes()
            .chunks_exact(3)
            .map(|chunk| {
                u8::from_str_radix(std::str::from_utf8(&chunk[1..]).expect("ASCII hex"), 16)
                    .expect("valid hex")
            })
            .collect::<Vec<_>>();
        String::from_utf8(decoded).expect("original input was UTF-8")
    }

    proptest! {
        #[test]
        fn percent_decoded_printable_unicode_and_spaces_parse(
            chars in proptest::collection::vec(
                any::<char>().prop_filter("printable non-reserved channel character", |ch| {
                    !ch.is_control() && *ch != '[' && *ch != ':'
                }),
                1..128,
            ),
        ) {
            let raw = chars.into_iter().collect::<String>();
            let decoded = percent_round_trip(&raw);
            let parsed = AblyChannelName::parse(decoded).expect("decoded printable name should parse");
            prop_assert_eq!(parsed.base(), raw);
        }

        #[test]
        fn control_characters_are_always_rejected(
            prefix in "[^\\p{C}]{0,32}",
            control in prop_oneof![Just('\0'), Just('\n'), Just('\r'), Just('\u{7f}'), Just('\u{85}')],
            suffix in "[^\\p{C}]{0,32}",
        ) {
            let raw = format!("{prefix}{control}{suffix}");
            prop_assert!(AblyChannelName::parse(raw).is_err());
        }

        #[test]
        fn valid_qualifiers_share_the_base_channel(
            qualifier in "[A-Za-z?][A-Za-z0-9_?=&.% -]{0,64}",
            base in "[A-Za-z0-9][A-Za-z0-9 _.:{}\"-]{0,64}",
        ) {
            let raw = format!("[{qualifier}]{base}");
            let parsed = AblyChannelName::parse(raw.clone()).expect("generated qualifier should parse");
            prop_assert_eq!(parsed.requested(), raw);
            prop_assert_eq!(parsed.base(), base);
        }

        #[test]
        fn malformed_or_missing_qualifier_close_is_rejected(
            qualifier in "[A-Za-z0-9?=&.% -]{0,64}",
            base in "[A-Za-z0-9][A-Za-z0-9 _.:{}\"-]{0,64}",
        ) {
            let raw = format!("[{qualifier}{base}");
            prop_assert!(AblyChannelName::parse(raw).is_err());
        }
    }
}
