//! Hardened Fossil delta decoder.
//!
//! Deltas arrive from the network, so unlike a trusting decoder this one never
//! panics, never indexes out of bounds, rejects arithmetic overflow, and caps
//! the output size before allocating.

use crate::error::{Error, Result};

const INVALID: u8 = 0xff;

/// Fossil's base-64 digit alphabet, inverted.
const DIGIT_VALUES: [u8; 256] = {
    let digits = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~";
    let mut table = [INVALID; 256];
    let mut index = 0;
    while index < digits.len() {
        table[digits[index] as usize] = index as u8;
        index += 1;
    }
    table
};

struct Reader<'a> {
    data: &'a [u8],
    position: usize,
}

impl Reader<'_> {
    fn has_more(&self) -> bool {
        self.position < self.data.len()
    }

    fn byte(&mut self) -> Result<u8> {
        let byte = *self
            .data
            .get(self.position)
            .ok_or_else(|| fail("unexpected end of delta"))?;
        self.position += 1;
        Ok(byte)
    }

    /// Read a base-64 integer. Stops (without consuming) at the first
    /// non-digit byte.
    fn integer(&mut self) -> Result<usize> {
        let mut value: usize = 0;
        let start = self.position;
        while let Some(&byte) = self.data.get(self.position) {
            let digit = DIGIT_VALUES[byte as usize];
            if digit == INVALID {
                break;
            }
            value = value
                .checked_mul(64)
                .and_then(|value| value.checked_add(usize::from(digit)))
                .ok_or_else(|| fail("integer overflow"))?;
            self.position += 1;
        }
        if self.position == start {
            return Err(fail("expected an integer"));
        }
        Ok(value)
    }
}

fn fail(message: &str) -> Error {
    Error::Delta(format!("fossil: {message}"))
}

/// Apply a Fossil delta to `base`, producing at most `max_output` bytes.
pub(crate) fn apply(base: &[u8], delta: &[u8], max_output: usize) -> Result<Vec<u8>> {
    let mut reader = Reader {
        data: delta,
        position: 0,
    };
    let output_size = reader.integer()?;
    if reader.byte()? != b'\n' {
        return Err(fail("size integer not terminated by newline"));
    }
    if output_size > max_output {
        return Err(fail("declared output exceeds the maximum message size"));
    }
    let mut output = Vec::with_capacity(output_size);
    while reader.has_more() {
        let count = reader.integer()?;
        match reader.byte()? {
            b'@' => {
                let offset = reader.integer()?;
                if reader.has_more() && reader.byte()? != b',' {
                    return Err(fail("copy command not terminated by comma"));
                }
                let end = offset
                    .checked_add(count)
                    .ok_or_else(|| fail("copy range overflow"))?;
                if output.len() + count > output_size {
                    return Err(fail("copy exceeds output file size"));
                }
                let chunk = base
                    .get(offset..end)
                    .ok_or_else(|| fail("copy extends past end of input"))?;
                output.extend_from_slice(chunk);
            }
            b':' => {
                if output.len() + count > output_size {
                    return Err(fail("insert command gives an output larger than predicted"));
                }
                let end = reader
                    .position
                    .checked_add(count)
                    .ok_or_else(|| fail("insert range overflow"))?;
                let chunk = delta
                    .get(reader.position..end)
                    .ok_or_else(|| fail("insert count exceeds size of delta"))?;
                output.extend_from_slice(chunk);
                reader.position = end;
            }
            b';' => {
                if u32::try_from(count).ok() != Some(checksum(&output)) {
                    return Err(fail("bad checksum"));
                }
                if output.len() != output_size {
                    return Err(fail("generated size does not match predicted size"));
                }
                return Ok(output);
            }
            _ => return Err(fail("unknown delta operator")),
        }
    }
    Err(fail("unterminated delta"))
}

/// Fossil's checksum: the wrapping sum of big-endian 32-bit words, with the
/// trailing partial word zero-padded.
fn checksum(data: &[u8]) -> u32 {
    let (words, tail) = data.as_chunks::<4>();
    let mut sum = words.iter().fold(0u32, |sum, word| {
        sum.wrapping_add(u32::from_be_bytes(*word))
    });
    if !tail.is_empty() {
        let mut padded = [0u8; 4];
        padded[..tail.len()].copy_from_slice(tail);
        sum = sum.wrapping_add(u32::from_be_bytes(padded));
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: usize = 1 << 20;

    /// The server computes `delta(new, old)` with the fossil-delta crate; the
    /// result applies to `old` to give `new`.
    fn server_delta(old: &[u8], new: &[u8]) -> Vec<u8> {
        fossil_delta::delta(new, old)
    }

    #[test]
    fn applies_server_generated_deltas() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"", b""),
            (b"", b"hello"),
            (b"hello", b""),
            (
                br#"{"event":"price","channel":"btc","data":"{\"price\":100.25,\"volume\":1}"}"#,
                br#"{"event":"price","channel":"btc","data":"{\"price\":100.75,\"volume\":3}"}"#,
            ),
            (b"abc", b"abcabcabcabcabcabcabcabcabcabcabcabc"),
        ];
        for (old, new) in cases {
            let delta = server_delta(old, new);
            assert_eq!(apply(old, &delta, MAX).unwrap(), *new);
        }
    }

    #[test]
    fn applies_large_pseudo_random_edits() {
        let mut state = 0x9e37_79b9_u32;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state
        };
        for round in 0..40 {
            let len = 64 + (next() as usize % 4096);
            let old: Vec<u8> = (0..len).map(|_| b'a' + (next() % 26) as u8).collect();
            let mut new = old.clone();
            for _ in 0..(round % 7 + 1) {
                let at = next() as usize % new.len();
                new[at] = b'0' + (next() % 10) as u8;
            }
            new.extend_from_slice(b"tail");
            let delta = server_delta(&old, &new);
            assert_eq!(apply(&old, &delta, MAX).unwrap(), new);
        }
    }

    #[test]
    fn checksum_matches_reference_for_all_tail_lengths() {
        for len in 0..9 {
            let data: Vec<u8> = (0..len as u8).map(|b| b.wrapping_mul(37)).collect();
            let delta = server_delta(b"", &data);
            assert_eq!(apply(b"", &delta, MAX).unwrap(), data);
        }
    }

    #[test]
    fn rejects_corrupt_and_hostile_input() {
        let old = b"the quick brown fox jumps over the lazy dog";
        let new = b"the quick brown cat jumps over the lazy dog!";
        let delta = server_delta(old, new);

        // Truncations never panic.
        for cut in 0..delta.len() {
            let _ = apply(old, &delta[..cut], MAX);
        }
        // Wrong base fails the checksum or bounds.
        assert!(
            apply(
                b"different base entirely......................",
                &delta,
                MAX
            )
            .is_err()
        );
        // Copy past the base.
        assert!(apply(b"x", b"5\n5@0,1;", MAX).is_err());
        // Declared size above the cap is rejected before allocating.
        assert!(apply(b"", b"~~~~~~~~\n0;", MAX).is_err());
        // Integer overflow.
        assert!(apply(b"", b"~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n", usize::MAX).is_err());
        // Unknown operator, missing newline, no terminator.
        assert!(apply(b"", b"0\n0?", MAX).is_err());
        assert!(apply(b"", b"0;", MAX).is_err());
        assert!(apply(b"ab", b"2\n2@0,", MAX).is_err());
        // Insert longer than the remaining delta.
        assert!(apply(b"", b"5\n5:ab", MAX).is_err());
    }
}
