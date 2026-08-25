//! HTTP Range request parsing for resumable downloads (RFC 9110 §14).
//!
//! Scope: a single byte range. Multi-range requests (`multipart/byteranges`)
//! are ignored (full 200 body), as are syntactically invalid headers — RFC
//! leniency keeps dumb clients working. Syntactically VALID but
//! unsatisfiable ranges (start ≥ size, start > end, zero suffix) are the
//! only 416 source.

/// The resolved byte window for one response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRange {
    /// No (usable) Range header — send the whole representation (200).
    Full,
    /// Inclusive [start, end] window — send 206 + Content-Range.
    Single { start: u64, end: u64 },
    /// Valid syntax, unsatisfiable against the representation size — 416.
    Unsatisfiable,
}

/// Parse a `Range` header value against a representation of `size` bytes.
pub fn parse_range(header: Option<&str>, size: u64) -> ByteRange {
    let Some(value) = header else {
        return ByteRange::Full;
    };
    // An unknown range unit (or a malformed spec) means "ignore the header".
    let Some(spec) = value.trim().strip_prefix("bytes=") else {
        return ByteRange::Full;
    };
    // Multi-range: out of scope — serve the full body.
    if spec.contains(',') {
        return ByteRange::Full;
    }
    let Some((first, last)) = spec.split_once('-') else {
        return ByteRange::Full;
    };
    let first = first.trim();
    let last = last.trim();
    if size == 0 {
        return if first.is_empty() && last.is_empty() {
            ByteRange::Full // garbage "bytes=-" — ignore
        } else {
            ByteRange::Unsatisfiable
        };
    }
    if first.is_empty() {
        // Suffix range: the last N bytes.
        let Ok(suffix) = last.parse::<u64>() else {
            return ByteRange::Full; // garbage — ignore
        };
        if suffix == 0 {
            return ByteRange::Unsatisfiable;
        }
        let n = suffix.min(size);
        return ByteRange::Single { start: size - n, end: size - 1 };
    }
    let (Ok(start), Ok(end)) = (first.parse::<u64>(), last.parse::<u64>()) else {
        // One side unparsable: only "start-" (open end) is legal syntax.
        return match (first.parse::<u64>(), last.is_empty()) {
            (Ok(start), true) if start < size => ByteRange::Single { start, end: size - 1 },
            (Ok(_), true) => ByteRange::Unsatisfiable,
            _ => ByteRange::Full, // garbage — ignore
        };
    };
    if start > end || start >= size {
        return ByteRange::Unsatisfiable;
    }
    ByteRange::Single { start, end: end.min(size - 1) }
}

/// Entity tag for a download representation: size + mtime in hex. Weak but
/// sufficient here — version dirs are written atomically (swap), and a
/// repacked temp .lma is a fresh file each time, so a changed file always
/// yields a different tag.
pub fn make_etag(size: u64, mtime_secs: u64) -> String {
    format!("\"{:x}-{:x}\"", size, mtime_secs)
}

/// Whether an `If-Range` value permits the range to be applied: only an
/// exact ETag match does (we never send Last-Modified-only validators, so a
/// date in If-Range is treated as a mismatch → full body).
pub fn if_range_allows(if_range: Option<&str>, etag: &str) -> bool {
    match if_range {
        None => true,
        Some(value) => value.trim() == etag,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_full_without_header() {
        assert_eq!(parse_range(None, 100), ByteRange::Full);
    }

    #[test]
    fn should_parse_open_ended_range() {
        assert_eq!(
            parse_range(Some("bytes=10-"), 100),
            ByteRange::Single { start: 10, end: 99 }
        );
    }

    #[test]
    fn should_parse_closed_range() {
        assert_eq!(
            parse_range(Some("bytes=0-9"), 100),
            ByteRange::Single { start: 0, end: 9 }
        );
    }

    #[test]
    fn should_clamp_end_beyond_size() {
        assert_eq!(
            parse_range(Some("bytes=0-999999"), 100),
            ByteRange::Single { start: 0, end: 99 }
        );
    }

    #[test]
    fn should_parse_suffix_range() {
        assert_eq!(
            parse_range(Some("bytes=-20"), 100),
            ByteRange::Single { start: 80, end: 99 }
        );
    }

    #[test]
    fn should_clamp_suffix_longer_than_size() {
        assert_eq!(
            parse_range(Some("bytes=-500"), 100),
            ByteRange::Single { start: 0, end: 99 }
        );
    }

    #[test]
    fn should_reject_start_at_or_past_size() {
        assert_eq!(parse_range(Some("bytes=100-"), 100), ByteRange::Unsatisfiable);
        assert_eq!(parse_range(Some("bytes=200-300"), 100), ByteRange::Unsatisfiable);
    }

    #[test]
    fn should_reject_inverted_range() {
        assert_eq!(parse_range(Some("bytes=50-20"), 100), ByteRange::Unsatisfiable);
    }

    #[test]
    fn should_reject_zero_suffix() {
        assert_eq!(parse_range(Some("bytes=-0"), 100), ByteRange::Unsatisfiable);
    }

    #[test]
    fn should_ignore_multi_range() {
        assert_eq!(parse_range(Some("bytes=0-9,20-29"), 100), ByteRange::Full);
    }

    #[test]
    fn should_ignore_garbage() {
        assert_eq!(parse_range(Some("bytes=abc"), 100), ByteRange::Full);
        assert_eq!(parse_range(Some("items=0-9"), 100), ByteRange::Full);
        assert_eq!(parse_range(Some("bytes=-x"), 100), ByteRange::Full);
        assert_eq!(parse_range(Some("bytes=x-9"), 100), ByteRange::Full);
        assert_eq!(parse_range(Some("bytes="), 100), ByteRange::Full);
    }

    #[test]
    fn should_handle_empty_representation() {
        assert_eq!(parse_range(Some("bytes=0-"), 0), ByteRange::Unsatisfiable);
        assert_eq!(parse_range(None, 0), ByteRange::Full);
    }

    #[test]
    fn should_build_etag_from_size_and_mtime() {
        assert_eq!(make_etag(256, 0x65c3a9), "\"100-65c3a9\"");
    }

    #[test]
    fn should_gate_range_on_if_range_match() {
        assert!(if_range_allows(None, "\"a\""));
        assert!(if_range_allows(Some("\"a\""), "\"a\""));
        assert!(!if_range_allows(Some("\"stale\""), "\"a\""));
        assert!(!if_range_allows(Some("Wed, 21 Oct 2015 07:28:00 GMT"), "\"a\""));
    }
}
