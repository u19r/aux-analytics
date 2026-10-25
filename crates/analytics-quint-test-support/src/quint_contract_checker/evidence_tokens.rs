use super::dates::{self, Expiry};

pub(super) fn parse_expiry(field: &str) -> Option<Expiry> {
    let words: Vec<_> = field.split_whitespace().collect();
    if words.len() != 3 || words[0] != "expiry" || !matches!(words[1], "before" | "by" | "on") {
        return None;
    }
    if let Some(day) = dates::parse_iso_date(words[2]) {
        Some(Expiry::Date {
            value: words[2].to_owned(),
            day,
        })
    } else {
        dates::phase_index(words[2]).map(|index| Expiry::Phase {
            value: words[2].to_owned(),
            index,
        })
    }
}

pub(super) fn split_paths(value: &str) -> Vec<String> {
    value.split(',').map(str::trim).map(str::to_owned).collect()
}

pub(super) fn pending_token(field: &str) -> Option<(String, bool)> {
    let lower = field.to_ascii_lowercase();
    for (start, _) in lower.match_indices("pending") {
        let before_ok = start == 0 || lower.as_bytes()[start - 1].is_ascii_whitespace();
        let after_word = start + "pending".len();
        let after_ok = lower
            .as_bytes()
            .get(after_word)
            .is_some_and(|byte| byte.is_ascii_whitespace());
        if before_ok && after_ok {
            let rest = lower.get(after_word..)?;
            let phase = rest.split_whitespace().next()?;
            let canonical = match phase {
                "u-a1" => "U-A1",
                "f-a2" => "F-A2",
                "f-a3" => "F-A3",
                _ => return None,
            };
            let phase_start = after_word + rest.find(phase)?;
            let end = phase_start + phase.len();
            return Some((
                canonical.to_owned(),
                lower
                    .as_bytes()
                    .get(end)
                    .is_some_and(|byte| !byte.is_ascii_whitespace()),
            ));
        }
    }
    None
}

pub(super) fn valid_nested_proof(value: &str) -> bool {
    let Some((key, nested)) = value.split_once('=') else {
        return false;
    };
    matches!(key.trim(), "type" | "parser" | "unit")
        && !nested.trim().is_empty()
        && !nested.contains('=')
}

pub(super) fn named_proof_present(value: &str) -> bool {
    ["type", "parser", "unit"].iter().any(|name| {
        for (start, _) in value.match_indices(name) {
            let before_ok = start == 0
                || (!value.as_bytes()[start - 1].is_ascii_alphanumeric()
                    && value.as_bytes()[start - 1] != b'_');
            let end = start + name.len();
            if before_ok {
                let mut separator_end = end;
                while value.as_bytes().get(separator_end).is_some_and(|byte| {
                    *byte == b'=' || *byte == b':' || byte.is_ascii_whitespace()
                }) {
                    separator_end += 1;
                }
                if separator_end > end
                    && value
                        .as_bytes()
                        .get(separator_end)
                        .is_some_and(|byte| *byte != b';' && !byte.is_ascii_whitespace())
                {
                    return true;
                }
            }
        }
        false
    })
}
