use super::{AnalyticsEngineError, AnalyticsEngineResult};

pub(super) fn validate_read_only_query(sql: &str) -> AnalyticsEngineResult<&str> {
    let mut query = ReadOnlyQueryScan {
        bytes: sql.as_bytes(),
        index: 0,
        saw_first_keyword: false,
        statement_terminator: None,
        last_significant_end: 0,
    };
    while query.index < query.bytes.len() {
        query.advance()?;
    }
    if !query.saw_first_keyword {
        return Err(AnalyticsEngineError::InvalidQuery);
    }
    let end = query
        .statement_terminator
        .unwrap_or(query.last_significant_end);
    Ok(sql
        .get(..end)
        .ok_or(AnalyticsEngineError::InvalidQuery)?
        .trim_end())
}

struct ReadOnlyQueryScan<'a> {
    bytes: &'a [u8],
    index: usize,
    saw_first_keyword: bool,
    statement_terminator: Option<usize>,
    last_significant_end: usize,
}

impl ReadOnlyQueryScan<'_> {
    fn advance(&mut self) -> AnalyticsEngineResult<()> {
        let byte = self.bytes[self.index];
        if byte.is_ascii_whitespace() {
            self.index += 1;
        } else if byte == b'-' && self.bytes.get(self.index + 1) == Some(&b'-') {
            self.index = skip_sql_line_comment(self.bytes, self.index + 2);
        } else if byte == b'/' && self.bytes.get(self.index + 1) == Some(&b'*') {
            self.index = skip_sql_block_comment(self.bytes, self.index + 2);
        } else if byte == b';' {
            if self.statement_terminator.is_some() {
                return Err(AnalyticsEngineError::InvalidQuery);
            }
            self.statement_terminator = Some(self.index);
            self.index += 1;
        } else if byte.is_ascii_alphabetic() || byte == b'_' {
            self.advance_keyword()?;
        } else {
            self.advance_literal_or_symbol(byte)?;
        }
        Ok(())
    }

    fn require_statement_content(&self) -> AnalyticsEngineResult<()> {
        if self.statement_terminator.is_some() || !self.saw_first_keyword {
            Err(AnalyticsEngineError::InvalidQuery)
        } else {
            Ok(())
        }
    }

    fn advance_keyword(&mut self) -> AnalyticsEngineResult<()> {
        if self.statement_terminator.is_some() {
            return Err(AnalyticsEngineError::InvalidQuery);
        }
        let start = self.index;
        self.index += 1;
        while self.index < self.bytes.len()
            && (self.bytes[self.index].is_ascii_alphanumeric() || self.bytes[self.index] == b'_')
        {
            self.index += 1;
        }
        let token = &self.bytes[start..self.index];
        if !self.saw_first_keyword {
            if !token.eq_ignore_ascii_case(b"select") && !token.eq_ignore_ascii_case(b"with") {
                return Err(AnalyticsEngineError::InvalidQuery);
            }
            self.saw_first_keyword = true;
        }
        if is_forbidden_sql_keyword(token) {
            return Err(AnalyticsEngineError::InvalidQuery);
        }
        self.last_significant_end = self.index;
        Ok(())
    }

    fn advance_literal_or_symbol(&mut self, byte: u8) -> AnalyticsEngineResult<()> {
        // Dollar-tag validation precedes statement validation to retain
        // malformed-tag rejection.
        let dollar_end = if byte == b'$' {
            skip_sql_dollar_quoted(self.bytes, self.index)
                .map_err(|()| AnalyticsEngineError::InvalidQuery)?
        } else {
            None
        };
        self.require_statement_content()?;
        self.index = match byte {
            b'\'' | b'"' | b'`' => skip_sql_quoted(self.bytes, self.index, byte),
            b'$' => dollar_end.unwrap_or(self.index + 1),
            _ => self.index + 1,
        };
        self.last_significant_end = self.index;
        Ok(())
    }
}

fn is_forbidden_sql_keyword(token: &[u8]) -> bool {
    use crate::constants::FORBIDDEN_SQL_KEYWORDS;
    FORBIDDEN_SQL_KEYWORDS
        .iter()
        .any(|keyword| token.eq_ignore_ascii_case(keyword))
}

fn skip_sql_quoted(bytes: &[u8], mut index: usize, quote: u8) -> usize {
    index += 1;
    while index < bytes.len() {
        if bytes[index] == quote {
            if bytes.get(index + 1) == Some(&quote) {
                index += 2;
            } else {
                return index + 1;
            }
        } else {
            index += 1;
        }
    }
    index
}

use crate::constants::MAX_SQL_DOLLAR_TAG_LEN;

/// Finds the end of a dollar-quoted SQL literal.
///
/// Read-only query validation accepts tags up to 32 bytes. The bound makes the
/// delimiter comparison below a constant-size operation, so the scan remains
/// linear in the query length instead of depending on an unbounded tag size.
fn skip_sql_dollar_quoted(bytes: &[u8], index: usize) -> Result<Option<usize>, ()> {
    let mut delimiter_end = index + 1;
    while delimiter_end < bytes.len()
        && (bytes[delimiter_end].is_ascii_alphanumeric() || bytes[delimiter_end] == b'_')
    {
        delimiter_end += 1;
    }
    if delimiter_end - (index + 1) > MAX_SQL_DOLLAR_TAG_LEN {
        return Err(());
    }
    if bytes.get(delimiter_end) != Some(&b'$') {
        return Ok(None);
    }

    let delimiter = &bytes[index..=delimiter_end];
    let mut cursor = delimiter_end + 1;
    while cursor + delimiter.len() <= bytes.len() {
        if &bytes[cursor..cursor + delimiter.len()] == delimiter {
            return Ok(Some(cursor + delimiter.len()));
        }
        cursor += 1;
    }
    Ok(Some(bytes.len()))
}

fn skip_sql_line_comment(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() && bytes[index] != b'\n' {
        index += 1;
    }
    index
}

fn skip_sql_block_comment(bytes: &[u8], mut index: usize) -> usize {
    while index + 1 < bytes.len() {
        if bytes[index] == b'*' && bytes[index + 1] == b'/' {
            return index + 2;
        }
        index += 1;
    }
    bytes.len()
}
