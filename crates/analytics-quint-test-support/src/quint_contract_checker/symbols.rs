fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum CodeToken<'a> {
    Identifier(&'a str, usize),
    Punctuation(u8),
}

pub(super) struct SourceSymbols<'a> {
    pub(super) tokens: Vec<CodeToken<'a>>,
}

impl<'a> SourceSymbols<'a> {
    pub(super) fn parse(source: &'a str) -> Self {
        Self {
            tokens: lex_code(source),
        }
    }

    pub(super) fn has_model_symbol(&self, keyword: &str, symbol: &str) -> bool {
        let keyword_count = keyword.split_whitespace().count();
        self.tokens.windows(keyword_count + 1).any(|window| {
            keyword.split_whitespace().zip(window).all(|(expected, token)| {
                matches!(token, CodeToken::Identifier(actual, _) if *actual == expected)
            }) && matches!(&window[keyword_count], CodeToken::Identifier(actual, _) if *actual == symbol)
        })
    }

    pub(super) fn has_model_value(&self, symbol: &str) -> bool {
        is_quint_identifier(symbol)
            && self.tokens.windows(3).any(|window| {
                matches!(window[0], CodeToken::Identifier("val", _))
                    && matches!(window[1], CodeToken::Identifier(actual, _) if actual == symbol)
                    && matches!(window[2], CodeToken::Punctuation(b'=' | b':'))
            })
    }

    pub(super) fn has_identifier(&self, symbol: &str) -> bool {
        self.tokens
            .iter()
            .any(|token| matches!(token, CodeToken::Identifier(actual, _) if *actual == symbol))
    }
}

#[cfg(test)]
pub(crate) fn model_symbol_present(source: &str, keyword: &str, symbol: &str) -> bool {
    SourceSymbols::parse(source).has_model_symbol(keyword, symbol)
}

#[cfg(test)]
pub(crate) fn identifier_present(source: &str, symbol: &str) -> bool {
    SourceSymbols::parse(source).has_identifier(symbol)
}

pub(crate) fn is_quint_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(
        bytes.next(),
        Some(byte) if byte.is_ascii_alphabetic() || byte == b'_'
    ) && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn lex_code(source: &str) -> Vec<CodeToken<'_>> {
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'/') {
            index = skip_line_comment(bytes, index + 2);
        } else if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
            index = skip_block_comment(bytes, index + 2);
        } else if let Some(end) = skip_prefixed_literal(bytes, index) {
            index = end;
        } else if bytes[index] == b'"' {
            index = skip_quoted_literal(bytes, index + 1, b'"');
        } else if bytes[index] == b'\'' {
            if let Some(end) = skip_char_literal(bytes, index) {
                index = end;
            } else {
                tokens.push(CodeToken::Punctuation(bytes[index]));
                index += 1;
            }
        } else if is_identifier_byte(bytes[index]) && !bytes[index].is_ascii_digit() {
            let start = index;
            index += 1;
            while index < bytes.len() && is_identifier_byte(bytes[index]) {
                index += 1;
            }
            tokens.push(CodeToken::Identifier(
                source
                    .get(start..index)
                    .expect("ASCII identifier boundaries"),
                start,
            ));
        } else if bytes[index].is_ascii_whitespace() {
            index += 1;
        } else {
            tokens.push(CodeToken::Punctuation(bytes[index]));
            index += 1;
        }
    }
    tokens
}

fn skip_line_comment(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() && bytes[index] != b'\n' {
        index += 1;
    }
    index
}

fn skip_block_comment(bytes: &[u8], mut index: usize) -> usize {
    let mut depth = 1;
    while index < bytes.len() && depth > 0 {
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
            depth += 1;
            index += 2;
        } else if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
            depth -= 1;
            index += 2;
        } else {
            index += 1;
        }
    }
    index
}

fn skip_prefixed_literal(bytes: &[u8], index: usize) -> Option<usize> {
    if (bytes[index] == b'b' || bytes[index] == b'c') && bytes.get(index + 1) == Some(&b'"') {
        return Some(skip_quoted_literal(bytes, index + 2, b'"'));
    } else if bytes[index] == b'b' && bytes.get(index + 1) == Some(&b'\'') {
        return skip_char_literal(bytes, index + 1);
    } else if bytes[index] != b'r' && !(bytes[index] == b'b' && bytes.get(index + 1) == Some(&b'r'))
    {
        return None;
    }

    let raw_start = if bytes[index] == b'r' {
        index + 1
    } else {
        index + 2
    };
    let mut quote = raw_start;
    while bytes.get(quote) == Some(&b'#') {
        quote += 1;
    }
    if bytes.get(quote) != Some(&b'"') {
        return None;
    }
    let hashes = quote - raw_start;
    let mut index = quote + 1;
    while index < bytes.len() {
        if bytes[index] == b'"'
            && bytes.get(index + 1..index + 1 + hashes) == Some(&bytes[quote - hashes..quote])
        {
            return Some(index + hashes + 1);
        }
        index += 1;
    }
    Some(bytes.len())
}

fn skip_quoted_literal(bytes: &[u8], mut index: usize, quote: u8) -> usize {
    let mut escaped = false;
    while index < bytes.len() {
        let byte = bytes[index];
        index += 1;
        if escaped {
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == quote {
            break;
        }
    }
    index
}

fn skip_char_literal(bytes: &[u8], index: usize) -> Option<usize> {
    let mut cursor = index + 1;
    let first = *bytes.get(cursor)?;
    if first == b'\'' || first == b'\n' || first == b'\r' {
        return None;
    }

    if first == b'\\' {
        cursor = skip_character_escape(bytes, cursor + 1)?;
    } else {
        let width = match first {
            0x00..=0x7f => 1,
            0xc2..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf4 => 4,
            _ => return None,
        };
        let end = cursor + width;
        if end > bytes.len() || std::str::from_utf8(&bytes[cursor..end]).is_err() {
            return None;
        }
        cursor = end;
    }
    (bytes.get(cursor) == Some(&b'\'')).then_some(cursor + 1)
}

fn skip_character_escape(bytes: &[u8], mut cursor: usize) -> Option<usize> {
    match bytes.get(cursor) {
        Some(b'u') if bytes.get(cursor + 1) == Some(&b'{') => {
            cursor += 2;
            let start = cursor;
            while bytes.get(cursor).is_some_and(|byte| *byte != b'}') {
                if !bytes[cursor].is_ascii_hexdigit() {
                    return None;
                }
                cursor += 1;
            }
            if cursor == start || bytes.get(cursor) != Some(&b'}') {
                return None;
            }
            Some(cursor + 1)
        }
        Some(b'x') => {
            cursor += 1;
            (bytes.get(cursor).is_some_and(u8::is_ascii_hexdigit)
                && bytes.get(cursor + 1).is_some_and(u8::is_ascii_hexdigit))
            .then_some(cursor + 2)
        }
        Some(_) => Some(cursor + 1),
        None => None,
    }
}

pub(super) fn camel_to_snake(value: &str) -> String {
    let mut output = String::new();
    let mut previous = None;
    for character in value.chars() {
        if character.is_ascii_uppercase()
            && previous.is_some_and(|previous: char| {
                previous.is_ascii_lowercase() || previous.is_ascii_digit()
            })
        {
            output.push('_');
        }
        output.push(character.to_ascii_lowercase());
        previous = Some(character);
    }
    output
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum DeclarationRole {
    Behavior,
    Oracle,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Declaration<'a> {
    pub(super) role: DeclarationRole,
    pub(super) name: &'a str,
    pub(super) start: usize,
}

#[cfg(test)]
pub(super) fn declaration_at(source: &str, offset: usize) -> Option<Declaration<'_>> {
    SourceSymbols::parse(source).find_declaration_at(offset)
}

impl<'a> SourceSymbols<'a> {
    pub(super) fn find_declaration_at(&self, offset: usize) -> Option<Declaration<'a>> {
        let mut scope = DeclarationScope::default();
        let mut declaration = None;
        for (index, token) in self.tokens.iter().enumerate() {
            if scope.is_module_scope()
                && let Some(candidate) = self.find_declaration(index)
                && candidate.start <= offset
            {
                declaration = Some(candidate);
            }
            scope.observe_token(*token);
        }
        declaration
    }

    fn find_declaration(&self, index: usize) -> Option<Declaration<'a>> {
        let CodeToken::Identifier(keyword, start) = *self.tokens.get(index)? else {
            return None;
        };
        let (role, name_offset) = if keyword == "pure" {
            let CodeToken::Identifier(kind, _) = *self.tokens.get(index + 1)? else {
                return None;
            };
            (declaration_role(kind)?, 2)
        } else {
            (declaration_role(keyword)?, 1)
        };
        let CodeToken::Identifier(name, _) = *self.tokens.get(index + name_offset)? else {
            return None;
        };
        Some(Declaration { role, name, start })
    }
}

#[derive(Default)]
struct DeclarationScope {
    brace_depth: usize,
    module_started: bool,
    module_pending: bool,
    module_name_seen: bool,
    module_body_depth: Option<usize>,
}

impl DeclarationScope {
    fn is_module_scope(&self) -> bool {
        self.module_started
            && self
                .module_body_depth
                .map_or(self.brace_depth == 0, |depth| self.brace_depth == depth)
    }

    fn observe_token(&mut self, token: CodeToken<'_>) {
        if self.module_pending {
            match token {
                CodeToken::Identifier(_, _) if !self.module_name_seen => {
                    self.module_name_seen = true
                }
                CodeToken::Punctuation(b'{') if self.module_name_seen => {
                    self.brace_depth += 1;
                    self.module_body_depth = Some(self.brace_depth);
                    self.module_pending = false;
                }
                _ => self.module_pending = false,
            }
            return;
        }
        match token {
            CodeToken::Identifier("module", _) if self.brace_depth == 0 => {
                self.module_started = true;
                self.module_pending = true;
                self.module_name_seen = false;
            }
            CodeToken::Punctuation(b'{') => self.brace_depth += 1,
            CodeToken::Punctuation(b'}') => self.brace_depth = self.brace_depth.saturating_sub(1),
            _ => {}
        }
    }
}

fn declaration_role(keyword: &str) -> Option<DeclarationRole> {
    match keyword {
        "action" | "def" => Some(DeclarationRole::Behavior),
        "val" | "run" | "type" | "var" => Some(DeclarationRole::Oracle),
        _ => None,
    }
}

#[derive(Clone, Copy)]
pub(super) enum SymbolKind {
    Model,
    Adapter,
}

pub(super) struct SymbolRequirements<'a> {
    pub(super) actions: &'a [String],
    pub(super) states: &'a [String],
    pub(super) kind: SymbolKind,
}
