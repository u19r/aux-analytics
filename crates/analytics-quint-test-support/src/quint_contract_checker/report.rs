use std::fmt;

#[derive(Debug, Eq, PartialEq)]
pub struct CheckReport {
    pub rules: usize,
    pub readable_models: usize,
}

#[derive(Debug, Default)]
pub struct RunPlan {
    pub records: Vec<String>,
    pub mutation_receipts: Vec<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct CheckFailure {
    pub(super) diagnostics: Vec<String>,
}

impl fmt::Display for CheckFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for diagnostic in &self.diagnostics {
            writeln!(formatter, "quint contract error: {diagnostic}")?;
        }
        Ok(())
    }
}

impl std::error::Error for CheckFailure {}
