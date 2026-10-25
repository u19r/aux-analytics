use super::*;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckRow {
    pub table: String,
    pub key: String,
    pub row_hash: u64,
    pub source_position: CheckSourcePosition,
    pub contains_private_data: bool,
}

impl Clone for CheckRow {
    fn clone(&self) -> Self {
        #[cfg(test)]
        CHECK_ROW_CLONE_COUNT.with(|count| count.set(count.get() + 1));
        Self {
            table: self.table.clone(),
            key: self.key.clone(),
            row_hash: self.row_hash,
            source_position: self.source_position.clone(),
            contains_private_data: self.contains_private_data,
        }
    }
}

#[cfg(test)]
thread_local! {
    static CHECK_ROW_CLONE_COUNT: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_check_row_clone_count() {
    CHECK_ROW_CLONE_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn check_row_clone_count() -> usize {
    CHECK_ROW_CLONE_COUNT.with(Cell::get)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalCheckDataset {
    pub rows: Vec<CheckRow>,
}

impl LocalCheckDataset {
    #[must_use]
    pub fn new(rows: Vec<CheckRow>) -> Self {
        Self { rows }
    }
}

pub(crate) fn filter_target_tables(rows: &[CheckRow], target_tables: &[String]) -> Vec<CheckRow> {
    if let [target_table] = target_tables {
        return rows
            .iter()
            .filter(|row| row.table == *target_table)
            .cloned()
            .collect();
    }

    let target_set = target_tables
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    rows.iter()
        .filter(|row| target_set.contains(row.table.as_str()))
        .cloned()
        .collect()
}

#[cfg(test)]
pub(crate) fn filter_target_tables_baseline(
    rows: &[CheckRow],
    target_tables: &[String],
) -> Vec<CheckRow> {
    rows.iter()
        .filter(|row| target_tables.contains(&row.table))
        .cloned()
        .collect()
}
