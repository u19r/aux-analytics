use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectedCurrentRowPage {
    pub rows: Vec<ProjectedCurrentRow>,
    pub next_cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentRowPageRequest {
    pub projection: CheckRowProjection,
    pub cursor: Option<String>,
    pub page_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentRowStreamOrder {
    GloballySortedByKey,
    PartitionSortedByKey,
    Unordered,
}
