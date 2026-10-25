use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedCurrentRowItem {
    pub partition: Option<String>,
    pub row: ProjectedCurrentRow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectedCurrentRow {
    pub key: String,
    /// Values stay in a compact map through the current-row pipeline. The
    /// serializer sorts keys to retain the previous canonical JSON bytes.
    #[serde(serialize_with = "serialize_projected_values")]
    pub values: VecMap<String, Option<String>>,
    pub source_position: CheckSourcePosition,
    pub contains_private_data: bool,
}

fn serialize_projected_values<S>(
    values: &VecMap<String, Option<String>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
    let mut map = serializer.serialize_map(Some(entries.len()))?;
    for (key, value) in entries {
        map.serialize_entry(key, value)?;
    }
    map.end()
}
