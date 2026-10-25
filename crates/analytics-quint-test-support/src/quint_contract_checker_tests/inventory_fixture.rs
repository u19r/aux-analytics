use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

pub(super) struct InventoryFixture {
    pub(super) root: PathBuf,
}

impl InventoryFixture {
    pub(super) fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("aux-quint-checker-{label}-{nonce}"));
        fs::create_dir_all(root.join("specs")).expect("spec directory");
        fs::create_dir_all(root.join("scripts")).expect("script directory");
        fs::create_dir_all(root.join("crates/test-owner")).expect("owner crate directory");
        Self { root }
    }

    pub(super) fn write_model(&self, name: &str, invariant: &str, witness: &str) {
        let source = format!(
            "module {name}\nvar state = 0\naction init = state' = 0\nval {invariant} = true\nval \
             {witness} = true\n"
        );
        fs::write(self.root.join(format!("specs/{name}.qnt")), source).expect("model");
    }

    pub(super) fn write_adapter(&self) {
        fs::create_dir_all(self.root.join("crates/test-owner/src")).expect("crate directory");
        fs::write(
            self.root.join("crates/test-owner/src/test_adapter.rs"),
            "fn init() {}\nlet state = 0;\n",
        )
        .expect("adapter");
    }

    #[cfg(unix)]
    pub(super) fn write_symlink_escape(&self) {
        use std::os::unix::fs::symlink;

        fs::create_dir_all(self.root.join("crates/test-owner/src")).expect("crate directory");
        fs::write(
            self.root.join("external.rs"),
            "fn init() {}\nlet state = 0;\n",
        )
        .expect("external adapter");
        symlink(
            self.root.join("external.rs"),
            self.root.join("crates/test-owner/src/escape.rs"),
        )
        .expect("adapter symlink");
    }

    pub(super) fn write_inventory(&self, rows: &[String]) {
        let header = concat!(
            "rule_id\tfamily\trule_summary\treadable_model\tinvariant\t",
            "witnesses\thostile_trace\tmutation_id\tmbt_model\trust_owner\t",
            "rust_adapter\tbdd_tags\tperformance_gate\tstatus\n"
        );
        let source = format!(
            "{header}{}",
            rows.iter()
                .map(|row| format!("{row}\n"))
                .collect::<String>()
        );
        fs::write(self.root.join("specs/model-inventory.tsv"), source).expect("inventory");
    }

    pub(super) fn write_feature(&self, source: &str) {
        let path = self.root.join("crates/cucumber-tests/features");
        fs::create_dir_all(&path).expect("feature directory");
        fs::write(path.join("analytics.feature"), source).expect("feature");
    }

    pub(super) fn with_bdd_tags(row: String, tags: &str) -> String {
        let mut fields = row.split('\t').map(str::to_owned).collect::<Vec<_>>();
        assert_eq!(fields.len(), 14);
        fields[11] = tags.to_owned();
        fields.join("\t")
    }

    pub(super) fn replace_expiry(&self, expiry: &str) {
        let path = self.root.join("specs/model-inventory.tsv");
        let source = fs::read_to_string(&path).expect("inventory");
        fs::write(&path, source.replace("expiry before 2026-02-30", expiry))
            .expect("inventory update");
    }

    pub(super) fn row(
        &self,
        model: &str,
        mutation: &str,
        adapter: &str,
        hostile: &str,
        gate: &str,
        status: &str,
    ) -> String {
        format!(
            concat!(
                "R-{}\ttest\tfixture rule\tspecs/{}.qnt\tinv\twitness\t",
                "{}\t{}\t-\ttest-owner\t{}\t-\t{}\t{}"
            ),
            model, model, hostile, mutation, adapter, gate, status,
        )
    }
}

impl Drop for InventoryFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
