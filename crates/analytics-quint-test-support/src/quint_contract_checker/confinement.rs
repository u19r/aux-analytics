use std::path::{Path, PathBuf};

use super::Checker;

impl Checker<'_> {
    pub(super) fn confined_adapter(&mut self, line: usize, display: &str) -> Option<PathBuf> {
        if !is_normal_relative_path(display) {
            self.error(line, format!("has non-normal adapter path {display}"));
            return None;
        }
        let Some(package) = adapter_package_component(display) else {
            self.error(line, format!("has invalid adapter path {display}"));
            return None;
        };
        let Some(canonical_repo) = self.repo_root.canonicalize().ok() else {
            self.error(
                line,
                format!("cannot resolve repository root for {display}"),
            );
            return None;
        };
        let crates_root = canonical_repo.join("crates");
        let Some(canonical_root) = crates_root.canonicalize().ok() else {
            self.error(line, format!("cannot resolve adapter root for {display}"));
            return None;
        };
        let package_path = crates_root.join(package);
        let Some(canonical_package) = package_path.canonicalize().ok() else {
            self.error(line, format!("references missing adapter crate {package}"));
            return None;
        };
        let path = canonical_repo.join(display);
        let Some(canonical_path) = path.canonicalize().ok() else {
            self.error(line, format!("references missing adapter {display}"));
            return None;
        };
        if !is_confined_adapter(&canonical_package, &canonical_root, &canonical_path) {
            self.error(
                line,
                format!("adapter path escapes declared crate {display}"),
            );
            return None;
        }
        Some(canonical_path)
    }

    pub(super) fn confined_model(&mut self, line: usize, display: &str) -> Option<PathBuf> {
        self.confined_specs_file(line, display, None)
    }

    pub(super) fn confined_mbt_model(&mut self, line: usize, display: &str) -> Option<PathBuf> {
        self.confined_specs_file(line, display, Some("qnt"))
    }

    fn validate_model_path(
        &mut self,
        line: usize,
        display: &str,
        required_extension: Option<&str>,
    ) -> bool {
        let label = required_extension.map_or("readable model", |_| "MBT model");
        if !valid_model_path_components(display)
            || required_extension.is_some_and(|extension| {
                Path::new(display)
                    .extension()
                    .is_none_or(|actual| actual != extension)
            })
        {
            let requirement = required_extension
                .map_or("relative under specs", |_| "a relative specs/*.qnt file");
            self.error(
                line,
                format!("{label} path must be {requirement}: {display}"),
            );
            return false;
        }
        true
    }

    fn confined_specs_file(
        &mut self,
        line: usize,
        display: &str,
        required_extension: Option<&str>,
    ) -> Option<PathBuf> {
        let label = required_extension.map_or("readable model", |_| "MBT model");
        if !is_normal_relative_path(display) {
            self.error(line, format!("has non-normal {label} path {display}"));
            return None;
        }
        if !self.validate_model_path(line, display, required_extension) {
            return None;
        }
        let Some(canonical_repo) = self.repo_root.canonicalize().ok() else {
            self.error(
                line,
                format!("cannot resolve repository root for {label} {display}"),
            );
            return None;
        };
        let specs_root = canonical_repo.join("specs");
        let Some(canonical_specs) = specs_root.canonicalize().ok() else {
            self.error(line, format!("cannot resolve {label} root for {display}"));
            return None;
        };
        let path = canonical_repo.join(display);
        let Some(canonical_path) = path.canonicalize().ok() else {
            self.error(line, format!("references missing {label} {display}"));
            return None;
        };
        if !canonical_specs.is_dir()
            || !canonical_specs.starts_with(&canonical_repo)
            || !canonical_path.starts_with(&canonical_specs)
            || !canonical_path.is_file()
        {
            self.error(
                line,
                format!("{label} escapes specs or is not a regular file {display}"),
            );
            return None;
        }
        Some(canonical_path)
    }
}

fn adapter_package_component(display: &str) -> Option<&str> {
    let mut parts = display.split('/');
    let first = parts.next();
    let package = parts.next();
    let has_path = parts.next().is_some();
    let package = package?;
    (first == Some("crates")
        && has_path
        && !package.is_empty()
        && package
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'))
    .then_some(package)
}

fn valid_model_path_components(display: &str) -> bool {
    let mut parts = display.split('/');
    parts.next() == Some("specs") && parts.next().is_some()
}

#[cfg(test)]
pub(super) fn performance_adapter_path_component(display: &str) -> Option<&str> {
    adapter_package_component(display)
}

#[cfg(test)]
pub(super) fn performance_model_path_components(display: &str) -> bool {
    valid_model_path_components(display)
}

fn is_normal_relative_path(display: &str) -> bool {
    !display.starts_with('/')
        && !display.contains('\\')
        && !display
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
}

fn is_confined_adapter(package: &Path, root: &Path, path: &Path) -> bool {
    package.starts_with(root)
        && package.is_dir()
        && path.starts_with(package)
        && path.starts_with(root)
        && path.is_file()
}
