//! Ownership and cleanup of a model file supplied to the Quint process.

use std::{
    env, fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub(in crate::quint_contract_checker) struct TemporaryModel {
    pub(in crate::quint_contract_checker) path: PathBuf,
    pub(in crate::quint_contract_checker) directory: PathBuf,
}

impl TemporaryModel {
    pub(in crate::quint_contract_checker) fn try_new(model: &str) -> Result<Self, String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_nanos();
        let mut directory =
            env::temp_dir().join(format!("quint-mutation-{}-{timestamp}", std::process::id()));
        for attempt in 0..100 {
            if attempt > 0 {
                directory.set_file_name(format!(
                    "quint-mutation-{}-{timestamp}-{attempt}",
                    std::process::id()
                ));
            }
            match fs::create_dir(&directory) {
                Ok(()) => {
                    let path = directory.join(
                        Path::new(model)
                            .file_name()
                            .ok_or_else(|| "mutation model has no file name".to_owned())?,
                    );
                    return Ok(Self { path, directory });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(format!("could not create temporary directory: {error}")),
            }
        }
        Err("could not allocate a unique temporary directory".to_owned())
    }
}

impl Drop for TemporaryModel {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}
