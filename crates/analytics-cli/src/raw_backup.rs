use std::{fs, path::Path};

use analytics_operations::{FilesystemRawBackupStore, RawBackupReplayRequest};

use crate::{error::CliResult, raw_backup_args::RawBackupCommand};

pub(crate) fn run_raw_backup_command(command: RawBackupCommand) -> CliResult<String> {
    match command {
        RawBackupCommand::ReplayDryRun {
            backup_root,
            request,
        } => {
            let store = FilesystemRawBackupStore::new(backup_root);
            let request = read_raw_backup_replay_request(request.as_path())?;
            let replay = request.execute(&store)?;
            Ok(serde_json::to_string_pretty(&replay.summary)?)
        }
    }
}

fn read_raw_backup_replay_request(path: &Path) -> CliResult<RawBackupReplayRequest> {
    let request: RawBackupReplayRequest = serde_json::from_str(fs::read_to_string(path)?.as_str())?;
    request.validate()?;
    Ok(request)
}
