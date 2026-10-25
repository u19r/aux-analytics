//! Trusted Quint command discovery and process execution.

use std::{
    env,
    process::{Command, Output, Stdio},
};

pub(in crate::quint_contract_checker) struct QuintCommand {
    program: String,
}

impl QuintCommand {
    pub(in crate::quint_contract_checker) fn discover_trusted() -> Result<Self, String> {
        if is_command_available("quint") {
            Ok(Self {
                program: "quint".to_owned(),
            })
        } else {
            Err("independent invariant evaluation requires an installed quint command".to_owned())
        }
    }

    pub(in crate::quint_contract_checker) fn run_status(
        &self,
        arguments: &[&str],
    ) -> Result<std::process::ExitStatus, String> {
        Command::new(&self.program)
            .args(arguments)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|error| error.to_string())
    }

    pub(in crate::quint_contract_checker) fn run_output(
        &self,
        arguments: &[String],
    ) -> Result<Output, String> {
        Command::new(&self.program)
            .args(arguments)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|error| error.to_string())
    }
}

fn is_command_available(command: &str) -> bool {
    env::var_os("PATH").is_some_and(|paths| {
        env::split_paths(&paths).any(|directory| directory.join(command).is_file())
    })
}
