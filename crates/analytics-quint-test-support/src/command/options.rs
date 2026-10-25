use std::path::PathBuf;

use super::CommandFailure;

pub(super) struct CommandOptions {
    pub(super) repo_root: Option<PathBuf>,
    pub(super) emit_plan: bool,
    pub(super) mutations: bool,
    pub(super) mutation_filter: Option<String>,
    pub(super) family: String,
    pub(super) mode: String,
    pub(super) skip_adapters: bool,
    pub(super) skip_mutations: bool,
    pub(super) combined: bool,
}

impl CommandOptions {
    pub(super) fn parse(
        arguments: impl IntoIterator<Item = String>,
    ) -> Result<Option<Self>, CommandFailure> {
        let mut arguments = arguments.into_iter();
        let mut options = Self {
            repo_root: None,
            emit_plan: false,
            mutations: false,
            mutation_filter: None,
            family: "all".to_owned(),
            mode: "standard".to_owned(),
            skip_adapters: false,
            skip_mutations: false,
            combined: false,
        };
        while let Some(argument) = arguments.next() {
            if !options.apply_argument(&argument, &mut arguments)? {
                return Ok(None);
            }
        }
        Ok(Some(options))
    }

    fn apply_argument(
        &mut self,
        argument: &str,
        arguments: &mut impl Iterator<Item = String>,
    ) -> Result<bool, CommandFailure> {
        match argument {
            "--repo-root" => {
                let Some(value) = arguments.next() else {
                    return Err(CommandFailure::usage("--repo-root requires a path"));
                };
                self.repo_root = Some(PathBuf::from(value));
            }
            "--run-plan" => self.emit_plan = true,
            "--mutations" => self.mutations = true,
            "--mutation" => {
                let Some(value) = arguments.next() else {
                    return Err(CommandFailure::usage("--mutation requires an ID"));
                };
                self.mutation_filter = Some(value);
            }
            "--family" => {
                let Some(value) = arguments.next() else {
                    return Err(CommandFailure::usage("--family requires a value"));
                };
                self.family = value;
            }
            "--mode" => {
                let Some(value) = arguments.next() else {
                    return Err(CommandFailure::usage("--mode requires a value"));
                };
                self.mode = value;
            }
            "--skip-adapters" => self.skip_adapters = true,
            "--skip-mutations" => self.skip_mutations = true,
            "--combined" => self.combined = true,
            "--help" | "-h" => {
                print_usage();
                return Ok(false);
            }
            _ => {
                return Err(CommandFailure::usage(format!(
                    "unknown argument {argument}"
                )));
            }
        }
        Ok(true)
    }
}

fn print_usage() {
    println!(
        "Usage: check-quint-contracts [--repo-root PATH] [--run-plan] [--mutations] [--mutation \
         ID] [--family FAMILY] [--mode standard|deep|all] [--skip-adapters] [--skip-mutations] \
         [--combined]"
    );
}
