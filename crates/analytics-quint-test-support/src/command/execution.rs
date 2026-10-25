use std::{env, path::Path};

use super::{CommandFailure, options::CommandOptions};
use crate::quint_contract_checker::{check_repository, run_mutations, run_plan};

impl CommandOptions {
    pub(super) fn execute(&self) -> Result<(), CommandFailure> {
        let default_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let repo_root = self.repo_root.as_deref().unwrap_or(&default_root);
        let phase = env::var("QUINT_CURRENT_PHASE").unwrap_or_else(|_| "U-A1".to_owned());
        if self.mutations {
            return self.execute_mutations(repo_root, &phase);
        }
        self.validate_plan_selection()?;
        if self.emit_plan {
            return self.execute_plan(repo_root, &phase);
        }
        if self.family != "all"
            || self.mode != "standard"
            || self.skip_adapters
            || self.skip_mutations
            || self.combined
        {
            return Err(CommandFailure::usage("run-plan options require --run-plan"));
        }
        let report = check_repository(repo_root, &phase)
            .map_err(|error| CommandFailure::check(error.to_string()))?;
        println!(
            "quint contract inventory: {} rules, {} readable models, all paths and exceptions \
             valid",
            report.rules, report.readable_models
        );
        Ok(())
    }

    fn validate_plan_selection(&self) -> Result<(), CommandFailure> {
        if self.mutation_filter.is_some() {
            return Err(CommandFailure::usage("--mutation requires --mutations"));
        }
        if self.combined && !self.emit_plan {
            return Err(CommandFailure::usage("--combined requires --run-plan"));
        }
        if self.emit_plan && !self.combined {
            return Err(CommandFailure::usage("--run-plan requires --combined"));
        }
        Ok(())
    }

    fn execute_mutations(&self, repo_root: &Path, phase: &str) -> Result<(), CommandFailure> {
        if self.emit_plan
            || self.combined
            || self.mode != "standard"
            || self.skip_adapters
            || self.skip_mutations
        {
            return Err(CommandFailure::usage(
                "mutation options cannot be combined with run-plan options",
            ));
        }
        let report = run_mutations(
            repo_root,
            phase,
            &self.family,
            self.mutation_filter.as_deref(),
        )
        .map_err(|error| CommandFailure::check(error.to_string()))?;
        for receipt in report.receipts {
            println!("{receipt}");
        }
        Ok(())
    }

    fn execute_plan(&self, repo_root: &Path, phase: &str) -> Result<(), CommandFailure> {
        let plan = run_plan(
            repo_root,
            phase,
            crate::quint_contract_checker::RunPlanOptions {
                family: &self.family,
                mode: &self.mode,
                skip_adapters: self.skip_adapters,
                skip_mutations: self.skip_mutations,
            },
        )
        .map_err(|error| CommandFailure::check(error.to_string()))?;
        for receipt in plan.mutation_receipts {
            eprintln!("{receipt}");
        }
        for record in plan.records {
            println!("{record}");
        }
        Ok(())
    }
}

pub fn execute_arguments(
    arguments: impl IntoIterator<Item = String>,
) -> Result<(), CommandFailure> {
    if let Some(options) = CommandOptions::parse(arguments)? {
        options.execute()?;
    }
    Ok(())
}
