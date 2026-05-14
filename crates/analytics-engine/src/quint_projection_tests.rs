use std::collections::HashMap;

use analytics_contract::{ProjectionColumn, StorageValue};
use analytics_quint_test_support::map_result;
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::projection::project_item;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ProjectionResult {
    NotChecked,
    EmailProjected,
    OrderTotalsProjected,
    MissingPathSkipped,
    EmptyAttributePath,
    InvalidAttributePath,
    MissingExpressionAttributeName,
    MissingExpressionAttributeNames,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct ProjectionState {
    #[serde(rename = "lastResult")]
    last_result: ProjectionResult,
}

impl State<ProjectionDriver> for ProjectionState {
    fn from_driver(driver: &ProjectionDriver) -> Result<Self> {
        Ok(Self {
            last_result: driver.last_result,
        })
    }
}

struct ProjectionDriver {
    last_result: ProjectionResult,
}

impl Default for ProjectionDriver {
    fn default() -> Self {
        Self {
            last_result: ProjectionResult::NotChecked,
        }
    }
}

impl Driver for ProjectionDriver {
    type State = ProjectionState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.last_result = ProjectionResult::NotChecked,
            ProjectAliasPath => {
                self.project_alias_path()?;
            },
            ProjectListExpansion => {
                self.project_list_expansion()?;
            },
            ProjectMissingPath => {
                self.project_missing_path()?;
            },
            RejectEmptyAttributePath => {
                self.project_error("", None, ProjectionResult::EmptyAttributePath)?;
            },
            RejectInvalidAttributePath => {
                self.project_error(
                    "profile..email",
                    None,
                    ProjectionResult::InvalidAttributePath,
                )?;
            },
            RejectAliasWithoutNameMap => {
                self.project_error(
                    "#email",
                    None,
                    ProjectionResult::MissingExpressionAttributeNames,
                )?;
            },
            RejectMissingAlias => {
                let names = HashMap::new();
                self.project_error(
                    "#email",
                    Some(&names),
                    ProjectionResult::MissingExpressionAttributeName,
                )?;
            },
        })
    }
}

impl ProjectionDriver {
    fn project_alias_path(&mut self) -> Result {
        let item = HashMap::from([(
            "profile".to_string(),
            StorageValue::M(HashMap::from([(
                "email-address".to_string(),
                StorageValue::S("a@example.com".to_string()),
            )])),
        )]);
        let names = HashMap::from([("#email".to_string(), "email-address".to_string())]);
        let projected = project_item(
            &item,
            &[projection_column("email", "profile.#email")],
            Some(&names),
        )?;
        self.last_result =
            if projected.get("email") == Some(&StorageValue::S("a@example.com".to_string())) {
                ProjectionResult::EmailProjected
            } else {
                return Err(anyhow::anyhow!(
                    "alias projection produced unexpected value"
                ));
            };
        Ok(())
    }

    fn project_list_expansion(&mut self) -> Result {
        let item = HashMap::from([(
            "orders".to_string(),
            StorageValue::L(vec![
                StorageValue::M(HashMap::from([(
                    "total".to_string(),
                    StorageValue::N("12.50".to_string()),
                )])),
                StorageValue::M(HashMap::from([(
                    "status".to_string(),
                    StorageValue::S("cancelled".to_string()),
                )])),
                StorageValue::M(HashMap::from([(
                    "total".to_string(),
                    StorageValue::N("99.00".to_string()),
                )])),
            ]),
        )]);
        let projected = project_item(
            &item,
            &[projection_column("order_totals", "orders[].total")],
            None,
        )?;
        self.last_result = if projected.get("order_totals")
            == Some(&StorageValue::L(vec![
                StorageValue::N("12.50".to_string()),
                StorageValue::N("99.00".to_string()),
            ])) {
            ProjectionResult::OrderTotalsProjected
        } else {
            return Err(anyhow::anyhow!("list projection produced unexpected value"));
        };
        Ok(())
    }

    fn project_missing_path(&mut self) -> Result {
        let item = HashMap::from([(
            "profile".to_string(),
            StorageValue::M(HashMap::from([(
                "email".to_string(),
                StorageValue::S("a@example.com".to_string()),
            )])),
        )]);
        let projected = project_item(
            &item,
            &[projection_column("missing", "profile.phone")],
            None,
        )?;
        self.last_result = if projected.is_empty() {
            ProjectionResult::MissingPathSkipped
        } else {
            return Err(anyhow::anyhow!("missing path was unexpectedly projected"));
        };
        Ok(())
    }

    fn project_error(
        &mut self,
        path: &str,
        names: Option<&HashMap<String, String>>,
        expected: ProjectionResult,
    ) -> Result {
        self.last_result = map_result(
            project_item(&HashMap::new(), &[projection_column("value", path)], names),
            |_| ProjectionResult::NotChecked,
            |error| projection_error_result(&error),
        );
        if self.last_result == expected {
            Ok(())
        } else {
            Err(anyhow::anyhow!("projection error did not match model"))
        }
    }
}

fn projection_error_result(error: &crate::projection::ProjectionError) -> ProjectionResult {
    match error.to_string().as_str() {
        text if text.starts_with("projection attribute path is empty") => {
            ProjectionResult::EmptyAttributePath
        }
        text if text.starts_with("projection attribute path is invalid") => {
            ProjectionResult::InvalidAttributePath
        }
        text if text.starts_with("projection attribute name is not found") => {
            ProjectionResult::MissingExpressionAttributeName
        }
        text if text
            .starts_with("projection attribute name requires expression attribute names") =>
        {
            ProjectionResult::MissingExpressionAttributeNames
        }
        _ => ProjectionResult::NotChecked,
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/projection.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0x3"
)]
fn quint_projection_model_matches_engine_projection() -> impl Driver {
    ProjectionDriver::default()
}

fn projection_column(column_name: &str, attribute_path: &str) -> ProjectionColumn {
    ProjectionColumn {
        column_name: column_name.to_string(),
        attribute_path: attribute_path.to_string(),
        column_type: None,
    }
}
