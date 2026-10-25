use analytics_contract::{ProjectionColumn, StorageValue};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;
use storage_types::VecMap;

use crate::projection_test_support::project_item;

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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ProjectionCase {
    #[serde(rename = "AliasPathProjection")]
    AliasPath,
    #[serde(rename = "ListExpansionProjection")]
    ListExpansion,
    #[serde(rename = "MissingPathProjection")]
    MissingPath,
    #[serde(rename = "EmptyAttributePathProjection")]
    EmptyAttributePath,
    #[serde(rename = "InvalidAttributePathProjection")]
    InvalidAttributePath,
    #[serde(rename = "AliasWithoutNameMapProjection")]
    AliasWithoutNameMap,
    #[serde(rename = "MissingAliasProjection")]
    MissingAlias,
    #[serde(rename = "NotCheckedProjection")]
    NotChecked,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct ProjectionState {
    #[serde(rename = "lastResult")]
    last_result: ProjectionResult,
    #[serde(rename = "lastCase")]
    last_case: ProjectionCase,
}

impl State<ProjectionDriver> for ProjectionState {
    fn from_driver(driver: &ProjectionDriver) -> Result<Self> {
        Ok(Self {
            last_result: driver.last_result,
            last_case: driver.last_case,
        })
    }
}

struct ProjectionDriver {
    last_result: ProjectionResult,
    last_case: ProjectionCase,
}

impl Default for ProjectionDriver {
    fn default() -> Self {
        Self {
            last_result: ProjectionResult::NotChecked,
            last_case: ProjectionCase::NotChecked,
        }
    }
}

impl Driver for ProjectionDriver {
    type State = ProjectionState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            ProjectAliasPath => {
                self.last_case = ProjectionCase::AliasPath;
                self.project_alias_path()?;
            },
            ProjectListExpansion => {
                self.last_case = ProjectionCase::ListExpansion;
                self.project_list_expansion()?;
            },
            ProjectMissingPath => {
                self.last_case = ProjectionCase::MissingPath;
                self.project_missing_path()?;
            },
            RejectEmptyAttributePath => {
                self.last_case = ProjectionCase::EmptyAttributePath;
                self.project_error("", None)?;
            },
            RejectInvalidAttributePath => {
                self.last_case = ProjectionCase::InvalidAttributePath;
                self.project_error("profile..email", None)?;
            },
            RejectAliasWithoutNameMap => {
                self.last_case = ProjectionCase::AliasWithoutNameMap;
                self.project_error("#email", None)?;
            },
            RejectMissingAlias => {
                let names = VecMap::new();
                self.last_case = ProjectionCase::MissingAlias;
                self.project_error("#email", Some(&names))?;
            },
        })
    }
}

impl ProjectionDriver {
    fn project_alias_path(&mut self) -> Result {
        let item = VecMap::from_iter([(
            "profile".to_string(),
            StorageValue::M(VecMap::from_iter([(
                "email-address".to_string(),
                StorageValue::S("a@example.com".to_string()),
            )])),
        )]);
        let names = VecMap::from_iter([("#email".to_string(), "email-address".to_string())]);
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
        let item = VecMap::from_iter([(
            "orders".to_string(),
            StorageValue::L(vec![
                StorageValue::M(VecMap::from_iter([(
                    "total".to_string(),
                    StorageValue::N("12.50".to_string()),
                )])),
                StorageValue::M(VecMap::from_iter([(
                    "status".to_string(),
                    StorageValue::S("cancelled".to_string()),
                )])),
                StorageValue::M(VecMap::from_iter([(
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
        let item = VecMap::from_iter([(
            "profile".to_string(),
            StorageValue::M(VecMap::from_iter([(
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

    fn project_error(&mut self, path: &str, names: Option<&VecMap<String, String>>) -> Result {
        match project_item(&VecMap::new(), &[projection_column("value", path)], names) {
            Ok(_) => Err(anyhow::anyhow!(
                "projection path {path:?} unexpectedly succeeded"
            )),
            Err(error) => {
                self.last_result = projection_error_result(&error)?;
                Ok(())
            }
        }
    }
}

fn projection_error_result(error: &crate::projection::ProjectionError) -> Result<ProjectionResult> {
    let result = match error.to_string().as_str() {
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
        _ => {
            return Err(anyhow::anyhow!(
                "unmapped production projection error: {error}"
            ));
        }
    };
    Ok(result)
}

#[quint_connect::quint_run(
    spec = "../../specs/projection.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0x3"
)]
fn given_quint_projection_model_when_replayed_then_engine_projection_matches() -> impl Driver {
    ProjectionDriver::default()
}

fn projection_column(column_name: &str, attribute_path: &str) -> ProjectionColumn {
    ProjectionColumn {
        column_name: column_name.to_string(),
        attribute_path: attribute_path.to_string(),
        column_type: None,
    }
}
