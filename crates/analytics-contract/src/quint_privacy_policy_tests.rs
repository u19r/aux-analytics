use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::PrivacyPolicy;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum PrivacyCase {
    PublicRecord,
    DeniedKey,
    DeniedValue,
    InvalidRegex,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum PrivacyResult {
    NotRun,
    Allowed,
    Denied,
    InvalidPolicyRejected,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct PrivacyReport {
    result: PrivacyResult,
    reached_raw_backup: bool,
    reached_projection: bool,
    policy_version_recorded: bool,
}

impl Default for PrivacyReport {
    fn default() -> Self {
        Self {
            result: PrivacyResult::NotRun,
            reached_raw_backup: false,
            reached_projection: false,
            policy_version_recorded: false,
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct PrivacyMbtState {
    last_report: PrivacyReport,
}

impl State<PrivacyDriver> for PrivacyMbtState {
    fn from_driver(driver: &PrivacyDriver) -> Result<Self> {
        Ok(Self {
            last_report: driver.last_report,
        })
    }
}

#[derive(Default)]
struct PrivacyDriver {
    last_report: PrivacyReport,
}

impl Driver for PrivacyDriver {
    type State = PrivacyMbtState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            PrivacyScenario(privacyCase: PrivacyCase) => {
                self.last_report = run_privacy_case(privacyCase)?;
            },
            step(privacyCase: PrivacyCase?) => {
                if let Some(privacy_case) = privacyCase {
                    self.last_report = run_privacy_case(privacy_case)?;
                }
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/privacy_policy_mbt.qnt",
    max_samples = 50,
    max_steps = 4,
    seed = "0x914ac7"
)]
fn quint_privacy_policy_mbt_compares_full_state() -> impl Driver {
    PrivacyDriver::default()
}

fn run_privacy_case(privacy_case: PrivacyCase) -> Result<PrivacyReport> {
    match privacy_case {
        PrivacyCase::PublicRecord => {
            let policy = PrivacyPolicy::new("privacy-v1")?.with_denied_key_name("secret");
            let denied = policy.denies_key("display_name")?;
            Ok(PrivacyReport {
                result: if denied {
                    PrivacyResult::Denied
                } else {
                    PrivacyResult::Allowed
                },
                reached_raw_backup: !denied,
                reached_projection: !denied,
                policy_version_recorded: true,
            })
        }
        PrivacyCase::DeniedKey => {
            let policy = PrivacyPolicy::new("privacy-v1")?.with_denied_key_name("secret");
            Ok(denied_report(policy.denies_key("secret")?))
        }
        PrivacyCase::DeniedValue => {
            let policy = PrivacyPolicy::new("privacy-v1")?.with_denied_value_regex("(?i)secret");
            Ok(denied_report(policy.denies_value("SECRET token")?))
        }
        PrivacyCase::InvalidRegex => {
            let policy = PrivacyPolicy {
                version: "privacy-v1".to_string(),
                denied_value_regexes: vec!["[".to_string()],
                ..PrivacyPolicy::default()
            };
            Ok(PrivacyReport {
                result: if policy.validate().is_err() {
                    PrivacyResult::InvalidPolicyRejected
                } else {
                    PrivacyResult::NotRun
                },
                reached_raw_backup: false,
                reached_projection: false,
                policy_version_recorded: false,
            })
        }
    }
}

fn denied_report(denied: bool) -> PrivacyReport {
    PrivacyReport {
        result: if denied {
            PrivacyResult::Denied
        } else {
            PrivacyResult::Allowed
        },
        reached_raw_backup: !denied,
        reached_projection: !denied,
        policy_version_recorded: true,
    }
}
