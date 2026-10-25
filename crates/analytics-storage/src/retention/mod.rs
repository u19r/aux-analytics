mod aws;
mod lookup;
mod remote;

#[cfg(test)]
mod retention_performance_tests;
#[cfg(test)]
mod retention_tests;

pub use lookup::RetentionPolicyLookup;
