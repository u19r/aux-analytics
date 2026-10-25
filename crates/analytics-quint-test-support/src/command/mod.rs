mod execution;
mod options;
#[cfg(test)]
mod options_tests;

mod error;
pub use error::CommandFailure;
pub use execution::execute_arguments;
