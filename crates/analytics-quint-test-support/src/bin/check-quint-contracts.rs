use std::{env, process};

use analytics_quint_test_support::command::execute_arguments;

fn main() {
    if let Err(error) = execute_arguments(env::args().skip(1)) {
        eprint!("{}", error.message);
        process::exit(error.exit_code);
    }
}
