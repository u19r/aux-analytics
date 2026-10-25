#[derive(Debug)]
pub struct CommandFailure {
    pub message: String,
    pub exit_code: i32,
}

impl CommandFailure {
    pub(super) fn usage(message: impl Into<String>) -> Self {
        Self {
            message: format!("quint contract error: {}\n", message.into()),
            exit_code: 64,
        }
    }
    pub(super) fn check(message: String) -> Self {
        Self {
            message,
            exit_code: 1,
        }
    }
}
