pub fn map_result<T, V, E>(
    result: Result<V, E>,
    success: impl FnOnce(V) -> T,
    failure: impl FnOnce(E) -> T,
) -> T {
    match result {
        Ok(value) => success(value),
        Err(error) => failure(error),
    }
}
