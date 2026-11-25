// Fixture for scripts/check_semgrep.sh --self-test (#2209). Not compiled.
// Every line marked `planted` must be reported, and nothing else.

async fn handler(state: AppState, body: String) {
    let n: u32 = body.parse().unwrap(); // planted: unwrap
    let v = std::env::var("X").expect("x"); // planted: expect
    let mut g = state.store.write().await; // planted: write lock
    engine.execute(&format!("MATCH (n {{name: '{}'}}) RETURN n", body), &store); // planted: format
}

async fn mutate(state: AppState) {
    let mut g = state.store.write().await; // allowed: inside mutate
}

mod tests {
    fn helper() {
        let x: u32 = "1".parse().unwrap(); // allowed: test module
    }
}
