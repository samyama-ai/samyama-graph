//! Does PCA give the same answer twice on the same data?
//!
//! `PcaSolver::Auto` picks the randomized solver once n > 500. Below that it
//! uses power iteration, which is deterministic -- so a fixture of 50 rows
//! answers "yes" and a production call of 5,000 answers "no".
use samyama_graph_algorithms::pca::{pca, PcaConfig, PcaSolver};

fn data(n: usize, d: usize) -> Vec<Vec<f64>> {
    // Deterministic, non-degenerate: no RNG here, so any difference in the
    // output comes from the solver and not from the input.
    (0..n).map(|i| (0..d).map(|j| ((i * 31 + j * 17) % 97) as f64 + (i as f64) * 0.01).collect()).collect()
}

fn first_component(n: usize, solver: PcaSolver) -> Vec<f64> {
    let cfg = PcaConfig { n_components: 3, solver, ..Default::default() };
    pca(&data(n, 20), cfg).components[0].clone()
}

fn main() {
    for (label, n, solver) in [
        ("n=100  (Auto -> power iteration)", 100usize, PcaSolver::Auto),
        ("n=600  (Auto -> randomized)", 600, PcaSolver::Auto),
    ] {
        let a = first_component(n, solver.clone());
        let b = first_component(n, solver);
        let same = a == b;
        let worst = a.iter().zip(&b).map(|(x, y)| (x - y).abs()).fold(0.0f64, f64::max);
        println!("{label:34} identical={same:<5} worst abs delta={worst:.3e}");
    }
}
