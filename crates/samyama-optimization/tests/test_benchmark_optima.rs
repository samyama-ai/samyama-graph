//! Verify each benchmark function reaches its documented global minimum
//! when evaluated at the known optimal point.

use ndarray::Array1;
use samyama_optimization::benchmarks::single_objective::*;

fn approx(a: f64, b: f64, eps: f64) -> bool {
    (a - b).abs() < eps
}

#[test]
fn sphere_at_origin() {
    let x = Array1::zeros(30);
    assert!(approx(sphere(&x), 0.0, 1e-12));
}

#[test]
fn rastrigin_at_origin() {
    let x = Array1::zeros(30);
    assert!(approx(rastrigin(&x), 0.0, 1e-12));
}

#[test]
fn ackley_at_origin() {
    let x = Array1::zeros(30);
    assert!(approx(ackley(&x), 0.0, 1e-12));
}

#[test]
fn rosenbrock_at_ones() {
    let x = Array1::ones(30);
    assert!(approx(rosenbrock(&x), 0.0, 1e-12));
}

#[test]
fn griewank_at_origin() {
    let x = Array1::zeros(30);
    assert!(approx(griewank(&x), 0.0, 1e-12));
}

#[test]
fn schwefel_at_optimum() {
    let x = Array1::from_elem(30, 420.9687);
    // Schwefel optimum is approximate; tolerance reflects the constant truncation.
    assert!(approx(schwefel(&x), 0.0, 1e-2), "schwefel = {}", schwefel(&x));
}

#[test]
fn levy_at_ones() {
    let x = Array1::ones(30);
    assert!(approx(levy(&x), 0.0, 1e-12));
}

#[test]
fn zakharov_at_origin() {
    let x = Array1::zeros(30);
    assert!(approx(zakharov(&x), 0.0, 1e-12));
}

#[test]
fn dixon_price_at_known_optimum() {
    // x_i = 2^(-(2^i - 2)/2^i); for i=0 -> x_0 = 1; for i=1 -> 2^(-0.5); etc.
    let dim = 5;
    let x = Array1::from_iter((0..dim).map(|i| {
        let p = (2.0_f64.powi(i as i32 + 1) - 2.0) / 2.0_f64.powi(i as i32 + 1);
        2.0_f64.powf(-p)
    }));
    assert!(dixon_price(&x).abs() < 1e-10, "dixon_price = {}", dixon_price(&x));
}

#[test]
fn styblinski_tang_at_optimum() {
    let dim = 30;
    let x = Array1::from_elem(dim, -2.903534);
    let expected = -39.16599 * dim as f64;
    let got = styblinski_tang(&x);
    assert!((got - expected).abs() < 1e-2, "got {} expected {}", got, expected);
}

#[test]
fn so_suite_has_ten_functions() {
    let s = so_suite(30);
    assert_eq!(s.len(), 10);
}
