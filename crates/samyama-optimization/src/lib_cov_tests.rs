//! Coverage tests for the crate's public surface that the integration tests
//! under `tests/` do not reach: the benchmark runner and its CSV writers, the
//! ZDT/DTLZ problem definitions, the CEC data loaders, the MOO helper edge
//! cases, and solver branches that only fire on particular problem shapes.

use crate::algorithms::*;
use crate::benchmarks::cec_data::{self, CecSuite};
use crate::benchmarks::multi_objective::build_mo_problem;
use crate::benchmarks::runner::{mo_solver_names, so_solver_names, write_mo_csv, write_so_csv};
use crate::benchmarks::single_objective::{bent_cigar, discus, high_conditioned_elliptic, sphere};
use crate::benchmarks::{
    moo_suite, run_mo_suite, run_so_suite, so_suite, SOProblemSpec, DTLZ, ZDT,
};
use crate::common::*;
use crate::moo;
use ndarray::{array, Array1};

fn tiny_cfg() -> SolverConfig {
    SolverConfig {
        population_size: 8,
        max_iterations: 4,
    }
}

fn unique_temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("samyama_opt_cov_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

// ───────────────────────────────────────────────────────── common / moo

#[test]
fn solver_config_default_is_population_50_iterations_100() {
    let cfg = SolverConfig::default();
    assert_eq!(cfg.population_size, 50);
    assert_eq!(cfg.max_iterations, 100);
}

#[test]
fn constrained_dominates_infeasible_never_beats_feasible() {
    assert!(!moo::constrained_dominates(
        &[0.0, 0.0],
        1.0,
        &[5.0, 5.0],
        0.0
    ));
    assert!(moo::constrained_dominates(
        &[5.0, 5.0],
        0.0,
        &[0.0, 0.0],
        1.0
    ));
    // Two infeasibles: smaller violation wins regardless of objectives.
    assert!(moo::constrained_dominates(
        &[9.0, 9.0],
        0.5,
        &[0.0, 0.0],
        2.0
    ));
    assert!(!moo::constrained_dominates(
        &[0.0, 0.0],
        2.0,
        &[9.0, 9.0],
        0.5
    ));
}

#[test]
fn crowding_distance_on_empty_indices_leaves_population_untouched() {
    let mut pop = vec![MultiObjectiveIndividual::new(
        array![0.0],
        vec![1.0, 2.0],
        0.0,
    )];
    pop[0].crowding_distance = 7.5;
    moo::crowding_distance(&mut pop, &[]);
    assert_eq!(pop[0].crowding_distance, 7.5);
}

#[test]
fn hypervolume_2d_is_zero_when_every_point_lies_beyond_the_reference() {
    let front = vec![vec![2.0, 0.5], vec![0.5, 3.0]];
    assert_eq!(moo::hypervolume_2d(&front, [1.0, 1.0]), 0.0);
    assert_eq!(moo::hypervolume_2d(&[], [1.0, 1.0]), 0.0);
}

#[test]
fn igd_of_an_empty_front_is_infinite() {
    let pts = vec![vec![0.0, 1.0]];
    assert_eq!(moo::igd(&[], &pts), f64::INFINITY);
    assert_eq!(moo::igd(&pts, &[]), f64::INFINITY);
    assert_eq!(moo::igd(&pts, &pts), 0.0);
}

// ─────────────────────────────────────────────── single-objective suite

#[test]
fn so_problem_spec_to_problem_carries_dim_bounds_and_function() {
    let spec = SOProblemSpec {
        name: "sphere",
        dim: 3,
        lower: -2.0,
        upper: 4.0,
        global_minimum: 0.0,
        func: sphere,
    };
    let p = spec.to_problem();
    assert_eq!(p.dim(), 3);
    let (lo, hi) = p.bounds();
    assert_eq!(lo, Array1::from_elem(3, -2.0));
    assert_eq!(hi, Array1::from_elem(3, 4.0));
    assert_eq!(p.objective(&array![1.0, 2.0, 3.0]), 14.0);
    // No penalty by default, so fitness equals the objective.
    assert_eq!(p.fitness(&array![1.0, 2.0, 3.0]), 14.0);
}

#[test]
fn bent_cigar_and_discus_are_zero_on_empty_input() {
    let empty: Array1<f64> = Array1::zeros(0);
    assert_eq!(bent_cigar(&empty), 0.0);
    assert_eq!(discus(&empty), 0.0);
    assert_eq!(bent_cigar(&array![1.0, 1.0]), 1.0 + 1e6);
    assert_eq!(discus(&array![1.0, 1.0]), 1e6 + 1.0);
}

#[test]
fn high_conditioned_elliptic_in_one_dimension_is_the_square() {
    assert_eq!(high_conditioned_elliptic(&array![3.0]), 9.0);
    // Two dims: weights 1 and 1e6.
    assert_eq!(high_conditioned_elliptic(&array![1.0, 1.0]), 1.0 + 1e6);
}

// ─────────────────────────────────────────────── multi-objective suite

#[test]
fn zdt_objectives_match_closed_forms_on_the_pareto_set() {
    // x[1..] = 0 puts g at its minimum of 1 for ZDT1/2/3/6.
    let x = array![0.25, 0.0, 0.0];
    let z = |variant| ZDT { variant, dim: 3 };
    assert_eq!(z(1).dim(), 3);
    assert_eq!(z(1).num_objectives(), 2);
    let (lo, hi) = z(1).bounds();
    assert_eq!(lo, Array1::zeros(3));
    assert_eq!(hi, Array1::ones(3));

    let f = z(1).objectives(&x);
    assert!(close(f[0], 0.25) && close(f[1], 0.5));
    let f = z(2).objectives(&x);
    assert!(close(f[1], 1.0 - 0.0625));
    // ZDT3: 1 - sqrt(0.25) - 0.25 * sin(2.5 pi) = 0.25
    let f = z(3).objectives(&x);
    assert!(close(f[1], 0.25), "zdt3 f2 = {}", f[1]);
    // ZDT4: g = 1 + 10(n-1) + sum(0 - 10 cos 0) = 1.
    let f = z(4).objectives(&x);
    assert!(close(f[1], 0.5), "zdt4 f2 = {}", f[1]);
    // ZDT6 at x0 = 0: f1 = 1, g = 1, f2 = 0.
    let f = z(6).objectives(&array![0.0, 0.0, 0.0]);
    assert!(close(f[0], 1.0) && close(f[1], 0.0), "zdt6 {:?}", f);
}

#[test]
#[should_panic(expected = "ZDT5 is binary-coded")]
fn zdt5_bounds_are_not_supported() {
    let _ = ZDT { variant: 5, dim: 3 }.bounds();
}

#[test]
#[should_panic(expected = "ZDT variant 7 not supported")]
fn zdt_unknown_variant_panics() {
    let _ = ZDT { variant: 7, dim: 3 }.objectives(&array![0.1, 0.1, 0.1]);
}

#[test]
fn dtlz_objectives_lie_on_their_known_fronts() {
    let m = 3;
    let dim = 5;
    // Position variables 0.3 / 0.6; distance variables at 0.5 make g = 0 for
    // DTLZ1-5.
    let x = array![0.3, 0.6, 0.5, 0.5, 0.5];
    let d = |variant| DTLZ { variant, dim, m };
    assert_eq!(d(1).dim(), dim);
    assert_eq!(d(1).num_objectives(), m);
    let (lo, hi) = d(1).bounds();
    assert_eq!(lo, Array1::zeros(dim));
    assert_eq!(hi, Array1::ones(dim));

    // DTLZ1: linear front, objectives sum to 0.5.
    let f = d(1).objectives(&x);
    assert_eq!(f.len(), m);
    assert!(close(f.iter().sum::<f64>(), 0.5), "dtlz1 {:?}", f);
    // DTLZ2-5: spherical front, squared objectives sum to 1.
    for v in [2u8, 3, 4, 5] {
        let f = d(v).objectives(&x);
        let s: f64 = f.iter().map(|v| v * v).sum();
        assert!(close(s, 1.0), "dtlz{} {:?}", v, f);
    }
    // DTLZ6: g = sum(x^0.1), zero at distance variables = 0.
    let f = d(6).objectives(&array![0.3, 0.6, 0.0, 0.0, 0.0]);
    let s: f64 = f.iter().map(|v| v * v).sum();
    assert!(close(s, 1.0), "dtlz6 {:?}", f);
    // DTLZ7 at the origin: g = 1, f_i = 0, h = 0, f_m = (1 + g) * m.
    let f = d(7).objectives(&Array1::zeros(dim));
    assert_eq!(f, vec![0.0, 0.0, 2.0 * m as f64]);
}

#[test]
#[should_panic(expected = "DTLZ variant 9 not supported")]
fn dtlz_unknown_variant_panics() {
    let _ = DTLZ {
        variant: 9,
        dim: 4,
        m: 2,
    }
    .objectives(&Array1::zeros(4));
}

#[test]
fn moo_suite_lists_five_zdt_and_seven_dtlz_problems() {
    let suite = moo_suite(10, 7, 3);
    let names: Vec<&str> = suite.iter().map(|s| s.name).collect();
    assert_eq!(
        names,
        vec![
            "ZDT1", "ZDT2", "ZDT3", "ZDT4", "ZDT6", "DTLZ1", "DTLZ2", "DTLZ3", "DTLZ4", "DTLZ5",
            "DTLZ6", "DTLZ7"
        ]
    );
    assert!(suite[..5]
        .iter()
        .all(|s| s.dim == 10 && s.num_objectives == 2 && s.hv_ref == vec![1.1, 1.1]));
    assert!(suite[5..]
        .iter()
        .all(|s| s.dim == 7 && s.num_objectives == 3 && s.hv_ref.len() == 3));
    assert!(suite.iter().all(|s| s.lower == 0.0 && s.upper == 1.0));
}

#[test]
fn build_mo_problem_constructs_zdt_and_dtlz_by_name() {
    let suite = moo_suite(6, 5, 3);
    for spec in &suite {
        let p = build_mo_problem(spec);
        assert_eq!(p.dim(), spec.dim, "{}", spec.name);
        assert_eq!(p.num_objectives(), spec.num_objectives, "{}", spec.name);
        let f = p.objectives(&Array1::from_elem(spec.dim, 0.5));
        assert_eq!(f.len(), spec.num_objectives);
        assert!(f.iter().all(|v| v.is_finite()), "{} {:?}", spec.name, f);
    }
}

#[test]
#[should_panic(expected = "unknown MO problem")]
fn build_mo_problem_rejects_unknown_name() {
    let mut spec = moo_suite(4, 4, 2).remove(0);
    spec.name = "WFG1";
    let _ = build_mo_problem(&spec);
}

// ───────────────────────────────────────────────────────────── runner

#[test]
fn solver_name_lists_match_the_paper() {
    let so = so_solver_names();
    assert_eq!(so.len(), 20);
    assert_eq!(so[0], "Jaya");
    assert!(so.contains(&"ABC") && so.contains(&"QO-Rao"));
    assert_eq!(
        mo_solver_names(),
        vec![
            "NSGA-II",
            "MOTLBO",
            "MO-BMWR",
            "MO-BMR",
            "MO-BWR",
            "MO-Rao+DE"
        ]
    );
}

#[test]
fn run_so_suite_records_every_solver_problem_seed_cell() {
    let solvers = so_solver_names();
    let problems: Vec<SOProblemSpec> = so_suite(2).into_iter().take(1).collect();
    let records = run_so_suite(&solvers, &problems, &tiny_cfg(), 2);
    assert_eq!(records.len(), solvers.len() * 2);
    for (i, r) in records.iter().enumerate() {
        assert_eq!(r.solver, solvers[i / 2]);
        assert_eq!(r.problem, "sphere");
        assert_eq!(r.dim, 2);
        assert_eq!(r.seed_index, i % 2);
        assert_eq!(r.global_minimum, 0.0);
        assert!(r.best_fitness >= 0.0, "{}: {}", r.solver, r.best_fitness);
        assert_eq!(r.gap, r.best_fitness - r.global_minimum);
        assert!(r.iterations > 0, "{} recorded no history", r.solver);
    }
}

#[test]
#[should_panic(expected = "unknown SO solver: Nope")]
fn run_so_suite_panics_on_unknown_solver() {
    let problems: Vec<SOProblemSpec> = so_suite(2).into_iter().take(1).collect();
    let _ = run_so_suite(&["Nope"], &problems, &tiny_cfg(), 1);
}

#[test]
fn run_mo_suite_reports_hv_and_igd_only_for_two_objectives() {
    let solvers = mo_solver_names();
    let suite = moo_suite(4, 4, 3);
    let problems: Vec<_> = suite
        .into_iter()
        .filter(|s| s.name == "ZDT1" || s.name == "DTLZ2")
        .collect();
    let records = run_mo_suite(&solvers, &problems, &tiny_cfg(), 1);
    assert_eq!(records.len(), solvers.len() * 2);
    for r in &records {
        assert!(r.pareto_size > 0, "{} on {}", r.solver, r.problem);
        if r.problem == "ZDT1" {
            assert_eq!(r.num_objectives, 2);
            let hv = r.hypervolume.expect("2-objective HV");
            assert!((0.0..=1.21).contains(&hv), "{} hv {}", r.solver, hv);
            assert!(r.igd.expect("2-objective IGD") >= 0.0);
        } else {
            assert_eq!(r.num_objectives, 3);
            assert!(r.hypervolume.is_none() && r.igd.is_none());
        }
    }
}

#[test]
#[should_panic(expected = "unknown MO solver: Nope")]
fn run_mo_suite_panics_on_unknown_solver() {
    let problems: Vec<_> = moo_suite(4, 4, 2).into_iter().take(1).collect();
    let _ = run_mo_suite(&["Nope"], &problems, &tiny_cfg(), 1);
}

#[test]
#[should_panic(expected = "unknown MO problem")]
fn run_mo_suite_panics_on_unknown_problem() {
    let mut spec = moo_suite(4, 4, 2).remove(0);
    spec.name = "WFG1";
    let _ = run_mo_suite(&["NSGA-II"], &[spec], &tiny_cfg(), 1);
}

#[test]
fn csv_writers_emit_a_header_and_one_row_per_record() {
    let dir = unique_temp_dir("csv");
    let problems: Vec<SOProblemSpec> = so_suite(2).into_iter().take(1).collect();
    let so = run_so_suite(&["Jaya"], &problems, &tiny_cfg(), 2);
    let so_path = dir.join("so.csv");
    write_so_csv(&so, &so_path).unwrap();
    let text = std::fs::read_to_string(&so_path).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(
        lines[0],
        "solver,problem,dim,seed,best_fitness,global_minimum,gap,iterations,wall_ms"
    );
    assert_eq!(lines.len(), 3);
    assert!(lines[1].starts_with("Jaya,sphere,2,0,"));
    assert!(lines[2].starts_with("Jaya,sphere,2,1,"));

    let problems: Vec<_> = moo_suite(4, 4, 3)
        .into_iter()
        .filter(|s| s.name == "ZDT1" || s.name == "DTLZ2")
        .collect();
    let mo = run_mo_suite(&["NSGA-II"], &problems, &tiny_cfg(), 1);
    let mo_path = dir.join("mo.csv");
    write_mo_csv(&mo, &mo_path).unwrap();
    let text = std::fs::read_to_string(&mo_path).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(
        lines[0],
        "solver,problem,dim,num_objectives,seed,pareto_size,hypervolume,igd,wall_ms"
    );
    assert_eq!(lines.len(), 3);
    let zdt: Vec<&str> = lines[1].split(',').collect();
    assert_eq!(&zdt[..5], &["NSGA-II", "ZDT1", "4", "2", "0"]);
    assert!(!zdt[6].is_empty() && !zdt[7].is_empty());
    // DTLZ2 at 3 objectives has no HV/IGD: those columns are empty.
    let dtlz: Vec<&str> = lines[2].split(',').collect();
    assert_eq!(&dtlz[..5], &["NSGA-II", "DTLZ2", "4", "3", "0"]);
    assert_eq!(dtlz[6], "");
    assert_eq!(dtlz[7], "");

    // A path in a directory that does not exist is an I/O error, not a panic.
    let bad = dir.join("missing").join("x.csv");
    assert!(write_so_csv(&so, &bad).is_err());
    assert!(write_mo_csv(&mo, &bad).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────── CEC data

/// One test, because the loaders read a process-wide environment variable.
#[test]
fn cec_loaders_read_shift_and_rotation_files_under_the_data_root() {
    std::env::remove_var("SAMYAMA_CEC_DATA");
    assert_eq!(cec_data::data_root(), std::path::PathBuf::from("data/cec"));

    let root = unique_temp_dir("cec");
    std::env::set_var("SAMYAMA_CEC_DATA", &root);
    assert_eq!(cec_data::data_root(), root);

    // Nothing there yet.
    assert!(cec_data::load_shift(CecSuite::Cec2017, 1, 2).is_none());
    assert!(cec_data::load_rotation(CecSuite::Cec2022, 1, 2).is_none());

    std::fs::create_dir_all(root.join("cec2017")).unwrap();
    std::fs::create_dir_all(root.join("cec2022")).unwrap();
    std::fs::write(root.join("cec2017/shift_data_3.txt"), "1.5 -2.0 x 3.25 9.0").unwrap();
    std::fs::write(root.join("cec2022/M_4_D2.txt"), "1 0\n0 1\n").unwrap();
    std::fs::write(root.join("cec2022/M_5_D2.txt"), "1 0 0").unwrap();

    // Unparseable tokens are skipped; only the first `dim` values are kept.
    let shift = cec_data::load_shift(CecSuite::Cec2017, 3, 3).unwrap();
    assert_eq!(shift.to_vec(), vec![1.5, -2.0, 3.25]);
    // Asking for more values than the file has is a miss, not a short vector.
    assert!(cec_data::load_shift(CecSuite::Cec2017, 3, 10).is_none());
    // Same file, other suite: different directory.
    assert!(cec_data::load_shift(CecSuite::Cec2022, 3, 3).is_none());

    let rot = cec_data::load_rotation(CecSuite::Cec2022, 4, 2).unwrap();
    assert_eq!(rot.shape(), &[2, 2]);
    assert_eq!(rot[[0, 0]], 1.0);
    assert_eq!(rot[[0, 1]], 0.0);
    assert_eq!(rot[[1, 1]], 1.0);
    // Wrong number of entries for D=2.
    assert!(cec_data::load_rotation(CecSuite::Cec2022, 5, 2).is_none());

    std::env::remove_var("SAMYAMA_CEC_DATA");
    let _ = std::fs::remove_dir_all(&root);
}

// ─────────────────────────────────────────── solver-specific branches

struct Constant(f64);
impl Problem for Constant {
    fn objective(&self, _: &Array1<f64>) -> f64 {
        self.0
    }
    fn dim(&self) -> usize {
        2
    }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (array![-1.0, -1.0], array![1.0, 1.0])
    }
}

/// Sphere shifted down by 5: every fitness is negative near the optimum.
struct ShiftedSphere;
impl Problem for ShiftedSphere {
    fn objective(&self, x: &Array1<f64>) -> f64 {
        x.iter().map(|v| v * v).sum::<f64>() - 5.0
    }
    fn dim(&self) -> usize {
        2
    }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (array![-1.0, -1.0], array![1.0, 1.0])
    }
}

/// Minimised at the lower corner of [0,1]^2, so the opposite point of any
/// candidate above the midpoint is strictly better.
struct SumOnUnitBox;
impl Problem for SumOnUnitBox {
    fn objective(&self, x: &Array1<f64>) -> f64 {
        x.sum()
    }
    fn dim(&self) -> usize {
        2
    }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (array![0.0, 0.0], array![1.0, 1.0])
    }
}

#[test]
fn abc_handles_negative_fitness_in_onlooker_probabilities() {
    let r = ABCSolver::new(SolverConfig {
        population_size: 20,
        max_iterations: 60,
    })
    .with_seed(7)
    .solve(&ShiftedSphere);
    assert!(r.best_fitness >= -5.0);
    assert!(r.best_fitness < -4.9, "abc reached {}", r.best_fitness);
}

#[test]
fn abc_scouts_abandon_food_sources_that_stop_improving() {
    // On a flat objective no source ever improves, so every trial counter
    // passes the limit and the scout phase resets them; the result is still
    // the constant.
    let mut s = ABCSolver::new(SolverConfig {
        population_size: 6,
        max_iterations: 10,
    })
    .with_seed(3);
    s.limit = 1;
    let r = s.solve(&Constant(3.0));
    assert_eq!(r.best_fitness, 3.0);
    assert!(r.history.iter().all(|&h| h == 3.0));
}

#[test]
fn gsa_on_a_flat_objective_gives_every_agent_equal_mass() {
    let r = GSASolver::new(SolverConfig {
        population_size: 6,
        max_iterations: 5,
    })
    .with_seed(1)
    .solve(&Constant(-2.0));
    assert_eq!(r.best_fitness, -2.0);
    let (lo, hi) = Constant(0.0).bounds();
    for j in 0..2 {
        assert!(r.best_variables[j] >= lo[j] && r.best_variables[j] <= hi[j]);
    }
}

#[test]
fn gotlbo_opposition_learning_improves_an_asymmetric_objective() {
    let r = GOTLBOSolver::new(SolverConfig {
        population_size: 10,
        max_iterations: 30,
    })
    .with_seed(11)
    .solve(&SumOnUnitBox);
    assert!(r.best_fitness < 0.05, "gotlbo reached {}", r.best_fitness);
    assert!(r.best_variables.iter().all(|&v| (0.0..=1.0).contains(&v)));
}

#[test]
fn cuckoo_with_pa_sets_discovery_probability() {
    let cfg = SolverConfig {
        population_size: 10,
        max_iterations: 30,
    };
    let s = CuckooSolver::with_pa(cfg, 0.5);
    assert_eq!(s.pa, 0.5);
    assert!(s.seed.is_none());
    let r = s.with_seed(5).solve(&ShiftedSphere);
    assert!(r.best_fitness < -4.0, "cuckoo reached {}", r.best_fitness);
}

#[test]
fn firefly_with_params_sets_alpha_beta_gamma() {
    let cfg = SolverConfig {
        population_size: 10,
        max_iterations: 20,
    };
    let s = FireflySolver::with_params(cfg, 0.1, 0.8, 2.0);
    assert_eq!((s.alpha, s.beta0, s.gamma), (0.1, 0.8, 2.0));
    assert!(s.seed.is_none());
    let r = s.with_seed(2).solve(&ShiftedSphere);
    assert!(r.best_fitness < -4.0, "firefly reached {}", r.best_fitness);
}

#[test]
fn ga_result_is_the_best_of_the_final_population() {
    // With elitism the returned best never regresses past the last recorded
    // generation's best.
    // One generation over a wide population: a child often beats the elite,
    // so the final best is found away from index 0.
    for seed in 0..8 {
        let r = GASolver::new(SolverConfig {
            population_size: 30,
            max_iterations: 1,
        })
        .with_seed(seed)
        .solve(&ShiftedSphere);
        assert!(
            r.best_fitness <= *r.history.last().unwrap(),
            "seed {}",
            seed
        );
        assert!(r.best_fitness >= -5.0);
    }
}

#[test]
fn rao1_and_rao2_converge_on_a_shifted_sphere() {
    for variant in [RaoVariant::Rao1, RaoVariant::Rao2] {
        let r = RaoSolver::new(
            SolverConfig {
                population_size: 20,
                max_iterations: 60,
            },
            variant,
        )
        .with_seed(4)
        .solve(&ShiftedSphere);
        assert!(
            r.best_fitness < -4.9,
            "{:?} reached {}",
            variant,
            r.best_fitness
        );
        assert_eq!(r.history.len(), 60);
    }
}

#[test]
fn qo_rao2_and_rao3_converge_on_a_shifted_sphere() {
    for variant in [RaoVariant::Rao2, RaoVariant::Rao3] {
        let r = QORaoSolver::new(
            SolverConfig {
                population_size: 20,
                max_iterations: 60,
            },
            variant,
        )
        .with_seed(4)
        .solve(&ShiftedSphere);
        assert!(
            r.best_fitness < -4.9,
            "{:?} reached {}",
            variant,
            r.best_fitness
        );
    }
}

fn assert_non_increasing(h: &[f64], who: &str) {
    for w in h.windows(2) {
        assert!(w[1] <= w[0], "{}: best got worse {} -> {}", who, w[0], w[1]);
    }
}

#[test]
fn qo_rao2_and_rao3_with_tiny_populations_never_regress() {
    // A population of three leaves members that a random point can beat, so
    // both orientations of the Rao-2/3 random term are taken.
    for variant in [RaoVariant::Rao2, RaoVariant::Rao3] {
        for seed in 0..8 {
            let r = QORaoSolver::new(
                SolverConfig {
                    population_size: 3,
                    max_iterations: 15,
                },
                variant,
            )
            .with_seed(seed)
            .solve(&ShiftedSphere);
            assert_non_increasing(&r.history, "QO-Rao");
            assert!(r.best_fitness <= r.history[0]);
            assert!(r.best_fitness >= -5.0);
        }
    }
}

/// Schwefel-style multimodal objective on an asymmetric box: the opposite
/// point `lower + upper - x` of a candidate can land in a better basin.
struct SchwefelBox;
impl Problem for SchwefelBox {
    fn objective(&self, x: &Array1<f64>) -> f64 {
        x.iter().map(|&v| -v * v.abs().sqrt().sin()).sum()
    }
    fn dim(&self) -> usize {
        3
    }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::from_elem(3, 0.0), Array1::from_elem(3, 60.0))
    }
}

#[test]
fn gotlbo_on_a_multimodal_box_never_regresses() {
    for seed in 0..8 {
        let r = GOTLBOSolver::new(SolverConfig {
            population_size: 6,
            max_iterations: 15,
        })
        .with_seed(seed)
        .solve(&SchwefelBox);
        assert_non_increasing(&r.history, "GOTLBO");
        assert!(r.best_fitness <= r.history[0]);
        assert!(r.best_variables.iter().all(|&v| (0.0..=60.0).contains(&v)));
    }
}

#[test]
fn seeded_saphr_runs_are_reproducible() {
    let cfg = SolverConfig {
        population_size: 10,
        max_iterations: 10,
    };
    let a = SAPHRSolver::new(cfg.clone())
        .with_seed(9)
        .solve(&ShiftedSphere);
    let b = SAPHRSolver::new(cfg).with_seed(9).solve(&ShiftedSphere);
    assert_eq!(a.best_fitness, b.best_fitness);
    assert_eq!(a.history, b.history);
}

/// Two objectives on [0,1] with a constraint x >= 0.5: the lower half of the
/// box is infeasible, so the constrained-dominance branches are exercised.
struct ConstrainedMO;
impl MultiObjectiveProblem for ConstrainedMO {
    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        vec![x[0], 1.0 - x[0]]
    }
    fn penalties(&self, x: &Array1<f64>) -> Vec<f64> {
        vec![(0.5 - x[0]).max(0.0)]
    }
    fn dim(&self) -> usize {
        1
    }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (array![0.0], array![1.0])
    }
    fn num_objectives(&self) -> usize {
        2
    }
}

fn assert_front_feasible(front: &[MultiObjectiveIndividual], who: &str) {
    assert!(!front.is_empty(), "{} returned an empty front", who);
    for ind in front {
        assert_eq!(
            ind.constraint_violation, 0.0,
            "{}: infeasible member {:?}",
            who, ind.variables
        );
        assert!(ind.variables[0] >= 0.5 - 1e-12);
    }
}

#[test]
fn nsga2_front_on_a_constrained_problem_is_feasible() {
    let r = NSGA2Solver::new(SolverConfig {
        population_size: 20,
        max_iterations: 20,
    })
    .with_seed(3)
    .solve(&ConstrainedMO);
    assert_front_feasible(&r.pareto_front, "NSGA-II");
}

#[test]
fn motlbo_front_on_a_constrained_problem_is_feasible() {
    let r = MOTLBOSolver::new(SolverConfig {
        population_size: 20,
        max_iterations: 20,
    })
    .with_seed(3)
    .solve(&ConstrainedMO);
    assert_front_feasible(&r.pareto_front, "MOTLBO");
}

#[test]
fn seeded_mo_solvers_are_reproducible() {
    let cfg = SolverConfig {
        population_size: 10,
        max_iterations: 5,
    };
    let p = ZDT { variant: 1, dim: 3 };
    let f = |r: MultiObjectiveResult| -> Vec<Vec<f64>> {
        r.pareto_front.into_iter().map(|i| i.fitness).collect()
    };
    for variant in [
        MOBMWRVariant::MOBMR,
        MOBMWRVariant::MOBWR,
        MOBMWRVariant::MOBMWR,
    ] {
        let a = MOBMWRSolver::new(cfg.clone(), variant)
            .with_seed(8)
            .solve(&p);
        let b = MOBMWRSolver::new(cfg.clone(), variant)
            .with_seed(8)
            .solve(&p);
        assert_eq!(a.history, b.history, "{:?}", variant);
        assert_eq!(f(a), f(b), "{:?}", variant);
    }
    let a = MORaoDESolver::new(cfg.clone()).with_seed(8).solve(&p);
    let b = MORaoDESolver::new(cfg).with_seed(8).solve(&p);
    assert_eq!(a.history, b.history);
    assert_eq!(f(a), f(b));
}

#[test]
fn mo_bmwr_history_on_three_objectives_tracks_min_first_objective() {
    let p = DTLZ {
        variant: 2,
        dim: 4,
        m: 3,
    };
    let r = MOBMWRSolver::new(
        SolverConfig {
            population_size: 12,
            max_iterations: 6,
        },
        MOBMWRVariant::MOBMWR,
    )
    .with_seed(2)
    .solve(&p);
    assert_eq!(r.history.len(), 6);
    let min_f0 = r
        .pareto_front
        .iter()
        .map(|i| i.fitness[0])
        .fold(f64::INFINITY, f64::min);
    assert_eq!(*r.history.last().unwrap(), min_f0);
    assert!(r.pareto_front.iter().all(|i| i.fitness.len() == 3));
}
