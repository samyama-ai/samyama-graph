use pyo3::prelude::*;
use pyo3::types::PyFunction;
use ndarray::Array1;
use numpy::{IntoPyArray, PyArray1, PyReadonlyArray1};
use ::samyama_optimization::algorithms::{JayaSolver, RaoSolver, RaoVariant, TLBOSolver, BMRSolver, BWRSolver, QOJayaSolver, ITLBOSolver};
use ::samyama_optimization::common::{Problem, SolverConfig};

/// Wrapper to use a Python function as a Rust Problem
struct PyProblem {
    objective: Py<PyFunction>,
    dim: usize,
    lower: Array1<f64>,
    upper: Array1<f64>,
}

impl Problem for PyProblem {
    fn objective(&self, variables: &Array1<f64>) -> f64 {
        Python::with_gil(|py| {
            let py_vars = variables.to_owned().into_pyarray(py);
            let args = (py_vars,);
            let result = self.objective.as_ref(py).call1(args).expect("Python objective function failed");
            result.extract::<f64>().expect("Objective must return a float")
        })
    }

    fn dim(&self) -> usize { self.dim }

    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (self.lower.clone(), self.upper.clone())
    }
}

#[pyclass]
pub struct PyOptimizationResult {
    #[pyo3(get)]
    pub best_variables: Py<PyArray1<f64>>,
    #[pyo3(get)]
    pub best_fitness: f64,
    #[pyo3(get)]
    pub history: Vec<f64>,
}

#[pyfunction]
#[pyo3(signature = (objective, lower, upper, population_size=50, max_iterations=100))]
fn solve_jaya(
    py: Python,
    objective: Py<PyFunction>,
    lower: PyReadonlyArray1<f64>,
    upper: PyReadonlyArray1<f64>,
    population_size: usize,
    max_iterations: usize,
) -> PyResult<PyOptimizationResult> {
    let lower_arr = lower.as_array().to_owned();
    let upper_arr = upper.as_array().to_owned();
    let problem = PyProblem { objective, dim: lower_arr.len(), lower: lower_arr, upper: upper_arr };
    let solver = JayaSolver::new(SolverConfig { population_size, max_iterations });
    let result = py.allow_threads(|| solver.solve(&problem));
    Ok(PyOptimizationResult {
        best_variables: result.best_variables.into_pyarray(py).to_owned(),
        best_fitness: result.best_fitness,
        history: result.history,
    })
}

#[pyfunction]
#[pyo3(signature = (objective, lower, upper, variant="Rao3", population_size=50, max_iterations=100))]
fn solve_rao(
    py: Python,
    objective: Py<PyFunction>,
    lower: PyReadonlyArray1<f64>,
    upper: PyReadonlyArray1<f64>,
    variant: &str,
    population_size: usize,
    max_iterations: usize,
) -> PyResult<PyOptimizationResult> {
    let lower_arr = lower.as_array().to_owned();
    let upper_arr = upper.as_array().to_owned();
    let problem = PyProblem { objective, dim: lower_arr.len(), lower: lower_arr, upper: upper_arr };
    
    let rao_variant = match variant {
        "Rao1" => RaoVariant::Rao1,
        "Rao2" => RaoVariant::Rao2,
        "Rao3" => RaoVariant::Rao3,
        _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid Rao variant. Use Rao1, Rao2, or Rao3")),
    };

    let solver = RaoSolver::new(SolverConfig { population_size, max_iterations }, rao_variant);
    let result = py.allow_threads(|| solver.solve(&problem));
    Ok(PyOptimizationResult {
        best_variables: result.best_variables.into_pyarray(py).to_owned(),
        best_fitness: result.best_fitness,
        history: result.history,
    })
}

#[pyfunction]
#[pyo3(signature = (objective, lower, upper, population_size=50, max_iterations=100))]
fn solve_tlbo(
    py: Python,
    objective: Py<PyFunction>,
    lower: PyReadonlyArray1<f64>,
    upper: PyReadonlyArray1<f64>,
    population_size: usize,
    max_iterations: usize,
) -> PyResult<PyOptimizationResult> {
    let lower_arr = lower.as_array().to_owned();
    let upper_arr = upper.as_array().to_owned();
    let problem = PyProblem { objective, dim: lower_arr.len(), lower: lower_arr, upper: upper_arr };
    let solver = TLBOSolver::new(SolverConfig { population_size, max_iterations });
    let result = py.allow_threads(|| solver.solve(&problem));
    Ok(PyOptimizationResult {
        best_variables: result.best_variables.into_pyarray(py).to_owned(),
        best_fitness: result.best_fitness,
        history: result.history,
    })
}

#[pyfunction]
#[pyo3(signature = (objective, lower, upper, population_size=50, max_iterations=100))]
fn solve_bmr(
    py: Python,
    objective: Py<PyFunction>,
    lower: PyReadonlyArray1<f64>,
    upper: PyReadonlyArray1<f64>,
    population_size: usize,
    max_iterations: usize,
) -> PyResult<PyOptimizationResult> {
    let lower_arr = lower.as_array().to_owned();
    let upper_arr = upper.as_array().to_owned();
    let problem = PyProblem { objective, dim: lower_arr.len(), lower: lower_arr, upper: upper_arr };
    let solver = BMRSolver::new(SolverConfig { population_size, max_iterations });
    let result = py.allow_threads(|| solver.solve(&problem));
    Ok(PyOptimizationResult {
        best_variables: result.best_variables.into_pyarray(py).to_owned(),
        best_fitness: result.best_fitness,
        history: result.history,
    })
}

#[pyfunction]
#[pyo3(signature = (objective, lower, upper, population_size=50, max_iterations=100))]
fn solve_bwr(
    py: Python,
    objective: Py<PyFunction>,
    lower: PyReadonlyArray1<f64>,
    upper: PyReadonlyArray1<f64>,
    population_size: usize,
    max_iterations: usize,
) -> PyResult<PyOptimizationResult> {
    let lower_arr = lower.as_array().to_owned();
    let upper_arr = upper.as_array().to_owned();
    let problem = PyProblem { objective, dim: lower_arr.len(), lower: lower_arr, upper: upper_arr };
    let solver = BWRSolver::new(SolverConfig { population_size, max_iterations });
    let result = py.allow_threads(|| solver.solve(&problem));
    Ok(PyOptimizationResult {
        best_variables: result.best_variables.into_pyarray(py).to_owned(),
        best_fitness: result.best_fitness,
        history: result.history,
    })
}

#[pyfunction]
#[pyo3(signature = (objective, lower, upper, population_size=50, max_iterations=100))]
fn solve_qojaya(
    py: Python,
    objective: Py<PyFunction>,
    lower: PyReadonlyArray1<f64>,
    upper: PyReadonlyArray1<f64>,
    population_size: usize,
    max_iterations: usize,
) -> PyResult<PyOptimizationResult> {
    let lower_arr = lower.as_array().to_owned();
    let upper_arr = upper.as_array().to_owned();
    let problem = PyProblem { objective, dim: lower_arr.len(), lower: lower_arr, upper: upper_arr };
    let solver = QOJayaSolver::new(SolverConfig { population_size, max_iterations });
    let result = py.allow_threads(|| solver.solve(&problem));
    Ok(PyOptimizationResult {
        best_variables: result.best_variables.into_pyarray(py).to_owned(),
        best_fitness: result.best_fitness,
        history: result.history,
    })
}

#[pyfunction]
#[pyo3(signature = (objective, lower, upper, population_size=50, max_iterations=100))]
fn solve_itlbo(
    py: Python,
    objective: Py<PyFunction>,
    lower: PyReadonlyArray1<f64>,
    upper: PyReadonlyArray1<f64>,
    population_size: usize,
    max_iterations: usize,
) -> PyResult<PyOptimizationResult> {
    let lower_arr = lower.as_array().to_owned();
    let upper_arr = upper.as_array().to_owned();
    let problem = PyProblem { objective, dim: lower_arr.len(), lower: lower_arr, upper: upper_arr };
    let solver = ITLBOSolver::new(SolverConfig { population_size, max_iterations });
    let result = py.allow_threads(|| solver.solve(&problem));
    Ok(PyOptimizationResult {
        best_variables: result.best_variables.into_pyarray(py).to_owned(),
        best_fitness: result.best_fitness,
        history: result.history,
    })
}

#[pymodule]
fn samyama_optimization(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyOptimizationResult>()?;
    m.add_function(wrap_pyfunction!(solve_jaya, m)?)?;
    m.add_function(wrap_pyfunction!(solve_rao, m)?)?;
    m.add_function(wrap_pyfunction!(solve_tlbo, m)?)?;
    m.add_function(wrap_pyfunction!(solve_bmr, m)?)?;
    m.add_function(wrap_pyfunction!(solve_bwr, m)?)?;
    m.add_function(wrap_pyfunction!(solve_qojaya, m)?)?;
    m.add_function(wrap_pyfunction!(solve_itlbo, m)?)?;
    m.add_function(wrap_pyfunction!(status, m)?)?;
    Ok(())
}

#[pyfunction]
fn status() -> PyResult<String> {
    Ok("Samyama Optimization Engine (Rust) is active".to_string())
}
