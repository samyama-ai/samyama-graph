//! Selectable constraint handling (OPT-10, #2266).
//!
//! A constrained problem reports how badly a candidate breaks its constraints
//! ([`Problem::penalty`], [`MultiObjectiveProblem::penalties`]; 0 when it
//! breaks none). What a solver *does* with that is a choice, and the three
//! classic answers give different results:
//!
//! | [`ConstraintHandling`] | What the solver sees |
//! |---|---|
//! | `Penalty` | the objective plus the penalty: a violating point is allowed and pays for it |
//! | `Repair` | the problem's own repair is applied first, so the point evaluated (and returned) is the repaired one |
//! | `FeasibilityRules` | Deb's rules: any feasible point beats any infeasible one, a smaller violation beats a larger, and only feasible points compete on objective |
//!
//! [`Constrained`] wraps a problem the way [`crate::budget::Budgeted`] does,
//! so every solver gets every strategy without being changed: the strategy is
//! in what the problem reports, not in how a solver searches.
//!
//! # What these are, and are not
//!
//! - **Repair needs a repair.** It is problem-specific, so a problem opts in by
//!   overriding `repair`; [`Constrained::new`] refuses `Repair` for one that
//!   has none, rather than quietly running a penalty.
//! - **Repair is Baldwinian.** The solver's population keeps the unrepaired
//!   point and is scored by its repaired one; [`Constrained::resolve`] turns
//!   the solver's answer into the point actually evaluated, which is the one
//!   to return. A solver never sees a repaired point it did not propose.
//! - **Feasibility rules are not feasibility-preserving operators.** OPT-10's
//!   third strategy, in the strict sense, is a crossover and mutation that
//!   cannot leave the feasible region; that would be a change to every
//!   solver's operators. Deb's rules are the solver-independent strategy that
//!   gives the same guarantee for the answer -- a feasible point found is never
//!   lost to an infeasible one -- and are what the multi-objective solvers
//!   already use through `penalties` (constrained dominance).
//! - **Single-objective feasibility rules are encoded in the fitness.** An
//!   infeasible point scores [`INFEASIBLE`] plus its violation, so every
//!   comparison a solver makes orders it correctly. That holds while feasible
//!   objectives stay below [`INFEASIBLE`] (1e12); a problem whose objective can
//!   exceed it should use `Penalty` or `Repair`.

use ndarray::Array1;

use crate::common::{MultiObjectiveProblem, Problem};

/// The fitness floor of an infeasible point under [`ConstraintHandling::FeasibilityRules`].
pub const INFEASIBLE: f64 = 1e12;

/// How a solver treats a candidate that breaks a constraint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConstraintHandling {
    /// Objective plus penalty.
    Penalty,
    /// The problem's repair first, then objective plus any penalty left.
    Repair,
    /// Deb's feasibility rules.
    FeasibilityRules,
}

impl ConstraintHandling {
    /// The spellings `from_name` accepts, for messages.
    pub const NAMES: &'static [&'static str] = &["penalty", "repair", "feasibility"];

    /// Which strategy `name` spells, case-insensitively.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "penalty" => Some(Self::Penalty),
            "repair" => Some(Self::Repair),
            "feasibility" | "feasibilityrules" | "feasibility_rules" | "deb" => {
                Some(Self::FeasibilityRules)
            }
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Penalty => "penalty",
            Self::Repair => "repair",
            Self::FeasibilityRules => "feasibility",
        }
    }
}

/// Why a strategy cannot be applied to a problem.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NoRepair;

impl std::fmt::Display for NoRepair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "this problem has no repair operator, so `repair` cannot be applied; use `penalty` \
             or `feasibility`",
        )
    }
}

impl std::error::Error for NoRepair {}

/// A problem with a constraint-handling strategy applied. See the module.
pub struct Constrained<P> {
    inner: P,
    handling: ConstraintHandling,
}

impl<P> Constrained<P> {
    pub fn handling(&self) -> ConstraintHandling {
        self.handling
    }

    pub fn inner(&self) -> &P {
        &self.inner
    }
}

impl<P: Problem> Constrained<P> {
    /// Apply `handling` to a single-objective problem. `Repair` is refused
    /// when the problem has no repair.
    pub fn new(inner: P, handling: ConstraintHandling) -> Result<Self, NoRepair> {
        if handling == ConstraintHandling::Repair && !inner.can_repair() {
            return Err(NoRepair);
        }
        Ok(Self { inner, handling })
    }

    /// The point the solver's answer stands for: the repaired one under
    /// `Repair`, the answer itself otherwise. Return this, not the raw answer.
    pub fn resolve(&self, variables: &Array1<f64>) -> Array1<f64> {
        match self.handling {
            ConstraintHandling::Repair => self
                .inner
                .repair(variables)
                .unwrap_or_else(|| variables.clone()),
            _ => variables.clone(),
        }
    }
}

impl<P: MultiObjectiveProblem> Constrained<P> {
    /// Apply `handling` to a multi-objective problem. `Repair` is refused
    /// when the problem has no repair.
    pub fn new_multi(inner: P, handling: ConstraintHandling) -> Result<Self, NoRepair> {
        if handling == ConstraintHandling::Repair && !inner.can_repair() {
            return Err(NoRepair);
        }
        Ok(Self { inner, handling })
    }

    /// As [`Constrained::resolve`], for a multi-objective problem.
    pub fn resolve_multi(&self, variables: &Array1<f64>) -> Array1<f64> {
        match self.handling {
            ConstraintHandling::Repair => self
                .inner
                .repair(variables)
                .unwrap_or_else(|| variables.clone()),
            _ => variables.clone(),
        }
    }

    fn repaired(&self, variables: &Array1<f64>) -> Option<Array1<f64>> {
        match self.handling {
            ConstraintHandling::Repair => self.inner.repair(variables),
            _ => None,
        }
    }
}

impl<P: Problem> Problem for Constrained<P> {
    fn objective(&self, variables: &Array1<f64>) -> f64 {
        self.inner.objective(&self.resolve(variables))
    }

    fn penalty(&self, variables: &Array1<f64>) -> f64 {
        self.inner.penalty(&self.resolve(variables))
    }

    fn fitness(&self, variables: &Array1<f64>) -> f64 {
        let x = self.resolve(variables);
        match self.handling {
            ConstraintHandling::Penalty | ConstraintHandling::Repair => self.inner.fitness(&x),
            ConstraintHandling::FeasibilityRules => {
                let violation = self.inner.penalty(&x);
                if violation > 0.0 {
                    INFEASIBLE + violation
                } else {
                    self.inner.objective(&x)
                }
            }
        }
    }

    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        self.inner.bounds()
    }

    fn budget_exhausted(&self) -> bool {
        self.inner.budget_exhausted()
    }

    fn can_repair(&self) -> bool {
        self.inner.can_repair()
    }

    fn repair(&self, variables: &Array1<f64>) -> Option<Array1<f64>> {
        self.inner.repair(variables)
    }
}

impl<P: MultiObjectiveProblem> MultiObjectiveProblem for Constrained<P> {
    fn objectives(&self, variables: &Array1<f64>) -> Vec<f64> {
        let repaired = self.repaired(variables);
        let x = repaired.as_ref().unwrap_or(variables);
        let objectives = self.inner.objectives(x);
        match self.handling {
            ConstraintHandling::Penalty => {
                // The penalty is paid in every objective, so a violating point
                // is dominated by the same point without the violation.
                let paid: f64 = self.inner.penalties(x).iter().sum();
                objectives.into_iter().map(|o| o + paid).collect()
            }
            _ => objectives,
        }
    }

    fn penalties(&self, variables: &Array1<f64>) -> Vec<f64> {
        match self.handling {
            // Paid in the objectives instead, so the solvers' constrained
            // dominance does not also act on it: that would be feasibility
            // rules under another name.
            ConstraintHandling::Penalty => Vec::new(),
            // Constrained dominance on what the repair could not fix.
            ConstraintHandling::Repair => {
                let repaired = self.repaired(variables);
                self.inner.penalties(repaired.as_ref().unwrap_or(variables))
            }
            ConstraintHandling::FeasibilityRules => self.inner.penalties(variables),
        }
    }

    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        self.inner.bounds()
    }

    fn num_objectives(&self) -> usize {
        self.inner.num_objectives()
    }

    fn budget_exhausted(&self) -> bool {
        self.inner.budget_exhausted()
    }

    fn can_repair(&self) -> bool {
        self.inner.can_repair()
    }

    fn repair(&self, variables: &Array1<f64>) -> Option<Array1<f64>> {
        self.inner.repair(variables)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimise x0 + x1 on [0, 10]² subject to x0 + x1 >= 4. The optimum is
    /// any point on x0 + x1 = 4, with objective 4. Repair raises x0 to close
    /// the gap.
    struct AtLeastFour {
        repairable: bool,
    }

    impl Problem for AtLeastFour {
        fn objective(&self, x: &Array1<f64>) -> f64 {
            x[0] + x[1]
        }
        fn penalty(&self, x: &Array1<f64>) -> f64 {
            (4.0 - (x[0] + x[1])).max(0.0) * 10.0
        }
        fn dim(&self) -> usize {
            2
        }
        fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
            (Array1::zeros(2), Array1::from_elem(2, 10.0))
        }
        fn can_repair(&self) -> bool {
            self.repairable
        }
        fn repair(&self, x: &Array1<f64>) -> Option<Array1<f64>> {
            if !self.repairable {
                return None;
            }
            let mut y = x.clone();
            let gap = 4.0 - (y[0] + y[1]);
            if gap > 0.0 {
                y[0] += gap;
            }
            Some(y)
        }
    }

    fn p(h: ConstraintHandling) -> Constrained<AtLeastFour> {
        Constrained::new(AtLeastFour { repairable: true }, h).unwrap()
    }

    #[test]
    fn names_round_trip() {
        for name in ConstraintHandling::NAMES {
            assert_eq!(ConstraintHandling::from_name(name).unwrap().name(), *name);
        }
        assert_eq!(ConstraintHandling::from_name("nope"), None);
    }

    #[test]
    fn penalty_adds_the_violation_to_the_objective() {
        let x = Array1::from(vec![1.0, 1.0]);
        assert_eq!(p(ConstraintHandling::Penalty).fitness(&x), 2.0 + 20.0);
        assert_eq!(p(ConstraintHandling::Penalty).resolve(&x), x);
    }

    #[test]
    fn repair_evaluates_and_returns_the_repaired_point() {
        let x = Array1::from(vec![1.0, 1.0]);
        let r = p(ConstraintHandling::Repair);
        assert_eq!(r.resolve(&x), Array1::from(vec![3.0, 1.0]));
        assert_eq!(
            r.fitness(&x),
            4.0,
            "the repaired point is feasible, so no penalty"
        );
        assert_eq!(r.penalty(&x), 0.0);
        let feasible = Array1::from(vec![2.0, 5.0]);
        assert_eq!(
            r.resolve(&feasible),
            feasible,
            "a feasible point is left alone"
        );
    }

    #[test]
    fn feasibility_rules_rank_any_feasible_point_above_any_infeasible_one() {
        let f = p(ConstraintHandling::FeasibilityRules);
        let worst_feasible = f.fitness(&Array1::from(vec![10.0, 10.0])); // 20
        let barely_infeasible = f.fitness(&Array1::from(vec![2.0, 1.99])); // violation 0.1
        let very_infeasible = f.fitness(&Array1::from(vec![0.0, 0.0])); // violation 40
        assert!(worst_feasible < barely_infeasible);
        assert!(
            barely_infeasible < very_infeasible,
            "a smaller violation is better"
        );
        // Under a penalty, the same infeasible point beats the feasible one.
        let pen = p(ConstraintHandling::Penalty);
        assert!(
            pen.fitness(&Array1::from(vec![2.0, 1.99]))
                < pen.fitness(&Array1::from(vec![10.0, 10.0]))
        );
    }

    #[test]
    fn repair_is_refused_for_a_problem_without_one() {
        assert_eq!(
            Constrained::new(
                AtLeastFour { repairable: false },
                ConstraintHandling::Repair
            )
            .err(),
            Some(NoRepair)
        );
        assert!(Constrained::new(
            AtLeastFour { repairable: false },
            ConstraintHandling::Penalty
        )
        .is_ok());
    }
}
