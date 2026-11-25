//! The graph-native optimisation templates as procedures (OPT-06, #1770).
//!
//! Each reads its problem from labelled nodes, their properties and the
//! relationships between them, hands it to the solver in
//! `samyama_graph_algorithms::or_templates`, and returns **one row**: the
//! answer and the result envelope every solve carries (OPT-09) --
//! `objective`, `feasible`, `gap`, `iterations`, `total_time_ms`.
//!
//! `gap` is `0` when the answer is proven optimal and null when it is the
//! best found by a search that hit its work limit: an unknown gap is never
//! reported as zero. When `feasible` is false, `objective` and `gap` are null
//! and the answer columns still say what could be done.
//!
//! All five take a single map of settings and read the graph as stored; none
//! runs on a projection, which keeps one weight and no node properties.

use super::{check_deadline, duration_ms, AlgorithmOperator};
use crate::graph::{GraphStore, Label, NodeId, PropertyValue};
use crate::query::ast::Expression;
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value};
use samyama_graph_algorithms::or_templates as t;
use std::collections::HashMap;

/// Settings of one template call.
struct Settings<'a> {
    call: &'static str,
    map: &'a HashMap<String, PropertyValue>,
}

impl Settings<'_> {
    fn text(&self, key: &str) -> ExecutionResult<Option<String>> {
        match self.map.get(key) {
            None => Ok(None),
            Some(PropertyValue::String(v)) if !v.trim().is_empty() => Ok(Some(v.clone())),
            Some(other) => Err(ExecutionError::bad_argument(format!(
                "{}: `{key}` must be a non-empty string, got {other:?}",
                self.call
            ))),
        }
    }

    fn required_text(&self, key: &str, what: &str) -> ExecutionResult<String> {
        self.text(key)?.ok_or_else(|| {
            ExecutionError::bad_argument(format!("{}: `{key}` is required: {what}", self.call))
        })
    }

    fn number(&self, key: &str) -> ExecutionResult<Option<f64>> {
        match self.map.get(key) {
            None => Ok(None),
            Some(PropertyValue::Integer(v)) => Ok(Some(*v as f64)),
            Some(PropertyValue::Float(v)) if v.is_finite() => Ok(Some(*v)),
            Some(other) => Err(ExecutionError::bad_argument(format!(
                "{}: `{key}` must be a finite number, got {other:?}",
                self.call
            ))),
        }
    }
}

impl AlgorithmOperator {
    /// The single settings map every template takes, with unknown keys refused.
    fn template_settings(
        &self,
        call: &'static str,
        allowed: &[&str],
        example: &str,
    ) -> ExecutionResult<Settings<'_>> {
        Self::reject_unknown_config_keys(&self.args, call, allowed)?;
        match (self.args.first(), self.args.len()) {
            (Some(Expression::Literal(PropertyValue::Map(map))), 1) => Ok(Settings { call, map }),
            _ => Err(ExecutionError::bad_argument(format!(
                "{call} takes one map of settings, e.g. {example}. A parameter inside the map is \
                 not read here; pass the whole map as one parameter instead, e.g. $config"
            ))),
        }
    }

    /// Nodes with `label`, in id order: the order every template reports in.
    fn labelled(store: &GraphStore, call: &str, label: &str) -> ExecutionResult<Vec<NodeId>> {
        let mut ids: Vec<NodeId> = store
            .nodes_with_label(&Label::new(label))
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        ids.sort_unstable();
        if ids.is_empty() {
            return Err(ExecutionError::bad_argument(format!(
                "{call}: no node has the label `{label}`"
            )));
        }
        Ok(ids)
    }

    /// A numeric property of a node, `default` where it is absent. A value
    /// that is present and not a number is refused, naming the node: reading
    /// it as the default would solve a different problem without saying so.
    fn node_number(
        store: &GraphStore,
        call: &str,
        id: NodeId,
        key: Option<&str>,
        default: f64,
    ) -> ExecutionResult<f64> {
        let Some(key) = key else { return Ok(default) };
        match store.node_property(id, key) {
            None | Some(PropertyValue::Null) => Ok(default),
            Some(PropertyValue::Integer(v)) => Ok(v as f64),
            Some(PropertyValue::Float(v)) => Ok(v),
            Some(other) => Err(ExecutionError::bad_argument(format!(
                "{call}: node {} has `{key}` = {other:?}, which is not a number",
                id.as_u64()
            ))),
        }
    }

    /// A numeric property of a relationship, `default` where it is absent.
    fn edge_number(
        store: &GraphStore,
        call: &str,
        edge: &crate::graph::Edge,
        key: Option<&str>,
        default: f64,
    ) -> ExecutionResult<f64> {
        let Some(key) = key else { return Ok(default) };
        match store.edge_property(edge.id, key) {
            None | Some(PropertyValue::Null) => Ok(default),
            Some(PropertyValue::Integer(v)) => Ok(v as f64),
            Some(PropertyValue::Float(v)) => Ok(v),
            Some(other) => Err(ExecutionError::bad_argument(format!(
                "{call}: relationship {} has `{key}` = {other:?}, which is not a number",
                edge.id.as_u64()
            ))),
        }
    }

    /// Relationships out of `from`, of `edge_type` when given, to a node in `to`.
    fn edges_to<'a>(
        store: &'a GraphStore,
        from: NodeId,
        edge_type: Option<&'a str>,
        to: &'a HashMap<NodeId, usize>,
    ) -> impl Iterator<Item = (crate::graph::Edge, usize)> + 'a {
        store
            .get_outgoing_edges(from)
            .into_iter()
            .filter(move |e| edge_type.is_none_or(|t| e.edge_type.as_str() == t))
            .filter_map(move |e| to.get(&e.target).map(|&i| (e, i)))
    }

    fn template_error(call: &str, e: t::TemplateError) -> ExecutionError {
        match e {
            t::TemplateError::Interrupted => {
                ExecutionError::RuntimeError(format!("Query timed out: {call} was still solving"))
            }
            other => ExecutionError::bad_argument(format!("{call}: {other}")),
        }
    }

    fn ids(ids: impl IntoIterator<Item = NodeId>) -> Value {
        Value::Property(PropertyValue::Array(
            ids.into_iter()
                .map(|n| PropertyValue::Integer(n.as_u64() as i64))
                .collect(),
        ))
    }

    fn rows_of_maps(rows: Vec<Vec<(&str, PropertyValue)>>) -> Value {
        Value::Property(PropertyValue::Array(
            rows.into_iter()
                .map(|r| {
                    PropertyValue::Map(r.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
                })
                .collect(),
        ))
    }

    /// The envelope, with the rule every template follows: an infeasible
    /// answer has no objective and no gap; a feasible one has its objective,
    /// and a gap of 0 when proven optimal or null when it is the best found.
    fn template_envelope(
        record: &mut Record,
        objective: f64,
        feasible: bool,
        exact: bool,
        iterations: u64,
        ms: f64,
    ) {
        let null = || Value::Property(PropertyValue::Null);
        let float = |v: f64| Value::Property(PropertyValue::Float(v));
        let (objective, gap) = match (feasible, exact) {
            (false, _) => (null(), null()),
            (true, true) => (float(objective), float(0.0)),
            (true, false) => (float(objective), null()),
        };
        Self::bind_result_envelope(
            record,
            objective,
            Value::Property(PropertyValue::Boolean(feasible)),
            gap,
            ms,
        );
        record.bind(
            "iterations".to_string(),
            Value::Property(PropertyValue::Integer(iterations as i64)),
        );
    }

    /// `algo.or.facilityLocation({facilityLabel, customerLabel, p
    /// [, distanceProperty, demandProperty, edgeType]})`, also
    /// `algo.or.pMedian`.
    ///
    /// Opens `p` of the `facilityLabel` nodes so that the demand-weighted
    /// distance from each `customerLabel` node to its nearest open facility
    /// is least. Distance is the shortest path along out-relationships
    /// (`edgeType` when given) summing `distanceProperty`, or hops without it.
    pub(super) fn execute_facility_location(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        let call = "algo.or.facilityLocation";
        let s = self.template_settings(
            call,
            &[
                "customerLabel",
                "demandProperty",
                "distanceProperty",
                "edgeType",
                "facilityLabel",
                "p",
            ],
            "{facilityLabel: 'Warehouse', customerLabel: 'Store', p: 2, distanceProperty: 'km'}",
        )?;
        let facility_label =
            s.required_text("facilityLabel", "the label of the candidate sites")?;
        let customer_label = s.required_text("customerLabel", "the label of the nodes to serve")?;
        let p = match s.number("p")? {
            Some(v) if v >= 1.0 && v.fract() == 0.0 => v as usize,
            Some(v) => {
                return Err(ExecutionError::bad_argument(format!(
                    "{call}: `p` must be a whole number >= 1, got {v}"
                )))
            }
            None => {
                return Err(ExecutionError::bad_argument(format!(
                    "{call}: `p` is required: how many facilities to open"
                )))
            }
        };
        let distance_property = s.text("distanceProperty")?;
        let demand_property = s.text("demandProperty")?;
        let edge_type = s.text("edgeType")?;

        let facilities = Self::labelled(store, call, &facility_label)?;
        let customers = Self::labelled(store, call, &customer_label)?;
        const MAX_CELLS: usize = 25_000_000;
        if facilities.len().saturating_mul(customers.len()) > MAX_CELLS {
            return Err(Self::template_error(
                call,
                t::TemplateError::TooLarge {
                    what: "facility-customer distances",
                    size: facilities.len() * customers.len(),
                    max: MAX_CELLS,
                },
            ));
        }
        let demand = customers
            .iter()
            .map(|&c| Self::node_number(store, call, c, demand_property.as_deref(), 1.0))
            .collect::<ExecutionResult<Vec<f64>>>()?;
        let view = crate::algo::build_view_with(
            store,
            None,
            edge_type.as_deref(),
            distance_property.as_deref(),
            &crate::algo::NoProgress,
        )
        .map_err(|_| {
            ExecutionError::RuntimeError(format!("{call}: could not build the graph view"))
        })?;
        let stop = || check_deadline().is_err();
        let started = std::time::Instant::now();
        let mut distance = Vec::with_capacity(facilities.len());
        for f in &facilities {
            let from = view
                .node_to_index
                .get(&f.as_u64())
                .copied()
                .unwrap_or(usize::MAX);
            let all = t::distances_from(
                &view.out_offsets,
                &view.out_targets,
                view.weights.as_deref(),
                from,
                &stop,
            )
            .map_err(|e| Self::template_error(call, e))?;
            distance.push(
                customers
                    .iter()
                    .map(|c| {
                        view.node_to_index
                            .get(&c.as_u64())
                            .map_or(f64::INFINITY, |&i| all[i])
                    })
                    .collect(),
            );
        }
        let problem = t::FacilityProblem {
            distance,
            demand,
            p,
        };
        let solved = t::p_median(&problem, &stop).map_err(|e| Self::template_error(call, e))?;
        let ms = duration_ms(started.elapsed());

        let mut record = Record::new();
        record.bind(
            "facilities".to_string(),
            Self::ids(solved.open.iter().map(|&f| facilities[f])),
        );
        let mut assignments = Vec::new();
        let mut unserved = Vec::new();
        for (c, serving) in solved.serving.iter().enumerate() {
            match serving {
                Some(f) => assignments.push(vec![
                    (
                        "customer",
                        PropertyValue::Integer(customers[c].as_u64() as i64),
                    ),
                    (
                        "facility",
                        PropertyValue::Integer(facilities[*f].as_u64() as i64),
                    ),
                    ("distance", PropertyValue::Float(problem.distance[*f][c])),
                ]),
                None if problem.demand[c] > 0.0 => unserved.push(customers[c]),
                None => {}
            }
        }
        record.bind("assignments".to_string(), Self::rows_of_maps(assignments));
        record.bind("unserved".to_string(), Self::ids(unserved));
        Self::template_envelope(
            &mut record,
            solved.cost,
            solved.feasible(),
            solved.exact,
            solved.iterations,
            ms,
        );
        self.results.push(record);
        Ok(())
    }

    /// Sets and elements: `setLabel` nodes cover the `elementLabel` nodes
    /// they have a relationship to (`edgeType` when given). Shared by
    /// set cover and max coverage.
    fn cover_problem(
        store: &GraphStore,
        call: &str,
        s: &Settings<'_>,
    ) -> ExecutionResult<(Vec<NodeId>, Vec<NodeId>, t::CoverProblem)> {
        let set_label = s.required_text("setLabel", "the label of the nodes to choose")?;
        let element_label = s.required_text("elementLabel", "the label of the nodes to cover")?;
        let edge_type = s.text("edgeType")?;
        let cost_property = s.text("costProperty")?;
        let sets = Self::labelled(store, call, &set_label)?;
        let elements = Self::labelled(store, call, &element_label)?;
        let index: HashMap<NodeId, usize> =
            elements.iter().enumerate().map(|(i, &e)| (e, i)).collect();
        let mut members = Vec::with_capacity(sets.len());
        let mut cost = Vec::with_capacity(sets.len());
        for &set in &sets {
            let mut covers: Vec<usize> = Self::edges_to(store, set, edge_type.as_deref(), &index)
                .map(|(_, i)| i)
                .collect();
            covers.sort_unstable();
            covers.dedup();
            members.push(covers);
            cost.push(Self::node_number(
                store,
                call,
                set,
                cost_property.as_deref(),
                1.0,
            )?);
        }
        let problem = t::CoverProblem {
            sets: members,
            cost,
            elements: elements.len(),
        };
        Ok((sets, elements, problem))
    }

    /// `algo.or.setCover({setLabel, elementLabel [, costProperty, edgeType]})`.
    ///
    /// The cheapest `setLabel` nodes that together reach every `elementLabel`
    /// node: the fewest fixes that remediate every finding, the cheapest
    /// sensors that watch every asset. Cost is `costProperty` on the set
    /// node, or 1 each without it (the fewest sets).
    pub(super) fn execute_set_cover(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        let call = "algo.or.setCover";
        let s = self.template_settings(
            call,
            &["costProperty", "edgeType", "elementLabel", "setLabel"],
            "{setLabel: 'Patch', elementLabel: 'Vulnerability', costProperty: 'hours'}",
        )?;
        let (sets, elements, problem) = Self::cover_problem(store, call, &s)?;
        let started = std::time::Instant::now();
        let stop = || check_deadline().is_err();
        let solved = t::set_cover(&problem, t::COVER_MAX_EXPANSIONS, &stop)
            .map_err(|e| Self::template_error(call, e))?;
        let ms = duration_ms(started.elapsed());
        let mut record = Record::new();
        record.bind(
            "chosen".to_string(),
            Self::ids(solved.chosen.iter().map(|&i| sets[i])),
        );
        record.bind(
            "cost".to_string(),
            Value::Property(PropertyValue::Float(solved.cost)),
        );
        record.bind(
            "uncoverable".to_string(),
            Self::ids(solved.uncoverable.iter().map(|&i| elements[i])),
        );
        Self::template_envelope(
            &mut record,
            solved.cost,
            solved.feasible(),
            solved.exact,
            solved.iterations,
            ms,
        );
        self.results.push(record);
        Ok(())
    }

    /// `algo.or.maxCoverage({setLabel, elementLabel, budget [, costProperty,
    /// weightProperty, edgeType]})`.
    ///
    /// The `setLabel` nodes, of total cost at most `budget`, that reach the
    /// most `elementLabel` weight: where to spend a fixed budget for the most
    /// coverage. Without `costProperty` each set costs 1, so `budget` is a
    /// count; without `weightProperty` each element weighs 1.
    pub(super) fn execute_max_coverage(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        let call = "algo.or.maxCoverage";
        let s = self.template_settings(
            call,
            &[
                "budget",
                "costProperty",
                "edgeType",
                "elementLabel",
                "setLabel",
                "weightProperty",
            ],
            "{setLabel: 'Tower', elementLabel: 'Village', budget: 3, weightProperty: 'population'}",
        )?;
        let budget = s.number("budget")?.ok_or_else(|| {
            ExecutionError::bad_argument(format!(
                "{call}: `budget` is required: the most the chosen sets may cost in total"
            ))
        })?;
        let weight_property = s.text("weightProperty")?;
        let (sets, elements, problem) = Self::cover_problem(store, call, &s)?;
        let weight = elements
            .iter()
            .map(|&e| Self::node_number(store, call, e, weight_property.as_deref(), 1.0))
            .collect::<ExecutionResult<Vec<f64>>>()?;
        let started = std::time::Instant::now();
        let stop = || check_deadline().is_err();
        let solved = t::max_coverage(&problem, &weight, budget, t::COVER_MAX_EXPANSIONS, &stop)
            .map_err(|e| Self::template_error(call, e))?;
        let ms = duration_ms(started.elapsed());
        let mut record = Record::new();
        record.bind(
            "chosen".to_string(),
            Self::ids(solved.chosen.iter().map(|&i| sets[i])),
        );
        record.bind(
            "covered".to_string(),
            Self::ids(solved.covered.iter().map(|&i| elements[i])),
        );
        record.bind(
            "cost".to_string(),
            Value::Property(PropertyValue::Float(solved.cost)),
        );
        // Choosing nothing is always within budget, so this is always feasible.
        Self::template_envelope(
            &mut record,
            solved.weight,
            true,
            solved.exact,
            solved.iterations,
            ms,
        );
        self.results.push(record);
        Ok(())
    }

    /// `algo.or.assignment({workerLabel, taskLabel [, costProperty, edgeType]})`.
    ///
    /// Pairs `workerLabel` nodes with `taskLabel` nodes, at most one task per
    /// worker and one worker per task, along the relationships between them
    /// (`edgeType` when given) and never otherwise. As many pairs as there
    /// can be, then the least total `costProperty` (on the relationship; 1
    /// without it). Exact.
    pub(super) fn execute_assignment(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        let call = "algo.or.assignment";
        let s = self.template_settings(
            call,
            &["costProperty", "edgeType", "taskLabel", "workerLabel"],
            "{workerLabel: 'Engineer', taskLabel: 'Ticket', costProperty: 'hours'}",
        )?;
        let worker_label =
            s.required_text("workerLabel", "the label of the nodes that take tasks")?;
        let task_label = s.required_text("taskLabel", "the label of the tasks")?;
        let edge_type = s.text("edgeType")?;
        let cost_property = s.text("costProperty")?;
        let workers = Self::labelled(store, call, &worker_label)?;
        let tasks = Self::labelled(store, call, &task_label)?;
        if workers.len().max(tasks.len()) > t::ASSIGNMENT_MAX_SIDE {
            return Err(Self::template_error(
                call,
                t::TemplateError::TooLarge {
                    what: "workers or tasks",
                    size: workers.len().max(tasks.len()),
                    max: t::ASSIGNMENT_MAX_SIDE,
                },
            ));
        }
        let index: HashMap<NodeId, usize> =
            tasks.iter().enumerate().map(|(i, &n)| (n, i)).collect();
        let mut cost = vec![vec![None; tasks.len()]; workers.len()];
        for (w, &worker) in workers.iter().enumerate() {
            for (edge, task) in Self::edges_to(store, worker, edge_type.as_deref(), &index) {
                let c = Self::edge_number(store, call, &edge, cost_property.as_deref(), 1.0)?;
                // Parallel relationships: the cheapest is the one that matters.
                let slot: &mut Option<f64> = &mut cost[w][task];
                *slot = Some(slot.map_or(c, |old| old.min(c)));
            }
        }
        let started = std::time::Instant::now();
        let stop = || check_deadline().is_err();
        let solved = t::assignment(workers.len(), tasks.len(), &cost, &stop)
            .map_err(|e| Self::template_error(call, e))?;
        let ms = duration_ms(started.elapsed());
        let mut record = Record::new();
        record.bind(
            "assignments".to_string(),
            Self::rows_of_maps(
                solved
                    .pairs
                    .iter()
                    .map(|&(w, task, c)| {
                        vec![
                            ("worker", PropertyValue::Integer(workers[w].as_u64() as i64)),
                            ("task", PropertyValue::Integer(tasks[task].as_u64() as i64)),
                            ("cost", PropertyValue::Float(c)),
                        ]
                    })
                    .collect(),
            ),
        );
        record.bind(
            "cost".to_string(),
            Value::Property(PropertyValue::Float(solved.cost)),
        );
        Self::template_envelope(
            &mut record,
            solved.cost,
            solved.complete,
            true,
            solved.iterations,
            ms,
        );
        self.results.push(record);
        Ok(())
    }

    /// `algo.or.minCostFlow({supplyProperty [, capacityProperty, costProperty,
    /// label, edgeType]})`.
    ///
    /// Nodes with a positive `supplyProperty` send it, nodes with a negative
    /// one receive it, along out-relationships (`edgeType` when given) each
    /// carrying at most `capacityProperty` (no limit without it) at
    /// `costProperty` per unit (1 without it). The cheapest way to meet every
    /// demand. Exact.
    pub(super) fn execute_min_cost_flow(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        let call = "algo.or.minCostFlow";
        let s = self.template_settings(
            call,
            &[
                "capacityProperty",
                "costProperty",
                "edgeType",
                "label",
                "supplyProperty",
            ],
            "{supplyProperty: 'supply', capacityProperty: 'capacity', costProperty: 'cost'}",
        )?;
        let supply_property = s.required_text(
            "supplyProperty",
            "the node property holding supply (positive) or demand (negative)",
        )?;
        let capacity_property = s.text("capacityProperty")?;
        let cost_property = s.text("costProperty")?;
        let label = s.text("label")?;
        let edge_type = s.text("edgeType")?;
        let nodes: Vec<NodeId> = match &label {
            Some(l) => Self::labelled(store, call, l)?,
            None => {
                let mut all: Vec<NodeId> = store.all_nodes().into_iter().map(|n| n.id).collect();
                all.sort_unstable();
                all
            }
        };
        let index: HashMap<NodeId, usize> =
            nodes.iter().enumerate().map(|(i, &n)| (n, i)).collect();
        let balance = nodes
            .iter()
            .map(|&n| Self::node_number(store, call, n, Some(&supply_property), 0.0))
            .collect::<ExecutionResult<Vec<f64>>>()?;
        if balance.iter().all(|b| *b == 0.0) {
            return Err(ExecutionError::bad_argument(format!(
                "{call}: no node has a non-zero `{supply_property}`, so there is nothing to move"
            )));
        }
        let mut arcs = Vec::new();
        let mut ends = Vec::new();
        for (i, &n) in nodes.iter().enumerate() {
            for (edge, j) in Self::edges_to(store, n, edge_type.as_deref(), &index) {
                arcs.push(t::Arc {
                    from: i,
                    to: j,
                    capacity: Self::edge_number(
                        store,
                        call,
                        &edge,
                        capacity_property.as_deref(),
                        f64::INFINITY,
                    )?,
                    cost: Self::edge_number(store, call, &edge, cost_property.as_deref(), 1.0)?,
                });
                ends.push((n, nodes[j]));
            }
        }
        let started = std::time::Instant::now();
        let stop = || check_deadline().is_err();
        let solved = t::min_cost_flow(nodes.len(), &arcs, &balance, &stop)
            .map_err(|e| Self::template_error(call, e))?;
        let ms = duration_ms(started.elapsed());
        let mut record = Record::new();
        let flows = solved
            .flow
            .iter()
            .zip(&arcs)
            .zip(&ends)
            .filter(|((f, _), _)| **f > 0.0)
            .map(|((&f, arc), &(a, b))| {
                vec![
                    ("source", PropertyValue::Integer(a.as_u64() as i64)),
                    ("target", PropertyValue::Integer(b.as_u64() as i64)),
                    ("flow", PropertyValue::Float(f)),
                    ("cost", PropertyValue::Float(f * arc.cost)),
                ]
            })
            .collect();
        record.bind("flows".to_string(), Self::rows_of_maps(flows));
        record.bind(
            "shipped".to_string(),
            Value::Property(PropertyValue::Float(solved.shipped)),
        );
        record.bind(
            "cost".to_string(),
            Value::Property(PropertyValue::Float(solved.cost)),
        );
        Self::template_envelope(
            &mut record,
            solved.cost,
            solved.feasible(),
            true,
            solved.iterations,
            ms,
        );
        self.results.push(record);
        Ok(())
    }
}
