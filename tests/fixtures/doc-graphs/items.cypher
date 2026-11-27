// The optimization README's modelling examples (OPT-07, #2264): three items
// with whole-number unit costs, the way a graph is usually written.
// Used by doc_check (DX-04); a block opts in with ```cypher graph=items.
CREATE (:Item {name: 'bolt', cost: 1}), (:Item {name: 'nut', cost: 2}), (:Item {name: 'gear', cost: 3})
