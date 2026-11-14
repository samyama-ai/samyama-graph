# Filtered vector strategy costs (#1970)

Supported literal node-property comparisons use existing label/property statistics
(MCV equality estimates and range histograms). AND estimates multiply under an
independence assumption. Missing statistics and unsupported predicates retain
adaptive runtime routing; EXPLAIN explicitly reports unknown selectivity.

The planner prices predicate and distance work for two implemented strategies:
pre-exact costs N predicates plus N*s distances; post-widen costs estimated search
width based on k/s and ef, times layer-0 connectivity (2m), plus fetched predicates.
These dimension-weighted work units are not calibrated microseconds. EXPLAIN shows
the strategy, selectivity, both costs and reason. The exact plan executes an exact
filtered scan; the widening plan keeps the existing bounded fallback. Predicate
checks and node-label validity are retained on both paths.

This is the initial routing portion of #1970. In-filter HNSW, graph reachable-set
estimates, operator reordering (#445), PROFILE distance/visit/widen counters,
calibration and measured matched-recall plan regret remain follow-ups. No measured
performance win is claimed. Use #1985's correlation sweep to calibrate the policy.
