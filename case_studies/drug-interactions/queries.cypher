// Drug Interactions & Pharmacogenomics KG — showcase queries
// Schema: Drug{name,drugbank_id,cas_number} Gene{gene_name} SideEffect{name,meddra_id}
//         Indication{name,meddra_id} Bioactivity{target_name,pchembl_value,...} AdverseEvent{term}
// Edges:  HAS_SIDE_EFFECT INTERACTS_WITH_GENE HAS_INDICATION HAS_ADVERSE_EVENT BIOACTIVITY_TARGET

// @query Drugs with the heaviest side-effect burden | Which compounds carry the longest documented side-effect profile (SIDER)
MATCH (d:Drug)-[:HAS_SIDE_EFFECT]->(s:SideEffect)
RETURN d.name AS drug, count(DISTINCT s) AS side_effects
ORDER BY side_effects DESC
LIMIT 5;

// @query Busiest drug-target genes | Genes hit by the most drugs — the metabolic choke-points (expect CYP enzymes near the top)
MATCH (d:Drug)-[:INTERACTS_WITH_GENE]->(g:Gene)
RETURN g.gene_name AS gene, count(DISTINCT d) AS drugs
ORDER BY drugs DESC
LIMIT 5;

// @query Polypharmacy risk: drug pairs sharing the most gene targets | Co-prescribing these competes at the same target — a graph-native interaction signal
MATCH (d1:Drug)-[:INTERACTS_WITH_GENE]->(g:Gene)<-[:INTERACTS_WITH_GENE]-(d2:Drug)
WHERE d1.name < d2.name
RETURN d1.name AS drug_a, d2.name AS drug_b, count(DISTINCT g) AS shared_targets
ORDER BY shared_targets DESC
LIMIT 5;

// @query Most widespread side effects | The adverse reactions reported across the most distinct drugs
MATCH (d:Drug)-[:HAS_SIDE_EFFECT]->(s:SideEffect)
RETURN s.name AS side_effect, count(DISTINCT d) AS drugs
ORDER BY drugs DESC
LIMIT 5;

// @query Most-indicated drugs | Compounds approved/studied for the widest range of conditions
MATCH (d:Drug)-[:HAS_INDICATION]->(i:Indication)
RETURN d.name AS drug, count(DISTINCT i) AS indications
ORDER BY indications DESC
LIMIT 5;

// @query Drugs with the most reported adverse events | OpenFDA post-market signal volume by drug
MATCH (d:Drug)-[:HAS_ADVERSE_EVENT]->(a:AdverseEvent)
RETURN d.name AS drug, count(DISTINCT a) AS adverse_events
ORDER BY adverse_events DESC
LIMIT 5;
