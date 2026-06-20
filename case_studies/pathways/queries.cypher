// Biological Pathways KG — showcase queries
// Schema: Protein{uniprot_id,name} GOTerm{go_id,name,namespace} Complex{name}
//         Reaction{reaction_id,reaction_type} Pathway{name,pathway_id,organism}
// Edges:  ANNOTATED_WITH INTERACTS_WITH PARTICIPATES_IN CATALYZES IS_A COMPONENT_OF PART_OF REGULATES CHILD_OF

// @query Most-connected protein hubs | Degree centrality over the protein interaction network — expect master regulators like TP53
MATCH (p:Protein)-[:INTERACTS_WITH]-(o:Protein)
RETURN p.name AS protein, count(DISTINCT o) AS partners
ORDER BY partners DESC
LIMIT 5;

// @query Most pleiotropic proteins | Proteins participating in the largest number of distinct pathways
MATCH (p:Protein)-[:PARTICIPATES_IN]->(pw:Pathway)
RETURN p.name AS protein, count(DISTINCT pw) AS pathways
ORDER BY pathways DESC
LIMIT 5;

// @query Largest pathways | The pathways assembled from the most protein participants
MATCH (pw:Pathway)<-[:PARTICIPATES_IN]-(p:Protein)
RETURN pw.name AS pathway, count(DISTINCT p) AS proteins
ORDER BY proteins DESC
LIMIT 5;

// @query Pathway crosstalk | Pairs of pathways sharing the most proteins — where biology's wiring overlaps (a two-hop graph join)
MATCH (a:Pathway)<-[:PARTICIPATES_IN]-(p:Protein)-[:PARTICIPATES_IN]->(b:Pathway)
WHERE a.pathway_id < b.pathway_id
RETURN a.name AS pathway_a, b.name AS pathway_b, count(DISTINCT p) AS shared_proteins
ORDER BY shared_proteins DESC
LIMIT 5;

// @query Most heavily annotated proteins | Proteins with the richest Gene Ontology annotation
MATCH (p:Protein)-[:ANNOTATED_WITH]->(g:GOTerm)
RETURN p.name AS protein, count(DISTINCT g) AS go_terms
ORDER BY go_terms DESC
LIMIT 5;
