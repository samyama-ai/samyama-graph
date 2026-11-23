// A few nodes in the shape of the README's PubMed + ClinicalTrials.gov demo
// (the real graph is 74M nodes). Enough for the cancer-trials query to return a row.
CREATE (m:MeSHTerm {name: 'Neoplasms'}),
       (a1:Article {pmid: 1}), (a2:Article {pmid: 2}),
       (t1:ClinicalTrial {nct_id: 'NCT00000001'}), (t2:ClinicalTrial {nct_id: 'NCT00000002'}),
       (i1:Intervention {name: 'Pembrolizumab'}), (i2:Intervention {name: 'Cisplatin'}),
       (a1)-[:ANNOTATED_WITH]->(m), (a2)-[:ANNOTATED_WITH]->(m),
       (a1)-[:REFERENCED_IN]->(t1), (a2)-[:REFERENCED_IN]->(t2),
       (t1)-[:TESTS]->(i1), (t2)-[:TESTS]->(i1), (t2)-[:TESTS]->(i2)
