// Bank Model-Risk Knowledge Graph — governance showcase queries
// Schema (synthetic):
//   Model{id,name,category,tier,status,regulatory_use,methodology,last_validated}
//   DataSource{id,name,type,system,sensitivity,refresh}  Feature{id,name,data_type,pii}
//   Assumption{id,description,category,status}  Validation{id,outcome,type,date}
//   ValidationFinding{id,title,category,severity,status,raised_date}
//   RegulatoryRequirement{id,framework,clause,title}  Control{id,name,type,frequency,status}
//   Person{id,name,role}  BusinessUnit{id,name}  Submission{id,name,regulator,frequency,next_due}
//   Decision{id,name,type}
// Edges: OWNED_BY DEVELOPED_BY BELONGS_TO MEMBER_OF DEPENDS_ON USES_FEATURE DERIVED_FROM
//   MAKES_ASSUMPTION GOVERNED_BY SATISFIES CONTROLLED_BY EVIDENCES VALIDATED_BY
//   PERFORMED_BY RAISED FEEDS USED_IN
// Every query is structure-based and returns real rows from the loaded graph.

// @query Data-source blast radius | If the Core Banking Ledger changes, which regulatory submissions are exposed — and through how many models
MATCH (ds:DataSource)<-[:DEPENDS_ON]-(m:Model)-[:FEEDS]->(s:Submission)
WHERE ds.name = "Core Banking Ledger"
RETURN s.name AS submission, count(DISTINCT m) AS models_affected
ORDER BY models_affected DESC;

// @query What feeds the CCAR stress test | Every model flowing into CCAR 2026 — directly or via an upstream risk parameter (PD/LGD → loss engine) — by category and tier
MATCH (m:Model)-[:FEEDS*1..3]->(s:Submission)
WHERE s.name = "CCAR 2026"
RETURN m.category AS model_category, m.tier AS tier, count(DISTINCT m) AS models
ORDER BY models DESC;

// @query Governance gaps on critical models | Open High-severity findings sitting on Tier-1 production models — the audit's first stop
MATCH (m:Model)-[:VALIDATED_BY]->(v:Validation)-[:RAISED]->(f:ValidationFinding)
WHERE m.tier = 1 AND m.status = "Production" AND f.severity = "High" AND f.status = "Open"
RETURN m.name AS model, f.category AS finding_type, f.title AS finding, m.methodology AS method
ORDER BY model
LIMIT 15;

// @query Regulatory coverage by framework | How many models each regulation governs across the inventory
MATCH (req:RegulatoryRequirement)<-[:GOVERNED_BY]-(m:Model)
RETURN req.framework AS framework, count(DISTINCT m) AS models_governed
ORDER BY models_governed DESC;

// @query Model explainability | For an AML model: the exact features and the source systems behind its decisions — the path an auditor can replay
MATCH (m:Model)-[:USES_FEATURE]->(f:Feature)-[:DERIVED_FROM]->(ds:DataSource)
WHERE m.category = "AML Transaction Monitoring"
RETURN m.name AS model, collect(DISTINCT f.name) AS features, collect(DISTINCT ds.name) AS source_systems
LIMIT 5;

// @query SR 11-7 audit readiness | Models governed by SR 11-7 carrying the most open / overdue validation findings
MATCH (m:Model)-[:GOVERNED_BY]->(:RegulatoryRequirement {framework: "SR 11-7"})
MATCH (m)-[:VALIDATED_BY]->(:Validation)-[:RAISED]->(f:ValidationFinding)
WHERE f.status = "Open" OR f.status = "Overdue"
RETURN m.name AS model, m.tier AS tier, count(f) AS open_findings
ORDER BY open_findings DESC
LIMIT 10;

// @query Data concentration risk | The source systems the most Tier-1 / Tier-2 models depend on — single points of failure
MATCH (ds:DataSource)<-[:DEPENDS_ON]-(m:Model)
WHERE m.tier <= 2
RETURN ds.name AS source_system, ds.sensitivity AS sensitivity, count(DISTINCT m) AS dependent_models
ORDER BY dependent_models DESC
LIMIT 8;

// @query Owner accountability | Who owns the most production models across the inventory
MATCH (p:Person)<-[:OWNED_BY]-(m:Model)
WHERE m.status = "Production"
RETURN p.name AS owner, p.role AS role, count(DISTINCT m) AS production_models
ORDER BY production_models DESC
LIMIT 10;
