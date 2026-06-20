// Disease Surveillance KG (WHO GHO) — showcase queries
// Schema: Country{iso_code,name} Region{name,who_code} Disease{name,indicator_code}
//         DiseaseReport{year,value,id} VaccineCoverage{coverage_pct,year,antigen} HealthIndicator{value,year,name}
// Edges:  REPORTED REPORT_OF HAS_COVERAGE HAS_INDICATOR IN_REGION
// Cross-KG: Country.iso_code joins the health-systems and health-determinants graphs.

// @query Diseases under surveillance | The notifiable diseases tracked, by volume of country-year reports
MATCH (d:Disease)<-[:REPORT_OF]-(r:DiseaseReport)
RETURN d.name AS disease, count(r) AS reports
ORDER BY reports DESC
LIMIT 8;

// @query Highest reported disease burden | Which diseases dominate the case counts when summed across all countries and years
MATCH (r:DiseaseReport)-[:REPORT_OF]->(d:Disease)
RETURN d.name AS disease, sum(r.value) AS total_reported
ORDER BY total_reported DESC
LIMIT 5;

// @query Countries reporting the most | Surveillance activity by country (count of disease reports filed)
MATCH (c:Country)-[:REPORTED]->(r:DiseaseReport)
RETURN c.name AS country, count(r) AS reports
ORDER BY reports DESC
LIMIT 5;

// @query Lowest average immunization coverage | The countries with the widest immunity gaps across tracked antigens
MATCH (c:Country)-[:HAS_COVERAGE]->(v:VaccineCoverage)
RETURN c.name AS country, round(avg(v.coverage_pct)) AS avg_coverage_pct, count(v) AS records
ORDER BY avg_coverage_pct ASC
LIMIT 5;

// @query WHO regions by member countries | How the surveillance network partitions the world
MATCH (c:Country)-[:IN_REGION]->(reg:Region)
RETURN reg.name AS region, count(c) AS countries
ORDER BY countries DESC;
