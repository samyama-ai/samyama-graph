// Health Systems KG — showcase queries
// Schema: Country{iso_code,name} EmergencyResponse{capacity_code, capacity_name, country_code, score, year}
//         `indicator_name` was never a property on this label (#1609).
// Edges:  CAPACITY_FOR  (matched undirected)
// WHO SPAR (IHR core-capacity) preparedness scores, country by country.

// @query Least-prepared countries for a health emergency | Lowest average IHR core-capacity (SPAR) score
MATCH (c:Country)-[:CAPACITY_FOR]-(e:EmergencyResponse)
RETURN c.name AS country, round(avg(e.score)) AS avg_spar, count(e) AS dimensions
ORDER BY avg_spar ASC
LIMIT 5;

// @query Best-prepared countries | Highest average IHR core-capacity score
MATCH (c:Country)-[:CAPACITY_FOR]-(e:EmergencyResponse)
RETURN c.name AS country, round(avg(e.score)) AS avg_spar
ORDER BY avg_spar DESC
LIMIT 5;

// @query Global preparedness over time | Average IHR core-capacity score by year — is the world getting more prepared?
MATCH (e:EmergencyResponse)
RETURN e.year AS year, round(avg(e.score)) AS avg_spar, count(*) AS assessments
ORDER BY year;

// @query Most comprehensively assessed countries | Countries with the most capacity assessments on record
MATCH (c:Country)-[:CAPACITY_FOR]-(e:EmergencyResponse)
RETURN c.name AS country, count(e) AS assessments
ORDER BY assessments DESC
LIMIT 5;
