// Health Determinants KG — showcase queries
// Schema: Country{iso_code,name} Region{name,code,who_code}
//         SocioeconomicIndicator/EnvironmentalFactor/WaterResource/NutritionIndicator/
//         DemographicProfile {indicator_name, indicator_code, value, year}
// Edges:  HAS_INDICATOR ENVIRONMENT_OF WATER_RESOURCE_OF NUTRITION_STATUS DEMOGRAPHIC_OF IN_REGION
// Edges matched undirected so direction never silently empties a result.

// @query Heaviest ambient air-pollution burden | Countries with the highest average environmental (air-quality) values
MATCH (c:Country)-[:ENVIRONMENT_OF]-(e:EnvironmentalFactor)
RETURN c.name AS country, round(avg(e.value)) AS avg_pollution, count(e) AS records
ORDER BY avg_pollution DESC
LIMIT 5;

// @query Richest socioeconomic profiles | Countries tracked across the most World Bank development indicators
MATCH (c:Country)-[:HAS_INDICATOR]-(s:SocioeconomicIndicator)
RETURN c.name AS country, count(s) AS indicators
ORDER BY indicators DESC
LIMIT 5;

// @query Lowest water-resource availability | Countries with the lowest average water-resource value (FAO AQUASTAT)
MATCH (c:Country)-[:WATER_RESOURCE_OF]-(w:WaterResource)
RETURN c.name AS country, round(avg(w.value)) AS avg_water, count(w) AS records
ORDER BY avg_water ASC
LIMIT 5;

// @query Most-profiled demographics | Countries with the richest demographic profiling (UNDP / WDI)
MATCH (c:Country)-[:DEMOGRAPHIC_OF]-(d:DemographicProfile)
RETURN c.name AS country, count(d) AS demographic_records
ORDER BY demographic_records DESC
LIMIT 5;

// @query World regions by member countries | How the determinants graph partitions the world
MATCH (c:Country)-[:IN_REGION]-(r:Region)
RETURN r.name AS region, count(DISTINCT c) AS countries
ORDER BY countries DESC;
