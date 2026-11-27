"""Schemathesis hooks for scripts/api_fuzz.py, loaded through SCHEMATHESIS_HOOKS.

`POST /api/sparql` takes its query as a bare `application/sparql-query` body,
the SPARQL 1.1 Protocol's form. Schemathesis has no serializer for that media
type and reports the operation as "Serialization not possible" without
sending anything, so the body is sent as the plain text it is.
"""

import schemathesis

schemathesis.serializer.alias("application/sparql-query", "text/plain")
