//! Declarative contracts and bounded repair for structured agent output.

use super::{AgentError, AgentResult};
use crate::graph::GraphStore;
use crate::nlq::client::{NLQClient, ResponseSchema, TokenUsage};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

/// An input/output example for a signature. Demo promotion remains explicit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Demonstration {
    pub input: Value,
    pub output: Value,
}

/// A validation rule applied after decoding and before returning output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutputInvariant {
    ValidCypherReadOnly {
        field: String,
    },
    ValidCypherParseable {
        field: String,
    },
    RegisteredToolNamesOnly {
        field: String,
        allowed: Vec<String>,
    },
    /// Each object in the named array has a JSON object encoded in this string field.
    ValidJsonObjectsInArray {
        field: String,
        string_field: String,
    },
    NonEmptyProvenanceCitations {
        field: String,
    },
}

/// The declarative contract for one model operation.
#[derive(Clone, Debug)]
pub struct SignatureSpec {
    pub id: &'static str,
    pub instruction: String,
    pub input_schema: Value,
    pub output_schema: Value,
    pub demos: Vec<Demonstration>,
    pub invariants: Vec<OutputInvariant>,
}

/// Structured diagnostics included in the next repair attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairFeedback {
    pub attempt: usize,
    pub error_code: String,
    pub span: Option<String>,
    pub cause: String,
    pub suggested_repair: String,
}

/// The validated output and aggregate model-call measurements.
#[derive(Clone, Debug)]
pub struct SignatureOutcome<O> {
    pub output: O,
    pub attempts_to_success: usize,
    /// Total across attempts, or `None` if any provider call omitted usage.
    pub total_tokens: Option<u64>,
    /// Per-direction token counts, preserved for consumers such as NLQ billing.
    pub usage: TokenUsage,
    pub latency_ms: u64,
}

/// Runs a signature with a deterministic, bounded output-repair loop.
#[derive(Clone, Copy, Debug)]
pub struct SignatureRunner {
    max_repairs: usize,
}

impl Default for SignatureRunner {
    fn default() -> Self {
        Self { max_repairs: 2 }
    }
}

impl SignatureRunner {
    pub fn new(max_repairs: usize) -> Self {
        Self { max_repairs }
    }

    pub fn max_repairs(&self) -> usize {
        self.max_repairs
    }

    /// Invoke a signature and return only output that passes its schema,
    /// deserialization, and invariants. `store` is reserved for invariants
    /// that require graph context; the initial invariant set is self-contained.
    pub async fn invoke<I, O>(
        &self,
        client: &NLQClient,
        _store: &GraphStore,
        signature: &SignatureSpec,
        input: &I,
    ) -> AgentResult<SignatureOutcome<O>>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        self.invoke_without_store(client, signature, input).await
    }

    /// Invoke a signature that has no graph-backed invariant. This keeps
    /// callers such as NLQ and enrichment from constructing a dummy store.
    pub async fn invoke_without_store<I, O>(
        &self,
        client: &NLQClient,
        signature: &SignatureSpec,
        input: &I,
    ) -> AgentResult<SignatureOutcome<O>>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let input = serde_json::to_value(input)
            .map_err(|e| AgentError::ExecutionError(format!("signature input is not JSON: {e}")))?;
        validate_schema(&input, &signature.input_schema, "input")
            .map_err(|e| AgentError::ConfigError(format!("invalid signature input: {e}")))?;

        let mut prompt = build_prompt(signature, &input);
        let response_schema = ResponseSchema {
            name: schema_name(signature.id),
            schema: signature.output_schema.clone(),
        };
        let mut tokens = Some(0u64);
        let mut prompt_tokens = Some(0u64);
        let mut completion_tokens = Some(0u64);
        let mut latency = Duration::ZERO;
        let max_attempts = self.max_repairs.saturating_add(1);

        for attempt in 1..=max_attempts {
            let completion = client
                .complete_structured(&prompt, Some(&response_schema))
                .await
                .map_err(|e| AgentError::LLMError(e.to_string()))?;
            tokens = match (tokens, completion.usage.total_tokens) {
                (Some(total), Some(call_tokens)) => Some(total.saturating_add(call_tokens)),
                _ => None,
            };
            prompt_tokens = add_reported(prompt_tokens, completion.usage.prompt_tokens);
            completion_tokens = add_reported(completion_tokens, completion.usage.completion_tokens);
            latency = latency.saturating_add(completion.latency);

            let validation =
                parse_and_validate::<O>(&completion.text, signature).map_err(|mut feedback| {
                    feedback.attempt = attempt;
                    feedback
                });
            match validation {
                Ok(output) => {
                    return Ok(SignatureOutcome {
                        output,
                        attempts_to_success: attempt,
                        total_tokens: tokens,
                        usage: TokenUsage {
                            prompt_tokens,
                            completion_tokens,
                            total_tokens: tokens,
                        },
                        latency_ms: latency.as_millis().min(u64::MAX as u128) as u64,
                    });
                }
                Err(feedback) if attempt < max_attempts => {
                    prompt = format!(
                        "{prompt}\n\nRepair feedback (attempt {}):\n{}",
                        feedback.attempt,
                        serde_json::to_string(&feedback).unwrap_or_default(),
                    );
                }
                Err(feedback) => {
                    return Err(AgentError::ExecutionError(format!(
                        "signature {} failed after {attempt} attempt(s): {}",
                        signature.id, feedback.cause
                    )));
                }
            }
        }

        Err(AgentError::ExecutionError(format!(
            "signature {} exhausted its attempt budget",
            signature.id
        )))
    }
}

fn add_reported(total: Option<u64>, next: Option<u64>) -> Option<u64> {
    match (total, next) {
        (Some(total), Some(next)) => Some(total.saturating_add(next)),
        _ => None,
    }
}

fn schema_name(id: &str) -> String {
    let name: String = id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if name.is_empty() {
        "signature".into()
    } else {
        name
    }
}

fn build_prompt(signature: &SignatureSpec, input: &Value) -> String {
    let demos = signature
        .demos
        .iter()
        .map(|demo| serde_json::json!({ "input": demo.input, "output": demo.output }).to_string())
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "Signature: {}\nInstruction: {}\nInput schema: {}\nOutput schema: {}\nExamples:\n{}\nInput: {}\nReturn only JSON matching the output schema.",
        signature.id,
        signature.instruction,
        signature.input_schema,
        signature.output_schema,
        if demos.is_empty() { "(none)" } else { &demos },
        input,
    )
}

fn parse_and_validate<O: DeserializeOwned>(
    text: &str,
    signature: &SignatureSpec,
) -> Result<O, RepairFeedback> {
    let value: Value = serde_json::from_str(text).map_err(|error| RepairFeedback {
        attempt: 0,
        error_code: "invalid_json".into(),
        span: None,
        cause: error.to_string(),
        suggested_repair: "Return one complete JSON value without markdown fences or commentary."
            .into(),
    })?;
    validate_schema(&value, &signature.output_schema, "output").map_err(|cause| {
        RepairFeedback {
            attempt: 0,
            error_code: "schema_violation".into(),
            span: None,
            cause,
            suggested_repair: "Make the output satisfy every required field and JSON Schema type."
                .into(),
        }
    })?;
    for invariant in &signature.invariants {
        validate_invariant(&value, invariant).map_err(|cause| RepairFeedback {
            attempt: 0,
            error_code: "invariant_violation".into(),
            span: None,
            cause,
            suggested_repair: "Correct the output to satisfy the declared invariant.".into(),
        })?;
    }
    serde_json::from_value(value).map_err(|error| RepairFeedback {
        attempt: 0,
        error_code: "deserialization_error".into(),
        span: None,
        cause: error.to_string(),
        suggested_repair: "Return values that deserialize to the requested output type.".into(),
    })
}

fn validate_invariant(value: &Value, invariant: &OutputInvariant) -> Result<(), String> {
    match invariant {
        OutputInvariant::ValidCypherParseable { field }
        | OutputInvariant::ValidCypherReadOnly { field } => {
            let query = value
                .get(field)
                .and_then(Value::as_str)
                .ok_or_else(|| format!("field `{field}` must be a Cypher string"))?;
            let parsed = crate::query::parse_query(query)
                .map_err(|e| format!("field `{field}` is not parseable Cypher: {e}"))?;
            if matches!(invariant, OutputInvariant::ValidCypherReadOnly { .. }) && parsed.is_write()
            {
                return Err(format!("field `{field}` contains write operations"));
            }
        }
        OutputInvariant::RegisteredToolNamesOnly { field, allowed } => {
            let calls = value
                .get(field)
                .and_then(Value::as_array)
                .ok_or_else(|| format!("field `{field}` must be an array of tool calls"))?;
            for (index, call) in calls.iter().enumerate() {
                let tool = call
                    .get("tool")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("{field}[{index}].tool must be a string"))?;
                if !allowed.iter().any(|registered| registered == tool) {
                    return Err(format!("tool `{tool}` is not registered"));
                }
            }
        }
        OutputInvariant::ValidJsonObjectsInArray {
            field,
            string_field,
        } => {
            let items = value
                .get(field)
                .and_then(Value::as_array)
                .ok_or_else(|| format!("field `{field}` must be an array"))?;
            for (index, item) in items.iter().enumerate() {
                let encoded = item
                    .get(string_field)
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        format!("{field}[{index}].{string_field} must be a JSON string")
                    })?;
                let parsed: Value = serde_json::from_str(encoded)
                    .map_err(|e| format!("{field}[{index}].{string_field} is invalid JSON: {e}"))?;
                if !parsed.is_object() {
                    return Err(format!(
                        "{field}[{index}].{string_field} must encode a JSON object"
                    ));
                }
            }
        }
        OutputInvariant::NonEmptyProvenanceCitations { field } => {
            if !value
                .get(field)
                .and_then(Value::as_array)
                .is_some_and(|items| !items.is_empty())
            {
                return Err(format!(
                    "field `{field}` must contain at least one provenance citation"
                ));
            }
        }
    }
    Ok(())
}

/// Validate the JSON Schema subset used by signatures. Unsupported keywords
/// are rejected rather than silently treated as satisfied.
fn validate_schema(value: &Value, schema: &Value, path: &str) -> Result<(), String> {
    let Some(object) = schema.as_object() else {
        return Err(format!("schema at {path} must be an object"));
    };
    for key in object.keys() {
        if !matches!(
            key.as_str(),
            "type"
                | "properties"
                | "required"
                | "items"
                | "enum"
                | "const"
                | "additionalProperties"
                | "minLength"
                | "minItems"
                | "minimum"
                | "maximum"
        ) {
            return Err(format!("unsupported JSON Schema keyword `{key}` at {path}"));
        }
    }
    if let Some(expected) = object.get("type").and_then(Value::as_str) {
        let matches = match expected {
            "object" => value.is_object(),
            "array" => value.is_array(),
            "string" => value.is_string(),
            "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
            "number" => value.is_number(),
            "boolean" => value.is_boolean(),
            "null" => value.is_null(),
            _ => return Err(format!("unsupported schema type `{expected}` at {path}")),
        };
        if !matches {
            return Err(format!("{path} must be {expected}"));
        }
    }
    if let Some(allowed) = object.get("enum").and_then(Value::as_array) {
        if !allowed.contains(value) {
            return Err(format!("{path} is not an allowed enum value"));
        }
    }
    if let Some(expected) = object.get("const") {
        if value != expected {
            return Err(format!("{path} does not match its constant value"));
        }
    }
    if let Some(minimum) = object.get("minimum").and_then(Value::as_f64) {
        if value.as_f64().is_some_and(|actual| actual < minimum) {
            return Err(format!("{path} is below minimum {minimum}"));
        }
    }
    if let Some(maximum) = object.get("maximum").and_then(Value::as_f64) {
        if value.as_f64().is_some_and(|actual| actual > maximum) {
            return Err(format!("{path} is above maximum {maximum}"));
        }
    }
    if let Some(minimum) = object.get("minLength").and_then(Value::as_u64) {
        if value
            .as_str()
            .is_some_and(|actual| actual.chars().count() < minimum as usize)
        {
            return Err(format!("{path} is shorter than minLength {minimum}"));
        }
    }
    if let Some(minimum) = object.get("minItems").and_then(Value::as_u64) {
        if value
            .as_array()
            .is_some_and(|actual| actual.len() < minimum as usize)
        {
            return Err(format!("{path} has fewer than minItems {minimum}"));
        }
    }
    let has_object_rules = object.contains_key("properties")
        || object.contains_key("required")
        || object.contains_key("additionalProperties");
    if has_object_rules {
        let Some(fields) = value.as_object() else {
            return Err(format!("{path} must be an object"));
        };
        if let Some(required) = object.get("required").and_then(Value::as_array) {
            for name in required.iter().filter_map(Value::as_str) {
                if !fields.contains_key(name) {
                    return Err(format!("{path}.{name} is required"));
                }
            }
        }
        let properties = object.get("properties").and_then(Value::as_object);
        if let Some(properties) = properties {
            for (name, child_schema) in properties {
                if let Some(child) = fields.get(name) {
                    validate_schema(child, child_schema, &format!("{path}.{name}"))?;
                }
            }
        }
        if object.get("additionalProperties") == Some(&Value::Bool(false)) {
            for name in fields.keys() {
                if !properties.is_some_and(|properties| properties.contains_key(name)) {
                    return Err(format!("{path}.{name} is not allowed"));
                }
            }
        }
    }
    if let (Some(items), Some(array)) = (object.get("items"), value.as_array()) {
        for (index, item) in array.iter().enumerate() {
            validate_schema(item, items, &format!("{path}[{index}]"))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> SignatureSpec {
        SignatureSpec {
            id: "smriti.tool_planner.v1",
            instruction: "Plan tool calls".into(),
            input_schema: json!({"type":"object", "required":["question"], "properties":{"question":{"type":"string"}}}),
            output_schema: json!({"type":"object", "required":["calls"], "properties":{"calls":{"type":"array", "items":{"type":"object", "required":["tool"], "properties":{"tool":{"type":"string"}}}}}}),
            demos: vec![],
            invariants: vec![OutputInvariant::RegisteredToolNamesOnly {
                field: "calls".into(),
                allowed: vec!["search".into()],
            }],
        }
    }

    #[test]
    fn validates_schema_types_required_and_additional_properties() {
        let schema = json!({"type":"object", "required":["name"], "properties":{"name":{"type":"string"}}, "additionalProperties":false});
        assert!(validate_schema(&json!({"name":"Ada"}), &schema, "output").is_ok());
        assert!(validate_schema(&json!({}), &schema, "output")
            .unwrap_err()
            .contains("required"));
        assert!(validate_schema(&json!({"name":2}), &schema, "output")
            .unwrap_err()
            .contains("must be string"));
        assert!(
            validate_schema(&json!({"name":"Ada", "extra":true}), &schema, "output")
                .unwrap_err()
                .contains("not allowed")
        );
    }

    #[test]
    fn refuses_schema_keywords_it_does_not_validate() {
        assert!(validate_schema(
            &json!("x"),
            &json!({"type":"string", "pattern":"^x$"}),
            "output"
        )
        .unwrap_err()
        .contains("unsupported"));
        assert!(
            validate_schema(&json!({}), &json!({"required":["x"]}), "output")
                .unwrap_err()
                .contains("required")
        );
    }

    #[test]
    fn tool_allowlist_and_read_only_cypher_invariants_fail_closed() {
        let value = json!({"calls":[{"tool":"shell"}]});
        assert!(validate_invariant(
            &value,
            &OutputInvariant::RegisteredToolNamesOnly {
                field: "calls".into(),
                allowed: vec!["search".into()]
            }
        )
        .unwrap_err()
        .contains("not registered"));
        let write = json!({"query":"MATCH (n) DELETE n"});
        assert!(validate_invariant(
            &write,
            &OutputInvariant::ValidCypherReadOnly {
                field: "query".into()
            }
        )
        .is_err());
        let read = json!({"query":"MATCH (n) RETURN n"});
        assert!(validate_invariant(
            &read,
            &OutputInvariant::ValidCypherReadOnly {
                field: "query".into()
            }
        )
        .is_ok());
    }

    #[test]
    fn tool_arguments_must_be_encoded_json_objects() {
        let rule = OutputInvariant::ValidJsonObjectsInArray {
            field: "calls".into(),
            string_field: "args_json".into(),
        };
        assert!(validate_invariant(
            &json!({"calls":[{"args_json":"{\"query\":\"MATCH (n) RETURN n\"}"}]}),
            &rule
        )
        .is_ok());
        assert!(
            validate_invariant(&json!({"calls":[{"args_json":"not json"}]}), &rule)
                .unwrap_err()
                .contains("invalid JSON")
        );
        assert!(
            validate_invariant(&json!({"calls":[{"args_json":"[]"}]}), &rule)
                .unwrap_err()
                .contains("must encode a JSON object")
        );
    }

    #[test]
    fn schema_names_are_safe_and_nonempty() {
        assert_eq!(
            schema_name("smriti.tool-planner.v1"),
            "smriti_tool_planner_v1"
        );
        assert_eq!(schema_name(""), "signature");
    }

    #[test]
    fn parses_output_and_returns_structured_repair_diagnostics() {
        let s = spec();
        let failure = parse_and_validate::<Value>("not json", &s).unwrap_err();
        assert_eq!(failure.error_code, "invalid_json");
        let failure =
            parse_and_validate::<Value>(r#"{"calls":[{"tool":"shell"}]}"#, &s).unwrap_err();
        assert_eq!(failure.error_code, "invariant_violation");
    }

    #[test]
    fn output_schema_rejects_invalid_nested_tool_plan_shape() {
        let failure =
            parse_and_validate::<Value>(r#"{"calls":[{"tool":42}]}"#, &spec()).unwrap_err();
        assert_eq!(failure.error_code, "schema_violation");
    }

    #[tokio::test]
    async fn invoke_repairs_invalid_json_once_and_returns_aggregate_telemetry() {
        use crate::nlq::test_http::MockHttp;
        use crate::persistence::tenant::{LLMProvider, NLQConfig};
        #[derive(Debug, Deserialize, PartialEq)]
        struct Plan {
            calls: Vec<Call>,
        }
        #[derive(Debug, Deserialize, PartialEq)]
        struct Call {
            tool: String,
        }
        let server = MockHttp::serve(vec![
            (200, json!({"response":"not json", "prompt_eval_count":5, "eval_count":2}).to_string()),
            (
                200,
                json!({"response":r#"{"calls":[{"tool":"search"}]}"#, "prompt_eval_count":5, "eval_count":2}).to_string(),
            ),
        ]);
        let client = NLQClient::new(&NLQConfig {
            enabled: true,
            provider: LLMProvider::Ollama,
            model: "test".into(),
            api_key: None,
            api_base_url: Some(server.base_url.clone()),
            system_prompt: None,
        })
        .unwrap();
        let result = SignatureRunner::default()
            .invoke::<_, Plan>(
                &client,
                &GraphStore::new(),
                &spec(),
                &json!({"question":"find"}),
            )
            .await
            .unwrap();
        assert_eq!(result.attempts_to_success, 2);
        assert_eq!(result.total_tokens, Some(14));
        assert_eq!(result.output.calls[0].tool, "search");
        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].json()["format"].is_object());
        assert!(requests[1].json()["prompt"]
            .as_str()
            .unwrap()
            .contains("invalid_json"));
    }

    #[tokio::test]
    async fn invoke_stops_at_repair_budget_and_fails_closed() {
        use crate::nlq::test_http::MockHttp;
        use crate::persistence::tenant::{LLMProvider, NLQConfig};
        let server = MockHttp::serve(vec![(200, json!({"response":"not json"}).to_string()); 2]);
        let client = NLQClient::new(&NLQConfig {
            enabled: true,
            provider: LLMProvider::Ollama,
            model: "test".into(),
            api_key: None,
            api_base_url: Some(server.base_url.clone()),
            system_prompt: None,
        })
        .unwrap();
        let error = SignatureRunner::new(1)
            .invoke::<_, Value>(
                &client,
                &GraphStore::new(),
                &spec(),
                &json!({"question":"find"}),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("failed after 2 attempt(s)"));
        assert_eq!(server.requests().len(), 2);
    }
}
