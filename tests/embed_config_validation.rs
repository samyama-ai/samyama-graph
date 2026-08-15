//! An embed config that cannot work is rejected when it is set (#275, failure mode 1).
//!
//! An embedding model has exactly one output dimension, but `vector_dimension` is set by
//! hand and independently of `embedding_model`. When they disagreed, nothing said so at
//! config time: the contradiction surfaced far downstream as a `DimensionMismatch` per
//! vector at insert, discarded by callers that ignored the result — so a tenant could be
//! configured, run, call the provider thousands of times, and store nothing.

use std::collections::HashMap;

use samyama::persistence::tenant::{
    native_dimension_for_model, AutoEmbedConfig, LLMProvider,
};
use samyama::persistence::TenantManager;

fn config(model: &str, dimension: usize) -> AutoEmbedConfig {
    AutoEmbedConfig {
        provider: LLMProvider::Ollama,
        embedding_model: model.to_string(),
        api_key: None,
        api_base_url: None,
        chunk_size: 512,
        chunk_overlap: 0,
        vector_dimension: dimension,
        embedding_policies: HashMap::new(),
        embedding_property: "embedding".to_string(),
    }
}

#[test]
fn a_dimension_contradicting_the_model_is_rejected() {
    let tenants = TenantManager::new();

    // nomic-embed-text is 768-dimensional; 1536 is the value the fixtures habitually carry
    let err = tenants
        .update_embed_config("default", Some(config("nomic-embed-text", 1536)))
        .expect_err("should not accept a config that can never store a vector");

    let msg = format!("{err}");
    assert!(msg.contains("768"), "should name the model's real dimension: {msg}");
    assert!(msg.contains("1536"), "should name the configured one: {msg}");
}

#[test]
fn a_matching_dimension_is_accepted() {
    let tenants = TenantManager::new();
    tenants
        .update_embed_config("default", Some(config("nomic-embed-text", 768)))
        .expect("768 is correct for this model");
    tenants
        .update_embed_config("default", Some(config("text-embedding-3-small", 1536)))
        .expect("1536 is correct for this model");
}

#[test]
fn an_unrecognised_model_is_not_blocked() {
    // This is a convenience check, not a whitelist: self-hosted and fine-tuned models are
    // legitimate and we have no way to know their dimension.
    let tenants = TenantManager::new();
    tenants
        .update_embed_config("default", Some(config("acme/our-own-embedder", 512)))
        .expect("unknown models must still be configurable");
}

#[test]
fn a_zero_dimension_is_rejected_whatever_the_model() {
    let tenants = TenantManager::new();
    let err = tenants
        .update_embed_config("default", Some(config("acme/our-own-embedder", 0)))
        .expect_err("zero cannot be right");
    assert!(format!("{err}").contains("greater than zero"));
}

#[test]
fn model_names_are_recognised_through_tags_and_namespaces() {
    // Ollama names carry a `:tag`, and registries prefix an `org/`. Both refer to the same
    // model and must resolve to the same dimension, or the check would quietly stop
    // applying to the spellings people actually use.
    for name in [
        "nomic-embed-text",
        "nomic-embed-text:latest",
        "nomic-embed-text:v1.5",
        "library/nomic-embed-text",
        "  NOMIC-EMBED-TEXT  ",
    ] {
        assert_eq!(
            native_dimension_for_model(name),
            Some(768),
            "unrecognised spelling: {name:?}"
        );
    }
    assert_eq!(native_dimension_for_model("text-embedding-3-large"), Some(3072));
    assert_eq!(native_dimension_for_model("something-nobody-has-heard-of"), None);
}
