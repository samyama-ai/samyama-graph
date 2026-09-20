//! An unrecognised LLM provider is refused, not sent to OpenAI.
//!
//! Both places that read `NLQ_PROVIDER` ended `_ => LLMProvider::OpenAI`. Two
//! consequences, and the second is the one that cost something:
//!
//! - a typo — `olama`, `local`, `none` — quietly meant OpenAI;
//! - **`claudecode` was on neither list**, so an operator who set the provider
//!   to the local CLI had the prompt sent to a third party instead.
//!
//! There were three such sites, not two: `EMBED_PROVIDER` in `main.rs` had the
//! same catch-all and did not even list `azure`, so that spelling meant OpenAI.
//!
//! The enrichment path is the worse of the two HTTP ones: `/api/nlq` sends the question
//! and a schema summary — label, relationship and property *names* — while
//! `/api/enrich` sends the gap node's actual property **values**.
//!
//! Where a graph's content is sent is not a defaultable decision. The failure
//! is silent, the data is already gone by the time anyone looks, and the
//! operator's evidence that they chose a local provider is the environment
//! variable they set.

use samyama::persistence::tenant::LLMProvider;

#[test]
fn claudecode_is_recognised_rather_than_meaning_openai() {
    // The defect, exactly. This spelling appeared in neither match arm.
    assert_eq!(
        LLMProvider::parse("claudecode"),
        Ok(LLMProvider::ClaudeCode)
    );
}

#[test]
fn a_typo_is_an_error_and_not_a_third_party() {
    for typo in ["olama", "openAI2", "local", "none", "gpt4", "llama"] {
        let err = LLMProvider::parse(typo)
            .expect_err("an unknown provider must be refused, not defaulted");
        assert!(err.contains(typo), "the message must quote what was set: {err}");
        assert!(
            err.contains("ollama") && err.contains("claudecode"),
            "the message must list the accepted names: {err}"
        );
    }
}

#[test]
fn an_unset_provider_is_an_error_too() {
    // Unset is the commonest way to arrive at a default, so it is the commonest
    // way to send data somewhere nobody chose.
    let err = LLMProvider::parse("").expect_err("unset must be refused");
    assert!(err.contains("not set"), "{err}");
    assert!(err.contains("third party"), "the message must say why: {err}");
}

#[test]
fn every_accepted_name_parses_and_every_variant_has_one() {
    // The half that stops the fix being "refuse everything". A parser that
    // rejected all input would pass the two tests above.
    for name in LLMProvider::NAMES {
        assert!(
            LLMProvider::parse(name).is_ok(),
            "{name} is advertised in the error message but does not parse"
        );
    }
    // And the reverse: a variant added later without a spelling is unreachable
    // from configuration, which is how `claudecode` got lost in the first place.
    let parsed: Vec<LLMProvider> = LLMProvider::NAMES
        .iter()
        .filter_map(|n| LLMProvider::parse(n).ok())
        .collect();
    for variant in [
        LLMProvider::OpenAI,
        LLMProvider::Ollama,
        LLMProvider::Gemini,
        LLMProvider::AzureOpenAI,
        LLMProvider::Anthropic,
        LLMProvider::ClaudeCode,
        LLMProvider::Mock,
    ] {
        assert!(
            parsed.contains(&variant),
            "{variant:?} cannot be selected by any accepted name"
        );
    }
}

#[test]
fn the_error_names_the_variable_it_came_from() {
    // `EMBED_PROVIDER` has the same shape and the same old defect — and it is
    // the worse one, because embedding sends property values rather than a
    // schema summary. An error naming `NLQ_PROVIDER` sends the reader to the
    // wrong line of their config.
    let err = LLMProvider::parse_named("EMBED_PROVIDER", "olama").unwrap_err();
    assert!(err.contains("EMBED_PROVIDER"), "{err}");
    assert!(!err.contains("NLQ_PROVIDER"), "{err}");
    let unset = LLMProvider::parse_named("EMBED_PROVIDER", "").unwrap_err();
    assert!(unset.contains("EMBED_PROVIDER"), "{unset}");
}

#[test]
fn case_and_surrounding_space_do_not_change_the_answer() {
    // An env var pasted from a shell often carries whitespace. Accepting it is
    // fine; silently turning it into OpenAI was not.
    assert_eq!(LLMProvider::parse("  Ollama \n"), Ok(LLMProvider::Ollama));
    assert_eq!(LLMProvider::parse("GEMINI"), Ok(LLMProvider::Gemini));
}
