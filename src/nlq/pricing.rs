//! What an LLM call cost, in dollars (#1732, AI-15).
//!
//! The engine held no price for any model, so the token counts it returned
//! (#1790) could not become money. Prices change, differ by provider, and are
//! zero for a local model or a subscription, so none is built in: the operator
//! lists them, per model, in `NLQ_PRICES`, as dollars per million input and
//! output tokens:
//!
//! ```text
//! NLQ_PRICES="gpt-4o=2.50/10.00, gemini-1.5-pro=1.25/5.00, llama3=0/0"
//! ```
//!
//! Keyed by model rather than by tenant, so the price follows the model a call
//! actually used, whichever tenant configured it. A call is costed only when
//! its model is listed **and** the provider reported both token counts;
//! otherwise the cost is `None`, never a guess or a zero that looks measured.

use super::client::TokenUsage;

/// Dollars per million input and output tokens for `model`, from `NLQ_PRICES`.
pub fn price_for(model: &str) -> Option<(f64, f64)> {
    price_in(&std::env::var("NLQ_PRICES").ok()?, model)
}

/// `price_for`, from a given `NLQ_PRICES` value. Entries that do not parse
/// are skipped rather than failing the call they would have priced.
pub fn price_in(list: &str, model: &str) -> Option<(f64, f64)> {
    list.split(',').find_map(|entry| {
        let (name, prices) = entry.split_once('=')?;
        if name.trim() != model {
            return None;
        }
        let (input, output) = prices.split_once('/')?;
        let input: f64 = input.trim().parse().ok()?;
        let output: f64 = output.trim().parse().ok()?;
        (input.is_finite() && output.is_finite() && input >= 0.0 && output >= 0.0).then_some((input, output))
    })
}

/// The dollar cost of `usage` at `price`, or `None` when either is unknown.
pub fn cost_at(usage: &TokenUsage, price: Option<(f64, f64)>) -> Option<f64> {
    let (input, output) = price?;
    let prompt = usage.prompt_tokens? as f64;
    let completion = usage.completion_tokens? as f64;
    Some((prompt * input + completion * output) / 1_000_000.0)
}

/// The dollar cost of a call to `model` that used `usage`.
pub fn cost_usd(usage: &TokenUsage, model: &str) -> Option<f64> {
    cost_at(usage, price_for(model))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(p: Option<u64>, c: Option<u64>) -> TokenUsage {
        TokenUsage { prompt_tokens: p, completion_tokens: c, total_tokens: None }
    }

    #[test]
    fn a_listed_model_is_priced_per_million_tokens() {
        let list = "gpt-4o=2.50/10.00, gemini-1.5-pro = 1.25 / 5.00";
        assert_eq!(price_in(list, "gpt-4o"), Some((2.5, 10.0)));
        assert_eq!(price_in(list, "gemini-1.5-pro"), Some((1.25, 5.0)));
        let cost = cost_at(&usage(Some(1_000), Some(500)), price_in(list, "gpt-4o")).unwrap();
        assert!((cost - 0.0075).abs() < 1e-12, "{cost}");
    }

    #[test]
    fn unknown_is_none_not_zero() {
        let list = "gpt-4o=2.50/10.00";
        assert_eq!(price_in(list, "gpt-4o-mini"), None, "a model not listed has no price");
        assert_eq!(cost_at(&usage(Some(10), Some(5)), None), None);
        assert_eq!(cost_at(&usage(None, Some(5)), price_in(list, "gpt-4o")), None, "no prompt count");
        assert_eq!(cost_at(&usage(Some(10), None), price_in(list, "gpt-4o")), None, "no completion count");
        // A free model is priced at zero, which is a measured zero.
        assert_eq!(cost_at(&usage(Some(10), Some(5)), price_in("llama3=0/0", "llama3")), Some(0.0));
    }

    #[test]
    fn a_malformed_or_negative_entry_prices_nothing() {
        for list in ["gpt-4o=2.5", "gpt-4o=a/b", "gpt-4o=-1/2", "gpt-4o=NaN/1", "gpt-4o"] {
            assert_eq!(price_in(list, "gpt-4o"), None, "{list}");
        }
        assert_eq!(price_in("bad, gpt-4o=1/2", "gpt-4o"), Some((1.0, 2.0)), "one bad entry spoils no other");
    }
}
