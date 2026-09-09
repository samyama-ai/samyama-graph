//! What a catalog may not carry into a published artifact (#1159).
//!
//! A catalog derived from observed traffic is user text. Publishing it
//! publishes what people asked, including the entity names they asked about,
//! which in several of our KGs are the sensitive part. Templates make this
//! worse in one specific way: a parameter's `sample` value exists so the
//! build-time execution gate has something to run, and on a KG holding personal
//! or customer data that sample is a real value embedded in a published file.
//!
//! ## TRUST-10 has no scan to extend
//!
//! The issue asks that "the existing PII scan" cover the catalog. There is no
//! existing scan: spec row TRUST-10 reads "policy by construction" for H1 and
//! lists "automated scan in release CI" as the *next* horizon. So this is the
//! first one, and it is written narrowly on purpose.
//!
//! ## Precision over recall, deliberately
//!
//! A scan that cries wolf gets switched off, and then it protects nothing. This
//! codebase has the scar: a link checker over 3,278 URLs produced 2 real
//! failures and 253 false ones. So every pattern here is one that is hard to
//! trigger by accident:
//!
//! * email — requires a local part, `@`, a dotted domain, and a plausible TLD
//! * card number — 13-19 digits **that pass Luhn**, so an arbitrary long number
//!   does not match
//! * US SSN — the `NNN-NN-NNNN` form with separators, not any nine digits
//! * phone — an explicit `+` country prefix, not any run of digits
//!
//! Deliberately *not* matched: names, addresses, dates of birth, free text that
//! looks personal. Those need judgement, and a regex claiming to find them
//! would report mostly noise while implying the file had been checked.

/// Where a catalog's questions came from. Required in the file: a reader must
/// not have to infer it, and the safe default cannot be the silent one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provenance {
    /// Written by hand. The questions are ours.
    Authored,
    /// Derived from traffic an instance served. The questions are someone's.
    Observed,
}

/// One thing the scan found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    /// Catalog entry id, or `"<header>"`.
    pub where_: String,
    pub kind: &'static str,
    /// The matched text, truncated. Enough to locate, not enough to leak the
    /// whole value into a log that may itself be published.
    pub excerpt: String,
}

fn redact(s: &str) -> String {
    let n = s.chars().count();
    if n <= 4 {
        return "*".repeat(n);
    }
    let head: String = s.chars().take(2).collect();
    let tail: String = s.chars().skip(n - 2).collect();
    format!("{head}{}{tail}", "*".repeat(n - 4))
}

fn luhn_ok(digits: &[u8]) -> bool {
    let mut sum = 0u32;
    let mut double = false;
    for d in digits.iter().rev() {
        let mut v = u32::from(*d);
        if double {
            v *= 2;
            if v > 9 {
                v -= 9;
            }
        }
        sum += v;
        double = !double;
    }
    sum % 10 == 0
}

/// Scan one string. Returns every pattern hit.
pub fn scan_text(where_: &str, text: &str) -> Vec<Finding> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();

    // email
    for (i, _) in text.match_indices('@') {
        let start = text[..i].rfind(|c: char| c.is_whitespace() || c == '\'' || c == '"' || c == '(')
            .map(|p| p + 1).unwrap_or(0);
        let end = text[i..].find(|c: char| c.is_whitespace() || c == '\'' || c == '"' || c == ')')
            .map(|p| i + p).unwrap_or(text.len());
        let cand = &text[start..end];
        let (local, domain) = match cand.split_once('@') {
            Some(x) => x,
            None => continue,
        };
        let tld_ok = domain.rsplit('.').next().map(|t| t.len() >= 2 && t.chars().all(|c| c.is_ascii_alphabetic())).unwrap_or(false);
        if !local.is_empty()
            && local.chars().all(|c| c.is_ascii_alphanumeric() || "._%+-".contains(c))
            && domain.contains('.')
            && tld_ok
            && domain.chars().all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
        {
            out.push(Finding { where_: where_.into(), kind: "email", excerpt: redact(cand) });
        }
    }

    // card number: 13-19 digits, separators allowed, must pass Luhn
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i].is_ascii_digit() {
            let start = i;
            let mut digits: Vec<u8> = Vec::new();
            let mut j = i;
            while j < bytes.len() && (bytes[j].is_ascii_digit() || bytes[j] == b'-' || bytes[j] == b' ') {
                if bytes[j].is_ascii_digit() {
                    digits.push(bytes[j] - b'0');
                }
                j += 1;
            }
            // Trim a trailing separator run.
            let raw = text[start..j].trim_end_matches([' ', '-']);

            if (13..=19).contains(&digits.len()) && luhn_ok(&digits) {
                out.push(Finding { where_: where_.into(), kind: "card-number", excerpt: redact(raw) });
            }
            // US SSN, with separators only.
            if digits.len() == 9 && raw.len() == 11 && raw.as_bytes()[3] == b'-' && raw.as_bytes()[6] == b'-' {
                out.push(Finding { where_: where_.into(), kind: "us-ssn", excerpt: redact(raw) });
            }
            i = j;
        } else if bytes[i] == b'+' {
            // phone with an explicit country prefix
            let start = i;
            let mut j = i + 1;
            let mut count = 0usize;
            while j < bytes.len() && (bytes[j].is_ascii_digit() || bytes[j] == b'-' || bytes[j] == b' ') {
                if bytes[j].is_ascii_digit() {
                    count += 1;
                }
                j += 1;
            }
            if (10..=15).contains(&count) {
                let raw = text[start..j].trim_end();
                out.push(Finding { where_: where_.into(), kind: "phone", excerpt: redact(raw) });
            }
            i = j.max(i + 1);
        } else {
            i += 1;
        }
    }
    out
}

/// Whether a catalog may be published, and why not.
#[derive(Debug, Clone)]
pub struct GateVerdict {
    pub publishable: bool,
    pub reasons: Vec<String>,
    pub findings: Vec<Finding>,
}

/// Decide whether a catalog may be published.
///
/// `allow_observed` is the explicit flag requirement 1 asks for. It does not
/// silence the PII scan: a sign-off that the questions may be published is not
/// a sign-off that a card number in one of them may be.
pub fn gate(
    provenance: Provenance,
    texts: &[(String, String)],
    allow_observed: bool,
) -> GateVerdict {
    let mut reasons = Vec::new();
    let mut findings = Vec::new();

    for (where_, text) in texts {
        findings.extend(scan_text(where_, text));
    }

    if provenance == Provenance::Observed && !allow_observed {
        reasons.push(
            "the catalog is derived from observed traffic, so its questions are \
             user text. Publishing needs --allow-observed and a recorded sign-off."
                .to_string(),
        );
    }
    if !findings.is_empty() {
        let mut kinds: Vec<&str> = findings.iter().map(|f| f.kind).collect();
        kinds.sort_unstable();
        kinds.dedup();
        reasons.push(format!(
            "the scan found {} value(s) matching {}. --allow-observed does not \
             cover this: agreeing to publish the questions is not agreeing to \
             publish a card number inside one.",
            findings.len(),
            kinds.join(", ")
        ));
    }

    GateVerdict { publishable: reasons.is_empty(), reasons, findings }
}
