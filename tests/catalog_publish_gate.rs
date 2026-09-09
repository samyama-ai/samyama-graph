//! A catalog derived from traffic must not be published unchecked (#1159).
//!
//! The half of this that decides whether the check survives contact with a real
//! KG is `realistic_graph_text_does_not_trip_the_scan`. A scan that cries wolf
//! gets switched off, and then it protects nothing — this codebase already has
//! that scar, from a link checker that produced 2 real failures and 253 false
//! ones over 3,278 URLs.

use samyama::snapshot::publish_gate::{gate, scan_text, Provenance};

fn texts(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs.iter().map(|(a, b)| (a.to_string(), b.to_string())).collect()
}

#[test]
fn an_authored_clean_catalog_is_publishable() {
    let v = gate(
        Provenance::Authored,
        &texts(&[("q1", "What genes does Metformin target?")]),
        false,
    );
    assert!(v.publishable, "{:?}", v.reasons);
    assert!(v.findings.is_empty());
}

#[test]
fn an_observed_catalog_needs_an_explicit_flag() {
    let t = texts(&[("q1", "What genes does Metformin target?")]);
    let v = gate(Provenance::Observed, &t, false);
    assert!(!v.publishable);
    assert!(v.reasons[0].contains("observed traffic"), "{:?}", v.reasons);

    let v = gate(Provenance::Observed, &t, true);
    assert!(v.publishable, "{:?}", v.reasons);
}

/// Agreeing to publish the questions is not agreeing to publish a card number
/// inside one, so the flag does not silence the scan.
#[test]
fn the_observed_flag_does_not_silence_the_pii_scan() {
    let t = texts(&[("q1", "Which orders used card 4111 1111 1111 1111 last week?")]);
    let v = gate(Provenance::Observed, &t, true);
    assert!(!v.publishable, "the flag suppressed a PII finding");
    assert_eq!(v.findings.len(), 1);
    assert_eq!(v.findings[0].kind, "card-number");
    assert!(v.reasons.iter().any(|r| r.contains("does not cover this")), "{:?}", v.reasons);
}

/// A parameter sample is a data excerpt: it exists so the build-time execution
/// gate has something to run, and on a KG holding personal data it is a real
/// value in a published file.
#[test]
fn a_parameter_sample_is_scanned_like_any_other_text() {
    let v = gate(
        Provenance::Authored,
        &texts(&[("q1.params.email", "priya.sharma@example.com")]),
        false,
    );
    assert!(!v.publishable);
    assert_eq!(v.findings[0].kind, "email");
    // The excerpt locates the value without reproducing it.
    assert!(!v.findings[0].excerpt.contains("priya.sharma"), "{:?}", v.findings[0]);
    assert!(v.findings[0].excerpt.contains('*'));
}

#[test]
fn each_pattern_is_found() {
    for (text, kind) in [
        ("contact a.b@sub.example.org for access", "email"),
        ("card 4111111111111111", "card-number"),
        ("ssn 123-45-6789", "us-ssn"),
        ("call +91 98765 43210", "phone"),
    ] {
        let f = scan_text("t", text);
        assert!(
            f.iter().any(|x| x.kind == kind),
            "{kind} not found in {text:?}, got {:?}", f.iter().map(|x| x.kind).collect::<Vec<_>>()
        );
    }
}

/// The precision test. Every string here is ordinary content from KGs we
/// actually ship, and none of it may trigger a finding.
#[test]
fn realistic_graph_text_does_not_trip_the_scan() {
    const CLEAN: &[&str] = &[
        // identifiers that are long runs of digits
        "What is the abstract of PMID 12345678?",
        "Which adverse events were reported in trial NCT00835861?",
        "Find the compound with CID 2244 and CAS 50-78-2",
        "ISBN 9780262033848 is cited by how many papers?",
        "Show gene ENSG00000141510 and its transcripts",
        // versions, dates, sizes, ports
        "Which nodes changed between 2026-09-08 and 2026-09-09?",
        "Rows where value > 1234567890123456789",
        "Which service listens on port 7687 at 192.168.1.10?",
        "Return snapshots larger than 1073741824 bytes",
        // an @ that is not an email
        "Match the handle @maintainer in the commit message",
        "Rate limit is 100 @ 60s",
        // a hyphenated code with nine digits but the wrong shape
        "Part number 12-3456789 in the bill of materials",
        // ordinary questions
        "What drugs interact with Warfarin?",
        "Which pathways contain both TP53 and MDM2?",
    ];
    let mut noise = Vec::new();
    for t in CLEAN {
        let f = scan_text("t", t);
        if !f.is_empty() {
            noise.push((*t, f.iter().map(|x| x.kind).collect::<Vec<_>>()));
        }
    }
    assert!(
        noise.is_empty(),
        "the scan fired on ordinary graph content, which is how a scan gets \
         switched off: {noise:?}"
    );
}

/// A 16-digit number that is not a card must not be reported as one. Luhn is
/// what separates them; without it every long identifier matches.
#[test]
fn a_long_number_that_fails_luhn_is_not_a_card() {
    assert!(scan_text("t", "4111111111111112").is_empty(), "Luhn is not being checked");
    assert!(!scan_text("t", "4111111111111111").is_empty(), "a valid card was missed");
}
