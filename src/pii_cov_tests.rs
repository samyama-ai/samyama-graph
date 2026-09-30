//! Detectors, checksums, the snapshot scan and waiver triage.

use super::*;
use std::io::{Cursor, Write};

const CARD: &str = "4111111111111111";
const AADHAAR: &str = "234123412346";
const IBAN: &str = "GB82WEST12345698765432";

#[test]
fn redact_keeps_two_characters_each_side() {
    assert_eq!(redact(""), "");
    assert_eq!(redact("abcd"), "****");
    assert_eq!(redact("abcde"), "ab*de");
    assert_eq!(
        redact("alice@example.com"),
        format!("al{}om", "*".repeat(13))
    );
    // Counted in characters, not bytes.
    assert_eq!(redact("ééééé"), "éé*éé");
}

#[test]
fn luhn_accepts_valid_and_rejects_short_or_wrong() {
    let d = |s: &str| s.bytes().map(|b| b - b'0').collect::<Vec<u8>>();
    assert!(luhn_ok(&d(CARD)));
    // Doubled digits above 9 exercise the subtract-nine step.
    assert!(luhn_ok(&d("5555555555554444")));
    assert!(!luhn_ok(&d("5555555555554445")));
    assert!(!luhn_ok(&d("4111111111111112")));
    assert!(!luhn_ok(&d("00000000000")), "under 12 digits never passes");
    assert!(luhn_ok(&d("000000000000")));
}

#[test]
fn verhoeff_needs_twelve_decimal_digits_and_a_valid_check() {
    let d = |s: &str| s.bytes().map(|b| b - b'0').collect::<Vec<u8>>();
    assert!(verhoeff_ok(&d(AADHAAR)));
    assert!(!verhoeff_ok(&d("234123412347")));
    assert!(!verhoeff_ok(&d("23412341234")));
    let mut bad = d(AADHAAR);
    bad[3] = 12;
    assert!(!verhoeff_ok(&bad), "a non-decimal digit is refused");
}

#[test]
fn digits_if_allows_spaces_and_dashes_only() {
    assert_eq!(digits_if("12 3-4", 4), Some(vec![1, 2, 3, 4]));
    assert_eq!(digits_if("1234", 5), None);
    assert_eq!(digits_if("12a4", 4), None);
}

#[test]
fn email_shapes() {
    assert!(is_email("alice.b+tag@example.co.uk"));
    for bad in [
        "noat",
        "@example.com",
        "a@b@example.com",
        "a@nodot",
        "a@.com",
        "a@x.c",
        "a@x.c0m",
        "a!b@x.com",
        "a@x_y.com",
    ] {
        assert!(!is_email(bad), "{bad}");
    }
    assert!(!is_email(&format!("{}@x.com", "a".repeat(65))));
}

#[test]
fn payment_card_needs_prefix_length_and_luhn() {
    assert!(is_payment_card(CARD));
    assert!(is_payment_card("4111 1111 1111 1111"));
    assert!(!is_payment_card("4111111111111112"));
    // Passes Luhn but starts with 0.
    assert!(!is_payment_card("0000000000000000"));
    assert!(!is_payment_card("41111111111"));
}

#[test]
fn aadhaar_needs_leading_digit_two_or_more() {
    assert!(is_aadhaar(AADHAAR));
    assert!(is_aadhaar("2341 2341 2346"));
    assert!(
        !is_aadhaar("123412341234"),
        "passes Verhoeff but starts with 1"
    );
    assert!(!is_aadhaar("234123412347"));
    assert!(!is_aadhaar("2341234123"));
}

#[test]
fn pan_shape() {
    assert!(is_pan("ABCDE1234F"));
    for bad in ["abcde1234f", "ABCD11234F", "ABCDE12345", "ABCDE1234"] {
        assert!(!is_pan(bad), "{bad}");
    }
}

#[test]
fn ssn_shape_and_never_issued_ranges() {
    assert!(is_ssn("123-45-6789"));
    for bad in [
        "123456789",
        "12-345-6789",
        "12a-45-6789",
        "000-45-6789",
        "666-45-6789",
        "900-45-6789",
        "123-00-6789",
        "123-45-0000",
    ] {
        assert!(!is_ssn(bad), "{bad}");
    }
}

#[test]
fn phone_is_international_form_only() {
    assert!(is_phone("+1 (415) 555-2671"));
    assert!(is_phone("  +441632960961 "));
    for bad in [
        "4155552671",
        "+1234567",
        "+1234567890123456",
        "+1 415x5552671",
        "+",
    ] {
        assert!(!is_phone(bad), "{bad}");
    }
}

#[test]
fn iban_country_length_and_checksum() {
    assert!(is_iban(IBAN));
    assert!(is_iban("GB82 WEST 1234 5698 7654 32"));
    for bad in [
        "GB82WEST12345698765433", // checksum
        "ZZ82WEST12345698765432", // not an IBAN country
        "GB82WEST1234569876543",  // wrong length for GB
        "gb82WEST12345698765432", // lowercase country
        "GBX2WEST12345698765432", // check digits not digits
        "GB82WEST1234569876543!", // non-alphanumeric
        "GB82WEST",               // too short
    ] {
        assert!(!is_iban(bad), "{bad}");
    }
    assert!(!is_iban(&format!("GB82{}", "1".repeat(40))), "too long");
}

#[test]
fn classify_whole_values() {
    assert_eq!(classify("  alice@example.com  "), Some("email"));
    assert_eq!(classify(CARD), Some("payment_card"));
    assert_eq!(classify("123-45-6789"), Some("ssn"));
    assert_eq!(classify(AADHAAR), Some("aadhaar"));
    assert_eq!(classify("ABCDE1234F"), Some("pan"));
    assert_eq!(classify(IBAN), Some("iban"));
    assert_eq!(classify("+14155552671"), Some("phone"));
    assert_eq!(classify("   "), None);
    assert_eq!(classify(&"a".repeat(4097)), None);
    assert_eq!(
        classify("1.0.0.0"),
        None,
        "version strings are not IPs, and IPs are not scanned"
    );
}

#[test]
fn classify_free_text_uses_only_the_improbable_shapes() {
    assert_eq!(classify("contact alice@example.com today"), Some("email"));
    assert_eq!(
        classify("paid with 4111111111111111."),
        Some("payment_card")
    );
    assert_eq!(classify("ssn (123-45-6789) on file"), Some("ssn"));
    assert_eq!(
        classify("dose schedule 234123412346 applies"),
        None,
        "aadhaar is whole-value only"
    );
    assert_eq!(
        classify("call +14155552671 now"),
        None,
        "phone is whole-value only"
    );
    assert_eq!(classify("PAN ABCDE1234F given"), None);
    assert_eq!(classify("just some prose"), None);
}

#[test]
fn scanner_groups_by_kind_and_location_and_sorts_by_distinct() {
    let mut s = Scanner::new();
    s.note_node();
    s.note_node();
    s.note_edge();
    for v in [
        "a1@example.com",
        "a2@example.com",
        "a3@example.com",
        "a4@example.com",
        "a1@example.com",
    ] {
        s.observe("Person.email", v);
    }
    s.observe("Person.card", CARD);
    s.observe("Person.name", "Alice");
    let r = s.finish();
    assert!(!r.is_clean());
    assert_eq!(
        (r.nodes_scanned, r.edges_scanned, r.values_scanned),
        (2, 1, 7)
    );
    assert_eq!(r.findings.len(), 2);
    let email = &r.findings[0];
    assert_eq!(
        (email.kind, email.location.as_str(), email.distinct),
        ("email", "Person.email", 4)
    );
    assert_eq!(email.samples.len(), MAX_SAMPLES);
    assert!(email
        .samples
        .iter()
        .all(|v| v.contains('*') && !v.contains("@example")));
    let card = &r.findings[1];
    assert_eq!((card.kind, card.distinct), ("payment_card", 1));
    assert_eq!(card.samples, vec![redact(CARD)]);

    let clean = Scanner::new().finish();
    assert!(clean.is_clean());
}

#[test]
fn scan_snapshot_walks_nodes_edges_and_nested_values() {
    let snap = format!(
        "{{\"t\":\"h\",\"version\":1}}\n\
         \n\
         {{\"t\":\"n\",\"labels\":[\"Person\"],\"props\":{{\"email\":\"bob@example.org\",\"n\":1,\"ok\":true}}}}\n\
         {{\"t\":\"n\",\"props\":{{\"contacts\":[\"x\",{{\"iban\":\"{IBAN}\"}}]}}}}\n\
         {{\"t\":\"n\",\"labels\":[\"Bare\"]}}\n\
         {{\"t\":\"e\",\"type\":\"PAID\",\"props\":{{\"card\":{CARD}}}}}\n\
         {{\"t\":\"e\",\"props\":{{\"note\":\"x\"}}}}\n\
         {{\"t\":\"z\",\"props\":{{\"email\":\"ignored@example.org\"}}}}\n"
    );
    let r = scan_snapshot(Cursor::new(snap)).unwrap();
    assert_eq!((r.nodes_scanned, r.edges_scanned), (3, 2));
    // email, n, contacts[0], contacts[1].iban, card, note (booleans are not offered)
    assert_eq!(r.values_scanned, 6);
    let mut got: Vec<(&str, &str)> = r
        .findings
        .iter()
        .map(|f| (f.kind, f.location.as_str()))
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![
            ("email", "Person.email"),
            ("iban", "<unlabelled>.contacts.iban"),
            ("payment_card", "<edge:PAID>.card"),
        ]
    );
}

#[test]
fn scan_snapshot_refuses_corrupt_lines() {
    let err = scan_snapshot(Cursor::new("{\"t\":\"h\"}\nnot json\n")).unwrap_err();
    assert!(err.starts_with("line 2 is not JSON"), "{err}");
    let err = scan_snapshot(Cursor::new(vec![0xffu8, 0xfe, b'\n'])).unwrap_err();
    assert!(err.starts_with("line 1: "), "{err}");
}

#[test]
fn scan_snapshot_path_reads_plain_and_gzip_and_reports_bad_paths() {
    let dir = tempfile::tempdir().unwrap();
    let snap = "{\"t\":\"n\",\"labels\":[\"P\"],\"props\":{\"e\":\"carol@example.net\"}}\n";
    let plain = dir.path().join("a.sgsnap");
    std::fs::write(&plain, snap).unwrap();
    let gz = dir.path().join("a.sgsnap.gz");
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(snap.as_bytes()).unwrap();
    std::fs::write(&gz, enc.finish().unwrap()).unwrap();

    for p in [&plain, &gz] {
        let r = scan_snapshot_path(p).unwrap();
        assert_eq!(r.findings.len(), 1, "{}", p.display());
        assert_eq!(r.findings[0].location, "P.e");
    }

    let missing = dir.path().join("missing.sgsnap");
    assert!(scan_snapshot_path(&missing)
        .unwrap_err()
        .contains("missing.sgsnap"));
    let empty = dir.path().join("empty.sgsnap");
    std::fs::write(&empty, "").unwrap();
    assert!(scan_snapshot_path(&empty)
        .unwrap_err()
        .contains("empty.sgsnap"));
}

fn waiver(kind: &str, location: &str, max: usize) -> Waiver {
    Waiver {
        kind: kind.into(),
        location: location.into(),
        max_distinct: max,
        why: "public record".into(),
        decided_in: "#1".into(),
    }
}

#[test]
fn read_waivers_requires_reason_and_decision() {
    let dir = tempfile::tempdir().unwrap();
    let good = dir.path().join("good.json");
    std::fs::write(
        &good,
        r##"[{"kind":"email","location":"P.e","max_distinct":2,"why":"public","decided_in":"#9"}]"##,
    )
    .unwrap();
    let w = read_waivers(&good).unwrap();
    assert_eq!(w.len(), 1);
    assert_eq!((w[0].kind.as_str(), w[0].max_distinct), ("email", 2));

    let no_why = dir.path().join("no_why.json");
    std::fs::write(
        &no_why,
        r##"[{"kind":"email","location":"P.e","max_distinct":2,"why":"  ","decided_in":"#9"}]"##,
    )
    .unwrap();
    let err = read_waivers(&no_why).unwrap_err();
    assert!(
        err.contains("mute button") && err.contains("email/P.e"),
        "{err}"
    );

    let no_decision = dir.path().join("no_decision.json");
    std::fs::write(
        &no_decision,
        r#"[{"kind":"ssn","location":"X","max_distinct":1,"why":"ok","decided_in":""}]"#,
    )
    .unwrap();
    assert!(read_waivers(&no_decision).is_err());

    let bad_json = dir.path().join("bad.json");
    std::fs::write(&bad_json, "{").unwrap();
    assert!(read_waivers(&bad_json).unwrap_err().contains("bad.json"));
    assert!(read_waivers(&dir.path().join("absent.json"))
        .unwrap_err()
        .contains("absent.json"));
}

#[test]
fn triage_accepts_within_limit_fails_above_it_and_lists_unused_waivers() {
    let finding = |kind: &'static str, loc: &str, distinct: usize| Finding {
        kind,
        location: loc.into(),
        samples: vec![],
        distinct,
    };
    let report = Report {
        findings: vec![
            finding("email", "P.e", 2),
            finding("email", "P.other", 1),
            finding("ssn", "P.s", 5),
        ],
        ..Report::default()
    };
    let waivers = vec![
        waiver("email", "P.e", 2),
        waiver("ssn", "P.s", 4),
        waiver("iban", "Q.i", 1),
    ];
    let t = triage(&report, &waivers);
    assert_eq!(t.accepted.len(), 1);
    assert_eq!(t.accepted[0].0.location, "P.e");
    let unwaived: Vec<&str> = t.unwaived.iter().map(|f| f.location.as_str()).collect();
    assert_eq!(
        unwaived,
        vec!["P.other", "P.s"],
        "a count above the waiver is not waived"
    );
    let unused: Vec<&str> = t.unused.iter().map(|w| w.location.as_str()).collect();
    assert_eq!(unused, vec!["P.s", "Q.i"]);
}
