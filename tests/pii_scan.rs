//! The PII scan finds identifiers and does not cry wolf (TRUST-10).
//!
//! Half of these cases are negative, and they are the half that decides
//! whether the control survives contact with a real release. A scanner that
//! reports every sixteen-digit integer on a graph of measurements is switched
//! off after the second run, and then TRUST-10 has a green tick over a
//! workflow step nobody reads.
//!
//! So each detector is asserted twice: it fires on a well-formed identifier,
//! and it stays silent on the thing that looks like one. For the patterns that
//! carry a checksum the negative case is the *same digits with one changed*,
//! which is the strongest form available -- a length-and-shape check cannot
//! tell those apart, and a detector that passed this test cannot be one.

use samyama::pii::{classify, luhn_ok, redact, scan_snapshot, verhoeff_ok, Scanner};

fn digits(s: &str) -> Vec<u8> {
    s.chars().filter_map(|c| c.to_digit(10)).map(|d| d as u8).collect()
}

// ─────────────────────────────────────────────────────────────── checksums

#[test]
fn luhn_accepts_a_valid_number_and_rejects_the_same_digits_altered() {
    // A widely published test number, and the same with one digit changed.
    assert!(luhn_ok(&digits("4539578763621486")));
    assert!(!luhn_ok(&digits("4539578763621487")));
    assert!(!luhn_ok(&digits("4539578763621496")));
}

#[test]
fn verhoeff_accepts_a_valid_number_and_rejects_the_same_digits_altered() {
    // Verhoeff catches every single-digit error and every adjacent
    // transposition, which is the reason Aadhaar uses it and the reason this
    // detector can be narrow.
    let good = digits("234567890124");
    assert!(
        verhoeff_ok(&good),
        "the fixture itself must be valid or every case below proves nothing"
    );
    let mut single_error = good.clone();
    single_error[5] = (single_error[5] + 1) % 10;
    assert!(!verhoeff_ok(&single_error));

    let mut transposed = good.clone();
    transposed.swap(3, 4);
    assert!(
        !verhoeff_ok(&transposed) || good[3] == good[4],
        "an adjacent transposition must not pass"
    );
}

// ─────────────────────────────────────────────────────────────── detectors

#[test]
fn an_email_address_is_found() {
    assert_eq!(classify("alice.smith@example.com"), Some("email"));
    assert_eq!(classify("a_b+c%d@sub.example.co.uk"), Some("email"));
}

#[test]
fn a_string_with_an_at_sign_is_not_an_email() {
    // The shapes that occur in a graph and are not addresses.
    for s in [
        "@handle",
        "user@",
        "@",
        "a@b",                  // no dot in the domain
        "a@b.c",                // single-character TLD
        "cost@2x",              // the domain is not a hostname
        "see note@page 4",      // a space
        "a@b@example.com",      // two at-signs
    ] {
        assert_eq!(classify(s), None, "{s:?} should not be an email");
    }
}

#[test]
fn a_payment_card_is_found_and_an_ordinary_long_number_is_not() {
    assert_eq!(classify("4539578763621486"), Some("payment_card"));
    assert_eq!(classify("4539 5787 6362 1486"), Some("payment_card"));
    assert_eq!(classify("4539-5787-6362-1486"), Some("payment_card"));

    // One digit out: same length, same shape, fails Luhn.
    assert_eq!(classify("4539578763621487"), None);
    // Passes Luhn but does not start in a card range -- a sequential id.
    assert_eq!(classify("1234567812345670"), None);
    // The kind of sixteen-digit number a measurement graph is full of.
    assert_eq!(classify("1000000000000000"), None);
    assert_eq!(classify("2016010112000000"), None);
}

#[test]
fn an_aadhaar_is_found_and_a_twelve_digit_id_is_not() {
    assert_eq!(classify("234567890124"), Some("aadhaar"));
    // Same length, one digit different.
    assert_eq!(classify("234567890125"), None);
    // Leading 0 and 1 are never issued, which also excludes zero-padded ids.
    assert_eq!(classify("012345678901"), None);
    assert_eq!(classify("112345678901"), None);
    // A unix timestamp in milliseconds is thirteen digits; in microseconds,
    // sixteen. Twelve-digit counters are common enough to be worth the check.
    assert_eq!(classify("100000000000"), None);
}

#[test]
fn a_pan_is_found_and_a_similar_looking_code_is_not() {
    assert_eq!(classify("ABCDE1234F"), Some("pan"));
    for s in ["ABCD1234F", "ABCDE1234", "abcde1234f", "ABCDE12345", "ABCDEF234F"] {
        assert_eq!(classify(s), None, "{s:?} should not be a PAN");
    }
}

#[test]
fn an_ssn_is_found_only_in_its_written_form() {
    assert_eq!(classify("123-45-6789"), Some("ssn"));
    // Bare nine-digit numbers are far too common to call an SSN.
    assert_eq!(classify("123456789"), None);
    // Ranges the SSA does not issue.
    for s in ["000-45-6789", "666-45-6789", "900-45-6789", "123-00-6789", "123-45-0000"] {
        assert_eq!(classify(s), None, "{s:?} is not an issued SSN range");
    }
}

#[test]
fn a_phone_number_is_found_only_in_international_form() {
    assert_eq!(classify("+91 98765 43210"), Some("phone"));
    assert_eq!(classify("+1 (415) 555-0132"), Some("phone"));
    // A national form is indistinguishable from an ordinary number without
    // knowing the country, so it is not reported rather than guessed at.
    assert_eq!(classify("9876543210"), None);
    assert_eq!(classify("+12345"), None, "too short to be a number");
}

#[test]
fn an_iban_is_found_and_a_mistyped_one_is_not() {
    assert_eq!(classify("GB82 WEST 1234 5698 7654 32"), Some("iban"));
    assert_eq!(classify("DE89370400440532013000"), Some("iban"));
    // One digit changed: passes every shape check, fails mod-97.
    assert_eq!(classify("DE89370400440532013001"), None);
    assert_eq!(classify("GB82WEST12345698765433"), None);
}

#[test]
fn an_ip_address_is_deliberately_not_reported() {
    // IP addresses are personal data under GDPR, and this detector was written
    // and then removed. The cyber KGs are *about* addresses, so it fired on
    // every row of the datasets it was added for; and a dotted quad is
    // indistinguishable from a version string. Asserted rather than left
    // silent, so that re-adding it is a deliberate act that fails a test
    // first -- and so the gap is visible to somebody reading the suite for
    // what TRUST-10 covers.
    assert_eq!(classify("203.0.113.7"), None);
    assert_eq!(classify("8.8.8.8"), None);
}

#[test]
fn an_identifier_inside_free_text_is_found() {
    // The case the scan exists for. A value is classified whole and then by
    // token, so a note carrying an address is a finding.
    assert_eq!(classify("contact alice@example.com for access"), Some("email"));
    assert_eq!(classify("card on file: 4539578763621486 (visa)"), Some("payment_card"));
    assert_eq!(classify("ref ABCDE1234F filed 2016"), Some("pan"));
    // And the token split does not create matches that were not there: a
    // sentence of ordinary words is still nothing.
    assert_eq!(classify("the judgment was delivered on 12 January 2016"), None);
    assert_eq!(classify("values 4539578763621487 and 1234567812345670"), None);
}

// ───────────────────────────────────────────────────────────────── reporting

#[test]
fn a_sample_is_redacted() {
    assert_eq!(redact("alice@example.com"), "al*************om");
    assert_eq!(redact("abcd"), "****");
    assert_eq!(redact("ab"), "**");
    // The point: the finding locates the row and carries none of the value.
    // A CI log is itself a published artifact.
    assert!(!redact("4539578763621486").contains("39578763621"));
}

#[test]
fn repeated_values_count_once_per_location() {
    let mut s = Scanner::new();
    for _ in 0..1000 {
        s.observe("Person.email", "alice@example.com");
    }
    s.observe("Person.email", "bob@example.com");
    let report = s.finish();
    assert_eq!(report.findings.len(), 1);
    assert_eq!(
        report.findings[0].distinct, 2,
        "a template string repeated across ten thousand rows is one finding"
    );
    assert_eq!(report.values_scanned, 1001);
}

#[test]
fn findings_are_ordered_by_how_much_there_is_to_look_at() {
    let mut s = Scanner::new();
    s.observe("A.one", "alice@example.com");
    for i in 0..5 {
        s.observe("B.many", &format!("user{i}@example.com"));
    }
    let report = s.finish();
    assert_eq!(report.findings.len(), 2);
    assert_eq!(report.findings[0].location, "B.many");
    assert_eq!(report.findings[0].distinct, 5);
}

// ───────────────────────────────────────────────────────────────── snapshots

const SNAPSHOT: &str = r#"{"format":"sgsnap","version":2,"tenant":"default","node_count":3,"edge_count":1,"labels":["Person"],"edge_types":["PAID"],"created_at":"2026-09-22T00:00:00Z","samyama_version":"1.9.0"}
{"t":"n","id":1,"labels":["Person"],"props":{"name":"Alice","email":"alice@example.com","age":34}}
{"t":"n","id":2,"labels":["Person"],"props":{"name":"Bob","contacts":["bob@example.com","+91 98765 43210"]}}
{"t":"n","id":3,"labels":["Account"],"props":{"meta":{"nested":{"card":"4539578763621486"}},"balance":1200}}
{"t":"e","id":1,"src":1,"tgt":3,"type":"PAID","props":{"note":"approved by carol@example.com"}}
"#;

#[test]
fn a_snapshot_is_scanned_including_nested_and_edge_values() {
    let report = scan_snapshot(SNAPSHOT.as_bytes()).expect("well-formed snapshot");
    assert_eq!(report.nodes_scanned, 3);
    assert_eq!(report.edges_scanned, 1);

    let kinds: Vec<&str> = report.findings.iter().map(|f| f.kind).collect();
    assert!(kinds.contains(&"email"), "{kinds:?}");
    assert!(kinds.contains(&"phone"), "a value inside an array must be reached");
    assert!(
        kinds.contains(&"payment_card"),
        "a value two maps deep must be reached: that is where this hides"
    );

    // The header line is not a record and must not be scanned as one.
    assert!(
        !report.findings.iter().any(|f| f.location.contains("format")),
        "the header was scanned as a node: {:?}",
        report.findings
    );

    let card = report.findings.iter().find(|f| f.kind == "payment_card").unwrap();
    assert_eq!(card.location, "Account.meta.nested.card");
    assert!(!card.samples[0].contains("39578763621"), "the sample is redacted");
    assert!(!report.is_clean());
}

#[test]
fn an_identifier_in_an_edge_property_is_attributed_to_the_edge_type() {
    let report = scan_snapshot(SNAPSHOT.as_bytes()).expect("well-formed");
    let found = report
        .findings
        .iter()
        .find(|f| f.location.starts_with("<edge:"))
        .expect("the edge property carries an email in free text");
    assert_eq!(found.location, "<edge:PAID>.note");
    assert_eq!(found.kind, "email");
}

#[test]
fn a_snapshot_without_identifiers_is_clean() {
    // The case that has to hold for the control to be usable: an ordinary
    // graph of measurements reports nothing.
    let plain = r#"{"format":"sgsnap","version":2,"labels":["Reading"]}
{"t":"n","id":1,"labels":["Reading"],"props":{"sensor":"S-1","value":1234567812345670,"at":"2026-09-22T10:00:00Z","version":"1.9.0"}}
{"t":"n","id":2,"labels":["Reading"],"props":{"sensor":"S-2","value":100000000000,"host":"10.0.0.4"}}
"#;
    let report = scan_snapshot(plain.as_bytes()).expect("well-formed");
    assert!(
        report.is_clean(),
        "an ordinary measurement graph must not fire: {:?}",
        report.findings
    );
    assert_eq!(report.nodes_scanned, 2);
}

#[test]
fn a_corrupt_file_is_an_error_and_not_a_clean_result() {
    // The failure that matters most: a scan that cannot read the file must not
    // report it clean. That is how a control passes on an artifact it never
    // looked at.
    let err = scan_snapshot("this is not json\n".as_bytes())
        .expect_err("a file that is not the snapshot format must be an error");
    assert!(err.contains("not JSON"), "{err}");
}

#[test]
fn a_number_that_is_a_card_number_is_found_even_when_stored_as_an_integer() {
    // JSON does not record which it was, and the engine accepts either.
    let snap = r#"{"format":"sgsnap","version":2}
{"t":"n","id":1,"labels":["Payment"],"props":{"pan":4539578763621486}}
"#;
    let report = scan_snapshot(snap.as_bytes()).expect("well-formed");
    assert_eq!(report.findings.len(), 1);
    assert_eq!(report.findings[0].kind, "payment_card");
}
