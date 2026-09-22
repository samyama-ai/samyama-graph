//! Scan a snapshot for personal identifiers before it is published (TRUST-10).
//!
//! TRUST-10 asks for "an automated scan of every published KG and snapshot for
//! personal data patterns before release". The published set includes health
//! and cyber datasets, so this is not a formality.
//!
//! # What it looks for, and what it deliberately does not
//!
//! It looks for **identifiers**: things that pick out one person and that no
//! public dataset has a reason to carry. Email addresses, phone numbers,
//! payment card numbers, Aadhaar and PAN, US Social Security numbers, and
//! IBANs.
//!
//! It does **not** look for names, and it does **not** look for IP addresses.
//!
//! Names, because several published graphs are built from public records —
//! court judgments name their judges and their parties, a football graph names
//! its players — and a scanner that flagged those would fire on every row of
//! them, be switched off within a week, and then be a control that exists and
//! does nothing.
//!
//! IP addresses are personal data under GDPR and were in an earlier draft of
//! this module. Two things removed them. The cyber KGs are *about* addresses,
//! so the detector fires on every row of exactly the datasets it was added
//! for; and a dotted quad is indistinguishable from a version string —
//! `1.0.0.0` is both, and this repository publishes the second constantly. The
//! exclusion list needed to make it usable would have left a detector that
//! fires only where nobody needs it. It is recorded as a gap in
//! `docs/DATA-HANDLING.md` rather than shipped as a control that gets switched
//! off. The scanner is narrow so that a hit means something.
//!
//! # A regex is not a finding
//!
//! Every pattern with a checksum is checked against it. A sixteen-digit number
//! is not a card number unless it passes Luhn; a twelve-digit number is not an
//! Aadhaar unless it passes Verhoeff. This is the difference between a scan
//! and an alarm: on a graph of measurements, sixteen-digit integers are
//! ordinary, and a scanner that reported each one would be turned off by the
//! second release.
//!
//! The patterns without a checksum — email, PAN, phone — are
//! reported with the count of *distinct* values, so a single template string
//! repeated across ten thousand rows reads as one finding rather than ten
//! thousand.
//!
//! # What it cannot say
//!
//! That a snapshot is free of personal data. It can only say that these
//! patterns did not appear. Free text carrying a home address in prose passes
//! this scan, and so does a name paired with a diagnosis, which is more
//! sensitive than anything in the list above. The scan is a floor, and
//! `docs/DATA-HANDLING.md` says so rather than letting a green run stand in
//! for a judgement nobody made.

use std::collections::{BTreeMap, BTreeSet};

/// One kind of identifier, and where it was seen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    /// The detector that fired, e.g. `email`.
    pub kind: &'static str,
    /// `Label.property` the value sat on, or `<edge>.property`.
    pub location: String,
    /// Distinct matching values, capped. Redacted: enough to find the row,
    /// never enough to leak the identifier into a CI log, which is itself a
    /// published artifact.
    pub samples: Vec<String>,
    /// Distinct matching values at this location.
    pub distinct: usize,
}

/// Everything the scan saw.
#[derive(Debug, Default, Clone)]
pub struct Report {
    pub findings: Vec<Finding>,
    pub values_scanned: usize,
    pub nodes_scanned: usize,
    pub edges_scanned: usize,
}

impl Report {
    pub fn is_clean(&self) -> bool {
        self.findings.is_empty()
    }
}

/// At most this many distinct samples per location in the report.
const MAX_SAMPLES: usize = 3;

/// Show enough of a match to locate the row and not enough to carry the
/// identifier out of the scan.
///
/// A CI log is a published artifact, so a scanner that printed what it found
/// would move the personal data from a snapshot nobody reads into a log
/// everybody can.
pub fn redact(s: &str) -> String {
    let n = s.chars().count();
    if n <= 4 {
        return "*".repeat(n);
    }
    let head: String = s.chars().take(2).collect();
    let tail: String = s.chars().skip(n - 2).collect();
    format!("{head}{}{tail}", "*".repeat(n - 4))
}

// ───────────────────────────────────────────────────────────────── checksums

/// Luhn, as used by payment cards.
pub fn luhn_ok(digits: &[u8]) -> bool {
    if digits.len() < 12 {
        return false;
    }
    let mut sum = 0u32;
    for (i, d) in digits.iter().rev().enumerate() {
        let mut v = u32::from(*d);
        if i % 2 == 1 {
            v *= 2;
            if v > 9 {
                v -= 9;
            }
        }
        sum += v;
    }
    sum % 10 == 0
}

/// Verhoeff, as used by Aadhaar.
///
/// Implemented rather than approximated by a length check: twelve-digit
/// numbers are common in any dataset with identifiers or timestamps in it, and
/// without the checksum this detector would fire on most of them.
pub fn verhoeff_ok(digits: &[u8]) -> bool {
    const D: [[u8; 10]; 10] = [
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        [1, 2, 3, 4, 0, 6, 7, 8, 9, 5],
        [2, 3, 4, 0, 1, 7, 8, 9, 5, 6],
        [3, 4, 0, 1, 2, 8, 9, 5, 6, 7],
        [4, 0, 1, 2, 3, 9, 5, 6, 7, 8],
        [5, 9, 8, 7, 6, 0, 4, 3, 2, 1],
        [6, 5, 9, 8, 7, 1, 0, 4, 3, 2],
        [7, 6, 5, 9, 8, 2, 1, 0, 4, 3],
        [8, 7, 6, 5, 9, 3, 2, 1, 0, 4],
        [9, 8, 7, 6, 5, 4, 3, 2, 1, 0],
    ];
    const P: [[u8; 10]; 8] = [
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        [1, 5, 7, 6, 2, 8, 3, 0, 9, 4],
        [5, 8, 0, 3, 7, 9, 6, 1, 4, 2],
        [8, 9, 1, 6, 0, 4, 3, 5, 2, 7],
        [9, 4, 5, 3, 1, 2, 6, 8, 7, 0],
        [4, 2, 8, 6, 5, 7, 3, 9, 0, 1],
        [2, 7, 9, 3, 8, 0, 6, 4, 1, 5],
        [7, 0, 4, 6, 9, 1, 3, 2, 5, 8],
    ];
    if digits.len() != 12 {
        return false;
    }
    let mut c = 0u8;
    for (i, d) in digits.iter().rev().enumerate() {
        if *d > 9 {
            return false;
        }
        c = D[c as usize][P[i % 8][*d as usize] as usize];
    }
    c == 0
}

// ───────────────────────────────────────────────────────────────── detectors

/// Digits of `s`, if `s` is digits and separators only and has `want` of them.
fn digits_if(s: &str, want: usize) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(want);
    for ch in s.chars() {
        if ch.is_ascii_digit() {
            out.push(ch as u8 - b'0');
        } else if ch == ' ' || ch == '-' {
            continue;
        } else {
            return None;
        }
    }
    (out.len() == want).then_some(out)
}

fn is_email(s: &str) -> bool {
    // Deliberately stricter than the RFC. The RFC permits addresses nobody
    // writes, and a permissive pattern here fires on every `a@b` in free text.
    let Some((local, domain)) = s.split_once('@') else {
        return false;
    };
    if local.is_empty() || local.len() > 64 || s.split('@').count() != 2 {
        return false;
    }
    let Some((host, tld)) = domain.rsplit_once('.') else {
        return false;
    };
    if host.is_empty() || tld.len() < 2 || !tld.chars().all(|c| c.is_ascii_alphabetic()) {
        return false;
    }
    local
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "._%+-".contains(c))
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
}

fn is_payment_card(s: &str) -> bool {
    for want in [13usize, 14, 15, 16, 19] {
        if let Some(d) = digits_if(s, want) {
            // A card number starts 3-6; excluding 0-2 and 7-9 drops most
            // sequential identifiers that happen to pass Luhn.
            if (3..=6).contains(&d[0]) && luhn_ok(&d) {
                return true;
            }
        }
    }
    false
}

fn is_aadhaar(s: &str) -> bool {
    match digits_if(s, 12) {
        // Aadhaar never begins 0 or 1, which is also what keeps this off
        // zero-padded internal identifiers.
        Some(d) if d[0] >= 2 => verhoeff_ok(&d),
        _ => false,
    }
}

fn is_pan(s: &str) -> bool {
    // AAAAA9999A. No checksum exists, so the shape carries the whole weight;
    // it is specific enough that a false positive is unlikely in a graph.
    let b = s.as_bytes();
    b.len() == 10
        && b[..5].iter().all(|c| c.is_ascii_uppercase())
        && b[5..9].iter().all(|c| c.is_ascii_digit())
        && b[9].is_ascii_uppercase()
}

fn is_ssn(s: &str) -> bool {
    // NNN-NN-NNNN only. Bare nine-digit numbers are far too common to call an
    // SSN, and reporting them would be the alarm this module refuses to be.
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 || parts[0].len() != 3 || parts[1].len() != 2 || parts[2].len() != 4 {
        return false;
    }
    if !parts.iter().all(|p| p.chars().all(|c| c.is_ascii_digit())) {
        return false;
    }
    // Ranges the SSA never issues.
    let area: u32 = parts[0].parse().unwrap_or(0);
    let group: u32 = parts[1].parse().unwrap_or(0);
    let serial: u32 = parts[2].parse().unwrap_or(0);
    area != 0 && area != 666 && area < 900 && group != 0 && serial != 0
}

fn is_phone(s: &str) -> bool {
    // International form only: a `+`, a country code, then 8 to 14 digits.
    // National forms are indistinguishable from ordinary numbers without
    // knowing the country, and guessing is how a scanner starts crying wolf.
    let t = s.trim();
    if !t.starts_with('+') {
        return false;
    }
    let digits: Vec<char> = t[1..]
        .chars()
        .filter(|c| !matches!(c, ' ' | '-' | '(' | ')'))
        .collect();
    if digits.len() < 9 || digits.len() > 15 || !digits.iter().all(|c| c.is_ascii_digit()) {
        return false;
    }
    t[1..]
        .chars()
        .all(|c| c.is_ascii_digit() || matches!(c, ' ' | '-' | '(' | ')'))
}

/// IBAN length per ISO 3166 country, for the countries that have one.
///
/// The country and its exact length carry most of the weight here, and the
/// reason is arithmetic. mod-97 admits 1 in 97 candidates, and the
/// clinical-trials snapshot has 27 million property values -- so on shape and
/// checksum alone it reported seventeen "IBANs" in `ArmGroup.label`, which are
/// pharmaceutical arm names. A two-letter prefix that is not an IBAN country,
/// or a length that is not that country's, is not a near-miss: it is not an
/// IBAN at all, and checking it removes almost every chance match.
const IBAN_LENGTHS: &[(&str, usize)] = &[
    ("AD", 24), ("AE", 23), ("AL", 28), ("AT", 20), ("AZ", 28), ("BA", 20),
    ("BE", 16), ("BG", 22), ("BH", 22), ("BI", 27), ("BR", 29), ("BY", 28),
    ("CH", 21), ("CR", 22), ("CY", 28), ("CZ", 24), ("DE", 22), ("DK", 18),
    ("DO", 28), ("EE", 20), ("EG", 29), ("ES", 24), ("FI", 18), ("FO", 18),
    ("FR", 27), ("GB", 22), ("GE", 22), ("GI", 23), ("GL", 18), ("GR", 27),
    ("GT", 28), ("HR", 21), ("HU", 28), ("IE", 22), ("IL", 23), ("IQ", 23),
    ("IS", 26), ("IT", 27), ("JO", 30), ("KW", 30), ("KZ", 20), ("LB", 28),
    ("LC", 32), ("LI", 21), ("LT", 20), ("LU", 20), ("LV", 21), ("LY", 25),
    ("MC", 27), ("MD", 24), ("ME", 22), ("MK", 19), ("MR", 27), ("MT", 31),
    ("MU", 30), ("NL", 18), ("NO", 15), ("PK", 24), ("PL", 28), ("PS", 29),
    ("PT", 25), ("QA", 29), ("RO", 24), ("RS", 22), ("SA", 24), ("SC", 31),
    ("SD", 18), ("SE", 24), ("SI", 19), ("SK", 24), ("SM", 27), ("ST", 25),
    ("SV", 28), ("TL", 23), ("TN", 24), ("TR", 26), ("UA", 29), ("VA", 22),
    ("VG", 24), ("XK", 20),
];

fn is_iban(s: &str) -> bool {
    let t: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    let b = t.as_bytes();
    if b.len() < 15 || b.len() > 34 {
        return false;
    }
    if !(b[0].is_ascii_uppercase() && b[1].is_ascii_uppercase()) {
        return false;
    }
    if !(b[2].is_ascii_digit() && b[3].is_ascii_digit()) {
        return false;
    }
    if !b[4..].iter().all(|c| c.is_ascii_alphanumeric()) {
        return false;
    }
    // The country must issue IBANs, and the length must be that country's.
    let cc = &t[..2];
    if !IBAN_LENGTHS.iter().any(|(c, n)| *c == cc && *n == t.len()) {
        return false;
    }
    // mod-97 over the rearranged string, which is the IBAN checksum.
    let rearranged = format!("{}{}", &t[4..], &t[..4]);
    let mut rem: u32 = 0;
    for ch in rearranged.chars() {
        let v = if ch.is_ascii_digit() {
            ch as u32 - '0' as u32
        } else {
            ch as u32 - 'A' as u32 + 10
        };
        rem = if v > 9 { (rem * 100 + v) % 97 } else { (rem * 10 + v) % 97 };
    }
    rem == 1
}

/// Every detector: name, test, and whether it may be applied to a *token*
/// inside free text as well as to a whole property value.
///
/// The third field is the lesson from the first run against a real artifact.
/// Scanning the 745 MB clinical-trials snapshot -- 36.5 million property
/// values, so on the order of a billion tokens -- returned thirty-five
/// findings, and most were chance checksum passes inside prose. The arithmetic
/// says they had to be: **1 in 10** random twelve-digit numbers satisfies
/// Verhoeff, and **1 in 97** strings of the right shape satisfy the IBAN
/// mod-97. At a billion tokens a filter that admits one in ten admits a great
/// many, and a report with thirty-four wrong entries is an alarm, not a scan.
///
/// So a detector runs inside free text only when its *shape* is improbable on
/// its own and the checksum is confirmation rather than the whole argument:
///
/// - `email` -- an `@` with a hostname and a real TLD either side
/// - `ssn` -- `NNN-NN-NNNN`, with the ranges the SSA never issued excluded
/// - `payment_card` -- a specific length, a leading digit of 3-6, *and* Luhn
///
/// The rest are matched against a whole property value only. An Aadhaar alone
/// in a field is a finding; twelve digits inside a paragraph about a dosing
/// schedule is a coincidence that happens every tenth time.
///
/// `phone` is whole-value for a different reason: a written phone number
/// contains spaces, so splitting free text on whitespace takes it apart before
/// the detector sees it. Finding one in prose would need its own scan, and the
/// case it exists for -- a number sitting in a field of its own, which is how
/// the clinical-trials snapshot carries them -- is covered without it.
type Detector = (&'static str, fn(&str) -> bool, bool);

const DETECTORS: &[Detector] = &[
    ("email", is_email, true),
    ("payment_card", is_payment_card, true),
    ("ssn", is_ssn, true),
    ("aadhaar", is_aadhaar, false),
    ("pan", is_pan, false),
    ("iban", is_iban, false),
    ("phone", is_phone, false),
];

/// The detector a single value trips, if any.
///
/// A value trips at most one: they are mutually exclusive by shape, and
/// reporting one string under two names would double a finding count that is
/// meant to be read as "how many things to look at".
pub fn classify(value: &str) -> Option<&'static str> {
    let t = value.trim();
    if t.is_empty() || t.len() > 4096 {
        return None;
    }
    if let Some(kind) = DETECTORS.iter().find(|(_, f, _)| f(t)).map(|(k, _, _)| *k) {
        return Some(kind);
    }
    // Then each token, because an identifier inside free text is the case this
    // exists for: "contact alice@example.com" is not an email address and
    // contains one. Splitting keeps every checksum applied to a whole
    // candidate, rather than to a window slid across the string -- which is
    // what makes substring scanning cry wolf.
    //
    // `.`, `-`, `+` and `@` are not separators: each sits *inside* one of the
    // identifiers above.
    t.split(|c: char| c.is_whitespace() || ",;:\"'<>()[]{}|/\\".contains(c))
        .filter(|tok| !tok.is_empty() && tok.len() <= 128)
        .find_map(|tok| {
            let tok = tok.trim_matches(|c: char| c == '.' || c == '-');
            DETECTORS
                .iter()
                .filter(|(_, _, in_free_text)| *in_free_text)
                .find(|(_, f, _)| f(tok))
                .map(|(k, _, _)| *k)
        })
}

// ─────────────────────────────────────────────────────────────── the scan

/// Accumulates hits by `(kind, location)`.
#[derive(Default)]
pub struct Scanner {
    hits: BTreeMap<(&'static str, String), BTreeSet<String>>,
    report: Report,
}

impl Scanner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Offer one property value, seen at `location`.
    pub fn observe(&mut self, location: &str, value: &str) {
        self.report.values_scanned += 1;
        if let Some(kind) = classify(value) {
            self.hits
                .entry((kind, location.to_string()))
                .or_default()
                .insert(value.to_string());
        }
    }

    pub fn note_node(&mut self) {
        self.report.nodes_scanned += 1;
    }

    pub fn note_edge(&mut self) {
        self.report.edges_scanned += 1;
    }

    pub fn finish(mut self) -> Report {
        for ((kind, location), values) in self.hits {
            let samples = values.iter().take(MAX_SAMPLES).map(|v| redact(v)).collect();
            self.report.findings.push(Finding {
                kind,
                location,
                samples,
                distinct: values.len(),
            });
        }
        // Most distinct values first: the thing to look at before the one-off.
        self.report.findings.sort_by(|a, b| {
            b.distinct
                .cmp(&a.distinct)
                .then_with(|| a.kind.cmp(b.kind))
                .then_with(|| a.location.cmp(&b.location))
        });
        self.report
    }
}

/// Scan a `.sgsnap` stream, gzip or plain.
///
/// Reads the export format directly rather than importing into a store: the
/// scan runs in release CI on an artifact, and requiring an import would make
/// the check need as much memory as the graph. A snapshot too big to load is
/// exactly the one nobody would scan.
pub fn scan_snapshot<R: std::io::BufRead>(reader: R) -> Result<Report, String> {
    use std::io::BufRead;

    let mut scanner = Scanner::new();
    for (i, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| format!("line {}: {e}", i + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            // The first line is the header and every later line is a record;
            // a line that is not JSON at all is a corrupt file, and saying so
            // beats scanning the rest and reporting it clean.
            Err(e) => return Err(format!("line {} is not JSON: {e}", i + 1)),
        };
        let kind = v.get("t").and_then(|t| t.as_str()).unwrap_or("");
        let where_ = match kind {
            "n" => {
                scanner.note_node();
                v.get("labels")
                    .and_then(|l| l.as_array())
                    .and_then(|a| a.first())
                    .and_then(|s| s.as_str())
                    .unwrap_or("<unlabelled>")
                    .to_string()
            }
            "e" => {
                scanner.note_edge();
                format!(
                    "<edge:{}>",
                    v.get("type").and_then(|t| t.as_str()).unwrap_or("?")
                )
            }
            // The header line, or a record shape this version does not know.
            _ => continue,
        };
        let Some(props) = v.get("props").and_then(|p| p.as_object()) else {
            continue;
        };
        for (key, value) in props {
            observe_json(&mut scanner, &format!("{where_}.{key}"), value);
        }
    }
    Ok(scanner.finish())
}

/// Offer a property value, descending into arrays and maps.
///
/// Nested values are where this kind of thing hides: a `contacts` array of
/// strings is a list of identifiers, and a scan that only looked at scalars
/// would report the graph clean.
fn observe_json(scanner: &mut Scanner, location: &str, value: &serde_json::Value) {
    match value {
        serde_json::Value::String(s) => scanner.observe(location, s),
        // Numbers are offered as text: a card number stored as an integer is
        // still a card number, and JSON does not say which it was.
        serde_json::Value::Number(n) => scanner.observe(location, &n.to_string()),
        serde_json::Value::Array(a) => {
            for v in a {
                observe_json(scanner, location, v);
            }
        }
        serde_json::Value::Object(o) => {
            for (k, v) in o {
                observe_json(scanner, &format!("{location}.{k}"), v);
            }
        }
        _ => {}
    }
}

/// Open a snapshot path, transparently decompressing gzip, and scan it.
pub fn scan_snapshot_path(path: &std::path::Path) -> Result<Report, String> {
    use std::io::{BufReader, Read};

    let mut file = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut magic = [0u8; 2];
    let gzipped = match file.read_exact(&mut magic) {
        Ok(()) => magic == [0x1f, 0x8b],
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    let file = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    if gzipped {
        scan_snapshot(BufReader::new(flate2::read::GzDecoder::new(file)))
    } else {
        scan_snapshot(BufReader::new(file))
    }
}
