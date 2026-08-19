//! What an example does when the data it needs is not in this repository.
//!
//! Several demos read a `.sgsnap` snapshot or a CSV from a sibling repository.
//! Each of them printed a clear message and then `exit(1)` — indistinguishable
//! from a genuine failure. So a CI job over every example could not be written
//! without a skip-list, a skip-list rots as demos are added, and **a demo that
//! actually breaks among them is invisible**, because its exit code already
//! says failure (#566).
//!
//! `case_studies/_lib/run_case_study.sh` already had the answer, and this is it
//! in Rust: report the absence, say plainly that nothing is broken, and exit
//! with a code a runner can tell apart. A CI job then reads
//! `0 = ok, 2 = skipped, anything else = fails`, and stays correct as demos are
//! added.
//!
//! `--require-data` turns the skip back into a failure, which is what the KG
//! repositories — where the data *is* present — should pass to assert that
//! these demos really run.

/// Exit code for "this example needs data that is not here".
///
/// Distinct from 1 so a runner can tell a skip from a break.
pub const SKIP: i32 = 2;

/// Report absent input data and exit.
///
/// `what` names the data in prose, `flag` is the option that points at a local
/// copy — both appear in the message, so a reader knows what is missing and how
/// to supply it without opening the source.
pub fn skip(what: &str, path: &std::path::Path, error: &std::io::Error, flag: &str) -> ! {
    let required = std::env::args().any(|a| a == "--require-data");
    let label = if required { "Error" } else { "SKIP" };

    eprintln!("{label}: cannot read the {what}: {}", path.display());
    eprintln!("       {error}");
    eprintln!("       this example needs data that is not part of this repository;");
    eprintln!("       pass {flag} PATH to point at your own copy.");

    if required {
        eprintln!("       --require-data was given, so this is a failure.");
        std::process::exit(1);
    }
    eprintln!("       Skipping rather than failing: nothing here is broken.");
    eprintln!("       (exit {SKIP}; pass --require-data to make this a failure.)");
    std::process::exit(SKIP)
}
