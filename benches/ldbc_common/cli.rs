//! Argument checks the LDBC benches share, kept where they can be tested.
//!
//! `ldbc_benchmark` is `harness = false` with no `test = true`, so a
//! `#[cfg(test)]` block inside it never runs. These live here instead, in a
//! module `examples/ldbc_loader.rs` pulls in — that target *is* tested, which
//! is how `params::tests` already runs (#733).

/// Single-valued flags, which may not be given twice.
///
/// `--query` is deliberately absent: it is repeatable, and
/// [`selected_query_ids`] collects every occurrence.
pub const SINGLE_VALUED: &[&str] = &[
    "--data-dir", "--derive-params", "--params-file", "--runs", "--write-params",
];

/// Which single-valued flags appear more than once.
///
/// Every option in these benches is read with
/// `args.iter().position(|a| a == "--x")`, which finds the **first** and
/// ignores the rest. So `--runs 3 --runs 10` runs three times, and
/// `--params-file a.json --params-file b.json` decides which dataset the
/// numbers describe — silently, in both cases.
///
/// The bench already refuses an *unknown* flag, with the reason spelled out in
/// its own message: an ignored flag produces a number measured under settings
/// nobody chose, and the output cannot tell you that happened. A known flag
/// given twice is the same defect, and the check did not cover it.
pub fn repeated_single_valued(args: &[String]) -> Vec<&'static str> {
    SINGLE_VALUED
        .iter()
        .filter(|f| args.iter().filter(|a| a.as_str() == **f).count() > 1)
        .copied()
        .collect()
}

/// Every `--query` value, upper-cased, in the order given.
///
/// Repeatable on purpose: `--query IC2 --query IC12` names two queries, which
/// is the only reading of that command line anyone intends. It used to run IC2
/// and drop IC12 without a word.
pub fn selected_query_ids(args: &[String]) -> Vec<String> {
    args.iter()
        .enumerate()
        .filter(|(_, a)| a.as_str() == "--query")
        .map(|(i, _)| {
            args.get(i + 1)
                .unwrap_or_else(|| panic!("--query requires a query ID (e.g. IS1, IC3)"))
                .to_uppercase()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_flag_given_once_is_not_a_repeat() {
        assert!(repeated_single_valued(&args(&["bench", "--runs", "3"])).is_empty());
    }

    /// The defect this exists for: the second value is dropped and the run
    /// uses the first, with nothing in the output to say so.
    #[test]
    fn a_single_valued_flag_given_twice_is_reported() {
        assert_eq!(
            repeated_single_valued(&args(&["bench", "--runs", "3", "--runs", "10"])),
            vec!["--runs"]
        );
    }

    #[test]
    fn every_repeated_flag_is_reported_not_just_the_first() {
        let got = repeated_single_valued(&args(&[
            "bench", "--runs", "3", "--runs", "10", "--data-dir", "a", "--data-dir", "b",
        ]));
        assert_eq!(got, vec!["--data-dir", "--runs"]);
    }

    /// `--query` is the one flag that may repeat, so it must not be caught by
    /// the check that forbids repeating.
    #[test]
    fn query_may_repeat() {
        assert!(repeated_single_valued(&args(&[
            "bench", "--query", "IC2", "--query", "IC12"
        ]))
        .is_empty());
    }

    #[test]
    fn no_query_flag_selects_nothing_which_the_caller_reads_as_everything() {
        assert!(selected_query_ids(&args(&["bench", "--runs", "3"])).is_empty());
    }

    #[test]
    fn a_repeated_query_flag_names_every_query() {
        assert_eq!(
            selected_query_ids(&args(&["bench", "--query", "ic2", "--query", "IC12"])),
            vec!["IC2".to_string(), "IC12".to_string()]
        );
    }
}
