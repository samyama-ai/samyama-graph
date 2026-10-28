//! "Did you mean …?" for a name the engine does not know (DX-06, #1781).
//!
//! An error that names what is wrong but not what is right leaves the reader to
//! search for the spelling the engine wanted. When the unknown name is a likely
//! typo of a known one, the error can say which.

/// The largest edit distance still read as a typo, for a name of `len`
/// characters: one edit for a short name, where two would turn `id` into any
/// two-letter name; two for a longer one.
fn max_typo_distance(len: usize) -> usize {
    /// Names this long or shorter allow one edit.
    const SHORT_NAME: usize = 4;
    if len <= SHORT_NAME {
        1
    } else {
        2
    }
}

/// Levenshtein distance, counting characters, case-insensitively.
fn distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.to_lowercase().chars().collect();
    let b: Vec<char> = b.to_lowercase().chars().collect();
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.iter().enumerate() {
        let mut current = vec![i + 1; b.len() + 1];
        for (j, cb) in b.iter().enumerate() {
            let substitute = previous[j] + usize::from(ca != cb);
            current[j + 1] = substitute.min(previous[j + 1] + 1).min(current[j] + 1);
        }
        previous = current;
    }
    previous[b.len()]
}

/// The candidate `name` is most likely a misspelling of, if any is close
/// enough to be one. Ties go to the earlier candidate, so the answer does not
/// depend on hash order when callers pass a sorted list.
pub fn closest<'a>(name: &str, candidates: impl IntoIterator<Item = &'a str>) -> Option<&'a str> {
    let limit = max_typo_distance(name.chars().count());
    candidates
        .into_iter()
        .filter(|c| !c.eq_ignore_ascii_case(name))
        .map(|c| (distance(name, c), c))
        .filter(|(d, _)| *d <= limit)
        .min_by_key(|(d, _)| *d)
        .map(|(_, c)| c)
}

/// ` Did you mean `{suggestion}`?`, or nothing, for appending to a message.
pub fn did_you_mean(suggestion: Option<&str>) -> String {
    suggestion.map_or_else(String::new, |s| format!(" Did you mean `{s}`?"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance_counts_edits() {
        assert_eq!(distance("length", "length"), 0);
        assert_eq!(distance("lenght", "length"), 2, "a swap is two edits");
        assert_eq!(distance("tolower", "toLower"), 0, "case does not count");
        assert_eq!(distance("", "abc"), 3);
        assert_eq!(distance("abc", ""), 3);
    }

    #[test]
    fn a_typo_finds_the_name_it_was_meant_to_be() {
        let functions = ["length", "toLower", "toUpper", "size"];
        assert_eq!(closest("lenght", functions), Some("length"));
        assert_eq!(closest("toLowr", functions), Some("toLower"));
        assert_eq!(closest("siz", functions), Some("size"));
    }

    #[test]
    fn a_name_that_is_nothing_like_any_candidate_gets_no_suggestion() {
        assert_eq!(closest("frobnicate", ["length", "size"]), None);
        // Short names allow one edit only, so `id` does not become `ab`.
        assert_eq!(closest("id", ["ab"]), None);
        assert_eq!(closest("x", std::iter::empty()), None);
    }

    #[test]
    fn the_closest_wins_and_ties_go_to_the_first() {
        assert_eq!(closest("nme", ["name", "nmae"]), Some("name"));
        assert_eq!(closest("abd", ["abc", "abe"]), Some("abc"));
    }

    #[test]
    fn the_same_name_in_another_case_is_not_a_suggestion() {
        assert_eq!(closest("LENGTH", ["length"]), None);
    }

    #[test]
    fn did_you_mean_is_a_sentence_or_nothing() {
        assert_eq!(did_you_mean(Some("length")), " Did you mean `length`?");
        assert_eq!(did_you_mean(None), "");
    }
}
