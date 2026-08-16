//! Substitution parameters derived from the dataset by sampling its
//! distributions, rather than hand-picked (#505).
//!
//! # Why this exists
//!
//! LDBC SNB query templates carry `{{personId}}`, `{{firstName}}`,
//! `{{tagName}}` and friends. Ids are assigned per `datagen` run and are not
//! portable between extracts, so a fixed set of parameters is right for
//! exactly one copy of the data. We had three ways of choosing them, and all
//! three were wrong in a different direction:
//!
//! * the **built-in defaults** resolve only against the extract the download
//!   script fetches — against any other one, every read returns zero rows and
//!   the suite measures nothing (#449, #450, #502);
//! * the **example params file** matched a different extract again;
//! * **deriving by "pick the busiest person"** guarantees non-empty results and
//!   guarantees *worst-case* ones. At SF10 that chose a person with 1,455
//!   friends against a median of 14 — a 104× outlier that every
//!   friend-traversal query then inherits (#505).
//!
//! `SLT-2` is a cross-engine comparison, and a comparison is only meaningful
//! if both engines answer the *same question*. A maximum-degree parameter asks
//! a different, much harder question than a competitor running LDBC's own
//! parameters would.
//!
//! # What this does instead
//!
//! LDBC's own generator ships substitution parameters chosen for a
//! representative distribution alongside the dataset. Where those are present
//! they should be used and this module is unnecessary. Where they are absent —
//! which is the case for every extract in this repo — this derives parameters
//! by **sampling the distribution at a stated percentile**, defaulting to the
//! median, and records what it sampled.
//!
//! Every choice is made against a percentile of a real distribution, and every
//! choice is reported in [`Provenance`] so a table of timings cannot be read
//! without knowing what it measured.
//!
//! # Why derivation reads the CSVs, not the loaded graph
//!
//! Deliberately: parameters must not depend on the engine under test. Reading
//! the source files means the same parameters can be handed to Neo4j, Kuzu or
//! FalkorDB unchanged, which is the entire point of deriving them.
//!
//! # Non-empty by construction
//!
//! Each parameter is drawn from the population the query that uses it will
//! actually traverse — `tagName` from tags on posts by the chosen person's
//! 2-hop neighbourhood, not from the tag table at large. A tag sampled from
//! the whole table is representative and returns nothing, which under #450
//! fails the run. Sampling the *reachable* population is what makes
//! "representative" and "non-empty" compatible.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;

/// How a parameter set was arrived at, printed above any table of timings.
#[derive(Debug, Clone)]
pub struct Provenance {
    /// The percentile of the KNOWS-degree distribution the anchor person was
    /// drawn from.
    pub percentile: u8,
    /// KNOWS degree of the chosen person.
    pub person_degree: usize,
    /// Median KNOWS degree across all persons, for comparison.
    pub median_degree: usize,
    /// Maximum KNOWS degree — the value the old "busiest person" rule picked.
    pub max_degree: usize,
    /// Persons reachable within 1, 2 and 3 KNOWS hops of the anchor.
    pub neighbourhood: [usize; 3],
    /// Graph distance from the anchor to `person2_id`, for the path queries.
    pub person2_distance: usize,
    /// One line per derived parameter saying what population it was sampled
    /// from and how large that population was.
    pub notes: Vec<String>,
}

impl Provenance {
    /// The header block printed above the results table.
    pub fn format(&self) -> String {
        let mut s = String::new();
        s.push_str(&format!(
            "Parameter provenance: derived from dataset at p{} of the KNOWS-degree distribution\n",
            self.percentile
        ));
        s.push_str(&format!(
            "  anchor person degree {} (median {}, max {})\n",
            self.person_degree, self.median_degree, self.max_degree
        ));
        s.push_str(&format!(
            "  neighbourhood 1/2/3 hops: {} / {} / {} persons; person2 at distance {}\n",
            self.neighbourhood[0], self.neighbourhood[1], self.neighbourhood[2], self.person2_distance
        ));
        for n in &self.notes {
            s.push_str(&format!("  {}\n", n));
        }
        s
    }
}

/// A parameter set with the record of how it was chosen.
#[derive(Debug, Clone)]
pub struct Derived {
    pub person_id: i64,
    pub person2_id: i64,
    pub message_id: i64,
    pub post_id: i64,
    pub first_name: String,
    pub country_x: String,
    pub country_y: String,
    pub tag_name: String,
    pub tag_class_name: String,
    pub organisation_name: String,
    pub max_date: i64,
    pub start_date: i64,
    pub end_date: i64,
    pub provenance: Provenance,
}

impl Derived {
    /// Serialise to the same JSON shape `--params-file` reads, with the
    /// provenance carried alongside as a comment block so a file committed to
    /// a repo still says where it came from.
    pub fn to_json(&self) -> String {
        let notes: Vec<String> = self
            .provenance
            .notes
            .iter()
            .map(|n| format!("    {:?}", n))
            .collect();
        format!(
            "{{\n  \"_provenance\": [\n    {:?},\n    {:?},\n    {:?},\n{}\n  ],\n  \
             \"personId\": {},\n  \"person2Id\": {},\n  \"messageId\": {},\n  \"postId\": {},\n  \
             \"firstName\": {:?},\n  \"countryX\": {:?},\n  \"countryY\": {:?},\n  \
             \"tagName\": {:?},\n  \"tagClassName\": {:?},\n  \"organisationName\": {:?},\n  \
             \"maxDate\": {},\n  \"startDate\": {},\n  \"endDate\": {}\n}}\n",
            format!(
                "Derived by sampling the dataset at p{} of the KNOWS-degree distribution (#505).",
                self.provenance.percentile
            ),
            format!(
                "Anchor person degree {} (median {}, max {}).",
                self.provenance.person_degree, self.provenance.median_degree, self.provenance.max_degree
            ),
            "Ids are per-datagen-run and not portable to another extract.",
            notes.join(",\n"),
            self.person_id,
            self.person2_id,
            self.message_id,
            self.post_id,
            self.first_name,
            self.country_x,
            self.country_y,
            self.tag_name,
            self.tag_class_name,
            self.organisation_name,
            self.max_date,
            self.start_date,
            self.end_date,
        )
    }
}

// ------------------------------------------------------------------ helpers

/// Read a `|`-separated LDBC CSV, calling `f` with the fields of each data row.
/// The header line is skipped; LDBC never quotes or escapes the separator.
fn for_each_row<F: FnMut(&[&str])>(path: &Path, mut f: F) -> Result<(), String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("read {}: {}", path.display(), e))?;
    for line in text.lines().skip(1) {
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('|').collect();
        f(&fields);
    }
    Ok(())
}

/// Parse an LDBC identifier.
///
/// Not every id column is written as an integer: `comment_replyOf_post_0_0.csv`
/// holds `1236950581248.0`, a float that `datagen` produced and that a plain
/// `parse::<i64>()` rejects. The loader already carries this workaround
/// (`ldbc_common::load_edges`); derivation needs it too, and the failure mode
/// if it does not is silent — every row fails to parse, the resulting set is
/// empty, and a filter built from that set matches everything instead of
/// nothing.
fn parse_id(s: &str) -> Option<i64> {
    s.parse().ok().or_else(|| s.split('.').next()?.parse().ok())
}

/// The value at `percentile` of a sorted slice, clamped to its bounds.
///
/// Nearest-rank, not interpolated: every parameter here is an identifier or a
/// name drawn from the sample, so an interpolated value would not exist in the
/// data.
fn at_percentile<T: Copy>(sorted: &[T], percentile: u8) -> Option<T> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (percentile as f64 / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted.get(rank.min(sorted.len() - 1)).copied()
}

/// Pick the key at the given percentile of a frequency distribution.
///
/// Ordered by (count, key) so the result is deterministic across runs and
/// across machines — a benchmark parameter that moves with `HashMap` iteration
/// order would make two runs incomparable for no reason.
fn sample_by_frequency(counts: &HashMap<String, usize>, percentile: u8) -> Option<(String, usize)> {
    if counts.is_empty() {
        return None;
    }
    let mut ranked: Vec<(&String, &usize)> = counts.iter().collect();
    ranked.sort_by(|a, b| a.1.cmp(b.1).then_with(|| a.0.cmp(b.0)));
    let rank = (percentile as f64 / 100.0 * (ranked.len() - 1) as f64).round() as usize;
    let (name, count) = ranked[rank.min(ranked.len() - 1)];
    Some((name.clone(), *count))
}

// ------------------------------------------------------------------ derive

/// Derive a parameter set from the CSVs under `data_dir`.
///
/// `percentile` selects where in each distribution to sample: 50 is the
/// median and the default, 90 gives a deliberately heavier — but still
/// stated — workload. Both are legitimate; publishing either without saying
/// which is not.
pub fn derive(data_dir: &Path, percentile: u8) -> Result<Derived, String> {
    let dynamic = data_dir.join("dynamic");
    let static_dir = data_dir.join("static");
    let mut notes = Vec::new();

    // ---- KNOWS adjacency, undirected and deduplicated.
    // LDBC emits the edge once per direction in some extracts and once in
    // others; deduplicating makes the derived degree mean the same thing
    // either way.
    let mut adj: HashMap<i64, HashSet<i64>> = HashMap::new();
    for_each_row(&dynamic.join("person_knows_person_0_0.csv"), |f| {
        if f.len() < 2 {
            return;
        }
        if let (Some(a), Some(b)) = (parse_id(f[0]), parse_id(f[1])) {
            adj.entry(a).or_default().insert(b);
            adj.entry(b).or_default().insert(a);
        }
    })?;
    if adj.is_empty() {
        return Err("no KNOWS edges found — is this an LDBC SNB CSV extract?".into());
    }

    // ---- Anchor person: the one at `percentile` of the degree distribution.
    let mut by_degree: Vec<(usize, i64)> = adj.iter().map(|(&id, ns)| (ns.len(), id)).collect();
    by_degree.sort_unstable();
    let degrees: Vec<usize> = by_degree.iter().map(|&(d, _)| d).collect();
    let median_degree = at_percentile(&degrees, 50).unwrap_or(0);
    let max_degree = *degrees.last().unwrap_or(&0);
    let (person_degree, person_id) =
        at_percentile(&by_degree, percentile).ok_or("empty degree distribution")?;

    // ---- Breadth-first levels 1..3 from the anchor.
    let mut distance: HashMap<i64, usize> = HashMap::from([(person_id, 0)]);
    let mut queue = VecDeque::from([person_id]);
    let mut levels: [Vec<i64>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    while let Some(cur) = queue.pop_front() {
        let d = distance[&cur];
        if d == 3 {
            continue;
        }
        for &next in adj.get(&cur).into_iter().flatten() {
            if !distance.contains_key(&next) {
                distance.insert(next, d + 1);
                levels[d].push(next);
                queue.push_back(next);
            }
        }
    }
    let hop1: HashSet<i64> = levels[0].iter().copied().collect();
    let hop2: HashSet<i64> = hop1.iter().chain(levels[1].iter()).copied().collect();
    let hop3: HashSet<i64> = hop2.iter().chain(levels[2].iter()).copied().collect();
    let neighbourhood = [hop1.len(), hop2.len(), hop3.len()];

    // ---- person2: the far end of the path queries. Distance 3 if the
    // neighbourhood reaches that far, so IC13/IC14 traverse rather than
    // answering in one hop; the nearest available level otherwise.
    let (person2_id, person2_distance) = levels[2]
        .iter()
        .min()
        .map(|&p| (p, 3))
        .or_else(|| levels[1].iter().min().map(|&p| (p, 2)))
        .or_else(|| levels[0].iter().min().map(|&p| (p, 1)))
        .ok_or("anchor person has no KNOWS neighbours")?;

    // ---- firstName: sampled from the names *inside* the 3-hop neighbourhood,
    // which is the population IC1 searches.
    let mut names_in_hop3: HashMap<String, usize> = HashMap::new();
    let mut person_first_name: HashMap<i64, String> = HashMap::new();
    for_each_row(&dynamic.join("person_0_0.csv"), |f| {
        if f.len() < 3 {
            return;
        }
        let Some(id) = parse_id(f[1]) else { return };
        let name = f[2].to_string();
        if hop3.contains(&id) && id != person_id {
            *names_in_hop3.entry(name.clone()).or_insert(0) += 1;
        }
        person_first_name.insert(id, name);
    })?;
    let (first_name, first_name_hits) = sample_by_frequency(&names_in_hop3, percentile)
        .ok_or("no persons within 3 hops of the anchor")?;
    notes.push(format!(
        "firstName {:?} — p{} of {} distinct names in the 3-hop neighbourhood, {} persons match",
        first_name,
        percentile,
        names_in_hop3.len(),
        first_name_hits
    ));

    // ---- Posts: creator and creation date. Kept as two maps because the
    // date window, the country choice and `postId` all sample the same rows.
    let mut post_creator: HashMap<i64, i64> = HashMap::new();
    for_each_row(&dynamic.join("post_hasCreator_person_0_0.csv"), |f| {
        if f.len() < 2 {
            return;
        }
        if let (Some(p), Some(c)) = (parse_id(f[0]), parse_id(f[1])) {
            post_creator.insert(p, c);
        }
    })?;
    let mut post_date: HashMap<i64, i64> = HashMap::new();
    for_each_row(&dynamic.join("post_0_0.csv"), |f| {
        if f.len() < 2 {
            return;
        }
        if let (Some(d), Some(id)) = (parse_id(f[0]), parse_id(f[1])) {
            post_date.insert(id, d);
        }
    })?;

    // Posts written by the 2-hop neighbourhood: the population IC2/IC3/IC4/IC9
    // aggregate over, and therefore the population the date window has to come
    // from if those queries are to return rows.
    let mut hop2_post_dates: Vec<i64> = post_creator
        .iter()
        .filter(|(_, c)| hop2.contains(c))
        .filter_map(|(p, _)| post_date.get(p).copied())
        .collect();
    hop2_post_dates.sort_unstable();
    if hop2_post_dates.is_empty() {
        return Err("the anchor's 2-hop neighbourhood wrote no posts — try a higher percentile".into());
    }
    // A window around the middle of the neighbourhood's activity, and an upper
    // bound above almost all of it. IC2/IC9 read `< maxDate`; IC3/IC4 read a
    // half-open window.
    let start_date = at_percentile(&hop2_post_dates, 40).unwrap();
    let end_date = at_percentile(&hop2_post_dates, 60).unwrap();
    let max_date = at_percentile(&hop2_post_dates, 95).unwrap();
    let in_window = hop2_post_dates
        .iter()
        .filter(|d| **d >= start_date && **d < end_date)
        .count();
    notes.push(format!(
        "date window [{}, {}) — p40..p60 of {} posts by the 2-hop neighbourhood, {} in window; maxDate {} at p95",
        start_date, end_date, hop2_post_dates.len(), in_window, max_date
    ));

    // ---- postId / messageId: a post and a comment by the anchor itself, so
    // the short reads that look them up are about the anchor rather than an
    // unrelated row. Falls back to the 1-hop neighbourhood.
    //
    // `require` narrows the candidates to messages that satisfy whatever the
    // query using the id also needs. IS7 reads the *replies* to `postId`, so a
    // post with no replies makes IS7 return nothing — which #450 correctly
    // fails the run over. "Authored by the anchor" is not by itself enough to
    // make an id usable; it has to be an id the query has an answer for.
    let pick_own = |owner: &HashMap<i64, i64>, who: i64, fallback: &HashSet<i64>, require: &HashSet<i64>| -> Option<i64> {
        let usable = |m: &i64| require.is_empty() || require.contains(m);
        let mut own: Vec<i64> = owner
            .iter()
            .filter(|(m, c)| **c == who && usable(m))
            .map(|(m, _)| *m)
            .collect();
        if own.is_empty() {
            own = owner
                .iter()
                .filter(|(m, c)| fallback.contains(c) && usable(m))
                .map(|(m, _)| *m)
                .collect();
        }
        own.sort_unstable();
        at_percentile(&own, 50)
    };

    let mut posts_with_replies: HashSet<i64> = HashSet::new();
    for_each_row(&dynamic.join("comment_replyOf_post_0_0.csv"), |f| {
        if f.len() < 2 {
            return;
        }
        if let Some(post) = parse_id(f[1]) {
            posts_with_replies.insert(post);
        }
    })?;
    let post_id = pick_own(&post_creator, person_id, &hop1, &posts_with_replies)
        .ok_or("no replied-to posts by the anchor or its immediate friends")?;

    let mut comment_creator: HashMap<i64, i64> = HashMap::new();
    for_each_row(&dynamic.join("comment_hasCreator_person_0_0.csv"), |f| {
        if f.len() < 2 {
            return;
        }
        if let (Some(c), Some(p)) = (parse_id(f[0]), parse_id(f[1])) {
            comment_creator.insert(c, p);
        }
    })?;
    let message_id = pick_own(&comment_creator, person_id, &hop1, &HashSet::new())
        .ok_or("no comments by the anchor or its immediate friends")?;
    notes.push(format!(
        "postId {} (has replies, so IS7 is answerable) and messageId {} — authored by the anchor where it has any, else by a 1-hop friend",
        post_id, message_id
    ));

    // ---- tagName: sampled from tags on posts by the 2-hop neighbourhood,
    // which is exactly the set IC6 intersects.
    let mut tag_name_of: HashMap<i64, String> = HashMap::new();
    for_each_row(&static_dir.join("tag_0_0.csv"), |f| {
        if f.len() < 2 {
            return;
        }
        if let Some(id) = parse_id(f[0]) {
            tag_name_of.insert(id, f[1].to_string());
        }
    })?;
    let mut hop2_tag_counts: HashMap<String, usize> = HashMap::new();
    let mut hop2_tag_ids: HashSet<i64> = HashSet::new();
    for_each_row(&dynamic.join("post_hasTag_tag_0_0.csv"), |f| {
        if f.len() < 2 {
            return;
        }
        let (Some(post), Some(tag)) = (parse_id(f[0]), parse_id(f[1])) else { return };
        if post_creator.get(&post).is_some_and(|c| hop2.contains(c)) {
            hop2_tag_ids.insert(tag);
            if let Some(name) = tag_name_of.get(&tag) {
                *hop2_tag_counts.entry(name.clone()).or_insert(0) += 1;
            }
        }
    })?;
    let (tag_name, tag_hits) = sample_by_frequency(&hop2_tag_counts, percentile)
        .ok_or("no tagged posts within 2 hops of the anchor")?;
    notes.push(format!(
        "tagName {:?} — p{} of {} distinct tags on posts by the 2-hop neighbourhood, {} posts carry it",
        tag_name, percentile, hop2_tag_counts.len(), tag_hits
    ));

    // ---- countryX / countryY: two countries the neighbourhood actually
    // posted from, inside the derived date window. Sampled either side of the
    // percentile so they are distinct without either being the maximum.
    let mut place_name: HashMap<i64, String> = HashMap::new();
    let mut is_country: HashSet<i64> = HashSet::new();
    for_each_row(&static_dir.join("place_0_0.csv"), |f| {
        if f.len() < 4 {
            return;
        }
        if let Some(id) = parse_id(f[0]) {
            place_name.insert(id, f[1].to_string());
            if f[3] == "Country" {
                is_country.insert(id);
            }
        }
    })?;
    let mut country_counts: HashMap<String, usize> = HashMap::new();
    for_each_row(&dynamic.join("post_isLocatedIn_place_0_0.csv"), |f| {
        if f.len() < 2 {
            return;
        }
        let (Some(post), Some(place)) = (parse_id(f[0]), parse_id(f[1])) else { return };
        if !is_country.contains(&place) {
            return;
        }
        if !post_creator.get(&post).is_some_and(|c| hop2.contains(c)) {
            return;
        }
        let Some(&d) = post_date.get(&post) else { return };
        if d >= start_date && d < end_date {
            if let Some(name) = place_name.get(&place) {
                *country_counts.entry(name.clone()).or_insert(0) += 1;
            }
        }
    })?;
    let (country_x, cx_hits) = sample_by_frequency(&country_counts, percentile)
        .ok_or("no posts located in a country within the derived window")?;
    // The neighbour rank, so the two are distinct; falls back to the same
    // country if the neighbourhood only ever posted from one, which IC3 still
    // answers (the predicate is a disjunction).
    let (country_y, cy_hits) = sample_by_frequency(
        &country_counts.iter().filter(|(k, _)| **k != country_x).map(|(k, v)| (k.clone(), *v)).collect(),
        percentile,
    )
    .unwrap_or((country_x.clone(), cx_hits));
    notes.push(format!(
        "countryX {:?} ({} posts) / countryY {:?} ({} posts) — p{} of {} countries the 2-hop neighbourhood posted from in the window",
        country_x, cx_hits, country_y, cy_hits, percentile, country_counts.len()
    ));

    // ---- tagClassName: the class of a tag the neighbourhood's posts carry.
    let mut tagclass_name: HashMap<i64, String> = HashMap::new();
    for_each_row(&static_dir.join("tagclass_0_0.csv"), |f| {
        if f.len() < 2 {
            return;
        }
        if let Some(id) = parse_id(f[0]) {
            tagclass_name.insert(id, f[1].to_string());
        }
    })?;
    let mut class_counts: HashMap<String, usize> = HashMap::new();
    for_each_row(&static_dir.join("tag_hasType_tagclass_0_0.csv"), |f| {
        if f.len() < 2 {
            return;
        }
        let (Some(tag), Some(class)) = (parse_id(f[0]), parse_id(f[1])) else { return };
        if hop2_tag_ids.contains(&tag) {
            if let Some(name) = tagclass_name.get(&class) {
                *class_counts.entry(name.clone()).or_insert(0) += 1;
            }
        }
    })?;
    let (tag_class_name, class_hits) = sample_by_frequency(&class_counts, percentile)
        .ok_or("no tag classes reachable from the anchor's neighbourhood")?;
    notes.push(format!(
        "tagClassName {:?} — p{} of {} classes on the neighbourhood's tags, {} tags in it",
        tag_class_name, percentile, class_counts.len(), class_hits
    ));

    // ---- organisationName: IC11 asks for friends-of-friends who worked
    // somewhere before a cutoff. This was hard-coded to "MDLR_Airlines",
    // which is a substitution parameter that was never parameterised — on any
    // other extract IC11 silently returned nothing.
    let mut org_name: HashMap<i64, String> = HashMap::new();
    for_each_row(&static_dir.join("organisation_0_0.csv"), |f| {
        if f.len() < 3 {
            return;
        }
        if let Some(id) = parse_id(f[0]) {
            org_name.insert(id, f[2].to_string());
        }
    })?;
    let mut employer_counts: HashMap<String, usize> = HashMap::new();
    for_each_row(&dynamic.join("person_workAt_organisation_0_0.csv"), |f| {
        if f.len() < 3 {
            return;
        }
        let (Some(p), Some(o)) = (parse_id(f[0]), parse_id(f[1])) else { return };
        let Some(from) = parse_id(f[2]) else { return };
        if from < 2012 && hop2.contains(&p) {
            if let Some(name) = org_name.get(&o) {
                *employer_counts.entry(name.clone()).or_insert(0) += 1;
            }
        }
    })?;
    let (organisation_name, org_hits) = sample_by_frequency(&employer_counts, percentile)
        .ok_or("nobody within 2 hops of the anchor worked anywhere before 2012")?;
    notes.push(format!(
        "organisationName {:?} — p{} of {} employers of the 2-hop neighbourhood before 2012, {} employees",
        organisation_name, percentile, employer_counts.len(), org_hits
    ));

    Ok(Derived {
        person_id,
        person2_id,
        message_id,
        post_id,
        first_name,
        country_x,
        country_y,
        tag_name,
        tag_class_name,
        organisation_name,
        max_date,
        start_date,
        end_date,
        provenance: Provenance {
            percentile,
            person_degree,
            median_degree,
            max_degree,
            neighbourhood,
            person2_distance,
            notes,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_is_nearest_rank_and_clamped() {
        let xs = [10, 20, 30, 40, 50];
        assert_eq!(at_percentile(&xs, 0), Some(10));
        assert_eq!(at_percentile(&xs, 50), Some(30));
        assert_eq!(at_percentile(&xs, 100), Some(50));
        // Nearest rank, never an interpolated value: every parameter is drawn
        // from the sample, so a value between two samples does not exist.
        assert_eq!(at_percentile(&xs, 30), Some(20));
        assert_eq!(at_percentile::<i32>(&[], 50), None);
    }

    #[test]
    fn percentile_of_a_single_sample_is_that_sample() {
        assert_eq!(at_percentile(&[7], 0), Some(7));
        assert_eq!(at_percentile(&[7], 100), Some(7));
    }

    #[test]
    fn frequency_sampling_picks_the_median_not_the_mode() {
        // The whole point of #505: the busiest value is the wrong choice.
        let counts = HashMap::from([
            ("rare".to_string(), 1),
            ("typical".to_string(), 5),
            ("everywhere".to_string(), 900),
        ]);
        assert_eq!(sample_by_frequency(&counts, 50), Some(("typical".into(), 5)));
        assert_eq!(sample_by_frequency(&counts, 100), Some(("everywhere".into(), 900)));
        assert_eq!(sample_by_frequency(&counts, 0), Some(("rare".into(), 1)));
    }

    #[test]
    fn frequency_sampling_is_deterministic_under_ties() {
        // Two runs must choose the same parameter, or their timings are not
        // comparable. HashMap iteration order is not stable, so the tie-break
        // has to be on the key.
        let counts = HashMap::from([
            ("b".to_string(), 3),
            ("a".to_string(), 3),
            ("c".to_string(), 3),
        ]);
        let first = sample_by_frequency(&counts, 50);
        for _ in 0..20 {
            assert_eq!(sample_by_frequency(&counts, 50), first);
        }
        assert_eq!(first, Some(("b".into(), 3)));
    }

    #[test]
    fn ids_written_as_floats_still_parse() {
        // `comment_replyOf_post_0_0.csv` holds `1236950581248.0`. Rejecting
        // those rows silently emptied the "posts that have replies" set, and
        // an empty filter matches everything rather than nothing -- so the
        // derivation happily returned a post with no replies and IS7 came back
        // empty while claiming otherwise.
        assert_eq!(parse_id("1236950581248"), Some(1236950581248));
        assert_eq!(parse_id("1236950581248.0"), Some(1236950581248));
        assert_eq!(parse_id(""), None);
        assert_eq!(parse_id("Person.id"), None);
    }

    #[test]
    fn frequency_sampling_of_nothing_is_none() {
        assert_eq!(sample_by_frequency(&HashMap::new(), 50), None);
    }

    #[test]
    fn provenance_states_the_percentile_and_the_outlier_it_avoided() {
        let p = Provenance {
            percentile: 50,
            person_degree: 14,
            median_degree: 14,
            max_degree: 1455,
            neighbourhood: [14, 300, 4000],
            person2_distance: 3,
            notes: vec!["tagName \"X\" — 12 posts carry it".into()],
        };
        let text = p.format();
        assert!(text.contains("p50"), "the percentile must be visible: {text}");
        assert!(text.contains("median 14"), "{text}");
        assert!(text.contains("max 1455"), "the outlier not chosen: {text}");
        assert!(text.contains("tagName"), "{text}");
    }
}
