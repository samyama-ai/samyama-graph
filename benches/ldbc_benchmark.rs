//! LDBC SNB Interactive Query Benchmark — Samyama Graph Database
//!
//! Benchmarks Samyama's query engine against all 21 LDBC SNB Interactive workload queries
//! (IS1-IS7, IC1-IC14) plus 8 update operations (INS1-INS8) via --updates flag.
//!
//! Prerequisites:
//!   Download and extract LDBC SF1 data to:
//!     data/ldbc-sf1/social_network-sf1-CsvBasic-LongDateFormatter/
//!
//! Usage:
//!   cargo bench --bench ldbc_benchmark
//!   cargo bench --bench ldbc_benchmark -- --runs 10
//!   cargo bench --bench ldbc_benchmark -- --query IS1
//!   cargo bench --bench ldbc_benchmark -- --data-dir /path/to/data

use std::path::PathBuf;
use std::time::{Duration, Instant};

use samyama_sdk::{EmbeddedClient, SamyamaClient};

#[path = "common/bench_setup.rs"]
mod bench_setup;

mod ldbc_common;
use ldbc_common::{format_duration, format_num};

type Error = Box<dyn std::error::Error>;

// ============================================================================
// QUERY DEFINITIONS
// ============================================================================

struct LdbcQuery {
    id: &'static str,
    name: &'static str,
    cypher: &'static str,
    category: &'static str,
}

/// LDBC SNB query substitution parameters, externalized per scale factor.
///
/// Query templates use `{{placeholder}}` tokens; these are filled from a
/// `--params-file <json>` (see the competitor-benchmarks repo,
/// `config/ldbc-snb-interactive/<sf>.json`). Omitted fields fall back to the
/// SF1 defaults below (person 933 = "Mahinda Perera"). Static reference data
/// (countries, tag class) and the dataset's time span are scale-independent, so
/// a larger-scale config need only override the entity ids, `firstName`, and `tagName`.
#[derive(serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Params {
    person_id: i64,
    person2_id: i64,
    message_id: i64,
    post_id: i64,
    first_name: String,
    country_x: String,
    country_y: String,
    tag_name: String,
    tag_class_name: String,
    /// IC11's employer. This was hard-coded in the query template as
    /// `"MDLR_Airlines"` -- a substitution parameter that was never
    /// parameterised, so IC11 returned nothing on any extract but the one it
    /// was written against (#505).
    organisation_name: String,
    max_date: i64,
    start_date: i64,
    end_date: i64,
}

impl Default for Params {
    fn default() -> Self {
        Params {
            person_id: 933,
            person2_id: 4139,
            message_id: 1236950581249,
            post_id: 1236950581248,
            first_name: "Mahinda".into(),
            country_x: "India".into(),
            country_y: "Pakistan".into(),
            tag_name: "Hamid_Karzai".into(),
            tag_class_name: "MusicalArtist".into(),
            organisation_name: "MDLR_Airlines".into(),
            max_date: 1354320000000,
            start_date: 1338508800000,
            end_date: 1341100800000,
        }
    }
}

impl From<ldbc_common::params::Derived> for Params {
    fn from(d: ldbc_common::params::Derived) -> Self {
        Params {
            person_id: d.person_id,
            person2_id: d.person2_id,
            message_id: d.message_id,
            post_id: d.post_id,
            first_name: d.first_name,
            country_x: d.country_x,
            country_y: d.country_y,
            tag_name: d.tag_name,
            tag_class_name: d.tag_class_name,
            organisation_name: d.organisation_name,
            max_date: d.max_date,
            start_date: d.start_date,
            end_date: d.end_date,
        }
    }
}

impl Params {
    fn load(path: &str) -> Result<Self, String> {
        let s = std::fs::read_to_string(path).map_err(|e| format!("read {}: {}", path, e))?;
        serde_json::from_str(&s).map_err(|e| format!("parse {}: {}", path, e))
    }

    /// Substitute `{{token}}` placeholders in a query template.
    fn apply(&self, template: &str) -> String {
        template
            .replace("{{personId}}", &self.person_id.to_string())
            .replace("{{person2Id}}", &self.person2_id.to_string())
            .replace("{{messageId}}", &self.message_id.to_string())
            .replace("{{postId}}", &self.post_id.to_string())
            .replace("{{firstName}}", &self.first_name)
            .replace("{{countryX}}", &self.country_x)
            .replace("{{countryY}}", &self.country_y)
            .replace("{{tagName}}", &self.tag_name)
            .replace("{{tagClassName}}", &self.tag_class_name)
            .replace("{{organisationName}}", &self.organisation_name)
            .replace("{{maxDate}}", &self.max_date.to_string())
            .replace("{{startDate}}", &self.start_date.to_string())
            .replace("{{endDate}}", &self.end_date.to_string())
    }
}

/// Build the list of 21 LDBC SNB Interactive queries adapted for Samyama.
///
/// Parameter choices (from SF1 dataset):
///   personId    = {{personId}}              ({{firstName}} Perera)
///   person2Id   = {{person2Id}}             ({{firstName}}'s first KNOWS target)
///   messageId   = {{messageId}}    (first comment)
///   postId      = {{postId}}    (first post, by person {{personId}})
///   firstName   = "{{firstName}}"
///   countryX    = "{{countryX}}", countryY = "{{countryY}}"
///   tagName     = "{{tagName}}"
///   tagClassName = "{{tagClassName}}"
///   maxDate     = {{maxDate}}    (2012-12-01)
///   startDate   = {{startDate}}    (2012-06-01)
///   endDate     = {{endDate}}    (2012-07-01)
fn ldbc_queries() -> Vec<LdbcQuery> {
    vec![
        // ================================================================
        // SHORT READS (IS1 - IS7)
        // ================================================================

        LdbcQuery {
            id: "IS1",
            name: "Person Profile",
            category: "short",
            cypher: "\
MATCH (p:Person {id: {{personId}}})
RETURN p.firstName, p.lastName, p.birthday, p.locationIP, p.browserUsed, p.gender, p.creationDate",
        },

        LdbcQuery {
            id: "IS2",
            name: "Recent Posts by Person",
            category: "short",
            // Adapted: query Posts only (Comment variant would be a separate UNION)
            cypher: "\
MATCH (p:Person {id: {{personId}}})<-[:HAS_CREATOR]-(m:Post)
RETURN m.id, m.content, m.creationDate
ORDER BY m.creationDate DESC
LIMIT 10",
        },

        LdbcQuery {
            id: "IS3",
            name: "Friends of Person",
            category: "short",
            cypher: "\
MATCH (p:Person {id: {{personId}}})-[:KNOWS]-(friend:Person)
RETURN friend.id, friend.firstName, friend.lastName
ORDER BY friend.firstName, friend.lastName",
        },

        LdbcQuery {
            id: "IS4",
            name: "Post Content",
            category: "short",
            cypher: "\
MATCH (m:Post {id: {{postId}}})
RETURN m.creationDate, coalesce(m.content, m.imageFile)",
        },

        LdbcQuery {
            id: "IS5",
            name: "Post Creator",
            category: "short",
            cypher: "\
MATCH (m:Post {id: {{postId}}})-[:HAS_CREATOR]->(p:Person)
RETURN p.id, p.firstName, p.lastName",
        },

        LdbcQuery {
            id: "IS6",
            name: "Forum of Post",
            category: "short",
            cypher: "\
MATCH (m:Post {id: {{postId}}})<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person)
RETURN f.id, f.title, mod.id, mod.firstName, mod.lastName",
        },

        LdbcQuery {
            id: "IS7",
            name: "Replies to Post",
            category: "short",
            // LDBC IS7: replies with isKnows check — uses EXISTS subquery (equivalent to OPTIONAL MATCH + CASE)
            // Note: OPTIONAL MATCH version is semantically correct but triggers full Post scan in planner
            cypher: "\
MATCH (m:Post {id: {{postId}}})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(author:Person)
MATCH (m)-[:HAS_CREATOR]->(op:Person)
RETURN c.id, c.content, c.creationDate, author.id, author.firstName, author.lastName, EXISTS { MATCH (op)-[:KNOWS]-(author) } AS isKnows
ORDER BY c.creationDate DESC
LIMIT 20",
        },

        // ================================================================
        // COMPLEX READS (IC1 - IC12)
        // ================================================================

        LdbcQuery {
            id: "IC1",
            name: "Transitive Friends by Name",
            category: "complex",
            // Friends up to distance 3 with a given first name
            cypher: "\
MATCH (p:Person {id: {{personId}}})-[:KNOWS*1..3]-(friend:Person {firstName: \"{{firstName}}\"})
WHERE friend.id <> {{personId}}
RETURN DISTINCT friend.id, friend.lastName, friend.birthday, friend.creationDate,
       friend.gender, friend.browserUsed, friend.locationIP
ORDER BY friend.lastName
LIMIT 20",
        },

        LdbcQuery {
            id: "IC2",
            name: "Recent Friend Posts",
            category: "complex",
            // Recent posts by direct friends
            cypher: "\
MATCH (p:Person {id: {{personId}}})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(m:Post)
WHERE m.creationDate < {{maxDate}}
RETURN friend.id, friend.firstName, friend.lastName,
       m.id, m.content, m.creationDate
ORDER BY m.creationDate DESC
LIMIT 20",
        },

        LdbcQuery {
            id: "IC3",
            name: "Friends in Countries",
            category: "complex",
            // Friends who posted in two given countries within a date range
            cypher: "\
MATCH (p:Person {id: {{personId}}})-[:KNOWS*1..2]-(friend:Person)
WHERE friend.id <> {{personId}}
WITH DISTINCT friend
MATCH (friend)<-[:HAS_CREATOR]-(m:Post)-[:IS_LOCATED_IN]->(place:Place)
WHERE m.creationDate >= {{startDate}} AND m.creationDate < {{endDate}}
  AND (place.name = \"{{countryX}}\" OR place.name = \"{{countryY}}\")
RETURN friend.id, friend.firstName, friend.lastName, count(m) AS msgCount
ORDER BY msgCount DESC
LIMIT 20",
        },

        LdbcQuery {
            id: "IC4",
            name: "Popular Tags in Period",
            category: "complex",
            // Tags on posts created by friends within a date window
            cypher: "\
MATCH (p:Person {id: {{personId}}})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag)
WHERE post.creationDate >= {{startDate}} AND post.creationDate < {{endDate}}
RETURN tag.name, count(post) AS postCount
ORDER BY postCount DESC
LIMIT 10",
        },

        LdbcQuery {
            id: "IC5",
            name: "New Forum Members",
            category: "complex",
            // Forums joined by friends-of-friends after a given date
            cypher: "\
MATCH (p:Person {id: {{personId}}})-[:KNOWS*1..2]-(friend:Person)
WHERE friend.id <> {{personId}}
WITH DISTINCT friend
MATCH (friend)<-[:HAS_MEMBER]-(forum:Forum)
RETURN forum.id, forum.title, count(friend) AS memberCount
ORDER BY memberCount DESC
LIMIT 20",
        },

        LdbcQuery {
            id: "IC6",
            name: "Tag Co-occurrence",
            category: "complex",
            // Tags that co-occur with a given tag on posts by friends-of-friends
            cypher: "\
MATCH (p:Person {id: {{personId}}})-[:KNOWS*1..2]-(friend:Person)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag {name: \"{{tagName}}\"})
WHERE friend.id <> {{personId}}
WITH DISTINCT post
MATCH (post)-[:HAS_TAG]->(otherTag:Tag)
WHERE otherTag.name <> \"{{tagName}}\"
RETURN otherTag.name, count(post) AS postCount
ORDER BY postCount DESC
LIMIT 10",
        },

        LdbcQuery {
            id: "IC7",
            name: "Recent Likers",
            category: "complex",
            // People who liked a person's posts, with recency
            cypher: "\
MATCH (p:Person {id: {{personId}}})<-[:HAS_CREATOR]-(m:Post)<-[:LIKES]-(liker:Person)
RETURN liker.id, liker.firstName, liker.lastName, m.id, m.creationDate
ORDER BY m.creationDate DESC
LIMIT 20",
        },

        LdbcQuery {
            id: "IC8",
            name: "Recent Replies",
            category: "complex",
            // Recent reply-comments to a person's posts
            cypher: "\
MATCH (p:Person {id: {{personId}}})<-[:HAS_CREATOR]-(m:Post)<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(author:Person)
RETURN author.id, author.firstName, author.lastName, c.creationDate, c.id, c.content
ORDER BY c.creationDate DESC
LIMIT 20",
        },

        LdbcQuery {
            id: "IC9",
            name: "Recent FoF Posts",
            category: "complex",
            // Recent posts by friends-of-friends
            cypher: "\
MATCH (p:Person {id: {{personId}}})-[:KNOWS*1..2]-(friend:Person)<-[:HAS_CREATOR]-(m:Post)
WHERE friend.id <> {{personId}} AND m.creationDate < {{maxDate}}
RETURN DISTINCT friend.id, friend.firstName, friend.lastName,
       m.id, coalesce(m.content, m.imageFile), m.creationDate
ORDER BY m.creationDate DESC
LIMIT 20",
        },

        LdbcQuery {
            id: "IC10",
            name: "Friend Recommendation",
            category: "complex",
            // Full LDBC IC10: friends-of-friends NOT already friends, ranked by shared interests
            cypher: "\
MATCH (p:Person {id: {{personId}}})-[:KNOWS*2]-(stranger:Person)
WHERE stranger.id <> {{personId}} AND NOT EXISTS { MATCH (p)-[:KNOWS]-(stranger) }
WITH DISTINCT stranger
MATCH (stranger)-[:HAS_INTEREST]->(tag:Tag)
RETURN stranger.id, stranger.firstName, stranger.lastName, count(tag) AS commonInterests
ORDER BY commonInterests DESC
LIMIT 10",
        },

        LdbcQuery {
            id: "IC11",
            name: "Job Referral",
            category: "complex",
            // Friends-of-friends who worked at a company before a given year
            cypher: "\
MATCH (p:Person {id: {{personId}}})-[:KNOWS*1..2]-(friend:Person)-[wa:WORK_AT]->(org:Organisation)
WHERE friend.id <> {{personId}} AND org.name = \"{{organisationName}}\" AND wa.workFrom < 2012
RETURN DISTINCT friend.id, friend.firstName, friend.lastName, wa.workFrom, org.name
ORDER BY wa.workFrom
LIMIT 10",
        },

        LdbcQuery {
            id: "IC12",
            name: "Expert Reply",
            category: "complex",
            // Full LDBC IC12: friends who replied to posts tagged with a given tag class, count distinct replies
            cypher: "\
MATCH (p:Person {id: {{personId}}})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tc:TagClass)
WHERE tc.name = \"{{tagClassName}}\"
RETURN friend.id, friend.firstName, friend.lastName, count(DISTINCT c) AS replyCount
ORDER BY replyCount DESC
LIMIT 10",
        },

        // IC13: Shortest path (uses shortestPath pattern)
        LdbcQuery {
            id: "IC13",
            name: "Single Shortest Path",
            category: "complex",
            cypher: "\
MATCH p = shortestPath((p1:Person {id: {{personId}}})-[:KNOWS*]-(p2:Person {id: {{person2Id}}}))
RETURN length(p) AS pathLength",
        },

        // IC14: Weighted paths (uses allShortestPaths)
        LdbcQuery {
            id: "IC14",
            name: "Trusted Connection Paths",
            category: "complex",
            cypher: "\
MATCH p = allShortestPaths((p1:Person {id: {{personId}}})-[:KNOWS*]-(p2:Person {id: {{person2Id}}}))
RETURN length(p) AS pathLength, nodes(p) AS pathNodes",
        },
    ]
}

/// Build the list of 8 LDBC SNB Interactive update operations
fn ldbc_updates() -> Vec<LdbcQuery> {
    vec![
        LdbcQuery {
            id: "INS1",
            name: "Add Person",
            category: "update",
            cypher: "\
CREATE (p:Person {id: 999999, firstName: \"TestUser\", lastName: \"Benchmark\", gender: \"male\", birthday: 631152000000, creationDate: 1709251200000, locationIP: \"1.2.3.4\", browserUsed: \"Firefox\"})",
        },
        LdbcQuery {
            id: "INS2",
            name: "Add Like to Post",
            category: "update",
            cypher: "\
MATCH (p:Person {id: 999999}), (m:Post {id: {{postId}}})
CREATE (p)-[:LIKES {creationDate: 1709251200000}]->(m)",
        },
        LdbcQuery {
            id: "INS3",
            name: "Add Like to Comment",
            category: "update",
            cypher: "\
MATCH (p:Person {id: 999999}), (m:Comment {id: {{messageId}}})
CREATE (p)-[:LIKES {creationDate: 1709251200000}]->(m)",
        },
        LdbcQuery {
            id: "INS4",
            name: "Add Forum",
            category: "update",
            cypher: "\
CREATE (f:Forum {id: 999998, title: \"Benchmark Forum\", creationDate: 1709251200000})",
        },
        LdbcQuery {
            id: "INS5",
            name: "Add Forum Member",
            category: "update",
            cypher: "\
MATCH (f:Forum {id: 999998}), (p:Person {id: {{personId}}})
CREATE (f)-[:HAS_MEMBER {joinDate: 1709251200000}]->(p)",
        },
        LdbcQuery {
            id: "INS6",
            name: "Add Post",
            category: "update",
            cypher: "\
CREATE (m:Post {id: 999997, imageFile: \"\", creationDate: 1709251200000, locationIP: \"1.2.3.4\", browserUsed: \"Firefox\", language: \"en\", content: \"Benchmark post content\", length: 24})",
        },
        LdbcQuery {
            id: "INS7",
            name: "Add Comment",
            category: "update",
            cypher: "\
CREATE (c:Comment {id: 999996, creationDate: 1709251200000, locationIP: \"1.2.3.4\", browserUsed: \"Firefox\", content: \"Benchmark comment\", length: 18})",
        },
        LdbcQuery {
            id: "INS8",
            name: "Add Friendship",
            category: "update",
            cypher: "\
MATCH (p1:Person {id: {{personId}}}), (p2:Person {id: 999999})
CREATE (p1)-[:KNOWS {creationDate: 1709251200000}]->(p2)",
        },
    ]
}

/// Build the list of 8 LDBC SNB Interactive v2 delete operations.
///
/// These mirror the INS1-INS8 inserts: they delete the entities created by
/// the insert operations.  Execute order: reads -> INS1-8 -> DEL1-8.
fn ldbc_deletes() -> Vec<LdbcQuery> {
    vec![
        // DEL-1: Remove a Person (cascading via DETACH DELETE)
        LdbcQuery {
            id: "DEL1",
            name: "Delete Person",
            category: "delete",
            cypher: "\
MATCH (p:Person {id: 999999})
DETACH DELETE p",
        },
        // DEL-2: Remove LIKES edge from Person to Post
        LdbcQuery {
            id: "DEL2",
            name: "Delete Like (Post)",
            category: "delete",
            cypher: "\
MATCH (p:Person {id: {{personId}}})-[l:LIKES]->(m:Post {id: {{postId}}})
DELETE l",
        },
        // DEL-3: Remove LIKES edge from Person to Comment
        LdbcQuery {
            id: "DEL3",
            name: "Delete Like (Comment)",
            category: "delete",
            cypher: "\
MATCH (p:Person {id: {{personId}}})-[l:LIKES]->(c:Comment {id: {{messageId}}})
DELETE l",
        },
        // DEL-4: Remove a Forum (cascading via DETACH DELETE)
        LdbcQuery {
            id: "DEL4",
            name: "Delete Forum",
            category: "delete",
            cypher: "\
MATCH (f:Forum {id: 999998})
DETACH DELETE f",
        },
        // DEL-5: Remove HAS_MEMBER edge
        LdbcQuery {
            id: "DEL5",
            name: "Delete Forum Member",
            category: "delete",
            cypher: "\
MATCH (f:Forum {id: 999998})-[m:HAS_MEMBER]->(p:Person {id: {{personId}}})
DELETE m",
        },
        // DEL-6: Remove a Post (cascading via DETACH DELETE)
        LdbcQuery {
            id: "DEL6",
            name: "Delete Post",
            category: "delete",
            cypher: "\
MATCH (m:Post {id: 999997})
DETACH DELETE m",
        },
        // DEL-7: Remove a Comment (cascading via DETACH DELETE)
        LdbcQuery {
            id: "DEL7",
            name: "Delete Comment",
            category: "delete",
            cypher: "\
MATCH (c:Comment {id: 999996})
DETACH DELETE c",
        },
        // DEL-8: Remove KNOWS edge
        LdbcQuery {
            id: "DEL8",
            name: "Delete Friendship",
            category: "delete",
            cypher: "\
MATCH (p1:Person {id: {{personId}}})-[k:KNOWS]->(p2:Person {id: 999999})
DELETE k",
        },
    ]
}

// ============================================================================
// BENCHMARK RUNNER
// ============================================================================

struct BenchResult {
    id: &'static str,
    name: &'static str,
    rows: usize,
    min: Duration,
    median: Duration,
    max: Duration,
    error: Option<String>,
}

fn format_ms(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 1.0 {
        format!("{:.2}ms", ms)
    } else if ms < 100.0 {
        format!("{:.1}ms", ms)
    } else if ms < 10_000.0 {
        format!("{:.0}ms", ms)
    } else {
        format!("{:.1}s", d.as_secs_f64())
    }
}

async fn run_benchmark(
    client: &EmbeddedClient,
    query: &LdbcQuery,
    cypher: &str,
    runs: usize,
) -> BenchResult {
    let is_update = query.category == "update" || query.category == "delete";
    // Warm-up: 1 run, discard (skip for updates — they mutate state)
    let warmup = if is_update {
        client.query("default", cypher).await
    } else {
        client.query_readonly("default", cypher).await
    };
    if let Err(e) = &warmup {
        return BenchResult {
            id: query.id,
            name: query.name,

            rows: 0,
            min: Duration::ZERO,
            median: Duration::ZERO,
            max: Duration::ZERO,
            error: Some(e.to_string()),
        };
    }

    let mut timings = Vec::with_capacity(runs);
    let mut row_count = 0;

    let actual_runs = if is_update { 1 } else { runs }; // updates run once
    for _ in 0..actual_runs {
        let start = Instant::now();
        let run_result = if is_update {
            client.query("default", cypher).await
        } else {
            client.query_readonly("default", cypher).await
        };
        match run_result {
            Ok(result) => {
                row_count = result.records.len();
                timings.push(start.elapsed());
            }
            Err(e) => {
                return BenchResult {
                    id: query.id,
                    name: query.name,
        
                    rows: 0,
                    min: Duration::ZERO,
                    median: Duration::ZERO,
                    max: Duration::ZERO,
                    error: Some(e.to_string()),
                };
            }
        }
    }

    timings.sort();

    BenchResult {
        id: query.id,
        name: query.name,
        rows: row_count,
        min: timings[0],
        median: timings[timings.len() / 2],
        max: timings[timings.len() - 1],
        error: None,
    }
}

// ============================================================================
// MAIN
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Error> {
    bench_setup::init();

    let args: Vec<String> = std::env::args().collect();

    let default_dir = "data/ldbc-sf1/social_network-sf1-CsvBasic-LongDateFormatter";
    let explicit_data_dir = args.iter().any(|a| a == "--data-dir");
    let data_dir = if let Some(pos) = args.iter().position(|a| a == "--data-dir") {
        PathBuf::from(args.get(pos + 1).expect("--data-dir requires a path argument"))
    } else {
        PathBuf::from(default_dir)
    };

    let runs: usize = if let Some(pos) = args.iter().position(|a| a == "--runs") {
        args.get(pos + 1).expect("--runs requires a number").parse().expect("--runs must be a positive integer")
    } else {
        5
    };

    let filter_query: Option<String> = if let Some(pos) = args.iter().position(|a| a == "--query") {
        Some(args.get(pos + 1).expect("--query requires a query ID (e.g. IS1, IC3)").to_uppercase())
    } else {
        None
    };

    let include_updates = args.iter().any(|a| a == "--updates");
    let include_deletes = args.iter().any(|a| a == "--deletes");

    // Substitution parameters, in precedence order:
    //   --derive-params [percentile]  sample them from this dataset (#505)
    //   --params-file <json>          a set someone already derived
    //   (neither)                     the built-in defaults, which resolve
    //                                 against exactly one extract
    //
    // `--write-params <path>` dumps whatever was derived, so a run can be
    // repeated later without re-deriving and the file records its own origin.
    let derive_percentile: Option<u8> = args.iter().position(|a| a == "--derive-params").map(|pos| {
        args.get(pos + 1)
            .and_then(|v| v.parse::<u8>().ok())
            .unwrap_or(50)
    });
    let write_params: Option<String> = args
        .iter()
        .position(|a| a == "--write-params")
        .map(|pos| args.get(pos + 1).expect("--write-params requires a path").clone());

    if !data_dir.exists() {
        // An explicitly requested directory that does not exist is a mistake worth failing
        // on. The *default* dataset simply not being present means this benchmark was not
        // run, which is not the same as it failing -- a clean host has no LDBC SF1 and
        // should report a skip, so "run every benchmark" is not permanently red.
        if explicit_data_dir {
            eprintln!("ERROR: Data directory not found: {}", data_dir.display());
            std::process::exit(1);
        }
        eprintln!("SKIP: LDBC SF1 dataset not present at {}", data_dir.display());
        eprintln!("      Download and extract it there, or pass --data-dir PATH.");
        eprintln!("      Skipping rather than failing: the benchmark did not run, it did not break.");
        return Ok(());
    }

    // Resolve the parameters now that the dataset is known to be present:
    // derivation reads the source CSVs, deliberately not the loaded graph, so
    // the same parameters can be handed to a competitor engine unchanged.
    let (params, param_provenance): (Params, String) = if let Some(pct) = derive_percentile {
        let derived = ldbc_common::params::derive(&data_dir, pct).unwrap_or_else(|e| {
            eprintln!("ERROR deriving parameters: {}", e);
            std::process::exit(1);
        });
        let provenance = derived.provenance.format();
        if let Some(path) = &write_params {
            if let Err(e) = std::fs::write(path, derived.to_json()) {
                eprintln!("WARN: could not write {}: {}", path, e);
            } else {
                eprintln!("Wrote derived parameters to {}", path);
            }
        }
        (derived.into(), provenance)
    } else if let Some(pos) = args.iter().position(|a| a == "--params-file") {
        let p = args.get(pos + 1).expect("--params-file requires a path argument");
        let loaded = Params::load(p).unwrap_or_else(|e| {
            eprintln!("ERROR loading params file: {}", e);
            std::process::exit(1);
        });
        (loaded, format!("Parameter provenance: file {}\n", p))
    } else {
        (
            Params::default(),
            "Parameter provenance: built-in defaults — these resolve against one \
             particular extract only.\n  Pass --derive-params to sample them from \
             the dataset in front of you instead (#505).\n"
                .to_string(),
        )
    };

    // ========================================================================
    // Load dataset
    // ========================================================================
    eprintln!(
        "LDBC SNB Interactive Benchmark — Samyama v{}",
        env!("CARGO_PKG_VERSION")
    );
    eprintln!();

    let client = EmbeddedClient::new();

    let load_start = Instant::now();
    let load_result = {
        let mut graph = client.store_write().await;
        ldbc_common::load_dataset(&mut graph, &data_dir)?
    };
    let load_time = load_start.elapsed();

    eprintln!();
    eprintln!("Dataset: {} nodes, {} edges (loaded in {})",
        format_num(load_result.total_nodes),
        format_num(load_result.total_edges),
        format_duration(load_time));

    // Build property indexes on `id` for the labels the bench queries
    // anchor on. Without these, `MATCH (m:Post {id: ...})` triggers a
    // full label scan (1.19M Posts on SF1, ~5–9s per IS4–7). With them
    // the inline-property MATCH lowers to an IndexScan.
    let idx_start = Instant::now();
    for (label, prop) in &[
        ("Person", "id"),
        ("Post", "id"),
        ("Comment", "id"),
        ("Forum", "id"),
        ("Place", "id"),
        ("Organisation", "id"),
        ("Tag", "id"),
        ("TagClass", "id"),
    ] {
        let stmt = format!("CREATE INDEX ON :{}({})", label, prop);
        if let Err(e) = client.query("default", &stmt).await {
            eprintln!("  WARN: index {}({}) failed: {}", label, prop, e);
        }
    }
    eprintln!(
        "Indexes built in {} (Person/Post/Comment/Forum/Place/Org/Tag/TagClass on id)",
        format_duration(idx_start.elapsed())
    );

    eprintln!("Runs per query: {}", runs);
    eprintln!(
        "Params: personId={} person2Id={} postId={} messageId={} firstName=\"{}\" tagName=\"{}\"",
        params.person_id, params.person2_id, params.post_id, params.message_id,
        params.first_name, params.tag_name
    );
    eprintln!(
        "        countryX=\"{}\" countryY=\"{}\" tagClass=\"{}\" org=\"{}\" window=[{}, {}) maxDate={}",
        params.country_x, params.country_y, params.tag_class_name, params.organisation_name,
        params.start_date, params.end_date, params.max_date
    );
    // A table of timings cannot be read without knowing what it measured, so
    // the provenance goes above it rather than in a separate artifact (#505).
    eprint!("{}", param_provenance);
    eprintln!();

    // ========================================================================
    // Run benchmarks
    // ========================================================================
    let mut all_queries = ldbc_queries();
    if include_updates {
        all_queries.extend(ldbc_updates());
    }
    if include_deletes {
        // Deletes require inserts to have run first (they delete INS-created entities)
        if !include_updates {
            eprintln!("NOTE: --deletes implies --updates (insert before delete)");
            all_queries.extend(ldbc_updates());
        }
        all_queries.extend(ldbc_deletes());
    }
    let queries: Vec<&LdbcQuery> = if let Some(ref filter) = filter_query {
        all_queries.iter().filter(|q| q.id == filter.as_str()).collect()
    } else {
        all_queries.iter().collect()
    };

    if queries.is_empty() {
        eprintln!("ERROR: No matching query found for filter '{}'", filter_query.unwrap_or_default());
        eprintln!("Available: IS1-IS7, IC1-IC14, INS1-INS8 (with --updates), DEL1-DEL8 (with --deletes)");
        std::process::exit(1);
    }

    // Print header
    println!("{:<6}{:<32}{:>8}{:>12}{:>12}{:>12}  {}",
        "ID", "Name", "Rows", "Min", "Median", "Max", "Status");
    println!("{:<6}{:<32}{:>8}{:>12}{:>12}{:>12}  {}",
        "----", "------------------------------", "------", "----------", "----------", "----------", "------");

    let mut passed = 0usize;
    let mut errors = 0usize;
    let mut empty_reads = 0usize;
    let mut last_category = "";
    let bench_start = Instant::now();

    for query in &queries {
        // Print section separator when category changes
        if query.category != last_category {
            if !last_category.is_empty() { println!(); }
            let label = match query.category {
                "short"   => "--- Short Reads ---",
                "complex" => "--- Complex Reads ---",
                "update"  => "--- Update Operations ---",
                "delete"  => "--- Delete Operations ---",
                other     => other,
            };
            println!("{}", label);
            last_category = query.category;
        }

        eprint!("  Running {}...\r", query.id);

        let cypher = params.apply(query.cypher);

        let result = run_benchmark(&client, query, &cypher, runs).await;

        if let Some(ref err) = result.error {
            println!("{:<6}{:<32}{:>8}{:>12}{:>12}{:>12}  ERROR",
                result.id, result.name, "-", "-", "-", "-");
            eprintln!("       {}", err);
            errors += 1;
        } else {
            // A read that returns nothing is not a passing benchmark. LDBC's short and
            // complex reads return rows by construction when their parameters resolve, so
            // 0 rows means the parameters missed the data -- and a 0.03 ms "OK" for a query
            // that traversed nothing is the most flattering possible wrong answer. Say so
            // in the status rather than reporting it as a pass (#449).
            let empty = result.rows == 0
                && matches!(query.category, "short" | "complex");
            println!("{:<6}{:<32}{:>8}{:>12}{:>12}{:>12}  {}",
                result.id, result.name,
                result.rows,
                format_ms(result.min),
                format_ms(result.median),
                format_ms(result.max),
                if empty { "EMPTY" } else { "OK" });
            if empty { empty_reads += 1; } else { passed += 1; }
        }
    }

    let bench_time = bench_start.elapsed();

    // ========================================================================
    // Summary
    // ========================================================================
    println!();
    println!("Summary: {}/{} passed, {} empty, {} errors (total benchmark time: {})",
        passed, queries.len(), empty_reads, errors, format_duration(bench_time));
    if empty_reads > 0 {
        println!();
        println!("WARNING: {empty_reads} read(s) returned 0 rows. LDBC reads return rows by");
        println!("         construction when their parameters resolve, so this almost certainly");
        println!("         means the substitution parameters do not match this dataset -- ids");
        println!("         are assigned per `datagen` run and are not portable between extracts.");
        println!("         Timings above are therefore not measuring traversal. Supply matching");
        println!("         parameters with --params-file <json>.");
    }

    // Cache stats
    let stats = client.cache_stats();
    println!("AST cache: {} hits, {} misses", stats.hits(), stats.misses());

    // Any empty read is a configuration failure, not a pass. A run with 17 of 21 reads
    // returning nothing measured nothing, and exiting 0 on it is how "21/21 passed in 32 ms"
    // came to look like a result (#449).
    if errors > 0 || empty_reads > 0 {
        std::process::exit(1);
    }

    Ok(())
}
