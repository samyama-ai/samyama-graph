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
//!   cargo run --release --example ldbc_benchmark
//!   cargo run --release --example ldbc_benchmark -- --runs 10
//!   cargo run --release --example ldbc_benchmark -- --query IS1
//!   cargo run --release --example ldbc_benchmark -- --data-dir /path/to/data

use std::path::PathBuf;
use std::time::{Duration, Instant};

use samyama_sdk::{EmbeddedClient, SamyamaClient};

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

/// Build the list of 21 LDBC SNB Interactive queries adapted for Samyama.
///
/// Parameter choices (from SF1 dataset):
///   personId    = 933              (Mahinda Perera)
///   person2Id   = 4139             (Mahinda's first KNOWS target)
///   messageId   = 1236950581249    (first comment)
///   postId      = 1236950581248    (first post, by person 933)
///   firstName   = "Mahinda"
///   countryX    = "India", countryY = "Pakistan"
///   tagName     = "Hamid_Karzai"
///   tagClassName = "MusicalArtist"
///   maxDate     = 1354320000000    (2012-12-01)
///   startDate   = 1338508800000    (2012-06-01)
///   endDate     = 1341100800000    (2012-07-01)
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
MATCH (p:Person {id: 933})
RETURN p.firstName, p.lastName, p.birthday, p.locationIP, p.browserUsed, p.gender, p.creationDate",
        },

        LdbcQuery {
            id: "IS2",
            name: "Recent Posts by Person",
            category: "short",
            // Adapted: query Posts only (Comment variant would be a separate UNION)
            cypher: "\
MATCH (p:Person {id: 933})<-[:HAS_CREATOR]-(m:Post)
RETURN m.id, m.content, m.creationDate
ORDER BY m.creationDate DESC
LIMIT 10",
        },

        LdbcQuery {
            id: "IS3",
            name: "Friends of Person",
            category: "short",
            cypher: "\
MATCH (p:Person {id: 933})-[:KNOWS]-(friend:Person)
RETURN friend.id, friend.firstName, friend.lastName
ORDER BY friend.firstName, friend.lastName",
        },

        LdbcQuery {
            id: "IS4",
            name: "Post Content",
            category: "short",
            cypher: "\
MATCH (m:Post {id: 1236950581248})
RETURN m.creationDate, coalesce(m.content, m.imageFile)",
        },

        LdbcQuery {
            id: "IS5",
            name: "Post Creator",
            category: "short",
            cypher: "\
MATCH (m:Post {id: 1236950581248})-[:HAS_CREATOR]->(p:Person)
RETURN p.id, p.firstName, p.lastName",
        },

        LdbcQuery {
            id: "IS6",
            name: "Forum of Post",
            category: "short",
            cypher: "\
MATCH (m:Post {id: 1236950581248})<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person)
RETURN f.id, f.title, mod.id, mod.firstName, mod.lastName",
        },

        LdbcQuery {
            id: "IS7",
            name: "Replies to Post",
            category: "short",
            // LDBC IS7: replies with isKnows check — uses EXISTS subquery (equivalent to OPTIONAL MATCH + CASE)
            // Note: OPTIONAL MATCH version is semantically correct but triggers full Post scan in planner
            cypher: "\
MATCH (m:Post {id: 1236950581248})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(author:Person)
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
MATCH (p:Person {id: 933})-[:KNOWS*1..3]-(friend:Person {firstName: \"Mahinda\"})
WHERE friend.id <> 933
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
MATCH (p:Person {id: 933})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(m:Post)
WHERE m.creationDate < 1354320000000
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
MATCH (p:Person {id: 933})-[:KNOWS*1..2]-(friend:Person)
WHERE friend.id <> 933
WITH DISTINCT friend
MATCH (friend)<-[:HAS_CREATOR]-(m:Post)-[:IS_LOCATED_IN]->(place:Place)
WHERE m.creationDate >= 1338508800000 AND m.creationDate < 1341100800000
  AND (place.name = \"India\" OR place.name = \"Pakistan\")
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
MATCH (p:Person {id: 933})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag)
WHERE post.creationDate >= 1338508800000 AND post.creationDate < 1341100800000
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
MATCH (p:Person {id: 933})-[:KNOWS*1..2]-(friend:Person)
WHERE friend.id <> 933
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
MATCH (p:Person {id: 933})-[:KNOWS*1..2]-(friend:Person)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag {name: \"Hamid_Karzai\"})
WHERE friend.id <> 933
WITH DISTINCT post
MATCH (post)-[:HAS_TAG]->(otherTag:Tag)
WHERE otherTag.name <> \"Hamid_Karzai\"
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
MATCH (p:Person {id: 933})<-[:HAS_CREATOR]-(m:Post)<-[:LIKES]-(liker:Person)
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
MATCH (p:Person {id: 933})<-[:HAS_CREATOR]-(m:Post)<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(author:Person)
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
MATCH (p:Person {id: 933})-[:KNOWS*1..2]-(friend:Person)<-[:HAS_CREATOR]-(m:Post)
WHERE friend.id <> 933 AND m.creationDate < 1354320000000
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
MATCH (p:Person {id: 933})-[:KNOWS*2]-(stranger:Person)
WHERE stranger.id <> 933 AND NOT EXISTS { MATCH (p)-[:KNOWS]-(stranger) }
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
MATCH (p:Person {id: 933})-[:KNOWS*1..2]-(friend:Person)-[wa:WORK_AT]->(org:Organisation)
WHERE friend.id <> 933 AND org.name = \"MDLR_Airlines\" AND wa.workFrom < 2012
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
MATCH (p:Person {id: 933})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tc:TagClass)
WHERE tc.name = \"MusicalArtist\"
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
MATCH p = shortestPath((p1:Person {id: 933})-[:KNOWS*]-(p2:Person {id: 4139}))
RETURN length(p) AS pathLength",
        },

        // IC14: Weighted paths (uses allShortestPaths)
        LdbcQuery {
            id: "IC14",
            name: "Trusted Connection Paths",
            category: "complex",
            cypher: "\
MATCH p = allShortestPaths((p1:Person {id: 933})-[:KNOWS*]-(p2:Person {id: 4139}))
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
MATCH (p:Person {id: 999999}), (m:Post {id: 1236950581248})
CREATE (p)-[:LIKES {creationDate: 1709251200000}]->(m)",
        },
        LdbcQuery {
            id: "INS3",
            name: "Add Like to Comment",
            category: "update",
            cypher: "\
MATCH (p:Person {id: 999999}), (m:Comment {id: 1236950581249})
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
MATCH (f:Forum {id: 999998}), (p:Person {id: 933})
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
MATCH (p1:Person {id: 933}), (p2:Person {id: 999999})
CREATE (p1)-[:KNOWS {creationDate: 1709251200000}]->(p2)",
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
    runs: usize,
) -> BenchResult {
    let is_update = query.category == "update";
    // Warm-up: 1 run, discard (skip for updates — they mutate state)
    let warmup = if is_update {
        client.query("default", query.cypher).await
    } else {
        client.query_readonly("default", query.cypher).await
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
            client.query("default", query.cypher).await
        } else {
            client.query_readonly("default", query.cypher).await
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
        median: timings[runs / 2],
        max: timings[runs - 1],
        error: None,
    }
}

// ============================================================================
// MAIN
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    let default_dir = "data/ldbc-sf1/social_network-sf1-CsvBasic-LongDateFormatter";
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

    if !data_dir.exists() {
        eprintln!("ERROR: Data directory not found: {}", data_dir.display());
        eprintln!("Download LDBC SF1 data and extract to: {}", default_dir);
        std::process::exit(1);
    }

    // ========================================================================
    // Load dataset
    // ========================================================================
    eprintln!("LDBC SNB Interactive Benchmark — Samyama v0.5.8");
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
    eprintln!("Runs per query: {}", runs);
    eprintln!();

    // ========================================================================
    // Run benchmarks
    // ========================================================================
    let mut all_queries = ldbc_queries();
    if include_updates {
        all_queries.extend(ldbc_updates());
    }
    let queries: Vec<&LdbcQuery> = if let Some(ref filter) = filter_query {
        all_queries.iter().filter(|q| q.id == filter.as_str()).collect()
    } else {
        all_queries.iter().collect()
    };

    if queries.is_empty() {
        eprintln!("ERROR: No matching query found for filter '{}'", filter_query.unwrap_or_default());
        eprintln!("Available: IS1-IS7, IC1-IC14, INS1-INS8 (with --updates)");
        std::process::exit(1);
    }

    // Print header
    println!("{:<6}{:<32}{:>8}{:>12}{:>12}{:>12}  {}",
        "ID", "Name", "Rows", "Min", "Median", "Max", "Status");
    println!("{:<6}{:<32}{:>8}{:>12}{:>12}{:>12}  {}",
        "----", "------------------------------", "------", "----------", "----------", "----------", "------");

    let mut passed = 0usize;
    let mut errors = 0usize;
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
                other     => other,
            };
            println!("{}", label);
            last_category = query.category;
        }

        eprint!("  Running {}...\r", query.id);

        let result = run_benchmark(&client, query, runs).await;

        if let Some(ref err) = result.error {
            println!("{:<6}{:<32}{:>8}{:>12}{:>12}{:>12}  ERROR",
                result.id, result.name, "-", "-", "-", "-");
            eprintln!("       {}", err);
            errors += 1;
        } else {
            println!("{:<6}{:<32}{:>8}{:>12}{:>12}{:>12}  OK",
                result.id, result.name,
                result.rows,
                format_ms(result.min),
                format_ms(result.median),
                format_ms(result.max));
            passed += 1;
        }
    }

    let bench_time = bench_start.elapsed();

    // ========================================================================
    // Summary
    // ========================================================================
    println!();
    println!("Summary: {}/{} passed, {} errors (total benchmark time: {})",
        passed, queries.len(), errors, format_duration(bench_time));

    // Cache stats
    let stats = client.cache_stats();
    println!("AST cache: {} hits, {} misses", stats.hits(), stats.misses());

    if errors > 0 {
        std::process::exit(1);
    }

    Ok(())
}
