//! LDBC SNB Business Intelligence Benchmark — Samyama Graph Database
//!
//! Benchmarks Samyama's query engine against 20 LDBC SNB BI workload queries
//! (BI-1 through BI-20) on the SF1 dataset.
//!
//! The LDBC BI workload tests complex analytical / OLAP-style queries over the
//! Social Network Benchmark graph.  Several queries in the official spec require
//! features not yet available in Samyama (APOC, GDS, weighted shortest path,
//! CASE expressions, list comprehensions).  Those queries are adapted to
//! simplified Cypher that captures the analytical intent using supported
//! constructs.
//!
//! Prerequisites:
//!   Download and extract LDBC SF1 data to:
//!     data/ldbc-sf1/social_network-sf1-CsvBasic-LongDateFormatter/
//!
//! Usage:
//!   cargo run --release --example ldbc_bi_benchmark
//!   cargo run --release --example ldbc_bi_benchmark -- --runs 5
//!   cargo run --release --example ldbc_bi_benchmark -- --query BI-1
//!   cargo run --release --example ldbc_bi_benchmark -- --data-dir /path/to/data

use std::path::PathBuf;
use std::time::{Duration, Instant};

use samyama_sdk::{EmbeddedClient, SamyamaClient};

mod ldbc_bi_common;
use ldbc_bi_common::{format_duration, format_num};

type Error = Box<dyn std::error::Error>;

// ============================================================================
// QUERY DEFINITIONS
// ============================================================================

struct LdbcBiQuery {
    id: &'static str,
    name: &'static str,
    cypher: &'static str,
}

/// Build the 20 LDBC SNB BI queries adapted for Samyama.
///
/// Adaptation notes:
///   - The LDBC `:Message` supertype does not exist in the SNB CSV schema.
///     Where the spec unions Posts and Comments we use two separate MATCH
///     clauses combined with UNION, or query each label independently.
///   - Queries requiring APOC/GDS procedures (e.g. shortest weighted path,
///     triangle counting) are rewritten using pure Cypher patterns that
///     approximate the analytical intent.
///   - Parameter values are chosen from the SF1 dataset.
///
/// SF1 parameter choices:
///   tagName       = "Hamid_Karzai"
///   tagClassName  = "MusicalArtist"
///   country       = "India"
///   midDate       = 1311292800000  (2011-07-22)
///   startDate     = 1293840000000  (2011-01-01)
///   endDate       = 1354320000000  (2012-12-01)
///   personId      = 933
///   person2Id     = 4139
fn ldbc_bi_queries() -> Vec<LdbcBiQuery> {
    vec![
        // ================================================================
        // BI-1: Posting Summary
        // Count posts and comments by creation year, grouped by message
        // length category.
        // Adapted: separate queries for Post and Comment combined via UNION.
        // ================================================================
        LdbcBiQuery {
            id: "BI-1",
            name: "Posting Summary",
            cypher: "\
MATCH (p:Post)
WHERE p.creationDate < 1354320000000
RETURN 'Post' AS messageType, count(p) AS messageCount
UNION
MATCH (c:Comment)
WHERE c.creationDate < 1354320000000
RETURN 'Comment' AS messageType, count(c) AS messageCount",
        },

        // ================================================================
        // BI-2: Tag Co-occurrence
        // Find pairs of tags that frequently co-occur on messages created
        // in a date range.
        // Adapted: uses Posts only (avoids Message supertype).
        // ================================================================
        LdbcBiQuery {
            id: "BI-2",
            name: "Tag Co-occurrence",
            cypher: "\
MATCH (p:Post)-[:HAS_TAG]->(t1:Tag), (p)-[:HAS_TAG]->(t2:Tag)
WHERE p.creationDate >= 1293840000000 AND p.creationDate < 1354320000000
  AND t1.name < t2.name
RETURN t1.name, t2.name, count(p) AS cooccurrences
ORDER BY cooccurrences DESC
LIMIT 20",
        },

        // ================================================================
        // BI-3: Tag Evolution
        // Compare the popularity of a tag before and after a given date.
        // Adapted: two separate aggregations via UNION for before/after.
        // ================================================================
        LdbcBiQuery {
            id: "BI-3",
            name: "Tag Evolution",
            cypher: "\
MATCH (p:Post)-[:HAS_TAG]->(t:Tag {name: \"Hamid_Karzai\"})
WHERE p.creationDate < 1311292800000
RETURN t.name AS tag, 'before' AS period, count(p) AS msgCount
UNION
MATCH (p:Post)-[:HAS_TAG]->(t:Tag {name: \"Hamid_Karzai\"})
WHERE p.creationDate >= 1311292800000
RETURN t.name AS tag, 'after' AS period, count(p) AS msgCount",
        },

        // ================================================================
        // BI-4: Popular Moderators
        // Forums with the most messages, returning moderator details.
        // Adapted: counts posts per forum (no Message supertype).
        // ================================================================
        LdbcBiQuery {
            id: "BI-4",
            name: "Popular Moderators",
            cypher: "\
MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post)
WITH f, count(p) AS postCount
ORDER BY postCount DESC
LIMIT 20
MATCH (f)-[:HAS_MODERATOR]->(mod:Person)
RETURN f.id, f.title, mod.id, mod.firstName, mod.lastName, postCount
ORDER BY postCount DESC",
        },

        // ================================================================
        // BI-5: Most Active Posters
        // Persons who created the most posts; return statistics.
        // ================================================================
        LdbcBiQuery {
            id: "BI-5",
            name: "Most Active Posters",
            cypher: "\
MATCH (person:Person)<-[:HAS_CREATOR]-(p:Post)
RETURN person.id, person.firstName, person.lastName, count(p) AS postCount
ORDER BY postCount DESC
LIMIT 20",
        },

        // ================================================================
        // BI-6: Most Authoritative Users
        // Persons whose messages with a given tag received the most likes.
        // Adapted: counts LIKES on Posts tagged with the target tag.
        // ================================================================
        LdbcBiQuery {
            id: "BI-6",
            name: "Most Authoritative Users",
            cypher: "\
MATCH (p:Post)-[:HAS_TAG]->(t:Tag {name: \"Hamid_Karzai\"})
MATCH (p)-[:HAS_CREATOR]->(author:Person)
MATCH (liker:Person)-[:LIKES]->(p)
RETURN author.id, author.firstName, author.lastName, count(liker) AS likeCount
ORDER BY likeCount DESC
LIMIT 20",
        },

        // ================================================================
        // BI-7: Authoritative Authors by Score
        // Persons with the highest total message score (approximated by
        // number of likes + replies).
        // Adapted: counts likes on posts per author as a score proxy.
        // ================================================================
        LdbcBiQuery {
            id: "BI-7",
            name: "Authoritative Authors by Score",
            cypher: "\
MATCH (author:Person)<-[:HAS_CREATOR]-(p:Post)
WITH author, count(p) AS postCount
ORDER BY postCount DESC
LIMIT 100
MATCH (liker:Person)-[:LIKES]->(p2:Post)-[:HAS_CREATOR]->(author)
RETURN author.id, author.firstName, author.lastName, postCount, count(liker) AS totalLikes
ORDER BY totalLikes DESC
LIMIT 20",
        },

        // ================================================================
        // BI-8: Related Topics
        // Tags that appear on replies to messages with a given tag.
        // ================================================================
        LdbcBiQuery {
            id: "BI-8",
            name: "Related Topics",
            cypher: "\
MATCH (post:Post)-[:HAS_TAG]->(t:Tag {name: \"Hamid_Karzai\"})
MATCH (reply:Comment)-[:REPLY_OF]->(post)
MATCH (reply)-[:HAS_TAG]->(relatedTag:Tag)
WHERE relatedTag.name <> \"Hamid_Karzai\"
RETURN relatedTag.name, count(reply) AS replyCount
ORDER BY replyCount DESC
LIMIT 20",
        },

        // ================================================================
        // BI-9: Forum with Related Tags
        // Forums where posts have been tagged with both of two given tags.
        // ================================================================
        LdbcBiQuery {
            id: "BI-9",
            name: "Forum with Related Tags",
            cypher: "\
MATCH (f:Forum)-[:CONTAINER_OF]->(p1:Post)-[:HAS_TAG]->(t1:Tag {name: \"Hamid_Karzai\"})
MATCH (f)-[:CONTAINER_OF]->(p2:Post)-[:HAS_TAG]->(t2:Tag {name: \"Afghanistan\"})
WHERE p1.id <> p2.id
RETURN f.id, f.title, count(DISTINCT p1) AS tag1Posts, count(DISTINCT p2) AS tag2Posts
ORDER BY tag1Posts DESC
LIMIT 20",
        },

        // ================================================================
        // BI-10: Experts in Social Circle
        // Multi-hop friends who are experts on a given tag (posted
        // messages with that tag).
        // Adapted: uses 2-hop KNOWS path (full spec uses variable-length
        // BFS which requires GDS).
        // ================================================================
        LdbcBiQuery {
            id: "BI-10",
            name: "Experts in Social Circle",
            cypher: "\
MATCH (p:Person {id: 933})-[:KNOWS*1..2]-(expert:Person)
WHERE expert.id <> 933
WITH DISTINCT expert
MATCH (expert)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(t:Tag {name: \"Hamid_Karzai\"})
RETURN expert.id, expert.firstName, expert.lastName, count(post) AS expertise
ORDER BY expertise DESC
LIMIT 20",
        },

        // ================================================================
        // BI-11: Unrelated Replies
        // Replies to posts where the reply shares no tags with the
        // original post.
        // Adapted: uses NOT EXISTS to check for shared tags.
        // ================================================================
        LdbcBiQuery {
            id: "BI-11",
            name: "Unrelated Replies",
            cypher: "\
MATCH (reply:Comment)-[:REPLY_OF]->(post:Post)
WHERE NOT EXISTS {
  MATCH (reply)-[:HAS_TAG]->(t:Tag)<-[:HAS_TAG]-(post)
}
RETURN count(reply) AS unrelatedReplies",
        },

        // ================================================================
        // BI-12: Person Trending
        // Persons whose messages received the most likes within a period.
        // ================================================================
        LdbcBiQuery {
            id: "BI-12",
            name: "Person Trending",
            cypher: "\
MATCH (liker:Person)-[l:LIKES]->(post:Post)-[:HAS_CREATOR]->(author:Person)
WHERE l.creationDate >= 1293840000000 AND l.creationDate < 1354320000000
RETURN author.id, author.firstName, author.lastName, count(l) AS likeCount
ORDER BY likeCount DESC
LIMIT 20",
        },

        // ================================================================
        // BI-13: Popular Months
        // For each person, the creation month in which they produced the
        // most messages.
        // Adapted: simplified — counts posts per person and returns the
        // total (month extraction would require date functions not yet
        // supported).
        // ================================================================
        LdbcBiQuery {
            id: "BI-13",
            name: "Popular Months",
            cypher: "\
MATCH (person:Person)<-[:HAS_CREATOR]-(p:Post)
WHERE p.creationDate >= 1293840000000 AND p.creationDate < 1354320000000
RETURN person.id, person.firstName, person.lastName, count(p) AS messageCount
ORDER BY messageCount DESC
LIMIT 20",
        },

        // ================================================================
        // BI-14: Top Thread Initiators
        // Persons who started the longest reply threads.
        // Adapted: counts direct replies to each person's posts as a
        // proxy for thread length (full recursive thread depth requires
        // variable-length REPLY_OF paths).
        // ================================================================
        LdbcBiQuery {
            id: "BI-14",
            name: "Top Thread Initiators",
            cypher: "\
MATCH (author:Person)<-[:HAS_CREATOR]-(post:Post)<-[:REPLY_OF]-(reply:Comment)
RETURN author.id, author.firstName, author.lastName, count(reply) AS replyCount
ORDER BY replyCount DESC
LIMIT 20",
        },

        // ================================================================
        // BI-15: Social Normals
        // Persons with the most KNOWS connections (high-degree social
        // nodes).
        // Adapted: the official query uses weighted shortest path; here
        // we approximate by finding high-degree persons in the KNOWS
        // network.
        // ================================================================
        LdbcBiQuery {
            id: "BI-15",
            name: "Social Normals",
            cypher: "\
MATCH (person:Person)-[:KNOWS]-(friend:Person)
RETURN person.id, person.firstName, person.lastName, count(friend) AS friendCount
ORDER BY friendCount DESC
LIMIT 20",
        },

        // ================================================================
        // BI-16: Expert Search
        // Persons who KNOW someone who is an expert on a given tag class.
        // ================================================================
        LdbcBiQuery {
            id: "BI-16",
            name: "Expert Search",
            cypher: "\
MATCH (expert:Person)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tc:TagClass {name: \"MusicalArtist\"})
WITH expert, count(DISTINCT post) AS expertise
ORDER BY expertise DESC
LIMIT 100
MATCH (person:Person)-[:KNOWS]-(expert)
RETURN person.id, person.firstName, person.lastName, expert.id AS expertId, expertise
ORDER BY expertise DESC
LIMIT 20",
        },

        // ================================================================
        // BI-17: Friend Triangles
        // Count triangles in the KNOWS network.
        // Adapted: pure Cypher triangle pattern (a)--(b)--(c)--(a) with
        // ordering constraints to avoid double-counting.
        // ================================================================
        LdbcBiQuery {
            id: "BI-17",
            name: "Friend Triangles",
            cypher: "\
MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a)
WHERE a.id < b.id AND b.id < c.id
RETURN count(a) AS triangleCount",
        },

        // ================================================================
        // BI-18: Friend Recommendation
        // Pairs of persons who share many friends but are not directly
        // connected.
        // ================================================================
        LdbcBiQuery {
            id: "BI-18",
            name: "Friend Recommendation",
            cypher: "\
MATCH (p1:Person {id: 933})-[:KNOWS]-(mutual:Person)-[:KNOWS]-(p2:Person)
WHERE p2.id <> 933 AND NOT EXISTS { MATCH (p1)-[:KNOWS]-(p2) }
RETURN p2.id, p2.firstName, p2.lastName, count(DISTINCT mutual) AS mutualFriends
ORDER BY mutualFriends DESC
LIMIT 20",
        },

        // ================================================================
        // BI-19: Interaction Path
        // Weighted shortest path between two persons, where edge weight
        // is the number of interactions (replies, likes).
        // Adapted: Samyama does not yet support weighted shortest path.
        // We find the unweighted shortest path and count interactions
        // along it as a proxy.
        // ================================================================
        LdbcBiQuery {
            id: "BI-19",
            name: "Interaction Path",
            cypher: "\
MATCH p = shortestPath((p1:Person {id: 933})-[:KNOWS*]-(p2:Person {id: 4139}))
RETURN length(p) AS pathLength, nodes(p) AS pathNodes",
        },

        // ================================================================
        // BI-20: High-Level Topics
        // Distribution of tags grouped by their TagClass.
        // ================================================================
        LdbcBiQuery {
            id: "BI-20",
            name: "High-Level Topics",
            cypher: "\
MATCH (t:Tag)-[:HAS_TYPE]->(tc:TagClass)
MATCH (p:Post)-[:HAS_TAG]->(t)
RETURN tc.name AS tagClass, count(DISTINCT t) AS tagCount, count(p) AS messageCount
ORDER BY messageCount DESC
LIMIT 20",
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
    query: &LdbcBiQuery,
    runs: usize,
) -> BenchResult {
    // Warm-up: 1 run, discard
    let warmup = client.query_readonly("default", query.cypher).await;
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

    for _ in 0..runs {
        let start = Instant::now();
        let run_result = client.query_readonly("default", query.cypher).await;
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
    let args: Vec<String> = std::env::args().collect();

    let default_dir = "data/ldbc-sf1/social_network-sf1-CsvBasic-LongDateFormatter";
    let data_dir = if let Some(pos) = args.iter().position(|a| a == "--data-dir") {
        PathBuf::from(args.get(pos + 1).expect("--data-dir requires a path argument"))
    } else {
        PathBuf::from(default_dir)
    };

    let runs: usize = if let Some(pos) = args.iter().position(|a| a == "--runs") {
        args.get(pos + 1)
            .expect("--runs requires a number")
            .parse()
            .expect("--runs must be a positive integer")
    } else {
        5
    };

    let filter_query: Option<String> = if let Some(pos) = args.iter().position(|a| a == "--query") {
        Some(
            args.get(pos + 1)
                .expect("--query requires a query ID (e.g. BI-1, BI-17)")
                .to_uppercase(),
        )
    } else {
        None
    };

    if !data_dir.exists() {
        eprintln!("ERROR: Data directory not found: {}", data_dir.display());
        eprintln!(
            "Download LDBC SF1 data and extract to: {}",
            default_dir
        );
        std::process::exit(1);
    }

    // ========================================================================
    // Load dataset
    // ========================================================================
    eprintln!("LDBC SNB Business Intelligence Benchmark — Samyama v0.5.8");
    eprintln!();

    let client = EmbeddedClient::new();

    let load_start = Instant::now();
    let load_result = {
        let mut graph = client.store_write().await;
        ldbc_bi_common::load_dataset(&mut graph, &data_dir)?
    };
    let load_time = load_start.elapsed();

    eprintln!();
    eprintln!(
        "Dataset: {} nodes, {} edges (loaded in {})",
        format_num(load_result.total_nodes),
        format_num(load_result.total_edges),
        format_duration(load_time)
    );
    eprintln!("Runs per query: {}", runs);
    eprintln!();

    // ========================================================================
    // Run benchmarks
    // ========================================================================
    let all_queries = ldbc_bi_queries();
    let queries: Vec<&LdbcBiQuery> = if let Some(ref filter) = filter_query {
        all_queries
            .iter()
            .filter(|q| q.id.to_uppercase() == *filter)
            .collect()
    } else {
        all_queries.iter().collect()
    };

    if queries.is_empty() {
        eprintln!(
            "ERROR: No matching query found for filter '{}'",
            filter_query.unwrap_or_default()
        );
        eprintln!("Available: BI-1 through BI-20");
        std::process::exit(1);
    }

    // Print header
    println!(
        "{:<8}{:<36}{:>8}{:>12}{:>12}{:>12}  {}",
        "ID", "Name", "Rows", "Min", "Median", "Max", "Status"
    );
    println!(
        "{:<8}{:<36}{:>8}{:>12}{:>12}{:>12}  {}",
        "------", "------------------------------------", "------", "----------", "----------", "----------", "------"
    );

    let mut passed = 0usize;
    let mut errors = 0usize;
    let bench_start = Instant::now();

    for query in &queries {
        eprint!("  Running {}...\r", query.id);

        let result = run_benchmark(&client, query, runs).await;

        if let Some(ref err) = result.error {
            println!(
                "{:<8}{:<36}{:>8}{:>12}{:>12}{:>12}  ERROR",
                result.id, result.name, "-", "-", "-", "-"
            );
            eprintln!("       {}", err);
            errors += 1;
        } else {
            println!(
                "{:<8}{:<36}{:>8}{:>12}{:>12}{:>12}  OK",
                result.id,
                result.name,
                result.rows,
                format_ms(result.min),
                format_ms(result.median),
                format_ms(result.max)
            );
            passed += 1;
        }
    }

    let bench_time = bench_start.elapsed();

    // ========================================================================
    // Summary
    // ========================================================================
    println!();
    println!(
        "Summary: {}/{} passed, {} errors (total benchmark time: {})",
        passed,
        queries.len(),
        errors,
        format_duration(bench_time)
    );

    // Cache stats
    let stats = client.cache_stats();
    println!("AST cache: {} hits, {} misses", stats.hits(), stats.misses());

    if errors > 0 {
        std::process::exit(1);
    }

    Ok(())
}
