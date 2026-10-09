//! IMDB Movies KG data loading utilities.
//!
//! Loads IMDB non-commercial TSV datasets (plain or .gz) into GraphStore
//! at high speed using direct API calls (no Cypher parsing).
//!
//! Schema: 6 node labels, 7 edge types.
//!   Movie{tconst,title,year,runtime_minutes,title_type}
//!   Series{tconst,title,year,end_year}
//!   Person{nconst,name,birth_year,death_year}
//!   Genre{name}
//!   Rating{average_rating,num_votes}
//!   AlternateTitle{title,region,language}
//!
//!   (:Movie)-[:HAS_GENRE]->(:Genre)
//!   (:Movie)-[:HAS_RATING]->(:Rating)
//!   (:Movie)-[:HAS_ALTERNATE_TITLE]->(:AlternateTitle)
//!   (:Series)-[:HAS_ALTERNATE_TITLE]->(:AlternateTitle)
//!   (:Person)-[:ACTED_IN]->(:Movie or :Series)
//!   (:Person)-[:DIRECTED]->(:Movie or :Series)
//!   (:Person)-[:WROTE]->(:Movie or :Series)
//!   (:Person)-[:PRODUCED]->(:Movie or :Series)
//!
//! Data source: https://developer.imdb.com/non-commercial-datasets/
//! License: IMDB Non-Commercial Use Only

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader, IsTerminal, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use flate2::read::GzDecoder;
use samyama_sdk::{GraphStore, NodeId, PropertyValue};

pub type Error = Box<dyn std::error::Error>;

// ============================================================================
// LOAD RESULT
// ============================================================================

pub struct LoadResult {
    pub total_nodes: usize,
    pub total_edges: usize,
    pub movie_count: usize,
    pub series_count: usize,
    pub person_count: usize,
    pub genre_count: usize,
    pub rating_count: usize,
    pub alt_title_count: usize,
}

// ============================================================================
// ID MAPS (dedup tracking)
// ============================================================================

struct IdMaps {
    title: HashMap<String, NodeId>,  // tconst -> NodeId (Movie or Series)
    person: HashMap<String, NodeId>, // nconst -> NodeId (Person)
    genre: HashMap<String, NodeId>,  // genre name -> NodeId
}

impl IdMaps {
    fn new() -> Self {
        Self {
            title: HashMap::new(),
            person: HashMap::new(),
            genre: HashMap::new(),
        }
    }
}

// ============================================================================
// FORMATTING HELPERS
// ============================================================================

pub fn format_num(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

pub fn format_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 60.0 {
        format!("{:.1}s", secs)
    } else {
        let mins = secs as u64 / 60;
        let rem = secs - (mins as f64 * 60.0);
        format!("{}m {:.1}s", mins, rem)
    }
}

// ============================================================================
// TSV PARSING HELPERS
// ============================================================================

fn tsv_field<'a>(fields: &[&'a str], idx: usize) -> &'a str {
    fields.get(idx).copied().unwrap_or("").trim()
}

// IMDB uses "\N" as the null sentinel
fn imdb_str(s: &str) -> Option<&str> {
    if s == r"\N" || s.is_empty() {
        None
    } else {
        Some(s)
    }
}

fn imdb_i64(s: &str) -> Option<i64> {
    imdb_str(s).and_then(|s| s.parse().ok())
}

fn imdb_f64(s: &str) -> Option<f64> {
    imdb_str(s).and_then(|s| s.parse().ok())
}

/// Open a TSV file — supports plain .tsv and gzip-compressed .tsv.gz.
/// Returns the parsed header columns and a boxed line reader.
fn open_tsv(path: &Path) -> Result<(Vec<String>, Box<dyn BufRead>), Error> {
    let gz_path = {
        let mut p = path.to_path_buf();
        let new_name = format!("{}.gz", p.file_name().unwrap_or_default().to_string_lossy());
        p.set_file_name(new_name);
        p
    };

    let mut reader: Box<dyn BufRead> = if path.exists() {
        Box::new(BufReader::new(File::open(path)?))
    } else if gz_path.exists() {
        Box::new(BufReader::new(GzDecoder::new(File::open(&gz_path)?)))
    } else {
        return Err(format!(
            "File not found: {} (also tried {})",
            path.display(),
            gz_path.display()
        )
        .into());
    };

    let mut header = String::new();
    reader.read_line(&mut header)?;
    let cols: Vec<String> = header.trim().split('\t').map(|s| s.to_string()).collect();
    Ok((cols, reader))
}

fn col_idx(headers: &[String], name: &str) -> Result<usize, Error> {
    headers
        .iter()
        .position(|h| h == name)
        .ok_or_else(|| format!("Missing required column: {name}").into())
}

// ============================================================================
// PUBLIC: LOAD DATASET
// ============================================================================

pub fn load_dataset(
    graph: &mut GraphStore,
    data_dir: &Path,
    min_votes: i64,
    min_year: i32,
    min_votes_series: i64,
    akas_path: Option<&Path>,
) -> Result<LoadResult, Error> {
    let mut maps = IdMaps::new();
    let mut node_count = 0usize;
    let mut edge_count = 0usize;
    let mut movie_count = 0usize;
    let mut series_count = 0usize;
    let mut rating_count = 0usize;
    let mut alt_title_count = 0usize;
    let is_tty = io::stderr().is_terminal();

    // ========================================================================
    // Phase 1: Load ratings → qualified tconst set
    // ========================================================================
    let total_phases = if akas_path.is_some() { 5 } else { 4 };
    eprintln!("Phase 1/{total_phases}: Loading ratings ...");
    let ratings_path = data_dir.join("title.ratings.tsv");
    let mut ratings: HashMap<String, (f64, i64)> = HashMap::new();
    {
        let (headers, reader) = open_tsv(&ratings_path)?;
        let c_tconst = col_idx(&headers, "tconst")?;
        let c_rating = col_idx(&headers, "averageRating")?;
        let c_votes = col_idx(&headers, "numVotes")?;
        let lower = min_votes.min(min_votes_series);

        for line in reader.lines() {
            let line = line?;
            let f: Vec<&str> = line.split('\t').collect();
            let tconst = tsv_field(&f, c_tconst);
            if tconst.is_empty() {
                continue;
            }
            let votes = imdb_i64(tsv_field(&f, c_votes)).unwrap_or(0);
            if votes < lower {
                continue;
            }
            let avg = imdb_f64(tsv_field(&f, c_rating)).unwrap_or(0.0);
            ratings.insert(tconst.to_string(), (avg, votes));
        }
    }
    eprintln!("  Titles with sufficient votes: {}", format_num(ratings.len()));

    // ========================================================================
    // Phase 2: Load titles → Movie/Series/Genre/Rating nodes
    // ========================================================================
    eprintln!("Phase 2/{total_phases}: Loading titles and genres ...");
    let basics_path = data_dir.join("title.basics.tsv");
    {
        let (headers, reader) = open_tsv(&basics_path)?;
        let c_tconst  = col_idx(&headers, "tconst")?;
        let c_type    = col_idx(&headers, "titleType")?;
        let c_title   = col_idx(&headers, "primaryTitle")?;
        let c_year    = col_idx(&headers, "startYear")?;
        let c_end     = col_idx(&headers, "endYear")?;
        let c_runtime = col_idx(&headers, "runtimeMinutes")?;
        let c_genres  = col_idx(&headers, "genres")?;

        let mut processed = 0usize;
        let t0 = Instant::now();

        for line in reader.lines() {
            let line = line?;
            let f: Vec<&str> = line.split('\t').collect();
            let tconst     = tsv_field(&f, c_tconst);
            let title_type = tsv_field(&f, c_type);

            let is_movie  = matches!(title_type, "movie" | "tvMovie");
            let is_series = title_type == "tvSeries";
            if !is_movie && !is_series {
                continue;
            }

            let (avg_rating, num_votes) = match ratings.get(tconst) {
                Some(&r) => r,
                None => continue,
            };

            let threshold = if is_movie { min_votes } else { min_votes_series };
            if num_votes < threshold {
                continue;
            }

            let start_year = imdb_i64(tsv_field(&f, c_year)).unwrap_or(0) as i32;
            if start_year > 0 && start_year < min_year {
                continue;
            }

            let primary_title = tsv_field(&f, c_title);
            let label = if is_movie { "Movie" } else { "Series" };

            let nid = graph.create_node(label);
            if let Some(n) = graph.get_node_mut(nid) {
                n.set_property("tconst", PropertyValue::String(tconst.to_string()));
                n.set_property("title", PropertyValue::String(primary_title.to_string()));
                if start_year > 0 {
                    n.set_property("year", PropertyValue::Integer(start_year as i64));
                }
                if is_movie {
                    n.set_property("title_type", PropertyValue::String(title_type.to_string()));
                    if let Some(rt) = imdb_i64(tsv_field(&f, c_runtime)) {
                        n.set_property("runtime_minutes", PropertyValue::Integer(rt));
                    }
                } else {
                    if let Some(ey) = imdb_i64(tsv_field(&f, c_end)) {
                        n.set_property("end_year", PropertyValue::Integer(ey));
                    }
                }
            }
            node_count += 1;
            maps.title.insert(tconst.to_string(), nid);
            if is_movie {
                movie_count += 1;
            } else {
                series_count += 1;
            }

            // Rating node
            let rid = graph.create_node("Rating");
            if let Some(r) = graph.get_node_mut(rid) {
                r.set_property("average_rating", PropertyValue::Float(avg_rating));
                r.set_property("num_votes", PropertyValue::Integer(num_votes));
            }
            node_count += 1;
            rating_count += 1;
            graph.create_edge(nid, rid, "HAS_RATING")?;
            edge_count += 1;

            // Genre nodes (comma-separated; "\N" when absent)
            let genres_raw = tsv_field(&f, c_genres);
            if let Some(genres_str) = imdb_str(genres_raw) {
                for genre in genres_str.split(',') {
                    let genre = genre.trim();
                    if genre.is_empty() {
                        continue;
                    }
                    let gid = if let Some(&g) = maps.genre.get(genre) {
                        g
                    } else {
                        let g = graph.create_node("Genre");
                        if let Some(gn) = graph.get_node_mut(g) {
                            gn.set_property("name", PropertyValue::String(genre.to_string()));
                        }
                        node_count += 1;
                        maps.genre.insert(genre.to_string(), g);
                        g
                    };
                    graph.create_edge(nid, gid, "HAS_GENRE")?;
                    edge_count += 1;
                }
            }

            processed += 1;
            if processed % 10_000 == 0 {
                let elapsed = t0.elapsed().as_secs_f64();
                if is_tty {
                    eprint!("\r");
                }
                eprint!(
                    "  {} titles ({} movies, {} series) — {:.0}s",
                    format_num(processed),
                    format_num(movie_count),
                    format_num(series_count),
                    elapsed,
                );
                if is_tty {
                    eprint!("     ");
                } else {
                    eprintln!();
                }
                io::stderr().flush().ok();
            }
        }
        if is_tty {
            eprintln!();
        }
    }
    eprintln!(
        "  Movies: {}   Series: {}   Genres: {}",
        format_num(movie_count),
        format_num(series_count),
        format_num(maps.genre.len())
    );

    // ========================================================================
    // Phase 3: Scan principals — collect matching records into memory
    // ========================================================================
    eprintln!("Phase 3/{total_phases}: Loading principals ...");
    let principals_path = data_dir.join("title.principals.tsv");

    struct PrincipalRec {
        tconst: String,
        nconst: String,
        category: String,
        characters: Option<String>,
    }

    let mut principal_recs: Vec<PrincipalRec> = Vec::new();
    let mut nconst_set: HashSet<String> = HashSet::new();
    {
        let (headers, reader) = open_tsv(&principals_path)?;
        let c_tconst     = col_idx(&headers, "tconst")?;
        let c_nconst     = col_idx(&headers, "nconst")?;
        let c_category   = col_idx(&headers, "category")?;
        let c_characters = col_idx(&headers, "characters")?;

        let mut scanned = 0usize;
        let t0 = Instant::now();

        for line in reader.lines() {
            let line = line?;
            let f: Vec<&str> = line.split('\t').collect();
            let tconst   = tsv_field(&f, c_tconst);
            let category = tsv_field(&f, c_category);

            scanned += 1;
            if scanned % 5_000_000 == 0 {
                let elapsed = t0.elapsed().as_secs_f64();
                if is_tty {
                    eprint!("\r");
                }
                eprint!(
                    "  Scanned {}M rows — {} matched — {:.0}s",
                    scanned / 1_000_000,
                    format_num(principal_recs.len()),
                    elapsed,
                );
                if is_tty {
                    eprint!("     ");
                } else {
                    eprintln!();
                }
                io::stderr().flush().ok();
            }

            if !maps.title.contains_key(tconst) {
                continue;
            }
            if !matches!(category, "actor" | "actress" | "director" | "writer" | "producer") {
                continue;
            }

            let nconst = tsv_field(&f, c_nconst);
            // Strip JSON array brackets and quotes from the characters field
            let characters = {
                let raw = tsv_field(&f, c_characters);
                imdb_str(raw)
                    .map(|s| s.replace(['[', ']', '"'], "").trim().to_string())
                    .filter(|s| !s.is_empty())
            };

            nconst_set.insert(nconst.to_string());
            principal_recs.push(PrincipalRec {
                tconst: tconst.to_string(),
                nconst: nconst.to_string(),
                category: category.to_string(),
                characters,
            });
        }
        if is_tty {
            eprintln!();
        }
    }
    eprintln!(
        "  Matched {} principal records ({} unique persons)",
        format_num(principal_recs.len()),
        format_num(nconst_set.len())
    );

    // ========================================================================
    // Phase 4: Load persons, then create all person-title edges
    // ========================================================================
    eprintln!("Phase 4/{total_phases}: Loading persons ...");
    let names_path = data_dir.join("name.basics.tsv");
    {
        let (headers, reader) = open_tsv(&names_path)?;
        let c_nconst = col_idx(&headers, "nconst")?;
        let c_name   = col_idx(&headers, "primaryName")?;
        let c_birth  = col_idx(&headers, "birthYear")?;
        let c_death  = col_idx(&headers, "deathYear")?;

        let mut scanned = 0usize;
        let t0 = Instant::now();

        for line in reader.lines() {
            let line = line?;
            let f: Vec<&str> = line.split('\t').collect();
            let nconst = tsv_field(&f, c_nconst);

            scanned += 1;
            if scanned % 2_000_000 == 0 {
                let elapsed = t0.elapsed().as_secs_f64();
                if is_tty {
                    eprint!("\r");
                }
                eprint!(
                    "  Scanned {}M names — {} matched — {:.0}s",
                    scanned / 1_000_000,
                    format_num(maps.person.len()),
                    elapsed,
                );
                if is_tty {
                    eprint!("     ");
                } else {
                    eprintln!();
                }
                io::stderr().flush().ok();
            }

            if !nconst_set.contains(nconst) {
                continue;
            }

            let primary_name = tsv_field(&f, c_name);
            let nid = graph.create_node("Person");
            if let Some(n) = graph.get_node_mut(nid) {
                n.set_property("nconst", PropertyValue::String(nconst.to_string()));
                n.set_property("name", PropertyValue::String(primary_name.to_string()));
                if let Some(by) = imdb_i64(tsv_field(&f, c_birth)) {
                    n.set_property("birth_year", PropertyValue::Integer(by));
                }
                if let Some(dy) = imdb_i64(tsv_field(&f, c_death)) {
                    n.set_property("death_year", PropertyValue::Integer(dy));
                }
            }
            node_count += 1;
            maps.person.insert(nconst.to_string(), nid);
        }
        if is_tty {
            eprintln!();
        }
    }
    eprintln!("  Persons created: {}", format_num(maps.person.len()));

    // Create person → title edges from the collected records
    eprintln!("  Creating person-title edges ...");
    for rec in &principal_recs {
        let title_nid = match maps.title.get(&rec.tconst) {
            Some(&id) => id,
            None => continue,
        };
        let person_nid = match maps.person.get(&rec.nconst) {
            Some(&id) => id,
            None => continue,
        };

        let edge_type = match rec.category.as_str() {
            "actor" | "actress" => "ACTED_IN",
            "director" => "DIRECTED",
            "writer" => "WROTE",
            "producer" => "PRODUCED",
            _ => continue,
        };

        if let Some(chars) = &rec.characters {
            let mut props = samyama_sdk::PropertyMap::new();
            props.insert(
                "characters".to_string(),
                PropertyValue::String(chars.clone()),
            );
            graph.create_edge_with_properties(person_nid, title_nid, edge_type, props)?;
        } else {
            graph.create_edge(person_nid, title_nid, edge_type)?;
        }
        edge_count += 1;
    }

    // ========================================================================
    // Phase 5 (optional): Load alternate titles from title.akas.tsv
    // ========================================================================
    if let Some(akas) = akas_path {
        eprintln!("Phase 5/{total_phases}: Loading alternate titles ...");
        let (headers, reader) = open_tsv(akas)?;
        let c_title_id   = col_idx(&headers, "titleId")?;
        let c_title      = col_idx(&headers, "title")?;
        let c_region     = col_idx(&headers, "region")?;
        let c_language   = col_idx(&headers, "language")?;
        let c_original   = col_idx(&headers, "isOriginalTitle")?;

        let mut scanned = 0usize;
        let t0 = Instant::now();

        for line in reader.lines() {
            let line = line?;
            let f: Vec<&str> = line.split('\t').collect();

            scanned += 1;
            if scanned % 5_000_000 == 0 {
                let elapsed = t0.elapsed().as_secs_f64();
                if is_tty {
                    eprint!("\r");
                }
                eprint!(
                    "  Scanned {}M akas rows — {} alt titles — {:.0}s",
                    scanned / 1_000_000,
                    format_num(alt_title_count),
                    elapsed,
                );
                if is_tty {
                    eprint!("     ");
                } else {
                    eprintln!();
                }
                io::stderr().flush().ok();
            }

            let title_id = tsv_field(&f, c_title_id);
            let title_nid = match maps.title.get(title_id) {
                Some(&id) => id,
                None => continue,
            };

            // skip the original title row (same as primaryTitle already stored)
            if tsv_field(&f, c_original) == "1" {
                continue;
            }

            let alt_title = match imdb_str(tsv_field(&f, c_title)) {
                Some(t) => t,
                None => continue,
            };

            let nid = graph.create_node("AlternateTitle");
            if let Some(n) = graph.get_node_mut(nid) {
                n.set_property("title", PropertyValue::String(alt_title.to_string()));
                if let Some(r) = imdb_str(tsv_field(&f, c_region)) {
                    n.set_property("region", PropertyValue::String(r.to_string()));
                }
                if let Some(l) = imdb_str(tsv_field(&f, c_language)) {
                    n.set_property("language", PropertyValue::String(l.to_string()));
                }
            }
            node_count += 1;
            alt_title_count += 1;
            graph.create_edge(title_nid, nid, "HAS_ALTERNATE_TITLE")?;
            edge_count += 1;
        }
        if is_tty {
            eprintln!();
        }
        eprintln!("  AlternateTitles created: {}", format_num(alt_title_count));
    }

    Ok(LoadResult {
        total_nodes: node_count,
        total_edges: edge_count,
        movie_count,
        series_count,
        person_count: maps.person.len(),
        genre_count: maps.genre.len(),
        rating_count,
        alt_title_count,
    })
}
