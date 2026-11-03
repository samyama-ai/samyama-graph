//! Cricket KG data loading utilities.
//!
//! Loads Cricsheet JSON files (ball-by-ball cricket data) into GraphStore
//! at high speed using direct API calls (no Cypher parsing).
//!
//! Schema: 6 node labels, 12 edge types.
//! Data source: https://cricsheet.org/downloads/all_json.zip (CC-BY-4.0)

use std::collections::HashMap;
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use samyama_sdk::{GraphStore, NodeId, PropertyValue};

pub type Error = Box<dyn std::error::Error>;

// ============================================================================
// LOAD RESULT
// ============================================================================

pub struct LoadResult {
    pub total_nodes: usize,
    pub total_edges: usize,
}

// ============================================================================
// ID MAPPINGS (dedup tracking)
// ============================================================================

pub struct IdMaps {
    pub player: HashMap<String, NodeId>,      // cricsheet_id -> NodeId
    pub team: HashMap<String, NodeId>,        // name.lower() -> NodeId
    pub venue: HashMap<String, NodeId>,       // name.lower() -> NodeId
    pub tournament: HashMap<String, NodeId>,  // name.lower() -> NodeId
    pub season: HashMap<String, NodeId>,      // year_str -> NodeId
    pub player_teams: std::collections::HashSet<String>, // "pid|team" edge dedup
}

impl IdMaps {
    pub fn new() -> Self {
        Self {
            player: HashMap::new(),
            team: HashMap::new(),
            venue: HashMap::new(),
            tournament: HashMap::new(),
            season: HashMap::new(),
            player_teams: std::collections::HashSet::new(),
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
// JSON HELPERS
// ============================================================================

fn json_str(val: &serde_json::Value, key: &str) -> String {
    val.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn json_str_or(val: &serde_json::Value, key: &str, default: &str) -> String {
    val.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or(default)
        .to_string()
}

fn json_i64(val: &serde_json::Value, key: &str) -> Option<i64> {
    val.get(key).and_then(|v| v.as_i64())
}

fn json_f64(val: &serde_json::Value, key: &str) -> Option<f64> {
    val.get(key).and_then(|v| v.as_f64())
}

fn clean_str(s: &str) -> String {
    s.replace('"', "").replace('\n', " ").replace('\r', "")
}

// ============================================================================
// NODE CREATION HELPERS
// ============================================================================

fn get_or_create_player(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    cricsheet_id: &str,
    name: &str,
    node_count: &mut usize,
) -> NodeId {
    if let Some(&id) = maps.player.get(cricsheet_id) {
        return id;
    }
    let id = graph.create_node("Player");
    if let Some(n) = graph.get_node_mut(id) {
        n.set_property("cricsheet_id", PropertyValue::String(cricsheet_id.to_string()));
        n.set_property("name", PropertyValue::String(clean_str(name)));
    }
    maps.player.insert(cricsheet_id.to_string(), id);
    *node_count += 1;
    id
}

fn get_or_create_team(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    name: &str,
    node_count: &mut usize,
) -> NodeId {
    let key = name.to_lowercase();
    if let Some(&id) = maps.team.get(&key) {
        return id;
    }
    let id = graph.create_node("Team");
    if let Some(n) = graph.get_node_mut(id) {
        n.set_property("name", PropertyValue::String(clean_str(name)));
    }
    maps.team.insert(key, id);
    *node_count += 1;
    id
}

fn get_or_create_venue(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    venue: &str,
    city: &str,
    node_count: &mut usize,
) -> NodeId {
    let key = venue.to_lowercase();
    if let Some(&id) = maps.venue.get(&key) {
        return id;
    }
    let id = graph.create_node("Venue");
    if let Some(n) = graph.get_node_mut(id) {
        n.set_property("name", PropertyValue::String(clean_str(venue)));
        if !city.is_empty() {
            n.set_property("city", PropertyValue::String(clean_str(city)));
        }
    }
    maps.venue.insert(key, id);
    *node_count += 1;
    id
}

fn get_or_create_tournament(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    name: &str,
    node_count: &mut usize,
) -> NodeId {
    let key = name.to_lowercase();
    if let Some(&id) = maps.tournament.get(&key) {
        return id;
    }
    let id = graph.create_node("Tournament");
    if let Some(n) = graph.get_node_mut(id) {
        n.set_property("name", PropertyValue::String(clean_str(name)));
    }
    maps.tournament.insert(key, id);
    *node_count += 1;
    id
}

fn get_or_create_season(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    season: &str,
    node_count: &mut usize,
) -> NodeId {
    if let Some(&id) = maps.season.get(season) {
        return id;
    }
    let id = graph.create_node("Season");
    if let Some(n) = graph.get_node_mut(id) {
        n.set_property("year", PropertyValue::String(season.to_string()));
    }
    maps.season.insert(season.to_string(), id);
    *node_count += 1;
    id
}

// ============================================================================
// MATCH INGESTION
// ============================================================================

fn ingest_match(
    graph: &mut GraphStore,
    data: &serde_json::Value,
    file_id: &str,
    maps: &mut IdMaps,
    node_count: &mut usize,
    edge_count: &mut usize,
) -> Result<(), Error> {
    let info = &data["info"];
    let innings_data = data["innings"].as_array();

    let teams = info["teams"].as_array().ok_or("missing teams")?;
    if teams.len() < 2 {
        return Ok(());
    }

    let match_type = json_str(info, "match_type");
    let gender = json_str(info, "gender");
    let season = info
        .get("season")
        .map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            _ => String::new(),
        })
        .unwrap_or_default();
    let venue = json_str(info, "venue");
    let city = json_str(info, "city");
    let event = &info["event"];
    let tournament_name = if event.is_object() {
        json_str(event, "name")
    } else {
        String::new()
    };
    let dates = info["dates"].as_array();
    let date = dates
        .and_then(|d| d.first())
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let outcome = &info["outcome"];
    let toss = &info["toss"];
    let registry = &info["registry"]["people"];
    let player_of_match = info["player_of_match"].as_array();

    let winner = json_str(outcome, "winner");
    let win_by_runs = outcome.get("by").and_then(|b| json_i64(b, "runs"));
    let win_by_wickets = outcome.get("by").and_then(|b| json_i64(b, "wickets"));
    let result_str = json_str_or(outcome, "result", "");

    // --- Create deduped nodes ---
    let mut team_ids = Vec::new();
    for team_val in teams {
        let tname = team_val.as_str().unwrap_or("");
        let tid = get_or_create_team(graph, maps, tname, node_count);
        team_ids.push((tname.to_string(), tid));
    }

    let venue_id = if !venue.is_empty() {
        Some(get_or_create_venue(graph, maps, &venue, &city, node_count))
    } else {
        None
    };

    let tournament_id = if !tournament_name.is_empty() {
        Some(get_or_create_tournament(graph, maps, &tournament_name, node_count))
    } else {
        None
    };

    let season_id = if !season.is_empty() {
        Some(get_or_create_season(graph, maps, &season, node_count))
    } else {
        None
    };

    // Create player nodes from registry
    let players_by_team = &info["players"];
    if let Some(obj) = players_by_team.as_object() {
        for (_team, player_list) in obj {
            if let Some(arr) = player_list.as_array() {
                for pname_val in arr {
                    let pname = pname_val.as_str().unwrap_or("");
                    let pid = registry
                        .get(pname)
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if !pid.is_empty() {
                        get_or_create_player(graph, maps, pid, pname, node_count);
                    }
                }
            }
        }
    }

    // --- Create Match node (always new) ---
    let match_id = graph.create_node("Match");
    if let Some(n) = graph.get_node_mut(match_id) {
        n.set_property("file_id", PropertyValue::String(file_id.to_string()));
        n.set_property("match_type", PropertyValue::String(match_type.clone()));
        n.set_property("gender", PropertyValue::String(gender));
        n.set_property("date", PropertyValue::String(date.to_string()));
        n.set_property("season", PropertyValue::String(season.clone()));
        let winner_or_result = if !winner.is_empty() {
            winner.clone()
        } else {
            result_str
        };
        if !winner_or_result.is_empty() {
            n.set_property("winner", PropertyValue::String(winner_or_result));
        }
        if let Some(r) = win_by_runs {
            n.set_property("win_by_runs", PropertyValue::Integer(r));
        }
        if let Some(w) = win_by_wickets {
            n.set_property("win_by_wickets", PropertyValue::Integer(w));
        }
    }
    *node_count += 1;

    // --- Create edges ---

    // COMPETED_IN
    for (_, tid) in &team_ids {
        graph.create_edge(*tid, match_id, "COMPETED_IN")?;
        *edge_count += 1;
    }

    // WON
    if !winner.is_empty() {
        for (tname, tid) in &team_ids {
            if tname == &winner {
                let mut props = samyama_sdk::PropertyMap::new();
                if let Some(r) = win_by_runs {
                    props.insert("by_runs".to_string(), PropertyValue::Integer(r));
                }
                if let Some(w) = win_by_wickets {
                    props.insert("by_wickets".to_string(), PropertyValue::Integer(w));
                }
                if props.is_empty() {
                    graph.create_edge(*tid, match_id, "WON")?;
                } else {
                    graph.create_edge_with_properties(*tid, match_id, "WON", props)?;
                }
                *edge_count += 1;
                break;
            }
        }
    }

    // WON_TOSS
    let toss_winner = json_str(toss, "winner");
    let toss_decision = json_str(toss, "decision");
    if !toss_winner.is_empty() {
        for (tname, tid) in &team_ids {
            if tname == &toss_winner {
                if toss_decision.is_empty() {
                    graph.create_edge(*tid, match_id, "WON_TOSS")?;
                } else {
                    let mut props = samyama_sdk::PropertyMap::new();
                    props.insert("decision".to_string(), PropertyValue::String(toss_decision.clone()));
                    graph.create_edge_with_properties(*tid, match_id, "WON_TOSS", props)?;
                }
                *edge_count += 1;
                break;
            }
        }
    }

    // HOSTED_AT
    if let Some(vid) = venue_id {
        graph.create_edge(match_id, vid, "HOSTED_AT")?;
        *edge_count += 1;
    }

    // PART_OF
    if let Some(tid) = tournament_id {
        let mut props = samyama_sdk::PropertyMap::new();
        if let Some(mn) = event.get("match_number").and_then(|v| v.as_i64()) {
            props.insert("match_number".to_string(), PropertyValue::Integer(mn));
        }
        if let Some(grp) = event.get("group").and_then(|v| v.as_str()) {
            props.insert("group".to_string(), PropertyValue::String(grp.to_string()));
        }
        if props.is_empty() {
            graph.create_edge(match_id, tid, "PART_OF")?;
        } else {
            graph.create_edge_with_properties(match_id, tid, "PART_OF", props)?;
        }
        *edge_count += 1;
    }

    // IN_SEASON
    if let Some(sid) = season_id {
        graph.create_edge(match_id, sid, "IN_SEASON")?;
        *edge_count += 1;
    }

    // PLAYED_FOR (deduped)
    if let Some(obj) = players_by_team.as_object() {
        for (team, player_list) in obj {
            let team_key = team.to_lowercase();
            let team_nid = maps.team.get(&team_key).copied();
            if let (Some(arr), Some(tid)) = (player_list.as_array(), team_nid) {
                for pname_val in arr {
                    let pname = pname_val.as_str().unwrap_or("");
                    let pid = registry
                        .get(pname)
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if pid.is_empty() {
                        continue;
                    }
                    let dedup_key = format!("{}|{}", pid, team_key);
                    if maps.player_teams.contains(&dedup_key) {
                        continue;
                    }
                    if let Some(&player_nid) = maps.player.get(pid) {
                        graph.create_edge(player_nid, tid, "PLAYED_FOR")?;
                        *edge_count += 1;
                        maps.player_teams.insert(dedup_key);
                    }
                }
            }
        }
    }

    // PLAYER_OF_MATCH
    if let Some(pom_arr) = player_of_match {
        for pname_val in pom_arr {
            let pname = pname_val.as_str().unwrap_or("");
            let pid = registry
                .get(pname)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if let Some(&player_nid) = maps.player.get(pid) {
                graph.create_edge(player_nid, match_id, "PLAYER_OF_MATCH")?;
                *edge_count += 1;
            }
        }
    }

    // --- Innings: batting, bowling, dismissals ---
    if let Some(innings_arr) = innings_data {
        for (inn_idx, innings) in innings_arr.iter().enumerate() {
            let overs = innings["overs"].as_array();
            let is_super_over = innings
                .get("super_over")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            // Aggregate stats
            let mut batting: HashMap<String, (i64, i64, i64, i64)> = HashMap::new(); // runs, balls, 4s, 6s
            let mut bowling: HashMap<String, (i64, i64, i64, i64)> = HashMap::new(); // balls, runs, wickets, maidens

            struct Dismissal {
                player_out: String,
                bowler: String,
                kind: String,
                fielders: Vec<String>,
                over: i64,
            }
            let mut dismissals: Vec<Dismissal> = Vec::new();

            if let Some(overs_arr) = overs {
                for over_data in overs_arr {
                    let over_num = over_data.get("over").and_then(|v| v.as_i64()).unwrap_or(0);
                    let deliveries = over_data["deliveries"].as_array();
                    let mut bowler_runs_this_over: i64 = 0;
                    let mut bowler_this_over = String::new();

                    if let Some(dlvs) = deliveries {
                        for dlv in dlvs {
                            let batter = json_str(dlv, "batter");
                            let bowler = json_str(dlv, "bowler");
                            let runs = &dlv["runs"];
                            let batter_runs = json_i64(runs, "batter").unwrap_or(0);
                            let total_runs = json_i64(runs, "total").unwrap_or(0);
                            let extras = &dlv["extras"];
                            let is_wide = extras.get("wides").is_some();
                            let is_noball = extras.get("noballs").is_some();

                            // Batting
                            if !batter.is_empty() {
                                let entry = batting.entry(batter.clone()).or_insert((0, 0, 0, 0));
                                entry.0 += batter_runs;
                                if !is_wide {
                                    entry.1 += 1;
                                }
                                if batter_runs == 4 {
                                    entry.2 += 1;
                                } else if batter_runs == 6 {
                                    entry.3 += 1;
                                }
                            }

                            // Bowling
                            if !bowler.is_empty() {
                                let entry = bowling.entry(bowler.clone()).or_insert((0, 0, 0, 0));
                                let extras_runs = if extras.is_object() {
                                    extras
                                        .as_object()
                                        .map(|m| m.values().filter_map(|v| v.as_i64()).sum::<i64>())
                                        .unwrap_or(0)
                                } else {
                                    0
                                };
                                entry.1 += batter_runs + extras_runs;
                                if !is_wide && !is_noball {
                                    entry.0 += 1;
                                }
                                bowler_this_over = bowler.clone();
                                bowler_runs_this_over += total_runs;
                            }

                            // Wickets
                            if let Some(wickets_arr) = dlv.get("wickets").and_then(|v| v.as_array())
                            {
                                for w in wickets_arr {
                                    let player_out = json_str(w, "player_out");
                                    let kind = json_str(w, "kind");
                                    let fielders: Vec<String> = w
                                        .get("fielders")
                                        .and_then(|v| v.as_array())
                                        .map(|arr| {
                                            arr.iter()
                                                .filter_map(|f| {
                                                    f.get("name").and_then(|n| n.as_str()).map(|s| s.to_string())
                                                })
                                                .collect()
                                        })
                                        .unwrap_or_default();

                                    if matches!(
                                        kind.as_str(),
                                        "bowled"
                                            | "caught"
                                            | "caught and bowled"
                                            | "lbw"
                                            | "stumped"
                                            | "hit wicket"
                                    ) {
                                        let bowl_entry =
                                            bowling.entry(bowler.clone()).or_insert((0, 0, 0, 0));
                                        bowl_entry.2 += 1;
                                    }

                                    dismissals.push(Dismissal {
                                        player_out,
                                        bowler: bowler.clone(),
                                        kind,
                                        fielders,
                                        over: over_num,
                                    });
                                }
                            }
                        }
                    }

                    // Maiden check
                    if !bowler_this_over.is_empty() && bowler_runs_this_over == 0 {
                        let entry = bowling
                            .entry(bowler_this_over)
                            .or_insert((0, 0, 0, 0));
                        entry.3 += 1;
                    }
                }
            }

            // BATTED_IN edges
            for (batter_name, (runs, balls, fours, sixes)) in &batting {
                if *balls == 0 {
                    continue;
                }
                let pid = registry
                    .get(batter_name.as_str())
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if let Some(&player_nid) = maps.player.get(pid) {
                    let sr = if *balls > 0 {
                        (*runs as f64 * 100.0 / *balls as f64 * 100.0).round() / 100.0
                    } else {
                        0.0
                    };
                    let mut props = samyama_sdk::PropertyMap::new();
                    props.insert("runs".to_string(), PropertyValue::Integer(*runs));
                    props.insert("balls".to_string(), PropertyValue::Integer(*balls));
                    props.insert("fours".to_string(), PropertyValue::Integer(*fours));
                    props.insert("sixes".to_string(), PropertyValue::Integer(*sixes));
                    props.insert("strike_rate".to_string(), PropertyValue::Float(sr));
                    props.insert(
                        "innings_num".to_string(),
                        PropertyValue::Integer(inn_idx as i64),
                    );
                    if is_super_over {
                        props.insert("super_over".to_string(), PropertyValue::Integer(1));
                    }
                    graph.create_edge_with_properties(
                        player_nid, match_id, "BATTED_IN", props,
                    )?;
                    *edge_count += 1;
                }
            }

            // BOWLED_IN edges
            for (bowler_name, (balls_bowled, runs_conceded, wickets, maidens)) in &bowling {
                if *balls_bowled == 0 {
                    continue;
                }
                let pid = registry
                    .get(bowler_name.as_str())
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if let Some(&player_nid) = maps.player.get(pid) {
                    let overs =
                        (*balls_bowled / 6) as f64 + (*balls_bowled % 6) as f64 / 10.0;
                    let economy = if *balls_bowled > 0 {
                        (*runs_conceded as f64 / (*balls_bowled as f64 / 6.0) * 100.0).round()
                            / 100.0
                    } else {
                        0.0
                    };
                    let mut props = samyama_sdk::PropertyMap::new();
                    props.insert(
                        "overs".to_string(),
                        PropertyValue::Float((overs * 10.0).round() / 10.0),
                    );
                    props.insert("maidens".to_string(), PropertyValue::Integer(*maidens));
                    props.insert(
                        "runs_conceded".to_string(),
                        PropertyValue::Integer(*runs_conceded),
                    );
                    props.insert("wickets".to_string(), PropertyValue::Integer(*wickets));
                    props.insert("economy".to_string(), PropertyValue::Float(economy));
                    props.insert(
                        "innings_num".to_string(),
                        PropertyValue::Integer(inn_idx as i64),
                    );
                    graph.create_edge_with_properties(
                        player_nid, match_id, "BOWLED_IN", props,
                    )?;
                    *edge_count += 1;
                }
            }

            // DISMISSED and FIELDED_DISMISSAL edges
            for d in &dismissals {
                let out_pid = registry
                    .get(d.player_out.as_str())
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let bowler_pid = registry
                    .get(d.bowler.as_str())
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                let out_nid = maps.player.get(out_pid).copied();

                if let Some(out_id) = out_nid {
                    // Bowler dismissed batsman
                    if !bowler_pid.is_empty()
                        && matches!(
                            d.kind.as_str(),
                            "bowled"
                                | "caught"
                                | "caught and bowled"
                                | "lbw"
                                | "stumped"
                                | "hit wicket"
                        )
                    {
                        if let Some(&bowler_nid) = maps.player.get(bowler_pid) {
                            let mut props = samyama_sdk::PropertyMap::new();
                            props.insert(
                                "kind".to_string(),
                                PropertyValue::String(d.kind.clone()),
                            );
                            props.insert(
                                "over".to_string(),
                                PropertyValue::Integer(d.over),
                            );
                            props.insert(
                                "match_file_id".to_string(),
                                PropertyValue::String(file_id.to_string()),
                            );
                            graph.create_edge_with_properties(
                                bowler_nid, out_id, "DISMISSED", props,
                            )?;
                            *edge_count += 1;
                        }
                    }

                    // Fielder involvement
                    for fname in &d.fielders {
                        let fpid = registry
                            .get(fname.as_str())
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        if !fpid.is_empty() && fpid != bowler_pid {
                            if let Some(&fielder_nid) = maps.player.get(fpid) {
                                let mut props = samyama_sdk::PropertyMap::new();
                                props.insert(
                                    "kind".to_string(),
                                    PropertyValue::String(d.kind.clone()),
                                );
                                props.insert(
                                    "over".to_string(),
                                    PropertyValue::Integer(d.over),
                                );
                                props.insert(
                                    "match_file_id".to_string(),
                                    PropertyValue::String(file_id.to_string()),
                                );
                                graph.create_edge_with_properties(
                                    fielder_nid, out_id, "FIELDED_DISMISSAL", props,
                                )?;
                                *edge_count += 1;
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

// ============================================================================
// PUBLIC: LOAD DATASET
// ============================================================================

pub fn load_dataset(
    graph: &mut GraphStore,
    data_dir: &Path,
    max_matches: usize,
) -> Result<LoadResult, Error> {
    let json_dir = data_dir.join("json");
    let search_dir = if json_dir.exists() { &json_dir } else { data_dir };

    // Collect JSON files
    let mut files: Vec<_> = fs::read_dir(search_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "json")
                .unwrap_or(false)
        })
        .map(|e| e.path())
        .collect();
    files.sort();

    let total_files = if max_matches > 0 {
        max_matches.min(files.len())
    } else {
        files.len()
    };
    eprintln!("Found {} JSON files in {}", format_num(files.len()), search_dir.display());

    let mut maps = IdMaps::new();
    let mut node_count = 0usize;
    let mut edge_count = 0usize;
    let mut loaded = 0usize;
    let mut errors = 0usize;
    let t0 = Instant::now();
    let is_tty = io::stderr().is_terminal();

    for fpath in &files {
        if max_matches > 0 && loaded >= max_matches {
            break;
        }

        let file_id = fpath
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("");

        let content = match fs::read_to_string(fpath) {
            Ok(c) => c,
            Err(_) => {
                errors += 1;
                continue;
            }
        };

        let data: serde_json::Value = match serde_json::from_str(&content) {
            Ok(d) => d,
            Err(_) => {
                errors += 1;
                continue;
            }
        };

        // Skip if missing teams
        let teams = data["info"]["teams"].as_array();
        if teams.map(|t| t.len()).unwrap_or(0) < 2 {
            continue;
        }

        match ingest_match(graph, &data, file_id, &mut maps, &mut node_count, &mut edge_count) {
            Ok(()) => loaded += 1,
            Err(_) => {
                errors += 1;
                continue;
            }
        }

        if loaded % 500 == 0 && loaded > 0 {
            let elapsed = t0.elapsed().as_secs_f64();
            let rate = loaded as f64 / elapsed;
            if is_tty {
                eprint!("\r");
            }
            eprint!(
                "  [{}/{}] {:.0}s ({:.0} matches/s) — {} players, {} edges",
                format_num(loaded),
                format_num(total_files),
                elapsed,
                rate,
                format_num(maps.player.len()),
                format_num(edge_count),
            );
            if is_tty {
                eprint!("          ");
            } else {
                eprintln!();
            }
            io::stderr().flush().ok();
        }
    }

    if is_tty {
        eprintln!();
    }

    if errors > 0 {
        eprintln!("  ({} files skipped due to errors)", errors);
    }

    eprintln!("  Players:     {}", format_num(maps.player.len()));
    eprintln!("  Teams:       {}", format_num(maps.team.len()));
    eprintln!("  Venues:      {}", format_num(maps.venue.len()));
    eprintln!("  Tournaments: {}", format_num(maps.tournament.len()));
    eprintln!("  Seasons:     {}", format_num(maps.season.len()));
    eprintln!("  Matches:     {}", format_num(loaded));

    Ok(LoadResult {
        total_nodes: node_count,
        total_edges: edge_count,
    })
}
