//! Football KG data loading utilities.
//!
//! Loads DataHub World Cup CSV datasets into GraphStore via direct API calls.
//!
//! Schema: 9 node labels, 11 edge types.
//!   Tournament{tournament_id,name,year,host_country,winner,count_teams}
//!   Country{name}
//!   Team{team_id,name,code,confederation}
//!   Stadium{stadium_id,name,city,country,capacity}
//!   Match{match_id,name,date,stage,home_score,away_score,result}
//!   Player{player_id,family_name,given_name,birth_date,position}
//!   Goal{goal_id,minute,own_goal,penalty,period}
//!   Manager{manager_id,family_name,given_name,country}
//!   Referee{referee_id,family_name,given_name,country}
//!
//!   (:Tournament)-[:HOSTED_BY]->(:Country)
//!   (:Tournament)-[:WON_BY]->(:Team)
//!   (:Team)-[:FROM]->(:Country)
//!   (:Match)-[:IN_TOURNAMENT]->(:Tournament)
//!   (:Match)-[:HOME_TEAM]->(:Team)
//!   (:Match)-[:AWAY_TEAM]->(:Team)
//!   (:Match)-[:PLAYED_AT]->(:Stadium)
//!   (:Goal)-[:SCORED_IN]->(:Match)
//!   (:Goal)-[:SCORED_BY]->(:Player)
//!
//! Data source: https://datahub.io/football/worldcup
//! License: Open Data Commons Public Domain Dedication and License (PDDL)

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, IsTerminal, Write};
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
    pub tournament_count: usize,
    pub country_count: usize,
    pub team_count: usize,
    pub stadium_count: usize,
    pub match_count: usize,
    pub player_count: usize,
    pub goal_count: usize,
    pub manager_count: usize,
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
// CSV PARSING HELPERS
// ============================================================================

/// Parse a single CSV line respecting double-quoted fields.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '"' => {
                if in_quotes {
                    if chars.peek() == Some(&'"') {
                        chars.next();
                        current.push('"');
                    } else {
                        in_quotes = false;
                    }
                } else {
                    in_quotes = true;
                }
            }
            ',' if !in_quotes => {
                fields.push(current.trim().to_string());
                current = String::new();
            }
            _ => current.push(c),
        }
    }
    fields.push(current.trim().to_string());
    fields
}

fn csv_field(fields: &[String], idx: usize) -> &str {
    fields.get(idx).map(|s| s.as_str()).unwrap_or("").trim()
}

fn opt_str(s: &str) -> Option<&str> {
    if s.is_empty() || s == "\\N" || s == "NA" {
        None
    } else {
        Some(s)
    }
}

fn opt_i64(s: &str) -> Option<i64> {
    opt_str(s).and_then(|s| s.parse().ok())
}

/// Open a CSV file and return (header_map, line_reader).
fn open_csv(path: &Path) -> Result<(HashMap<String, usize>, Box<dyn BufRead>), Error> {
    if !path.exists() {
        return Err(format!("File not found: {}", path.display()).into());
    }
    let mut reader: Box<dyn BufRead> = Box::new(BufReader::new(File::open(path)?));
    let mut header_line = String::new();
    reader.read_line(&mut header_line)?;
    let headers = parse_csv_line(header_line.trim());
    let map: HashMap<String, usize> = headers
        .into_iter()
        .enumerate()
        .map(|(i, h)| (h, i))
        .collect();
    Ok((map, reader))
}

fn col(hdr: &HashMap<String, usize>, name: &str) -> Result<usize, Error> {
    hdr.get(name)
        .copied()
        .ok_or_else(|| format!("Missing CSV column: {name}").into())
}

// ============================================================================
// PUBLIC: LOAD DATASET
// ============================================================================

pub fn load_dataset(
    graph: &mut GraphStore,
    data_dir: &Path,
) -> Result<LoadResult, Error> {
    let mut node_count = 0usize;
    let mut edge_count = 0usize;
    let is_tty = io::stderr().is_terminal();

    // ID maps
    let mut country_map: HashMap<String, NodeId> = HashMap::new();
    let mut team_map: HashMap<String, NodeId> = HashMap::new();      // team_id -> NodeId
    let mut tournament_map: HashMap<String, NodeId> = HashMap::new(); // tournament_id -> NodeId
    let mut stadium_map: HashMap<String, NodeId> = HashMap::new();    // stadium_id -> NodeId
    let mut match_map: HashMap<String, NodeId> = HashMap::new();      // match_id -> NodeId
    let mut player_map: HashMap<String, NodeId> = HashMap::new();     // player_id -> NodeId

    let mut tournament_count = 0usize;
    let mut team_count = 0usize;
    let mut stadium_count = 0usize;
    let mut match_count = 0usize;
    let mut player_count = 0usize;
    let mut goal_count = 0usize;
    let mut manager_count = 0usize;

    // Helper: get-or-create Country node
    macro_rules! get_or_create_country {
        ($name:expr) => {{
            let name: &str = $name;
            if name.is_empty() {
                None
            } else if let Some(&nid) = country_map.get(name) {
                Some(nid)
            } else {
                let nid = graph.create_node("Country");
                if let Some(n) = graph.get_node_mut(nid) {
                    n.set_property("name", PropertyValue::String(name.to_string()));
                }
                node_count += 1;
                country_map.insert(name.to_string(), nid);
                Some(nid)
            }
        }};
    }

    // ========================================================================
    // Phase 1: Teams
    // ========================================================================
    eprintln!("Phase 1/7: Loading teams ...");
    {
        let path = data_dir.join("teams.csv");
        let (hdr, reader) = open_csv(&path)?;
        let c_id    = col(&hdr, "team_id")?;
        let c_name  = col(&hdr, "team_name")?;
        let c_code  = col(&hdr, "team_code")?;
        let c_conf  = col(&hdr, "confederation_name")?;
        let c_region = col(&hdr, "region_name")?;

        for line in reader.lines() {
            let line = line?;
            let f = parse_csv_line(&line);
            let team_id = csv_field(&f, c_id);
            if team_id.is_empty() { continue; }

            let name = csv_field(&f, c_name);
            let code = csv_field(&f, c_code);
            let conf = csv_field(&f, c_conf);
            let region = csv_field(&f, c_region);

            let nid = graph.create_node("Team");
            if let Some(n) = graph.get_node_mut(nid) {
                n.set_property("team_id", PropertyValue::String(team_id.to_string()));
                n.set_property("name", PropertyValue::String(name.to_string()));
                n.set_property("code", PropertyValue::String(code.to_string()));
                n.set_property("confederation", PropertyValue::String(conf.to_string()));
                n.set_property("region", PropertyValue::String(region.to_string()));
            }
            node_count += 1;
            team_count += 1;
            team_map.insert(team_id.to_string(), nid);

            // FROM -> Country edge using region as country proxy
            if let Some(country_nid) = get_or_create_country!(region) {
                graph.create_edge(nid, country_nid, "FROM")?;
                edge_count += 1;
            }
        }
    }
    eprintln!("  Teams: {}   Countries: {}", format_num(team_count), format_num(country_map.len()));

    // ========================================================================
    // Phase 2: Stadiums
    // ========================================================================
    eprintln!("Phase 2/7: Loading stadiums ...");
    {
        let path = data_dir.join("stadiums.csv");
        let (hdr, reader) = open_csv(&path)?;
        let c_id       = col(&hdr, "stadium_id")?;
        let c_name     = col(&hdr, "stadium_name")?;
        let c_city     = col(&hdr, "city_name")?;
        let c_country  = col(&hdr, "country_name")?;
        let c_capacity = col(&hdr, "stadium_capacity")?;

        for line in reader.lines() {
            let line = line?;
            let f = parse_csv_line(&line);
            let sid = csv_field(&f, c_id);
            if sid.is_empty() { continue; }

            let nid = graph.create_node("Stadium");
            if let Some(n) = graph.get_node_mut(nid) {
                n.set_property("stadium_id", PropertyValue::String(sid.to_string()));
                n.set_property("name", PropertyValue::String(csv_field(&f, c_name).to_string()));
                n.set_property("city", PropertyValue::String(csv_field(&f, c_city).to_string()));
                n.set_property("country", PropertyValue::String(csv_field(&f, c_country).to_string()));
                if let Some(cap) = opt_i64(csv_field(&f, c_capacity)) {
                    n.set_property("capacity", PropertyValue::Integer(cap));
                }
            }
            node_count += 1;
            stadium_count += 1;
            stadium_map.insert(sid.to_string(), nid);
        }
    }
    eprintln!("  Stadiums: {}", format_num(stadium_count));

    // ========================================================================
    // Phase 3: Tournaments
    // ========================================================================
    eprintln!("Phase 3/7: Loading tournaments ...");
    {
        let path = data_dir.join("tournaments.csv");
        let (hdr, reader) = open_csv(&path)?;
        let c_id      = col(&hdr, "tournament_id")?;
        let c_name    = col(&hdr, "tournament_name")?;
        let c_year    = col(&hdr, "year")?;
        let c_host    = col(&hdr, "host_country")?;
        let c_winner  = col(&hdr, "winner")?;
        let c_teams   = col(&hdr, "count_teams")?;

        // We need winner team_id — look it up from tournament_standings later.
        // For now store winner name as string property.
        for line in reader.lines() {
            let line = line?;
            let f = parse_csv_line(&line);
            let tid = csv_field(&f, c_id);
            if tid.is_empty() { continue; }

            let year_str = csv_field(&f, c_year);
            let host = csv_field(&f, c_host);
            let winner = csv_field(&f, c_winner);

            let nid = graph.create_node("Tournament");
            if let Some(n) = graph.get_node_mut(nid) {
                n.set_property("tournament_id", PropertyValue::String(tid.to_string()));
                n.set_property("name", PropertyValue::String(csv_field(&f, c_name).to_string()));
                if let Some(y) = opt_i64(year_str) {
                    n.set_property("year", PropertyValue::Integer(y));
                }
                n.set_property("host_country", PropertyValue::String(host.to_string()));
                n.set_property("winner", PropertyValue::String(winner.to_string()));
                if let Some(ct) = opt_i64(csv_field(&f, c_teams)) {
                    n.set_property("count_teams", PropertyValue::Integer(ct));
                }
            }
            node_count += 1;
            tournament_count += 1;
            tournament_map.insert(tid.to_string(), nid);

            // HOSTED_BY -> Country
            if let Some(country_nid) = get_or_create_country!(host) {
                graph.create_edge(nid, country_nid, "HOSTED_BY")?;
                edge_count += 1;
            }
        }
    }
    eprintln!("  Tournaments: {}", format_num(tournament_count));

    // ========================================================================
    // Phase 4: Players
    // ========================================================================
    eprintln!("Phase 4/7: Loading players ...");
    {
        let path = data_dir.join("players.csv");
        let (hdr, reader) = open_csv(&path)?;
        let c_id         = col(&hdr, "player_id")?;
        let c_family     = col(&hdr, "family_name")?;
        let c_given      = col(&hdr, "given_name")?;
        let c_birth      = col(&hdr, "birth_date")?;
        let c_gk         = col(&hdr, "goal_keeper")?;
        let c_def        = col(&hdr, "defender")?;
        let c_mid        = col(&hdr, "midfielder")?;
        let c_fwd        = col(&hdr, "forward")?;
        let c_count_t    = col(&hdr, "count_tournaments")?;

        for line in reader.lines() {
            let line = line?;
            let f = parse_csv_line(&line);
            let pid = csv_field(&f, c_id);
            if pid.is_empty() { continue; }

            let position = if csv_field(&f, c_gk) == "1" { "Goalkeeper" }
                else if csv_field(&f, c_def) == "1" { "Defender" }
                else if csv_field(&f, c_mid) == "1" { "Midfielder" }
                else if csv_field(&f, c_fwd) == "1" { "Forward" }
                else { "Unknown" };

            let nid = graph.create_node("Player");
            if let Some(n) = graph.get_node_mut(nid) {
                n.set_property("player_id", PropertyValue::String(pid.to_string()));
                n.set_property("family_name", PropertyValue::String(csv_field(&f, c_family).to_string()));
                n.set_property("given_name", PropertyValue::String(csv_field(&f, c_given).to_string()));
                n.set_property("position", PropertyValue::String(position.to_string()));
                if let Some(b) = opt_str(csv_field(&f, c_birth)) {
                    n.set_property("birth_date", PropertyValue::String(b.to_string()));
                }
                if let Some(ct) = opt_i64(csv_field(&f, c_count_t)) {
                    n.set_property("count_tournaments", PropertyValue::Integer(ct));
                }
            }
            node_count += 1;
            player_count += 1;
            player_map.insert(pid.to_string(), nid);
        }
    }
    eprintln!("  Players: {}", format_num(player_count));

    // ========================================================================
    // Phase 5: Matches
    // ========================================================================
    eprintln!("Phase 5/7: Loading matches ...");
    {
        let path = data_dir.join("matches.csv");
        let (hdr, reader) = open_csv(&path)?;
        let c_match_id    = col(&hdr, "match_id")?;
        let c_match_name  = col(&hdr, "match_name")?;
        let c_tourn_id    = col(&hdr, "tournament_id")?;
        let c_stage       = col(&hdr, "stage_name")?;
        let c_date        = col(&hdr, "match_date")?;
        let c_stadium_id  = col(&hdr, "stadium_id")?;
        let c_home_id     = col(&hdr, "home_team_id")?;
        let c_away_id     = col(&hdr, "away_team_id")?;
        let c_home_score  = col(&hdr, "home_team_score")?;
        let c_away_score  = col(&hdr, "away_team_score")?;
        let c_result      = col(&hdr, "result")?;
        let c_extra_time  = col(&hdr, "extra_time")?;
        let c_penalties   = col(&hdr, "penalty_shootout")?;

        for line in reader.lines() {
            let line = line?;
            let f = parse_csv_line(&line);
            let mid = csv_field(&f, c_match_id);
            if mid.is_empty() { continue; }

            let nid = graph.create_node("Match");
            if let Some(n) = graph.get_node_mut(nid) {
                n.set_property("match_id", PropertyValue::String(mid.to_string()));
                n.set_property("name", PropertyValue::String(csv_field(&f, c_match_name).to_string()));
                n.set_property("stage", PropertyValue::String(csv_field(&f, c_stage).to_string()));
                n.set_property("date", PropertyValue::String(csv_field(&f, c_date).to_string()));
                n.set_property("result", PropertyValue::String(csv_field(&f, c_result).to_string()));
                if let Some(hs) = opt_i64(csv_field(&f, c_home_score)) {
                    n.set_property("home_score", PropertyValue::Integer(hs));
                }
                if let Some(as_) = opt_i64(csv_field(&f, c_away_score)) {
                    n.set_property("away_score", PropertyValue::Integer(as_));
                }
                if csv_field(&f, c_extra_time) == "1" {
                    n.set_property("extra_time", PropertyValue::Boolean(true));
                }
                if csv_field(&f, c_penalties) == "1" {
                    n.set_property("penalty_shootout", PropertyValue::Boolean(true));
                }
            }
            node_count += 1;
            match_count += 1;
            match_map.insert(mid.to_string(), nid);

            // IN_TOURNAMENT
            if let Some(&tnid) = tournament_map.get(csv_field(&f, c_tourn_id)) {
                graph.create_edge(nid, tnid, "IN_TOURNAMENT")?;
                edge_count += 1;
            }
            // HOME_TEAM
            if let Some(&hnid) = team_map.get(csv_field(&f, c_home_id)) {
                graph.create_edge(nid, hnid, "HOME_TEAM")?;
                edge_count += 1;
            }
            // AWAY_TEAM
            if let Some(&anid) = team_map.get(csv_field(&f, c_away_id)) {
                graph.create_edge(nid, anid, "AWAY_TEAM")?;
                edge_count += 1;
            }
            // PLAYED_AT
            let sid = csv_field(&f, c_stadium_id);
            if let Some(&snid) = stadium_map.get(sid) {
                graph.create_edge(nid, snid, "PLAYED_AT")?;
                edge_count += 1;
            }
        }
    }
    eprintln!("  Matches: {}", format_num(match_count));

    // ========================================================================
    // Phase 6: Goals
    // ========================================================================
    eprintln!("Phase 6/7: Loading goals ...");
    {
        let path = data_dir.join("goals.csv");
        let (hdr, reader) = open_csv(&path)?;
        let c_goal_id   = col(&hdr, "goal_id")?;
        let c_match_id  = col(&hdr, "match_id")?;
        let c_player_id = col(&hdr, "player_id")?;
        let c_minute    = col(&hdr, "minute_regulation")?;
        let c_period    = col(&hdr, "match_period")?;
        let c_own       = col(&hdr, "own_goal")?;
        let c_penalty   = col(&hdr, "penalty")?;

        for line in reader.lines() {
            let line = line?;
            let f = parse_csv_line(&line);
            let gid = csv_field(&f, c_goal_id);
            if gid.is_empty() { continue; }

            let nid = graph.create_node("Goal");
            if let Some(n) = graph.get_node_mut(nid) {
                n.set_property("goal_id", PropertyValue::String(gid.to_string()));
                n.set_property("period", PropertyValue::String(csv_field(&f, c_period).to_string()));
                n.set_property("own_goal", PropertyValue::Boolean(csv_field(&f, c_own) == "1"));
                n.set_property("penalty", PropertyValue::Boolean(csv_field(&f, c_penalty) == "1"));
                if let Some(m) = opt_i64(csv_field(&f, c_minute)) {
                    n.set_property("minute", PropertyValue::Integer(m));
                }
            }
            node_count += 1;
            goal_count += 1;

            // SCORED_IN -> Match
            if let Some(&mnid) = match_map.get(csv_field(&f, c_match_id)) {
                graph.create_edge(nid, mnid, "SCORED_IN")?;
                edge_count += 1;
            }
            // SCORED_BY -> Player
            if let Some(&pnid) = player_map.get(csv_field(&f, c_player_id)) {
                graph.create_edge(nid, pnid, "SCORED_BY")?;
                edge_count += 1;
            }
        }
    }
    eprintln!("  Goals: {}", format_num(goal_count));

    // ========================================================================
    // Phase 7: Managers
    // ========================================================================
    eprintln!("Phase 7/7: Loading managers ...");
    {
        let path = data_dir.join("managers.csv");
        let (hdr, reader) = open_csv(&path)?;
        let c_id      = col(&hdr, "manager_id")?;
        let c_family  = col(&hdr, "family_name")?;
        let c_given   = col(&hdr, "given_name")?;
        let c_country = col(&hdr, "country_name")?;

        // Track manager_id -> NodeId to avoid duplicate nodes
        let mut manager_map: HashMap<String, NodeId> = HashMap::new();

        for line in reader.lines() {
            let line = line?;
            let f = parse_csv_line(&line);
            let mgr_id = csv_field(&f, c_id);
            if mgr_id.is_empty() { continue; }
            if manager_map.contains_key(mgr_id) { continue; }

            let nid = graph.create_node("Manager");
            if let Some(n) = graph.get_node_mut(nid) {
                n.set_property("manager_id", PropertyValue::String(mgr_id.to_string()));
                n.set_property("family_name", PropertyValue::String(csv_field(&f, c_family).to_string()));
                n.set_property("given_name", PropertyValue::String(csv_field(&f, c_given).to_string()));
                n.set_property("country", PropertyValue::String(csv_field(&f, c_country).to_string()));
            }
            node_count += 1;
            manager_count += 1;
            manager_map.insert(mgr_id.to_string(), nid);
        }
    }
    eprintln!("  Managers: {}", format_num(manager_count));

    let country_count = country_map.len();

    Ok(LoadResult {
        total_nodes: node_count,
        total_edges: edge_count,
        tournament_count,
        country_count,
        team_count,
        stadium_count,
        match_count,
        player_count,
        goal_count,
        manager_count,
    })
}
