//! Samyama CLI — command-line interface for the Samyama Graph Database
//!
//! Uses the samyama-sdk RemoteClient to connect to a running server.

use clap::{Parser, Subcommand};
use comfy_table::{Table, ContentArrangement};
use samyama_sdk::{RemoteClient, SamyamaClient};

#[derive(Parser)]
#[command(name = "samyama", version, about = "Samyama Graph Database CLI")]
struct Cli {
    /// Server HTTP URL
    #[arg(long, default_value = "http://localhost:8080", global = true, env = "SAMYAMA_URL")]
    url: String,

    /// Output format
    #[arg(long, default_value = "table", global = true)]
    format: OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, clap::ValueEnum)]
enum OutputFormat {
    Table,
    Json,
    Csv,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a Cypher query
    Query {
        /// The Cypher query string
        cypher: String,

        /// Graph name
        #[arg(long, default_value = "default")]
        graph: String,

        /// Use read-only mode
        #[arg(long)]
        readonly: bool,
    },
    /// Get server status
    Status,
    /// Ping the server
    Ping,
    /// Start an interactive REPL
    Shell,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let client = RemoteClient::new(&cli.url);

    let result = match cli.command {
        Commands::Query { cypher, graph, readonly } => {
            run_query(&client, &graph, &cypher, readonly, &cli.format).await
        }
        Commands::Status => run_status(&client, &cli.format).await,
        Commands::Ping => run_ping(&client).await,
        Commands::Shell => run_shell(&client, &cli.format).await,
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

async fn run_query(
    client: &RemoteClient,
    graph: &str,
    cypher: &str,
    readonly: bool,
    format: &OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = if readonly {
        client.query_readonly(graph, cypher).await?
    } else {
        client.query(graph, cypher).await?
    };

    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Csv => {
            if !result.columns.is_empty() {
                println!("{}", result.columns.join(","));
                for row in &result.records {
                    let cells: Vec<String> = row.iter().map(|v| format_csv_value(v)).collect();
                    println!("{}", cells.join(","));
                }
            }
        }
        OutputFormat::Table => {
            if result.columns.is_empty() {
                println!("(no results)");
                return Ok(());
            }

            let mut table = Table::new();
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(&result.columns);

            for row in &result.records {
                let cells: Vec<String> = row.iter().map(|v| format_table_value(v)).collect();
                table.add_row(cells);
            }

            println!("{}", table);
            println!("{} row(s)", result.records.len());
        }
    }

    Ok(())
}

async fn run_status(
    client: &RemoteClient,
    format: &OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let status = client.status().await?;

    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&status)?);
        }
        _ => {
            println!("Status:  {}", status.status);
            println!("Version: {}", status.version);
            println!("Nodes:   {}", status.storage.nodes);
            println!("Edges:   {}", status.storage.edges);
        }
    }

    Ok(())
}

async fn run_ping(client: &RemoteClient) -> Result<(), Box<dyn std::error::Error>> {
    let result = client.ping().await?;
    println!("{}", result);
    Ok(())
}

async fn run_shell(
    client: &RemoteClient,
    format: &OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Samyama Interactive Shell");
    println!("Type Cypher queries, or :help for commands. :quit to exit.\n");

    let stdin = std::io::stdin();
    let mut line = String::new();

    loop {
        eprint!("samyama> ");

        line.clear();
        if stdin.read_line(&mut line)? == 0 {
            break; // EOF
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        match trimmed {
            ":quit" | ":exit" | ":q" => break,
            ":help" | ":h" => {
                println!("Commands:");
                println!("  :status   — Show server status");
                println!("  :ping     — Ping server");
                println!("  :quit     — Exit shell");
                println!("  <cypher>  — Execute a Cypher query");
            }
            ":status" => {
                if let Err(e) = run_status(client, format).await {
                    eprintln!("Error: {}", e);
                }
            }
            ":ping" => {
                if let Err(e) = run_ping(client).await {
                    eprintln!("Error: {}", e);
                }
            }
            cypher => {
                if let Err(e) = run_query(client, "default", cypher, false, format).await {
                    eprintln!("Error: {}", e);
                }
            }
        }
    }

    println!("Bye!");
    Ok(())
}

fn format_table_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Object(map) => {
            // If it looks like a node/edge, show a compact representation
            if let Some(id) = map.get("id") {
                if let Some(labels) = map.get("labels") {
                    return format!("({}:{})", id, labels);
                }
                if let Some(t) = map.get("type") {
                    return format!("[{}:{}]", id, t);
                }
            }
            serde_json::to_string(v).unwrap_or_default()
        }
        serde_json::Value::Array(_) => serde_json::to_string(v).unwrap_or_default(),
    }
}

fn format_csv_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "".to_string(),
        serde_json::Value::String(s) => {
            if s.contains(',') || s.contains('"') || s.contains('\n') {
                format!("\"{}\"", s.replace('"', "\"\""))
            } else {
                s.clone()
            }
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        _ => {
            let json = serde_json::to_string(v).unwrap_or_default();
            format!("\"{}\"", json.replace('"', "\"\""))
        }
    }
}
