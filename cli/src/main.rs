//! Samyama CLI — command-line interface for the Samyama Graph Database
//!
//! Uses the samyama-sdk RemoteClient to connect to a running server.

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::Shell;
use comfy_table::{Table, ContentArrangement};
use samyama_sdk::{RemoteClient, SamyamaClient};

mod doctor;

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
    Shell {
        /// Graph/tenant name
        #[arg(long, default_value = "default")]
        graph: String,
    },
    /// Check environment, server, memory and permissions (DX-09)
    Doctor {
        /// Data directory to test for writability
        #[arg(long, default_value = "./samyama_data", env = "SAMYAMA_DATA")]
        data_dir: std::path::PathBuf,
    },
    /// Print a shell completion script to stdout
    ///
    /// Install with e.g. `samyama-cli completions bash > \
    /// /etc/bash_completion.d/samyama-cli`.
    Completions {
        /// Shell to generate for
        shell: Shell,
    },
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
        Commands::Ping => run_ping(&client, &cli.format).await,
        Commands::Shell { graph } => run_shell(&client, &graph, &cli.format).await,
        Commands::Doctor { data_dir } => {
            // `doctor` reports its findings and exits on its own code: an
            // unreachable server is a finding, not a CLI error, and routing it
            // through the error path below would print it twice and lose the
            // checks that did run.
            let code = run_doctor(&client, &cli.url, &data_dir, &cli.format).await;
            std::process::exit(code);
        }
        Commands::Completions { shell } => {
            let mut cmd = Cli::command();
            // `CARGO_BIN_NAME`, not `cmd.get_name()`. The clap command is named
            // "samyama" while the installed binary is `samyama-cli`, so
            // generating for the command name emits `complete ... samyama` --
            // a completion script that binds to a name the user does not type
            // and therefore never fires. It looks shipped and does nothing,
            // which is worse than not shipping it.
            let name = option_env!("CARGO_BIN_NAME").unwrap_or("samyama-cli");
            clap_complete::generate(shell, &mut cmd, name, &mut std::io::stdout());
            Ok(())
        }
    };

    if let Err(e) = result {
        // API-14 asks for `--json` output *everywhere*. An error printed as
        // prose while the caller asked for JSON is the case that breaks a
        // script, because the failure is exactly when it is parsing output.
        match cli.format {
            OutputFormat::Json => {
                let body = serde_json::json!({"error": e.to_string()});
                eprintln!("{}", serde_json::to_string_pretty(&body)
                    .unwrap_or_else(|_| format!("{{\"error\": \"{e}\"}}")));
            }
            _ => eprintln!("Error: {}", e),
        }
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

async fn run_ping(
    client: &RemoteClient,
    format: &OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = client.ping().await?;
    // `ping` used to print its reply as bare text whatever `--format` said,
    // so `--format json` produced output no JSON parser could read. It was the
    // only subcommand that ignored the flag.
    match format {
        OutputFormat::Json => println!("{}",
            serde_json::to_string_pretty(&serde_json::json!({"ping": result}))?),
        _ => println!("{}", result),
    }
    Ok(())
}

async fn run_shell(
    client: &RemoteClient,
    graph: &str,
    format: &OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Samyama Interactive Shell (graph: {})", graph);
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
                if let Err(e) = run_ping(client, format).await {
                    eprintln!("Error: {}", e);
                }
            }
            cypher => {
                if let Err(e) = run_query(client, graph, cypher, false, format).await {
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

/// Run the doctor checks and print them. Returns the process exit code.
async fn run_doctor(
    client: &RemoteClient,
    url: &str,
    data_dir: &std::path::Path,
    format: &OutputFormat,
) -> i32 {
    let mut checks = doctor::local_checks(url, data_dir);
    // The server's own version, or the error that came back instead. Both are
    // findings; neither aborts the local checks, which is the point of running
    // `doctor` when the server is the thing that is broken.
    let status = client
        .status()
        .await
        .map(|s| s.version)
        .map_err(|e| e.to_string());
    checks.extend(doctor::server_checks(status, env!("CARGO_PKG_VERSION")));

    let report = doctor::report(checks);
    match format {
        OutputFormat::Json => {
            match serde_json::to_string_pretty(&report) {
                Ok(j) => println!("{j}"),
                Err(e) => {
                    eprintln!("{{\"error\": \"could not serialise report: {e}\"}}");
                    return 1;
                }
            }
        }
        _ => {
            for c in &report.checks {
                println!("  {:<7} {:<18} {}", c.verdict.to_string(), c.name, c.detail);
            }
            println!(
                "\n{} check(s): {} failed, {} warned, {} skipped",
                report.checks.len(), report.failed, report.warned, report.skipped
            );
        }
    }
    report.exit_code()
}

#[cfg(test)]
mod cli_tests {
    use super::*;

    #[test]
    fn the_argument_parser_is_internally_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn completions_bind_to_the_installed_binary_name() {
        // The clap command is named "samyama" while the binary is
        // `samyama-cli`. Generating for the command name produced
        // `complete ... samyama`, which binds to a name nobody types: the
        // script installs, reports success, and never fires. Assert on the
        // name the shell will actually match against.
        let bin = option_env!("CARGO_BIN_NAME").unwrap_or("samyama-cli");
        for shell in [Shell::Bash, Shell::Zsh, Shell::Fish] {
            let mut out = Vec::new();
            clap_complete::generate(shell, &mut Cli::command(), bin, &mut out);
            let script = String::from_utf8(out).expect("completion script is not UTF-8");
            assert!(!script.is_empty(), "{shell} produced an empty script");
            assert!(script.contains(bin), "{shell} script never mentions `{bin}`");
        }
    }

    #[test]
    fn every_subcommand_is_reachable_by_name() {
        // A subcommand that exists in the enum but is not registered would be
        // invisible; `doctor` and `completions` are the new ones and the two
        // this test was written for.
        let names: Vec<String> = Cli::command()
            .get_subcommands()
            .map(|c| c.get_name().to_string())
            .collect();
        for want in ["query", "status", "ping", "shell", "doctor", "completions"] {
            assert!(names.iter().any(|n| n == want), "missing subcommand `{want}` in {names:?}");
        }
    }
}
