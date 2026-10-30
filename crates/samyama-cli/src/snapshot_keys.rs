use comfy_table::{ContentArrangement, Table};
use std::error::Error;
use std::path::PathBuf;

pub fn run_snapshot_keys(
    files: &[PathBuf],
    label: Option<&str>,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let report = samyama::snapshot::keys::analyze_snapshot_keys(files, label)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    if report.keys.is_empty() {
        println!("No string or numeric properties found for dedup analysis.");
        return Ok(());
    }

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header([
        "label.property",
        "snapshot",
        "nodes with key / label nodes",
        "coverage",
        "distinct / injectivity",
        "shared values",
        "largest shared group",
        "shape",
        "recommendation",
    ]);

    for key in &report.keys {
        for snapshot in &key.snapshots {
            let largest = key.largest_collision_group.as_ref().map_or_else(
                || "—".to_string(),
                |group| {
                    let sample: String = group.value.chars().take(80).collect();
                    let sample = if group.value.chars().count() > 80 {
                        format!("{sample}…")
                    } else {
                        sample
                    };
                    format!("{sample} ({})", group.records)
                },
            );
            table.add_row([
                format!("{}.{}", key.label, key.property),
                snapshot.file.clone(),
                format!("{} / {}", snapshot.nodes_with_key, snapshot.label_nodes),
                format!("{:.1}%", snapshot.coverage * 100.0),
                format!(
                    "{} / {:.1}%",
                    snapshot.distinct_values,
                    snapshot.injectivity * 100.0
                ),
                key.cross_source_collisions.to_string(),
                largest,
                key.value_shape.clone(),
                key.recommendation.clone(),
            ]);
        }
    }
    println!("{table}");
    println!(
        "Recommended --dedup-keys: {}",
        if report.recommended_keys.is_empty() {
            "(none)".to_string()
        } else {
            report.recommended_keys.join(",")
        }
    );
    println!("Predicted merges: {}", report.predicted_merge_count);
    println!(
        "Recommendation thresholds: coverage ≥ {:.0}%, injectivity ≥ {:.0}% in every source carrying the key, and ≥ {:.0}% namespace-like values.",
        samyama::snapshot::keys::RECOMMENDED_MIN_COVERAGE * 100.0,
        samyama::snapshot::keys::RECOMMENDED_MIN_INJECTIVITY * 100.0,
        samyama::snapshot::keys::RECOMMENDED_MIN_NAMESPACE_SHAPE * 100.0,
    );
    Ok(())
}
