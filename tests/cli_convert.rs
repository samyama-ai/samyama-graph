//! `samyama convert`, run as the binary (#1909).
//!
//! Every exit code the command documents is produced here at least once, and
//! every format `--formats` lists as readable and writable is converted. A
//! format that is not yet supported is refused with exit 2 before the input is
//! touched.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use samyama::graph::GraphStore;

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_samyama"))
        .arg("convert")
        .args(args)
        .output()
        .expect("the samyama binary runs")
}

fn code(o: &Output) -> i32 {
    o.status.code().expect("exited with a code")
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

/// A snapshot with two nodes and, if `with_edge`, one edge between them. An
/// edge makes the `.sgsnap` writer report its creation time as dropped, which
/// is what exercises `--require-lossless`.
fn snapshot(dir: &Path, name: &str, with_edge: bool) -> PathBuf {
    let mut s = GraphStore::new();
    let a = s.create_node("Person");
    s.set_node_property("default", a, "name", "Ada").unwrap();
    let b = s.create_node("Person");
    s.set_node_property("default", b, "name", "Charles")
        .unwrap();
    if with_edge {
        s.create_edge(a, b, "KNOWS").unwrap();
    }
    let path = dir.join(name);
    samyama::snapshot::write_file(&s, &path, &Default::default()).unwrap();
    path
}

fn counts(path: &Path) -> (usize, usize) {
    let mut s = GraphStore::new();
    samyama::snapshot::import_tenant(&mut s, std::fs::File::open(path).unwrap()).unwrap();
    (s.all_nodes().len(), s.all_edges().len())
}

#[test]
fn every_readable_and_writable_format_converts_with_exit_0() {
    let dir = tempfile::tempdir().unwrap();
    let input = snapshot(dir.path(), "in.sgsnap", false);
    let output = dir.path().join("out/converted.sgsnap");
    let report = dir.path().join("report.json");

    let o = run(&[
        input.to_str().unwrap(),
        output.to_str().unwrap(),
        "--report",
        report.to_str().unwrap(),
    ]);
    assert_eq!(code(&o), 0, "{}", stderr(&o));
    assert_eq!(counts(&output), (2, 0));
    assert!(stderr(&o).contains("dropped: nothing"), "{}", stderr(&o));

    let json: serde_json::Value = serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap();
    assert_eq!(json["lossless"], true);
    assert_eq!(json["output"]["report"]["nodes"], 2);
    assert_eq!(json["input"]["format"], ".sgsnap");
}

#[test]
fn a_drop_with_require_lossless_exits_1_and_still_writes() {
    let dir = tempfile::tempdir().unwrap();
    let input = snapshot(dir.path(), "in.sgsnap", true);
    let output = dir.path().join("out.sgsnap");
    let report = dir.path().join("report.json");

    // Without the flag the same conversion is a success that reports the drop.
    let o = run(&[input.to_str().unwrap(), output.to_str().unwrap()]);
    assert_eq!(code(&o), 0, "{}", stderr(&o));
    assert!(
        stderr(&o).contains("edge_creation_timestamps"),
        "{}",
        stderr(&o)
    );

    let o = run(&[
        input.to_str().unwrap(),
        output.to_str().unwrap(),
        "--require-lossless",
        "--report",
        report.to_str().unwrap(),
    ]);
    assert_eq!(code(&o), 1, "{}", stderr(&o));
    assert_eq!(
        counts(&output),
        (2, 1),
        "exit 1 means converted, so the output is there"
    );
    let json: serde_json::Value = serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap();
    assert_eq!(json["lossless"], false);
    assert_eq!(
        json["output"]["report"]["dropped"][0]["what"],
        "edge_creation_timestamps"
    );
}

#[test]
fn usage_errors_exit_2_before_the_input_is_read() {
    let dir = tempfile::tempdir().unwrap();
    let input = snapshot(dir.path(), "in.sgsnap", false);
    let i = input.to_str().unwrap();
    let out = |name: &str| dir.path().join(name).to_str().unwrap().to_string();

    for (args, says) in [
        (vec![i.to_string(), out("out.ttl")], "Turtle"),
        (
            vec![i.to_string(), out("out.xyz")],
            "cannot tell the format",
        ),
        (
            vec![
                i.to_string(),
                out("out.sgsnap"),
                "--to".into(),
                "nonsense".into(),
            ],
            "not a format",
        ),
        (
            vec![
                i.to_string(),
                out("out.sgsnap"),
                "--preset".into(),
                "skos".into(),
            ],
            "#1912",
        ),
        (
            vec![
                i.to_string(),
                out("out.sgsnap"),
                "--missing-endpoints".into(),
                "maybe".into(),
            ],
            "fail, placeholder or drop",
        ),
        (vec![i.to_string()], "needs exactly"),
        (
            vec![i.to_string(), out("out.sgsnap"), "--bogus".into()],
            "unknown option",
        ),
    ] {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        let o = run(&args);
        assert_eq!(code(&o), 2, "{args:?}: {}", stderr(&o));
        assert!(stderr(&o).contains(says), "{args:?}: {}", stderr(&o));
    }
    // A refused output format is checked before a missing input is noticed.
    let o = run(&[&out("missing.sgsnap"), &out("out.nt")]);
    assert_eq!(code(&o), 2, "{}", stderr(&o));
    assert!(!dir.path().join("out.nt").exists());
}

#[test]
fn an_input_that_does_not_parse_exits_65() {
    let dir = tempfile::tempdir().unwrap();
    let bad = dir.path().join("bad.sgsnap");
    std::fs::write(&bad, b"this is not a snapshot").unwrap();
    let out = dir.path().join("out.sgsnap");
    let o = run(&[bad.to_str().unwrap(), out.to_str().unwrap()]);
    assert_eq!(code(&o), 65, "{}", stderr(&o));
    assert!(
        !out.exists(),
        "nothing is written when the input does not parse"
    );
}

#[test]
fn a_missing_input_exits_66() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.sgsnap");
    let out = dir.path().join("out.sgsnap");
    let o = run(&[missing.to_str().unwrap(), out.to_str().unwrap()]);
    assert_eq!(code(&o), 66, "{}", stderr(&o));
}

#[test]
fn an_output_that_cannot_be_written_exits_73() {
    let dir = tempfile::tempdir().unwrap();
    let input = snapshot(dir.path(), "in.sgsnap", false);
    // A regular file where the output's parent directory has to be.
    let blocker = dir.path().join("not-a-dir");
    std::fs::write(&blocker, b"").unwrap();
    let out = blocker.join("out.sgsnap");
    let o = run(&[input.to_str().unwrap(), out.to_str().unwrap()]);
    assert_eq!(code(&o), 73, "{}", stderr(&o));
}

#[test]
fn formats_and_help_list_the_matrix() {
    for flag in ["--formats", "--help"] {
        let o = run(&[flag]);
        assert_eq!(code(&o), 0);
        let text = format!("{}{}", String::from_utf8_lossy(&o.stdout), stderr(&o));
        for f in samyama::convert::Format::ALL {
            assert!(
                text.contains(&f.to_string()),
                "{flag} does not list {f}:\n{text}"
            );
        }
    }
}

/// `--to` names the format when the output's extension does not: an unknown
/// extension converts with exit 0 and reads back (review on #1939).
#[test]
fn to_overrides_an_unknown_output_extension() {
    let dir = tempfile::tempdir().unwrap();
    let input = snapshot(dir.path(), "in.sgsnap", false);
    let output = dir.path().join("out.bin");

    let o = run(&[
        input.to_str().unwrap(),
        output.to_str().unwrap(),
        "--to",
        "sgsnap",
    ]);
    assert_eq!(code(&o), 0, "{}", stderr(&o));
    assert_eq!(counts(&output), (2, 0));
}

/// The override writer stages in a new file of its own: an existing
/// `<output>.tmp` -- here the very input -- is neither truncated nor replaced
/// (review on #1939).
#[test]
fn an_existing_dot_tmp_beside_the_output_is_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let input = snapshot(dir.path(), "out.bin.tmp", false);
    let before = std::fs::read(&input).unwrap();
    let output = dir.path().join("out.bin");

    let o = run(&[
        input.to_str().unwrap(),
        output.to_str().unwrap(),
        "--from",
        "sgsnap",
        "--to",
        "sgsnap",
    ]);
    assert_eq!(code(&o), 0, "{}", stderr(&o));
    assert_eq!(std::fs::read(&input).unwrap(), before);
    assert_eq!(counts(&output), (2, 0));
}

/// A `--report` that names the input or the output, directly or by another
/// spelling of the same path, is refused with exit 2 before either graph is
/// touched (review on #1939).
#[test]
fn a_report_path_that_is_a_graph_path_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let input = snapshot(dir.path(), "in.sgsnap", false);
    let before = std::fs::read(&input).unwrap();
    let output = dir.path().join("out.sgsnap");
    std::fs::write(&output, b"previous output").unwrap();
    let same_output = dir.path().join(".").join("out.sgsnap");

    for report in [&input, &output, &same_output] {
        let o = run(&[
            input.to_str().unwrap(),
            output.to_str().unwrap(),
            "--report",
            report.to_str().unwrap(),
        ]);
        assert_eq!(code(&o), 2, "{}", stderr(&o));
        assert!(stderr(&o).contains("--report"), "{}", stderr(&o));
        assert_eq!(std::fs::read(&input).unwrap(), before);
        assert_eq!(std::fs::read(&output).unwrap(), b"previous output");
    }

    // An output that does not exist yet is caught through its directory.
    std::fs::create_dir(dir.path().join("sub")).unwrap();
    let fresh = dir.path().join("fresh.sgsnap");
    let o = run(&[
        input.to_str().unwrap(),
        fresh.to_str().unwrap(),
        "--report",
        dir.path().join("sub/../fresh.sgsnap").to_str().unwrap(),
    ]);
    assert_eq!(code(&o), 2, "{}", stderr(&o));
    assert!(!fresh.exists());
}

/// An output whose directory does not exist yet still cannot be named by
/// `--report` through another spelling, relative or absolute: the paths are
/// resolved through their nearest existing ancestor (review on #1939).
#[test]
fn a_report_alias_beneath_a_missing_output_directory_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let input = snapshot(dir.path(), "in.sgsnap", false);
    let in_dir = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_samyama"))
            .current_dir(dir.path())
            .arg("convert")
            .args(args)
            .output()
            .expect("the samyama binary runs")
    };
    let absolute = dir.path().join("new/out.sgsnap");

    for (output, report) in [
        ("new/out.sgsnap", absolute.to_str().unwrap()),
        (absolute.to_str().unwrap(), "new/out.sgsnap"),
        ("new/out.sgsnap", "new/../new/./out.sgsnap"),
        ("new/deeper/../out.sgsnap", "./new/out.sgsnap"),
    ] {
        let o = in_dir(&[input.to_str().unwrap(), output, "--report", report]);
        assert_eq!(code(&o), 2, "{output} / {report}: {}", stderr(&o));
        assert!(!dir.path().join("new").exists(), "{output} / {report}");
    }

    // A report beside the output, not on it, is still fine.
    let o = in_dir(&[
        input.to_str().unwrap(),
        "new/out.sgsnap",
        "--report",
        "new/report.json",
    ]);
    assert_eq!(code(&o), 0, "{}", stderr(&o));
    assert_eq!(counts(&absolute), (2, 0));
}

/// A report path that is a dangling symlink to the output is the output: it
/// is refused before anything is written (review on #1939).
#[cfg(unix)]
#[test]
fn a_report_symlink_to_the_output_not_yet_written_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let input = snapshot(dir.path(), "in.sgsnap", false);
    let output = dir.path().join("out.sgsnap");
    let report = dir.path().join("report.json");
    std::os::unix::fs::symlink("out.sgsnap", &report).unwrap();

    let o = run(&[
        input.to_str().unwrap(),
        output.to_str().unwrap(),
        "--report",
        report.to_str().unwrap(),
    ]);
    assert_eq!(code(&o), 2, "{}", stderr(&o));
    assert!(!output.exists());
}

/// The report is renamed into place, so a symlink at the report path is
/// replaced, never written through onto the file it names.
#[cfg(unix)]
#[test]
fn the_report_replaces_a_symlink_instead_of_writing_through_it() {
    let dir = tempfile::tempdir().unwrap();
    let input = snapshot(dir.path(), "in.sgsnap", false);
    let output = dir.path().join("out.sgsnap");
    let other = dir.path().join("notes.txt");
    std::fs::write(&other, b"mine").unwrap();
    let report = dir.path().join("report.json");
    std::os::unix::fs::symlink(&other, &report).unwrap();

    let o = run(&[
        input.to_str().unwrap(),
        output.to_str().unwrap(),
        "--report",
        report.to_str().unwrap(),
    ]);
    assert_eq!(code(&o), 0, "{}", stderr(&o));
    assert_eq!(std::fs::read(&other).unwrap(), b"mine");
    assert!(!std::fs::symlink_metadata(&report)
        .unwrap()
        .file_type()
        .is_symlink());
    assert!(std::fs::read_to_string(&report)
        .unwrap()
        .contains("lossless"));
    assert_eq!(counts(&output), (2, 0));
}

/// A missing directory followed by `..` and then a symlinked directory still
/// names the output: `new/../alias/out.sgsnap` with `alias -> real` is
/// `real/out.sgsnap`, so a report there is refused and the graph is kept
/// (review on #1939).
#[cfg(unix)]
#[test]
fn a_report_alias_through_dotdot_and_a_symlinked_directory_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let input = snapshot(dir.path(), "in.sgsnap", false);
    std::fs::create_dir(dir.path().join("real")).unwrap();
    std::os::unix::fs::symlink("real", dir.path().join("alias")).unwrap();

    let o = Command::new(env!("CARGO_BIN_EXE_samyama"))
        .current_dir(dir.path())
        .arg("convert")
        .args([
            input.to_str().unwrap(),
            "new/../alias/out.sgsnap",
            "--report",
            "real/out.sgsnap",
        ])
        .output()
        .expect("the samyama binary runs");
    assert_eq!(code(&o), 2, "{}", stderr(&o));
    let written = dir.path().join("real/out.sgsnap");
    if written.exists() {
        assert_eq!(counts(&written), (2, 0), "the graph, not the report");
    }
}
