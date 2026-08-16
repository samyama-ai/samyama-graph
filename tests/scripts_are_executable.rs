//! Every shell script tracked by git is executable (#527).
//!
//! This has now been the same defect twice. `#499` fixed
//! `scripts/download_ldbc_snb.sh` and the distributed-test script; `#527` was
//! `case_studies/football/run.sh` and `case_studies/imdb-movies/run.sh`,
//! committed at mode `100644` with byte-identical content to the nine that
//! worked.
//!
//! The failure mode is specific and bad: the documented command is `./run.sh`,
//! it answers `Permission denied`, and the only people who hit it are the ones
//! following the README on a clean clone — the exact audience `SLT-8` is
//! about. Locally, `bash run.sh` works, so anyone debugging it sees nothing
//! wrong.
//!
//! Checking the mode **git records**, not the mode on this filesystem, is the
//! point. A `chmod +x` in a working tree that never reaches a commit fixes
//! nothing for the next person to clone.

use std::process::Command;

#[test]
fn every_tracked_shell_script_is_executable() {
    let output = Command::new("git")
        .args(["ls-files", "-s", "*.sh"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output();

    // A source tarball or a vendored copy has no git metadata. That means this
    // test did not run, which is not the same as it failing.
    let Ok(output) = output else {
        eprintln!("SKIP: git is not available");
        return;
    };
    if !output.status.success() {
        eprintln!("SKIP: not a git checkout");
        return;
    }

    let listing = String::from_utf8_lossy(&output.stdout);
    if listing.trim().is_empty() {
        eprintln!("SKIP: no tracked shell scripts found");
        return;
    }

    let not_executable: Vec<&str> = listing
        .lines()
        .filter(|line| !line.starts_with("100755"))
        .collect();

    assert!(
        not_executable.is_empty(),
        "these shell scripts are tracked without the executable bit, so `./script.sh` \
         fails with Permission denied on a clean clone:\n{}\n\nFix with:\n  \
         git update-index --chmod=+x <path>",
        not_executable.join("\n")
    );
}

#[test]
fn every_case_study_has_a_runnable_entry_point() {
    // The README's one command is `cd case_studies/<domain> && ./run.sh`. A
    // case study without an executable `run.sh` is undemonstrable, whatever
    // else is right about it.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("case_studies");
    let Ok(entries) = std::fs::read_dir(&root) else {
        eprintln!("SKIP: no case_studies directory");
        return;
    };

    let mut checked = 0;
    let mut missing = Vec::new();
    for entry in entries.flatten() {
        let dir = entry.path();
        // `_lib` holds the shared runner; a directory without `case.env` is
        // not a case study.
        if !dir.is_dir() || !dir.join("case.env").exists() {
            continue;
        }
        checked += 1;
        let run = dir.join("run.sh");
        if !run.exists() {
            missing.push(format!("{}: no run.sh", dir.display()));
            continue;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&run).unwrap().permissions().mode();
            if mode & 0o111 == 0 {
                missing.push(format!("{}: run.sh is not executable ({:o})", dir.display(), mode));
            }
        }
    }

    assert!(checked > 0, "no case studies found — has the layout changed?");
    assert!(missing.is_empty(), "{}", missing.join("\n"));
}
