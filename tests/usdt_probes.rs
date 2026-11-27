//! The USDT probes listed in `samyama::usdt::PROBES` are the ones the binary
//! carries (#2219).
//!
//! On Linux the server binary is read with `readelf -n`, and every listed
//! probe must be there as a `stapsdt` note under provider `samyama`, with as
//! many arguments as the list says. Elsewhere the probes compile to nothing,
//! so what is checked is the source: the list and the probe sites agree, and
//! every probe is fired from somewhere.

use std::collections::BTreeMap;
use std::path::Path;

use samyama::usdt::PROBES;

fn source(rel: &str) -> String {
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(rel)).unwrap()
}

/// `name -> argument count` as `src/usdt.rs` defines them.
fn defined() -> BTreeMap<String, usize> {
    let src = source("src/usdt.rs");
    let mut out = BTreeMap::new();
    for (i, _) in src.match_indices("probe") {
        let rest = &src[i..];
        let Some(rest) = rest.strip_prefix("probe!(").or_else(|| rest.strip_prefix("probe_lazy!(")) else {
            continue;
        };
        let body = &rest[..rest.find(");").unwrap()];
        let parts: Vec<&str> = body.split(',').map(str::trim).filter(|p| !p.is_empty()).collect();
        // `provider, name, args...`; an argument's own commas are inside
        // parentheses, and the ones here have none.
        assert_eq!(parts[0], "samyama", "provider in {body}");
        out.insert(parts[1].to_string(), parts.len() - 2);
    }
    out
}

#[test]
fn the_list_and_the_probe_sites_agree() {
    let defined = defined();
    let listed: BTreeMap<String, usize> =
        PROBES.iter().map(|(n, args)| (n.to_string(), args.split(',').count())).collect();
    assert_eq!(defined, listed, "src/usdt.rs: PROBES and the probe! sites differ");
}

#[test]
fn every_probe_is_fired_from_the_engine() {
    let callers = [
        ("query_start", "src/query/mod.rs"),
        ("query_done", "src/query/mod.rs"),
        ("persist_start", "src/persistence/mod.rs"),
        ("persist_done", "src/persistence/mod.rs"),
        ("lock_wait_start", "src/store_lock.rs"),
        ("lock_acquired", "src/store_lock.rs"),
    ];
    assert_eq!(callers.len(), PROBES.len(), "a probe was added without a caller here");
    for (f, file) in callers {
        assert!(source(file).contains(&format!("crate::usdt::{f}(")), "{f} is not called from {file}");
    }
}

#[cfg(target_os = "linux")]
#[test]
fn on_linux_the_binary_carries_every_probe() {
    let bin = env!("CARGO_BIN_EXE_samyama");
    let out = std::process::Command::new("readelf")
        .args(["-n", "--wide", bin])
        .output()
        .expect("readelf (binutils) is needed to check the probes; it is on every CI image");
    assert!(out.status.success(), "readelf failed: {}", String::from_utf8_lossy(&out.stderr));
    let text = String::from_utf8_lossy(&out.stdout);
    // readelf prints, per note: `Provider: samyama` / `Name: query__start` /
    // `Location: ...` / `Arguments: -8@%rax -8@%rdx ...`.
    let mut found: BTreeMap<String, usize> = BTreeMap::new();
    let mut lines = text.lines().map(str::trim).peekable();
    while let Some(l) = lines.next() {
        if !l.ends_with("Provider: samyama") {
            continue;
        }
        let name = lines.next().and_then(|n| n.strip_prefix("Name: ")).unwrap_or("").to_string();
        let mut args = 0;
        for _ in 0..3 {
            if let Some(a) = lines.peek().and_then(|a| a.strip_prefix("Arguments:")) {
                args = a.split_whitespace().count();
                break;
            }
            lines.next();
        }
        found.insert(name, args);
    }
    // When nothing is found, say why: whether the binary has a
    // `.note.stapsdt` section at all (the linker dropped it) or has one this
    // parser does not read.
    let evidence = || {
        let sections = std::process::Command::new("readelf")
            .args(["-S", "--wide", bin])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
            .unwrap_or_default();
        let named: Vec<&str> = sections.lines().filter(|l| l.contains("stapsdt") || l.contains(".probes")).collect();
        let notes: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).take(40).collect();
        format!("sections named stapsdt/.probes: {named:#?}\nfirst lines of readelf -n:\n{}", notes.join("\n"))
    };
    for (name, args) in PROBES {
        let want = args.split(',').count();
        assert_eq!(found.get(*name), Some(&want),
                   "samyama:{name} with {want} arguments is not in {bin}; readelf found {found:?}\n{}", evidence());
    }
}

/// The store's lock is taken through `src/store_lock.rs` and nowhere else, so
/// the lock probes see every wait (#2219). A site that calls `read()`/`write()`
/// on it directly is invisible to them -- which is how the first version of
/// the helper missed the HTTP read path, `AppState::mutate`, RESP's read and
/// write and `INFO`: each had `.instrument(..)` or a line break between the
/// lock and `.await`, and a one-line rewrite did not see them.
#[test]
fn every_store_lock_goes_through_the_helper() {
    // Other locks that happen to guard a GraphStore: the optimisation
    // problems hold their own std::sync::RwLock, not the server's.
    const OTHER_LOCKS: &[&str] = &["src/http/uc_problems.rs"];
    let direct = regex::Regex::new(r"\bstore\s*\.\s*(read|write)(_owned)?\(\)").unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut offenders = Vec::new();
    let mut stack = vec![root.join("src")];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let rel = path.strip_prefix(root).unwrap().to_string_lossy().replace('\\', "/");
            if !rel.ends_with(".rs")
                || rel.ends_with("_cov_tests.rs")
                || rel == "src/store_lock.rs"
                || rel.starts_with("src/raft/")
                || OTHER_LOCKS.contains(&rel.as_str())
            {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap();
            let production = text.split("#[cfg(test)]").next().unwrap();
            for m in direct.find_iter(production) {
                let line = production[..m.start()].matches('\n').count() + 1;
                offenders.push(format!("{rel}:{line}"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these take the store's lock directly, so lock__wait_start/lock__acquired \
         never see them; use crate::store_lock::{{read, write, read_owned, write_owned}}: {offenders:?}"
    );
}
