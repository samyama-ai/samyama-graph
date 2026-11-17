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
        if l != "Provider: samyama" {
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
