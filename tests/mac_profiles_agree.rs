//! The AppArmor profile and the KubeArmor policy allow the same paths (#2218).
//!
//! CI runs the AppArmor profile under a real workload
//! (`scripts/apparmor_check.py`); the KubeArmor policy is not run in a cluster.
//! So the policy is held to the profile here: a path the profile allows must
//! be allowed by the policy, with the same read-only/read-write meaning, and a
//! path the policy allows must come from the profile or from one of the
//! AppArmor abstractions the profile includes (`FROM_ABSTRACTIONS`). A path
//! added to one file and not the other fails.

use std::collections::BTreeMap;

const PROFILE: &str = include_str!("../ops/security/apparmor/usr.bin.samyama");
const POLICY: &str = include_str!("../ops/security/kubearmor/samyama-ksp.yaml");

/// Policy entries that the profile gets from `abstractions/base`,
/// `nameservice` and `ssl_certs` rather than from a rule of its own.
const FROM_ABSTRACTIONS: &[&str] = &[
    "/etc/ssl", "/usr/share/ca-certificates", "/etc/resolv.conf", "/etc/hosts",
    "/etc/nsswitch.conf", "/lib", "/usr/lib",
];

/// Profile rules the policy cannot carry, each with the reason. KubeArmor
/// matches fixed paths and directories; it has no way to name a path that
/// contains the process's own pid. Kept here, not dropped from the profile, so
/// the difference is a line someone reads.
const POLICY_CANNOT_NAME: &[(&str, &str)] = &[(
    "/proc/PID/task/TID/comm",
    "writing a sibling thread's name (glibc pthread_setname_np); under KubeArmor \
     the write is refused and the thread is left unnamed, which nothing depends on",
)];

fn expand(path: &str, vars: &BTreeMap<String, Vec<String>>) -> Vec<String> {
    if let Some(start) = path.find("@{") {
        let end = start + path[start..].find('}').unwrap();
        let name = &path[start + 2..end];
        let values = match name {
            "PROC" => vec!["/proc".to_string()],
            "sys" => vec!["/sys".to_string()],
            "pid" => vec!["PID".to_string()],
            "tid" => vec!["TID".to_string()],
            _ => vars.get(name).unwrap_or_else(|| panic!("@{{{name}}} is not defined")).clone(),
        };
        return values.iter().flat_map(|v| expand(&format!("{}{v}{}", &path[..start], &path[end + 1..]), vars)).collect();
    }
    if let Some(start) = path.find('{') {
        let end = start + path[start..].find('}').unwrap();
        return path[start + 1..end]
            .split(',')
            .flat_map(|alt| expand(&format!("{}{alt}{}", &path[..start], &path[end + 1..]), vars))
            .collect();
    }
    vec![path.to_string()]
}

/// The profile's file rules: path (without a trailing `/` or `/**`) -> writable.
fn profile_paths() -> BTreeMap<String, bool> {
    let mut vars = BTreeMap::new();
    for line in PROFILE.lines().map(str::trim) {
        if let Some((name, values)) = line.strip_prefix("@{").and_then(|l| l.split_once("}=")) {
            vars.insert(name.to_string(), values.split_whitespace().map(String::from).collect());
        }
    }
    let mut out = BTreeMap::new();
    for line in PROFILE.lines().map(str::trim) {
        let rule = line.strip_prefix("owner ").unwrap_or(line);
        if !(rule.starts_with('/') || rule.starts_with("@{")) || rule.contains("}=") {
            continue;
        }
        let (path, perms) = rule.trim_end_matches(',').rsplit_once(' ').expect("a rule is `path perms,`");
        for p in expand(path, &vars) {
            let p = p.trim_end_matches("/**").trim_end_matches('/');
            let p = if p.is_empty() { "/" } else { p };
            let writable = perms.contains('w');
            out.entry(p.to_string()).and_modify(|w: &mut bool| *w |= writable).or_insert(writable);
        }
    }
    out
}

/// The policy's file entries: path -> writable, and the process paths.
fn policy() -> (BTreeMap<String, bool>, Vec<String>) {
    let doc: serde_yaml::Value = serde_yaml::from_str(POLICY).expect("the policy is valid YAML");
    let spec = &doc["spec"];
    assert_eq!(spec["action"].as_str(), Some("Allow"), "the policy must be an allow-list");
    let mut files = BTreeMap::new();
    for (list, key) in [("matchDirectories", "dir"), ("matchPaths", "path")] {
        for e in spec["file"][list].as_sequence().unwrap_or(&vec![]) {
            let p = e[key].as_str().unwrap().trim_end_matches('/').to_string();
            files.insert(p, !e["readOnly"].as_bool().unwrap_or(false));
        }
    }
    let procs = spec["process"]["matchPaths"].as_sequence().unwrap().iter()
        .map(|e| e["path"].as_str().unwrap().to_string()).collect();
    (files, procs)
}

/// The policy entry that covers `path`: itself, or the nearest directory above it.
fn covering<'a>(files: &'a BTreeMap<String, bool>, path: &str) -> Option<(&'a String, &'a bool)> {
    files.iter()
        .filter(|(p, _)| path == p.as_str() || path.starts_with(&format!("{p}/")))
        .max_by_key(|(p, _)| p.len())
}

#[test]
fn every_path_the_profile_allows_the_policy_allows_the_same_way() {
    let (files, procs) = policy();
    for (path, writable) in profile_paths() {
        if path.ends_with("bin/samyama") {
            assert!(procs.contains(&path), "the profile runs {path}; the policy's process list does not have it");
            continue;
        }
        if POLICY_CANNOT_NAME.iter().any(|(p, _)| *p == path) {
            continue;
        }
        let path = path.replace("/PID", "");
        let Some((entry, policy_writable)) = covering(&files, &path) else {
            panic!("the profile allows {path}; the policy does not");
        };
        assert_eq!(*policy_writable, writable,
                   "{path} is {} in the profile but {entry} is {} in the policy",
                   if writable { "writable" } else { "read-only" },
                   if *policy_writable { "writable" } else { "read-only" });
    }
}

#[test]
fn every_path_the_policy_allows_comes_from_the_profile() {
    let (files, _) = policy();
    let profile = profile_paths();
    for path in files.keys() {
        let from_rule = profile.keys().any(|p| {
            let p = p.replace("/PID", "");
            p == *path || p.starts_with(&format!("{path}/"))
        });
        assert!(from_rule || FROM_ABSTRACTIONS.contains(&path.as_str()),
                "the policy allows {path}, which the profile neither names nor gets from an abstraction");
    }
}

#[test]
fn neither_file_allows_the_paths_the_escape_test_reads() {
    let (files, _) = policy();
    for secret in ["/etc/shadow", "/root/.ssh/id_rsa", "/var/run/secrets/kubernetes.io/serviceaccount/token"] {
        assert!(covering(&files, secret).is_none(), "the policy allows {secret}");
        assert!(!profile_paths().keys().any(|p| secret == p || secret.starts_with(&format!("{p}/"))),
                "the profile allows {secret}");
    }
}
