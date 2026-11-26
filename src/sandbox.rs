//! `samyama --sandbox`: the server confines itself after start-up (#2218).
//!
//! A static AppArmor or SELinux profile cannot see the paths a server was
//! started with (`--data-path /mnt/nvme/graph`, a custom `--tls-cert`), so it
//! is written too wide or it breaks the server, and then it is turned off.
//! Here the process confines **itself**, from the paths it was actually given,
//! with two unprivileged Linux mechanisms:
//!
//! * **Landlock** (Linux 5.13+): filesystem access only beneath the paths in
//!   the [`Plan`]. The data directory, the temp directory and the audit log's
//!   directory are read-write; TLS, auth and key files, the import directory
//!   and the few system files a server reads at run time (CA certificates,
//!   resolver and time-zone data, `/proc/self`, cgroup CPU limits) are
//!   read-only. Everything else -- `~/.ssh`, `/etc/shadow`, sibling mounts --
//!   is refused by the kernel.
//! * **seccomp**: `execve`, `execveat`, `ptrace`, `process_vm_readv/writev`,
//!   `mount`, `umount2`, `bpf`, kernel-module and `kexec` calls fail with
//!   `EPERM`, so a compromised process cannot start a shell. Applied to every
//!   thread (`SECCOMP_FILTER_FLAG_TSYNC`), after `PR_SET_NO_NEW_PRIVS`.
//!
//! **It runs before the async runtime starts.** Landlock binds the calling
//! thread and the threads it creates afterwards, so the Tokio workers must be
//! created after it, not before; `main` builds the runtime once this returns.
//!
//! **It refuses rather than degrades.** On a kernel without Landlock, off
//! Linux, with a required path missing, or alongside an NLQ provider that
//! runs an external program (`claudecode`), [`apply`] returns an error and the
//! server does not start. An operator who asked for a sandbox and got none
//! would believe the opposite of what is true.

use std::path::{Path, PathBuf};

/// The paths the server may touch, from the arguments and environment it was
/// started with.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Plan {
    /// Read, write and create beneath these.
    pub read_write: Vec<PathBuf>,
    /// Read beneath these; the operator named them, so they must exist.
    pub read_only: Vec<PathBuf>,
    /// Read beneath these if present; system files whose location varies by
    /// distribution.
    pub system_read_only: Vec<PathBuf>,
}

/// What [`apply`] put in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applied {
    /// `"fully"` or `"partially"` (an older kernel enforcing fewer access
    /// rights than this build asks for).
    pub landlock: &'static str,
    pub denied_syscalls: usize,
}

/// System files a server reads after start-up, wherever the distribution keeps
/// them. Missing ones are skipped.
const SYSTEM_READ_ONLY: &[&str] = &[
    // TLS trust roots, for outbound HTTPS (NLQ and embedding providers).
    "/etc/ssl", "/etc/pki", "/etc/ca-certificates", "/usr/share/ca-certificates", "/usr/lib/ssl",
    // Name resolution.
    "/etc/resolv.conf", "/etc/hosts", "/etc/nsswitch.conf", "/etc/gai.conf",
    // Time zones, for temporal functions.
    "/etc/localtime", "/usr/share/zoneinfo",
    // The process's own status (the RSS gauge) and the CPU limits a runtime
    // sizes its thread pool from.
    "/proc/self", "/sys/fs/cgroup", "/sys/devices/system/cpu",
    "/dev/null", "/dev/urandom",
];

fn flag(argv: &[String], name: &str) -> Option<String> {
    argv.iter().position(|a| a == name).and_then(|i| argv.get(i + 1)).cloned()
}

/// The [`Plan`] for a server started with `argv`, reading the environment
/// through `env`. Creates the data directory if absent, as the server would,
/// so it can be granted.
pub fn plan(argv: &[String], env: impl Fn(&str) -> Option<String>) -> Result<Plan, String> {
    let mut p = Plan::default();
    let pick = |flag_name: &str, var: &str| flag(argv, flag_name).or_else(|| env(var));

    if !argv.iter().any(|a| a == "--ephemeral") {
        let data = flag(argv, "--data-path").unwrap_or_else(|| "./samyama_data".to_string());
        std::fs::create_dir_all(&data)
            .map_err(|e| format!("--sandbox: cannot create the data path {data}: {e}"))?;
        p.read_write.push(PathBuf::from(data));
    }
    p.read_write.push(std::env::temp_dir());
    if let Some(log) = pick("--audit-log", "SAMYAMA_AUDIT_LOG") {
        let dir = Path::new(&log).parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(Path::new("."));
        p.read_write.push(dir.to_path_buf());
    }
    for (f, var) in [
        ("--tls-cert", "SAMYAMA_TLS_CERT"),
        ("--tls-key", "SAMYAMA_TLS_KEY"),
        ("--auth-file", "SAMYAMA_AUTH_FILE"),
        ("--snapshot-key", "SAMYAMA_SNAPSHOT_KEY"),
    ] {
        if let Some(path) = pick(f, var) {
            p.read_only.push(PathBuf::from(path));
        }
    }
    if let Some(dir) = flag(argv, "--import-dir") {
        p.read_only.push(PathBuf::from(dir));
    }
    for path in p.read_write.iter().chain(&p.read_only) {
        if !path.exists() {
            return Err(format!(
                "--sandbox: {} does not exist; a path the sandbox cannot open cannot be granted, \
                 and the server would fail when it first needed it",
                path.display()
            ));
        }
    }
    p.system_read_only = SYSTEM_READ_ONLY.iter().map(PathBuf::from).collect();
    Ok(p)
}

/// Settings that cannot work inside the sandbox, refused before it starts.
pub fn incompatible(env: impl Fn(&str) -> Option<String>) -> Option<String> {
    let provider = env("NLQ_PROVIDER").unwrap_or_default().to_ascii_lowercase();
    if provider.replace(['-', '_'], "") == "claudecode" {
        return Some(
            "--sandbox: NLQ_PROVIDER=claudecode runs the `claude` program, and the sandbox \
             refuses to start programs; use an HTTP provider (openai, gemini, ollama) or drop \
             --sandbox"
                .to_string(),
        );
    }
    None
}

/// Confine this process to `plan`. Call on the main thread before any other
/// thread is started.
#[cfg(target_os = "linux")]
pub fn apply(plan: &Plan) -> Result<Applied, String> {
    use landlock::{
        path_beneath_rules, Access, AccessFs, Ruleset, RulesetAttr, RulesetCreatedAttr, RulesetStatus, ABI,
    };
    let abi = ABI::V3;
    let status = Ruleset::default()
        .handle_access(AccessFs::from_all(abi))
        .and_then(|r| r.create())
        .and_then(|r| r.add_rules(path_beneath_rules(&plan.read_write, AccessFs::from_all(abi))))
        .and_then(|r| r.add_rules(path_beneath_rules(&plan.read_only, AccessFs::from_read(abi))))
        .and_then(|r| r.add_rules(path_beneath_rules(&plan.system_read_only, AccessFs::from_read(abi))))
        .and_then(|r| r.restrict_self())
        .map_err(|e| format!("--sandbox: Landlock could not be applied: {e}"))?;
    let landlock = match status.ruleset {
        RulesetStatus::FullyEnforced => "fully",
        RulesetStatus::PartiallyEnforced => "partially",
        RulesetStatus::NotEnforced => {
            return Err("--sandbox: this kernel does not enforce Landlock (Linux 5.13+ with \
                        landlock in the LSM list); refusing to run unconfined"
                .to_string())
        }
    };
    if !status.no_new_privs {
        return Err("--sandbox: PR_SET_NO_NEW_PRIVS could not be set".to_string());
    }
    let denied_syscalls = deny_syscalls()?;
    Ok(Applied { landlock, denied_syscalls })
}

/// Off Linux there is no Landlock or seccomp, and `--sandbox` is refused.
#[cfg(not(target_os = "linux"))]
pub fn apply(_plan: &Plan) -> Result<Applied, String> {
    Err("--sandbox uses Linux Landlock and seccomp and is not available on this platform".to_string())
}

/// The system calls refused with `EPERM`.
#[cfg(target_os = "linux")]
pub fn denied_syscalls() -> Vec<i64> {
    vec![
        libc::SYS_execve, libc::SYS_execveat,
        libc::SYS_ptrace, libc::SYS_process_vm_readv, libc::SYS_process_vm_writev,
        libc::SYS_mount, libc::SYS_umount2, libc::SYS_pivot_root, libc::SYS_bpf,
        libc::SYS_init_module, libc::SYS_finit_module, libc::SYS_delete_module,
        libc::SYS_kexec_load,
    ]
}

#[cfg(target_os = "linux")]
fn deny_syscalls() -> Result<usize, String> {
    use seccompiler::{apply_filter_all_threads, BpfProgram, SeccompAction, SeccompFilter};
    use std::collections::BTreeMap;
    let calls = denied_syscalls();
    let rules: BTreeMap<i64, Vec<seccompiler::SeccompRule>> = calls.iter().map(|&c| (c, vec![])).collect();
    let arch = std::env::consts::ARCH
        .try_into()
        .map_err(|e| format!("--sandbox: seccomp does not support this architecture: {e:?}"))?;
    let program: BpfProgram = SeccompFilter::new(
        rules,
        SeccompAction::Allow,
        SeccompAction::Errno(libc::EPERM as u32),
        arch,
    )
    .and_then(BpfProgram::try_from)
    .map_err(|e| format!("--sandbox: seccomp filter: {e}"))?;
    apply_filter_all_threads(&program).map_err(|e| format!("--sandbox: seccomp could not be applied: {e}"))?;
    Ok(calls.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(a: &[&str]) -> Vec<String> {
        std::iter::once("samyama").chain(a.iter().copied()).map(String::from).collect()
    }

    #[test]
    fn the_plan_follows_the_paths_the_server_was_given() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let cert = dir.path().join("cert.pem");
        std::fs::write(&cert, "x").unwrap();
        let p = plan(&argv(&["--data-path", data.to_str().unwrap(), "--tls-cert", cert.to_str().unwrap()]), |_| None)
            .unwrap();
        assert!(p.read_write.contains(&data), "the data path is writable");
        assert!(data.is_dir(), "and created, so it can be granted");
        assert!(p.read_only.contains(&cert));
        assert!(p.read_write.contains(&std::env::temp_dir()));
        assert!(!p.read_write.iter().chain(&p.read_only).any(|x| x == Path::new("/etc") || x == Path::new("/")),
                "nothing wider than asked for");
    }

    #[test]
    fn ephemeral_grants_no_data_path_and_env_paths_count() {
        let dir = tempfile::tempdir().unwrap();
        let auth = dir.path().join("creds");
        std::fs::write(&auth, "x").unwrap();
        let a = auth.to_str().unwrap().to_string();
        let p = plan(&argv(&["--ephemeral"]), |k| (k == "SAMYAMA_AUTH_FILE").then(|| a.clone())).unwrap();
        assert_eq!(p.read_write, vec![std::env::temp_dir()]);
        assert_eq!(p.read_only, vec![auth]);
    }

    #[test]
    fn a_named_path_that_does_not_exist_is_refused() {
        let err = plan(&argv(&["--ephemeral", "--tls-cert", "/no/such/cert.pem"]), |_| None).unwrap_err();
        assert!(err.contains("/no/such/cert.pem"), "{err}");
    }

    #[test]
    fn an_nlq_provider_that_runs_a_program_is_incompatible() {
        assert!(incompatible(|k| (k == "NLQ_PROVIDER").then(|| "claudecode".into())).is_some());
        assert!(incompatible(|k| (k == "NLQ_PROVIDER").then(|| "claude-code".into())).is_some());
        assert!(incompatible(|k| (k == "NLQ_PROVIDER").then(|| "openai".into())).is_none());
        assert!(incompatible(|_| None).is_none());
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn off_linux_the_sandbox_is_refused_not_skipped() {
        assert!(apply(&Plan::default()).unwrap_err().contains("not available"));
    }
}
