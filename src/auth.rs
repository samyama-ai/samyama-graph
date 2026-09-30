//! Credentials, roles and tenant binding for the HTTP and RESP listeners (REL-08, #1328).
//!
//! One credential file serves both protocols. Each line is
//!
//! ```text
//! name:secret[:tenant=<graph>][:roles=<role>,<role>...]
//! ```
//!
//! where `secret` is the SHA-256 hex digest of a machine token or an argon2 PHC
//! string for a password. The trailing fields are optional and may come in
//! either order.
//!
//! A line with no `roles=` field is an admin. Before roles existed every
//! credential could do everything, and a file written then has to mean the same
//! thing after an upgrade: the other default, no roles, would turn every
//! existing deployment into one that refuses every request.

use std::path::Path;

/// What a credential may do. Ordered: each role includes the ones below it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    /// Read-only statements and read-only endpoints.
    Read,
    /// Also mutating statements, imports, index creation and enrichment.
    Write,
    /// Also tenant management and snapshot restore.
    Admin,
}

impl Role {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "admin" => Some(Role::Admin),
            "read" => Some(Role::Read),
            "write" => Some(Role::Write),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Role::Read => "Read",
            Role::Write => "Write",
            Role::Admin => "Admin",
        }
    }
}

/// What a credential line holds, and therefore how it is checked.
///
/// The two are told apart by the stored form, not by a flag: an argon2 PHC
/// string starts with `$argon2`, and a token digest is 64 hex characters. A
/// flag that disagreed with the hash would be a way to check a password with a
/// fast hash.
#[derive(Clone)]
pub enum Secret {
    /// SHA-256 of a machine token. Fast is correct here: a 32-byte token from
    /// `samyama auth-token` has nothing to guess, and the check runs on every
    /// request.
    Token([u8; 32]),
    /// An argon2 PHC string for a human-chosen password. Slow on purpose:
    /// against a stolen file the cost of each guess is the defence.
    Password(String),
}

/// Never prints the stored secret, so a credential can be logged.
impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Secret::Token(_) => f.write_str("Token(..)"),
            Secret::Password(_) => f.write_str("Password(..)"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Credential {
    /// Who this credential belongs to. What the audit log records, and what an
    /// operator revokes one line of.
    pub name: String,
    pub secret: Secret,
    /// If set, the only graph this credential may address.
    pub tenant: Option<String>,
    /// Never empty after parsing; see the module docs for the default.
    pub roles: Vec<Role>,
}

impl Credential {
    /// Parse one credential line. Blank lines and `#` comments give `None`.
    pub fn parse(line: &str) -> Option<Result<Self, String>> {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        let mut parts: Vec<&str> = line.split(':').collect();
        if parts.len() < 2 {
            return Some(Err(format!("no `:` in {line:?}")));
        }

        let mut tenant = None;
        let mut roles = None;
        // Neither an argon2 PHC string nor a hex digest contains `=` in its
        // last `:`-separated field in a way that starts with these keys, so
        // peeling them off the end cannot eat part of a secret.
        while parts.len() > 2 {
            let last = parts[parts.len() - 1].trim();
            if let Some(t) = last.strip_prefix("tenant=") {
                if t.is_empty() {
                    return Some(Err(format!("empty `tenant=` in {line:?}")));
                }
                tenant = Some(t.to_string());
            } else if let Some(r) = last.strip_prefix("roles=") {
                let mut parsed = Vec::new();
                for name in r.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                    // An unknown role is an error, not a skipped word. Skipping
                    // is how `roles=writ` becomes a read-only credential that
                    // the operator believes can write.
                    match Role::parse(name) {
                        Some(role) => parsed.push(role),
                        None => {
                            return Some(Err(format!(
                                "unknown role {name:?} (expected read, write or admin)"
                            )))
                        }
                    }
                }
                if parsed.is_empty() {
                    return Some(Err(format!("empty `roles=` in {line:?}")));
                }
                roles = Some(parsed);
            } else {
                break;
            }
            parts.pop();
        }

        let name = parts[0].trim().to_string();
        // A future hash scheme might contain `:`, so rejoin what is left.
        let secret_str = parts[1..].join(":");
        let secret_str = secret_str.trim();
        let tenant_roles = |secret| Credential {
            name: name.clone(),
            secret,
            tenant: tenant.clone(),
            roles: roles.clone().unwrap_or_else(|| vec![Role::Admin]),
        };

        if secret_str.starts_with("$argon2") {
            return Some(Ok(tenant_roles(Secret::Password(secret_str.to_string()))));
        }
        match parse_hex_digest(secret_str) {
            Ok(d) => Some(Ok(tenant_roles(Secret::Token(d)))),
            Err(e) => Some(Err(format!("{e} for {name:?}"))),
        }
    }

    /// Whether this credential holds `role` or one above it.
    pub fn has_role(&self, role: Role) -> bool {
        self.roles.iter().any(|r| *r >= role)
    }

    /// Whether `presented` is this credential's token or password.
    ///
    /// A token is hashed before the comparison: what the file stores is the
    /// digest, and accepting the digest itself would let anyone who can read
    /// the file log in.
    pub fn verify(&self, presented: &str) -> bool {
        match &self.secret {
            Secret::Token(d) => {
                use sha2::{Digest, Sha256};
                let got: [u8; 32] = Sha256::digest(presented.as_bytes()).into();
                digests_match(d, &got)
            }
            Secret::Password(phc) => {
                use argon2::password_hash::{PasswordHash, PasswordVerifier};
                PasswordHash::new(phc)
                    .map(|h| {
                        argon2::Argon2::default()
                            .verify_password(presented.as_bytes(), &h)
                            .is_ok()
                    })
                    .unwrap_or(false)
            }
        }
    }

    /// Whether this credential may run a statement against `graph`.
    ///
    /// `is_write` is the engine's classification. An error there counts as a
    /// write: a statement the classifier cannot read is not one to let a
    /// read-only credential run.
    pub fn authorize_statement<E>(&self, graph: &str, is_write: Result<bool, E>) -> Result<(), String> {
        self.authorize_graph(graph)?;
        let needed = if is_write.unwrap_or(true) { Role::Write } else { Role::Read };
        self.authorize_role(needed)
    }

    /// Whether this credential may address `graph` at all.
    pub fn authorize_graph(&self, graph: &str) -> Result<(), String> {
        match &self.tenant {
            Some(t) if t != graph => {
                Err(format!("unauthorized: credential is bound to tenant '{t}'"))
            }
            _ => Ok(()),
        }
    }

    pub fn authorize_role(&self, needed: Role) -> Result<(), String> {
        if self.has_role(needed) {
            Ok(())
        } else {
            Err(format!("unauthorized: missing {} role", needed.as_str()))
        }
    }
}

fn parse_hex_digest(s: &str) -> Result<[u8; 32], String> {
    if s.len() != 64 {
        return Err(format!(
            "expected a 64-character sha256 digest or an argon2 hash, got {} characters",
            s.len()
        ));
    }
    let mut digest = [0u8; 32];
    for (i, b) in digest.iter_mut().enumerate() {
        *b = u8::from_str_radix(s.get(i * 2..i * 2 + 2).unwrap_or("zz"), 16)
            .map_err(|_| format!("{s:?} is not hexadecimal"))?;
    }
    Ok(digest)
}

/// Read a credential file.
///
/// A malformed line is an error rather than a skipped line. Skipping is how a
/// typo in a credential file becomes a server that starts cleanly and accepts
/// one fewer token than the operator believes it does.
pub fn read_credentials(path: &Path) -> Result<Vec<Credential>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let mut out = Vec::new();
    for (n, line) in text.lines().enumerate() {
        match Credential::parse(line) {
            None => continue,
            Some(Ok(c)) => out.push(c),
            Some(Err(e)) => return Err(format!("{}:{}: {e}", path.display(), n + 1)),
        }
    }
    if out.is_empty() {
        return Err(format!(
            "{} names no credentials; a file that authenticates nobody would refuse \
             every request, which is not what an operator who configured one meant",
            path.display()
        ));
    }
    Ok(out)
}

/// Compare two digests without letting the time taken depend on where they differ.
///
/// `a == b` on a slice returns as soon as a byte differs, so the time it takes
/// leaks how long a common prefix was, and a token can be recovered one byte at
/// a time.
pub fn digests_match(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for i in 0..32 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// The credential named `username` whose secret is `presented`, for RESP `AUTH`.
///
/// Only the line naming this user is checked, so a failed login runs argon2 at
/// most once rather than once per credential.
pub fn authenticate_user(credentials: &[Credential], username: &str, presented: &str) -> Option<Credential> {
    credentials
        .iter()
        .find(|c| c.name == username)
        .filter(|c| c.verify(presented))
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    const D: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"; // sha256("test")

    fn parse(line: &str) -> Credential {
        Credential::parse(line).expect("a line").expect("valid")
    }

    #[test]
    fn a_line_without_roles_is_an_admin_so_old_files_keep_working() {
        let c = parse(&format!("ops:{D}"));
        assert_eq!(c.roles, vec![Role::Admin]);
        assert!(c.has_role(Role::Write) && c.has_role(Role::Read));
        assert_eq!(c.tenant, None);
    }

    #[test]
    fn roles_and_tenant_parse_in_either_order() {
        for line in [
            format!("bob:{D}:tenant=acme:roles=read"),
            format!("bob:{D}:roles=read:tenant=acme"),
        ] {
            let c = parse(&line);
            assert_eq!(c.roles, vec![Role::Read]);
            assert_eq!(c.tenant.as_deref(), Some("acme"));
        }
    }

    #[test]
    fn an_unknown_or_empty_role_is_an_error_not_a_skipped_word() {
        assert!(Credential::parse(&format!("bob:{D}:roles=writ")).unwrap().is_err());
        assert!(Credential::parse(&format!("bob:{D}:roles=")).unwrap().is_err());
        assert!(Credential::parse(&format!("bob:{D}:tenant=")).unwrap().is_err());
    }

    #[test]
    fn roles_are_ordered() {
        let w = parse(&format!("w:{D}:roles=write"));
        assert!(w.has_role(Role::Read) && w.has_role(Role::Write) && !w.has_role(Role::Admin));
        let r = parse(&format!("r:{D}:roles=read"));
        assert!(r.has_role(Role::Read) && !r.has_role(Role::Write));
    }

    #[test]
    fn a_token_verifies_by_its_preimage_never_by_the_stored_digest() {
        let c = parse(&format!("svc:{D}"));
        assert!(c.verify("test"));
        assert!(!c.verify(D), "the digest in the file must not be a password");
        assert!(authenticate_user(&[c.clone()], "svc", "test").is_some());
        assert!(authenticate_user(&[c], "svc", D).is_none());
    }

    #[test]
    fn a_statement_needs_the_role_its_classification_asks_for() {
        let r = parse(&format!("r:{D}:roles=read:tenant=default"));
        assert!(r.authorize_statement("default", Ok::<_, ()>(false)).is_ok());
        assert!(r.authorize_statement("default", Ok::<_, ()>(true)).is_err());
        // An unclassifiable statement is treated as a write.
        assert!(r.authorize_statement("default", Err(())).is_err());
        assert!(r.authorize_statement("other", Ok::<_, ()>(false)).is_err());
    }

    #[test]
    fn debug_never_prints_the_secret() {
        let c = parse(&format!("svc:{D}"));
        assert!(!format!("{c:?}").contains(&D[..16]));
    }

    /// An argon2 PHC string for `password`, made the way an operator would.
    fn argon2_phc(password: &str) -> String {
        use argon2::password_hash::{PasswordHasher, SaltString};
        let salt = SaltString::encode_b64(b"samyama-test-salt").unwrap();
        argon2::Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .unwrap()
            .to_string()
    }

    #[test]
    fn role_names_parse_case_insensitively_and_render_capitalised() {
        assert_eq!(Role::parse("ADMIN"), Some(Role::Admin));
        assert_eq!(Role::parse("Read"), Some(Role::Read));
        assert_eq!(Role::parse("wRiTe"), Some(Role::Write));
        assert_eq!(Role::parse("owner"), None);
        assert_eq!(Role::Read.as_str(), "Read");
        assert_eq!(Role::Write.as_str(), "Write");
        assert_eq!(Role::Admin.as_str(), "Admin");
        assert!(Role::Read < Role::Write && Role::Write < Role::Admin);
    }

    #[test]
    fn blank_lines_and_comments_are_not_credentials() {
        assert!(Credential::parse("").is_none());
        assert!(Credential::parse("   ").is_none());
        assert!(Credential::parse("# ops:deadbeef").is_none());
        assert!(Credential::parse("   # indented comment").is_none());
    }

    #[test]
    fn a_line_without_a_colon_is_an_error_naming_the_line() {
        let err = Credential::parse("justaname").unwrap().unwrap_err();
        assert!(err.contains("no `:`"), "{err}");
        assert!(err.contains("justaname"), "{err}");
    }

    #[test]
    fn a_secret_that_is_neither_digest_nor_argon2_is_refused() {
        let short = Credential::parse("svc:abc123").unwrap().unwrap_err();
        assert!(short.contains("64-character"), "{short}");
        assert!(short.contains("got 6 characters"), "{short}");
        assert!(short.contains("\"svc\""), "names the credential: {short}");

        let not_hex = "z".repeat(64);
        let err = Credential::parse(&format!("svc:{not_hex}")).unwrap().unwrap_err();
        assert!(err.contains("not hexadecimal"), "{err}");
    }

    #[test]
    fn an_unknown_trailing_field_is_kept_as_part_of_the_secret() {
        // `foo=bar` is not a key this format knows, so peeling stops there and
        // the secret becomes `<digest>:foo=bar`, which is not a valid digest.
        let err = Credential::parse(&format!("svc:{D}:foo=bar")).unwrap().unwrap_err();
        assert!(err.contains("64-character"), "{err}");
    }

    #[test]
    fn roles_list_may_hold_several_and_ignores_blank_entries() {
        let c = parse(&format!("svc:{D}:roles=read, ,write"));
        assert_eq!(c.roles, vec![Role::Read, Role::Write]);
        assert!(c.has_role(Role::Write) && !c.has_role(Role::Admin));
    }

    #[test]
    fn an_argon2_line_is_a_password_verified_with_argon2() {
        let phc = argon2_phc("hunter2");
        let c = parse(&format!("alice:{phc}:roles=write"));
        assert!(matches!(c.secret, Secret::Password(_)));
        assert_eq!(format!("{:?}", c.secret), "Password(..)");
        assert!(!format!("{c:?}").contains(&phc), "debug leaked the hash");
        assert!(c.verify("hunter2"));
        assert!(!c.verify("hunter3"));
        assert!(authenticate_user(&[c.clone()], "alice", "hunter2").is_some());
        assert!(authenticate_user(&[c.clone()], "alice", "wrong").is_none());
        assert!(authenticate_user(&[c], "bob", "hunter2").is_none(), "wrong user");
    }

    #[test]
    fn a_malformed_argon2_string_never_verifies() {
        let c = parse("alice:$argon2id$not-a-real-hash");
        assert!(!c.verify("anything"));
        assert!(!c.verify(""));
    }

    #[test]
    fn a_token_debug_hides_the_digest() {
        let c = parse(&format!("svc:{D}"));
        assert_eq!(format!("{:?}", c.secret), "Token(..)");
    }

    #[test]
    fn authorize_role_names_the_missing_role() {
        let r = parse(&format!("r:{D}:roles=read"));
        assert!(r.authorize_role(Role::Read).is_ok());
        assert_eq!(
            r.authorize_role(Role::Admin).unwrap_err(),
            "unauthorized: missing Admin role"
        );
        let e = r.authorize_graph("other");
        assert!(e.is_ok(), "an unbound credential may address any graph");
        let bound = parse(&format!("b:{D}:tenant=acme"));
        assert_eq!(
            bound.authorize_graph("other").unwrap_err(),
            "unauthorized: credential is bound to tenant 'acme'"
        );
        assert!(bound.authorize_graph("acme").is_ok());
    }

    #[test]
    fn digests_match_only_when_every_byte_does() {
        let a = [7u8; 32];
        let mut b = a;
        assert!(digests_match(&a, &b));
        b[31] ^= 1;
        assert!(!digests_match(&a, &b));
        b = a;
        b[0] = 0;
        assert!(!digests_match(&a, &b));
    }

    #[test]
    fn a_credential_file_is_read_line_by_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds");
        std::fs::write(
            &path,
            format!("# operators\n\nops:{D}\nro:{D}:roles=read:tenant=acme\n"),
        )
        .unwrap();
        let creds = read_credentials(&path).unwrap();
        assert_eq!(creds.len(), 2);
        assert_eq!(creds[0].name, "ops");
        assert_eq!(creds[1].name, "ro");
        assert_eq!(creds[1].tenant.as_deref(), Some("acme"));
    }

    #[test]
    fn a_bad_line_in_the_file_is_an_error_with_its_line_number() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds");
        std::fs::write(&path, format!("ops:{D}\n# fine\nbroken\n")).unwrap();
        let err = read_credentials(&path).unwrap_err();
        assert!(err.ends_with(&format!("{}:3: no `:` in \"broken\"", path.display())), "{err}");
    }

    #[test]
    fn a_file_with_no_credentials_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds");
        std::fs::write(&path, "# nobody\n\n").unwrap();
        let err = read_credentials(&path).unwrap_err();
        assert!(err.contains("names no credentials"), "{err}");
    }

    #[test]
    fn a_missing_file_is_an_error_naming_the_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent");
        let err = read_credentials(&path).unwrap_err();
        assert!(err.starts_with("cannot read "), "{err}");
        assert!(err.contains("absent"), "{err}");
    }
}
