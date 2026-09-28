use std::path::Path;

#[derive(Clone, Debug, PartialEq)]
pub enum Role {
    Admin,
    Read,
    Write,
}

impl Role {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "admin" => Some(Role::Admin),
            "read" => Some(Role::Read),
            "write" => Some(Role::Write),
            _ => None,
        }
    }
}

/// What a credential line holds, and therefore how it is checked.
#[derive(Clone, Debug)]
pub enum Secret {
    /// SHA-256 of a machine token. Fast is correct here.
    Token([u8; 32]),
    /// An argon2 PHC string for a human-chosen password. Slow on purpose.
    Password(String),
}

#[derive(Clone, Debug)]
pub struct Credential {
    pub name: String,
    pub secret: Secret,
    /// If Some, restricts the user to this specific tenant. If None, no restriction.
    pub tenant: Option<String>,
    /// The user's roles.
    pub roles: Vec<Role>,
}

impl Credential {
    /// Parse one credential line.
    /// Format: `name:secret[:tenant=...][:roles=...]`
    pub fn parse(line: &str) -> Option<Result<Self, String>> {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }

        // We split by ':' but we must be careful with argon2 hashes which contain ':'
        // if they don't use the standard PHC format, or if future ones do. Actually,
        // the original implementation used `split_once` or `rsplit_once` because argon2
        // standard PHC doesn't contain ':'. 
        // Let's split by ':' but handle tenant and roles first.
        let mut parts: Vec<&str> = line.split(':').collect();
        if parts.len() < 2 {
            return Some(Err(format!("no `:` in {line:?}")));
        }

        let mut tenant = None;
        let mut roles = Vec::new();
        
        // Extract trailing :tenant=... or :roles=...
        while parts.len() > 2 {
            let last = parts.last().unwrap();
            if last.starts_with("tenant=") {
                tenant = Some(last["tenant=".len()..].to_string());
                parts.pop();
            } else if last.starts_with("roles=") {
                let r_str = &last["roles=".len()..];
                for r in r_str.split(',') {
                    if let Some(role) = Role::parse(r.trim()) {
                        roles.push(role);
                    }
                }
                parts.pop();
            } else {
                break;
            }
        }

        // The remaining parts are `name` and `secret`. 
        // Re-join the secret if there were multiple colons in it (e.g. some hash).
        let name = parts[0].trim().to_string();
        let secret_str = parts[1..].join(":");
        let secret_str = secret_str.trim();

        if secret_str.contains("$argon2") {
            return Some(Ok(Credential {
                name,
                secret: Secret::Password(secret_str.to_string()),
                tenant,
                roles,
            }));
        }

        if secret_str.len() != 64 {
            return Some(Err(format!(
                "expected a 64-character sha256 digest or an argon2 hash for {name:?}, \
                 got {} characters",
                secret_str.len()
            )));
        }
        let mut digest = [0u8; 32];
        for (i, b) in digest.iter_mut().enumerate() {
            *b = match u8::from_str_radix(&secret_str[i * 2..i * 2 + 2], 16) {
                Ok(v) => v,
                Err(_) => return Some(Err(format!("{secret_str:?} is not hexadecimal"))),
            };
        }
        Some(Ok(Credential {
            name,
            secret: Secret::Token(digest),
            tenant,
            roles,
        }))
    }
}

pub fn read_credentials(path: &Path) -> Result<Vec<Credential>, String> {
    let data = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read {path:?}: {e}"))?;
    let mut creds = Vec::new();
    for (i, line) in data.lines().enumerate() {
        match Credential::parse(line) {
            Some(Ok(c)) => creds.push(c),
            Some(Err(e)) => return Err(format!("{path:?}:{} {e}", i + 1)),
            None => {}
        }
    }
    Ok(creds)
}

/// Compare two digests in constant time.
pub fn digests_match(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for i in 0..32 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// Helper to authenticate a username and secret (used by RESP AUTH)
pub fn authenticate_user(credentials: &[Credential], username: &str, secret_input: &str) -> Option<Credential> {
    for cred in credentials {
        if cred.name == username {
            match &cred.secret {
                Secret::Token(digest) => {
                    if secret_input.len() == 64 {
                        let mut input_digest = [0u8; 32];
                        let mut valid_hex = true;
                        for i in 0..32 {
                            if let Ok(v) = u8::from_str_radix(&secret_input[i * 2..i * 2 + 2], 16) {
                                input_digest[i] = v;
                            } else {
                                valid_hex = false;
                            }
                        }
                        if valid_hex && digests_match(digest, &input_digest) {
                            return Some(cred.clone());
                        }
                    }
                }
                Secret::Password(phc) => {
                    use argon2::{Argon2, PasswordHash, PasswordVerifier};
                    if let Ok(parsed_hash) = PasswordHash::new(phc) {
                        if Argon2::default().verify_password(secret_input.as_bytes(), &parsed_hash).is_ok() {
                            return Some(cred.clone());
                        }
                    }
                }
            }
        }
    }
    None
}
