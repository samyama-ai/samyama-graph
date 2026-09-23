//! At-rest encryption for snapshots (REL-09).
//!
//! A `.sgsnap` is a gzipped stream of JSON lines. This wraps that stream rather
//! than changing it, so what is encrypted is exactly what would otherwise have
//! been written, and an unencrypted snapshot still loads unchanged.
//!
//! # The container
//!
//! ```text
//! magic    12 bytes   "SGSNAPENC\0\0\1"
//! prefix    8 bytes   random, per file
//! frames    repeated  [u32 LE length][ciphertext+tag]
//! ```
//!
//! Each frame holds at most [`FRAME`] bytes of plaintext and is sealed
//! separately, so a snapshot larger than memory can be written and read a frame
//! at a time. Streaming is not a nicety here: these files are whole graphs.
//!
//! # Why the nonce is built this way
//!
//! ChaCha20-Poly1305's nonce is 12 bytes and **must never repeat for a key**.
//! It is the 8-byte random per-file prefix followed by a 4-byte little-endian
//! frame counter, so:
//!
//! - within a file, every frame has a different counter;
//! - across files, the prefix differs with overwhelming probability, so the
//!   same key may be used for many snapshots.
//!
//! A counter alone would repeat the moment the key was reused on a second file,
//! which is exactly how this construction is usually got wrong.
//!
//! # Why truncation is detected
//!
//! AEAD authenticates each frame, which stops a frame being altered, but says
//! nothing about a frame being *removed from the end* — and a snapshot missing
//! its last million nodes that imports cleanly is worse than one that fails.
//!
//! So the stream ends with an explicit terminator frame, sealed like any other.
//! Reading stops at the terminator, and hitting end-of-file first is an error.
//! An attacker cannot forge a terminator without the key, and cannot move one
//! from another file because the nonce, and therefore the tag, depends on the
//! frame's position.

use std::io::{Read, Write};

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};

/// File magic. The trailing byte is a format version, so a later change can be
/// refused by name rather than by a decryption failure that looks like a wrong
/// key.
const MAGIC: &[u8; 12] = b"SGSNAPENC\0\0\x01";

/// Plaintext bytes per frame. 64 KiB keeps the per-frame tag overhead under
/// 0.03% while bounding how much has to be held at once.
const FRAME: usize = 64 * 1024;

/// The marker that says a stream ended where it meant to.
const TERMINATOR: &[u8] = b"end";

/// Bytes of key material a snapshot key file must hold.
pub const KEY_BYTES: usize = 32;

/// Is this the start of an encrypted snapshot?
///
/// Reads only the first bytes, so an unencrypted snapshot is not disturbed.
pub fn looks_encrypted(head: &[u8]) -> bool {
    head.len() >= MAGIC.len() && &head[..MAGIC.len()] == MAGIC.as_slice()
}

/// Read a 32-byte key from a file holding either raw bytes or hex.
///
/// Hex is accepted because a key that can be pasted into a secret manager is a
/// key an operator can actually rotate, and `samyama snapshot-key` prints one.
pub fn read_key(path: &std::path::Path) -> Result<[u8; KEY_BYTES], String> {
    let raw = std::fs::read(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    if raw.len() == KEY_BYTES {
        let mut k = [0u8; KEY_BYTES];
        k.copy_from_slice(&raw);
        return Ok(k);
    }
    let text = String::from_utf8_lossy(&raw);
    let hex: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    if hex.len() != KEY_BYTES * 2 {
        return Err(format!(
            "{} holds {} bytes; a snapshot key is {KEY_BYTES} raw bytes or {} hex characters",
            path.display(),
            raw.len(),
            KEY_BYTES * 2
        ));
    }
    let mut k = [0u8; KEY_BYTES];
    for (i, b) in k.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| format!("{} is not hexadecimal", path.display()))?;
    }
    Ok(k)
}

/// A fresh key from the OS random source, as hex.
pub fn generate_key() -> Result<String, String> {
    let mut raw = [0u8; KEY_BYTES];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut raw))
        .map_err(|e| format!("cannot read /dev/urandom: {e}"))?;
    Ok(raw.iter().map(|b| format!("{b:02x}")).collect())
}

fn nonce_for(prefix: &[u8; 8], counter: u32) -> Nonce {
    let mut n = [0u8; 12];
    n[..8].copy_from_slice(prefix);
    n[8..].copy_from_slice(&counter.to_le_bytes());
    *Nonce::from_slice(&n)
}

/// A writer that seals everything written to it into frames.
///
/// **`finish` must be called.** `Drop` cannot report an error, so a terminator
/// written from `Drop` would be a silent failure on a full disk -- and a
/// snapshot missing its terminator is one that will not import. `finish`
/// returns the error instead.
pub struct EncryptingWriter<W: Write> {
    inner: W,
    cipher: ChaCha20Poly1305,
    prefix: [u8; 8],
    counter: u32,
    buf: Vec<u8>,
    finished: bool,
}

impl<W: Write> EncryptingWriter<W> {
    pub fn new(mut inner: W, key: &[u8; KEY_BYTES]) -> Result<Self, String> {
        let mut prefix = [0u8; 8];
        std::fs::File::open("/dev/urandom")
            .and_then(|mut f| f.read_exact(&mut prefix))
            .map_err(|e| format!("cannot read /dev/urandom: {e}"))?;

        inner.write_all(MAGIC).map_err(|e| e.to_string())?;
        inner.write_all(&prefix).map_err(|e| e.to_string())?;

        Ok(Self {
            inner,
            cipher: ChaCha20Poly1305::new(Key::from_slice(key)),
            prefix,
            counter: 0,
            buf: Vec::with_capacity(FRAME),
            finished: false,
        })
    }

    fn seal(&mut self, plaintext: &[u8]) -> std::io::Result<()> {
        let nonce = nonce_for(&self.prefix, self.counter);
        // The counter is in the associated data as well as in the nonce, so a
        // frame cannot be replayed at a different position even if an attacker
        // could arrange the same nonce.
        let aad = self.counter.to_le_bytes();
        let sealed = self
            .cipher
            .encrypt(&nonce, Payload { msg: plaintext, aad: &aad })
            .map_err(|_| std::io::Error::other("snapshot encryption failed"))?;
        self.counter = self.counter.checked_add(1).ok_or_else(|| {
            // 2^32 frames is 256 TiB at this frame size. Refusing beats reusing
            // a nonce, which would lose the confidentiality of the whole file.
            std::io::Error::other("snapshot too large: frame counter would wrap")
        })?;
        self.inner.write_all(&(sealed.len() as u32).to_le_bytes())?;
        self.inner.write_all(&sealed)
    }

    /// Seal what is buffered, write the terminator, and return the inner writer.
    pub fn finish(mut self) -> std::io::Result<W> {
        if !self.buf.is_empty() {
            let pending = std::mem::take(&mut self.buf);
            self.seal(&pending)?;
        }
        self.seal(TERMINATOR)?;
        self.finished = true;
        self.inner.flush()?;
        // `self` has a `Drop` that warns when `finish` was skipped; defuse it.
        let inner = unsafe { std::ptr::read(&self.inner) };
        std::mem::forget(self);
        Ok(inner)
    }
}

impl<W: Write> Write for EncryptingWriter<W> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(data);
        while self.buf.len() >= FRAME {
            let rest = self.buf.split_off(FRAME);
            let full = std::mem::replace(&mut self.buf, rest);
            self.seal(&full)?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // Deliberately does not seal a partial frame. `flush` is called by
        // `BufWriter` and by gzip finalisation, and sealing on each would cut
        // the stream into many tiny frames -- correct, but wasteful and a
        // needless spend of the frame counter.
        self.inner.flush()
    }
}

impl<W: Write> Drop for EncryptingWriter<W> {
    fn drop(&mut self) {
        if !self.finished {
            tracing::error!(
                "encrypted snapshot dropped without finish(): no terminator was \
                 written, so this file will not import"
            );
        }
    }
}

/// A reader that opens frames written by [`EncryptingWriter`].
pub struct DecryptingReader<R: Read> {
    inner: R,
    cipher: ChaCha20Poly1305,
    prefix: [u8; 8],
    counter: u32,
    buf: Vec<u8>,
    pos: usize,
    done: bool,
}

impl<R: Read> DecryptingReader<R> {
    /// `head` is the bytes already read from `inner` while sniffing the magic.
    pub fn new(mut inner: R, key: &[u8; KEY_BYTES], head: &[u8]) -> Result<Self, String> {
        if !looks_encrypted(head) {
            return Err("not an encrypted snapshot".to_string());
        }
        let mut prefix = [0u8; 8];
        let have = head.len().saturating_sub(MAGIC.len()).min(8);
        prefix[..have].copy_from_slice(&head[MAGIC.len()..MAGIC.len() + have]);
        if have < 8 {
            inner
                .read_exact(&mut prefix[have..])
                .map_err(|e| format!("truncated encrypted snapshot header: {e}"))?;
        }
        Ok(Self {
            inner,
            cipher: ChaCha20Poly1305::new(Key::from_slice(key)),
            prefix,
            counter: 0,
            buf: Vec::new(),
            pos: 0,
            done: false,
        })
    }

    /// Read and open one frame. `Ok(false)` at the terminator.
    fn next_frame(&mut self) -> std::io::Result<bool> {
        let mut len = [0u8; 4];
        match self.inner.read_exact(&mut len) {
            Ok(()) => {}
            // End of file *before* a terminator is a truncated snapshot. This
            // is the case AEAD alone does not cover, and the reason the
            // terminator exists.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "encrypted snapshot ends without its terminator: it is truncated",
                ))
            }
            Err(e) => return Err(e),
        }
        let len = u32::from_le_bytes(len) as usize;
        let mut sealed = vec![0u8; len];
        self.inner.read_exact(&mut sealed)?;

        let nonce = nonce_for(&self.prefix, self.counter);
        let aad = self.counter.to_le_bytes();
        let plain = self
            .cipher
            .decrypt(&nonce, Payload { msg: &sealed, aad: &aad })
            .map_err(|_| {
                std::io::Error::other(
                    "snapshot frame failed authentication: wrong key, or the file was altered",
                )
            })?;
        self.counter = self.counter.wrapping_add(1);

        if plain == TERMINATOR {
            self.done = true;
            return Ok(false);
        }
        self.buf = plain;
        self.pos = 0;
        Ok(true)
    }
}

impl<R: Read> Read for DecryptingReader<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if self.pos < self.buf.len() {
                let n = (self.buf.len() - self.pos).min(out.len());
                out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
                self.pos += n;
                return Ok(n);
            }
            if self.done {
                return Ok(0);
            }
            if !self.next_frame()? {
                return Ok(0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> [u8; KEY_BYTES] {
        [7u8; KEY_BYTES]
    }

    fn seal(data: &[u8], k: &[u8; KEY_BYTES]) -> Vec<u8> {
        let mut w = EncryptingWriter::new(Vec::new(), k).expect("writer");
        w.write_all(data).expect("write");
        w.finish().expect("finish")
    }

    fn open(bytes: &[u8], k: &[u8; KEY_BYTES]) -> std::io::Result<Vec<u8>> {
        let head = bytes[..MAGIC.len().min(bytes.len())].to_vec();
        let mut r = DecryptingReader::new(&bytes[head.len()..], k, &head)
            .map_err(std::io::Error::other)?;
        let mut out = Vec::new();
        r.read_to_end(&mut out)?;
        Ok(out)
    }

    #[test]
    fn a_round_trip_returns_the_bytes_that_went_in() {
        for len in [0usize, 1, 100, FRAME - 1, FRAME, FRAME + 1, FRAME * 3 + 7] {
            let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let sealed = seal(&data, &key());
            assert_eq!(open(&sealed, &key()).expect("open"), data, "length {len}");
        }
    }

    #[test]
    fn the_ciphertext_does_not_contain_the_plaintext() {
        // The check that says this is encryption and not framing.
        let data = b"the quick brown fox jumps over the lazy dog".repeat(100);
        let sealed = seal(&data, &key());
        assert!(
            !sealed.windows(16).any(|w| w == &data[..16]),
            "the plaintext is visible in the ciphertext"
        );
    }

    #[test]
    fn a_wrong_key_fails_rather_than_returning_rubbish() {
        let sealed = seal(b"secret", &key());
        let err = open(&sealed, &[9u8; KEY_BYTES]).expect_err("a wrong key must fail");
        assert!(err.to_string().contains("failed authentication"), "{err}");
    }

    #[test]
    fn an_altered_byte_fails() {
        let mut sealed = seal(b"secret payload", &key());
        let last = sealed.len() - 1;
        sealed[last] ^= 0x01;
        assert!(open(&sealed, &key()).is_err(), "a flipped bit must not open");
    }

    #[test]
    fn a_truncated_stream_fails_instead_of_returning_a_short_graph() {
        // AEAD authenticates each frame and says nothing about a frame removed
        // from the end. A snapshot missing its last million nodes that imports
        // cleanly is worse than one that fails.
        let data: Vec<u8> = (0..FRAME * 3).map(|i| (i % 251) as u8).collect();
        let sealed = seal(&data, &key());
        let cut = sealed.len() / 2;
        let err = open(&sealed[..cut], &key()).expect_err("a truncated stream must fail");
        assert!(
            err.to_string().contains("truncated") || err.kind() == std::io::ErrorKind::UnexpectedEof,
            "{err}"
        );
    }

    #[test]
    fn two_files_with_one_key_use_different_nonces() {
        // The property that makes reusing a key across snapshots safe. Equal
        // prefixes would mean equal nonces for equal frame numbers, which loses
        // the confidentiality of both files.
        let a = seal(b"same plaintext", &key());
        let b = seal(b"same plaintext", &key());
        assert_ne!(
            a[MAGIC.len()..MAGIC.len() + 8],
            b[MAGIC.len()..MAGIC.len() + 8],
            "the per-file nonce prefix repeated"
        );
        assert_ne!(a, b, "two sealings of the same bytes produced the same file");
    }

    #[test]
    fn the_magic_identifies_an_encrypted_file_and_nothing_else() {
        let sealed = seal(b"x", &key());
        assert!(looks_encrypted(&sealed));
        assert!(!looks_encrypted(b"\x1f\x8b\x08\x00 gzip header here"));
        assert!(!looks_encrypted(b""));
        assert!(!looks_encrypted(b"SGSNAP"));
    }

    #[test]
    fn a_key_file_may_be_raw_or_hex() {
        let dir = std::env::temp_dir().join(format!("samyama-key-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");

        let raw_path = dir.join("raw.key");
        std::fs::write(&raw_path, [3u8; KEY_BYTES]).expect("write");
        assert_eq!(read_key(&raw_path).expect("raw"), [3u8; KEY_BYTES]);

        let hex_path = dir.join("hex.key");
        std::fs::write(&hex_path, "03".repeat(KEY_BYTES) + "\n").expect("write");
        assert_eq!(read_key(&hex_path).expect("hex"), [3u8; KEY_BYTES]);

        let bad_path = dir.join("bad.key");
        std::fs::write(&bad_path, "too short").expect("write");
        assert!(read_key(&bad_path).is_err());
    }

    #[test]
    fn a_generated_key_is_hex_of_the_right_length_and_not_constant() {
        let a = generate_key().expect("key");
        let b = generate_key().expect("key");
        assert_eq!(a.len(), KEY_BYTES * 2);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b, "two generated keys were identical");
    }
}
