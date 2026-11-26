# bpftrace scripts for a live Samyama server

Linux only, as root, with [bpftrace](https://github.com/bpftrace/bpftrace) 0.20 or later. Each script attaches
to the server's USDT probes (`src/usdt.rs`, #2219) and to kernel events. The server does not need restarting.
While no script is attached, each probe is a single `nop`.

```bash
sudo bpftrace -p "$(pidof samyama)" ops/bpftrace/samyama-tail-latency.bt 100   # threshold in ms
sudo bpftrace -p "$(pidof samyama)" ops/bpftrace/samyama-mmap-faults.bt 1000   # minimum faults
sudo bpftrace -l "usdt:$(command -v samyama):samyama:*"                         # the probes
```

| Script | Prints | Answers |
|---|---|---|
| `samyama-tail-latency.bt` | A `SLOW query` or `SLOW persist` line per span over the threshold: on-CPU, futex, other syscalls, run-queue, fsync. Ctrl-C prints the histograms. | Was it the CPU, a lock, the disk or the scheduler? |
| `samyama-mmap-faults.bt` | A `FAULTS` line per span over the minimum, with minor and major faults. Ctrl-C prints faults outside any span, by thread, and the stacks that took major faults. | Is the statement touching new memory or paging from disk? |

## What the numbers mean

- **A write is two spans.** `query` runs the statement in the engine. `persist` writes its mutations to the WAL
  and RocksDB, with fsync when `SAMYAMA_FSYNC=1`. A write that waited on a slow disk shows a slow `persist` and
  a fast `query`.
- **`fsync` is timed at libc's `fsync`/`fdatasync`,** not at the syscall. So the whole wait counts, whoever
  caused it.
- **The store lock's wait does not appear.** The store lock is a tokio `RwLock` taken before `query__start`. A
  task waiting on it parks without blocking a thread. Attributing that wait needs lock probes, which don't
  exist yet.

## Checked in CI

`scripts/bpftrace_check.py` runs in the `bpftrace` CI job. It starts a server with `SAMYAMA_FSYNC=1`, makes
every fsync 300 ms slower with `strace`, and checks that:

- the slow writes are reported as `persist` spans spent mostly in `fsync`;
- a CPU-bound read is reported as mostly `oncpu`;
- a 100,000-node write's page faults are reported against its spans.

If the fsync timing goes back to syscall tracepoints, the check fails: the injected delay then lands in `other`.
