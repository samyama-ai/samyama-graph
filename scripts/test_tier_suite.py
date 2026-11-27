#!/usr/bin/env python3
"""Tests for scripts/tier_suite.py (HW-01, #1676).

    python3 scripts/test_tier_suite.py

Runs against the real docs/HARDWARE-TIERS.md as well as small manifests written
here, so a change to the page that the runner cannot read fails here.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tier_suite as ts  # noqa: E402

HEAD = ("| Capability | Tests | Server tier | Workstation tier | Edge tier | Micro tier | Browser tier | Note |\n"
        "|---|---|---|---|---|---|---|---|\n")


def manifest(*rows: str) -> str:
    return f"intro\n{ts.START}\n{HEAD}" + "".join(r + "\n" for r in rows) + f"{ts.END}\nafter\n"


class RealManifest(unittest.TestCase):
    def setUp(self):
        self.caps = ts.parse_manifest(ts.DOC.read_text(encoding="utf-8"))

    def test_every_tier_but_browser_runs_something(self):
        for tier in ts.TIERS:
            p = ts.plan(self.caps, tier, with_optional=False)
            if tier == "browser":
                self.assertEqual(p.commands, [], "the Browser tier has no build; it must run nothing")
            else:
                self.assertTrue(p.commands, f"the {tier} tier runs nothing")

    def test_the_gpu_tests_run_only_when_asked_and_cannot_skip(self):
        without = ts.plan(self.caps, "workstation", with_optional=False)
        self.assertFalse(any("--features" in c for c in without.commands))
        self.assertNotIn("SAMYAMA_REQUIRE_GPU", without.env)
        with_gpu = ts.plan(self.caps, "workstation", with_optional=True)
        self.assertTrue(any(c[c.index("-p") + 1] == "samyama-graph-algorithms" for c in with_gpu.commands if "-p" in c))
        self.assertEqual(with_gpu.env.get("SAMYAMA_REQUIRE_GPU"), "1")

    def test_server_reports_the_ha_gap(self):
        p = ts.plan(self.caps, "server", with_optional=False)
        self.assertTrue(any(g.startswith("HA failover") for g in p.gaps), p.gaps)


class Parsing(unittest.TestCase):
    def test_root_and_crate_tests_become_separate_commands(self):
        caps = ts.parse_manifest(manifest(
            "| A | `one` `two` | required | — | — | — | — | |",
            "| B | `two` `my-crate+feat::flt` | required | — | — | — | — | |",
        ))
        p = ts.plan(caps, "server", with_optional=False)
        self.assertEqual(p.commands[0], ["cargo", "test", "--no-fail-fast", "--test", "one", "--test", "two"])
        self.assertEqual(p.commands[1], ["cargo", "test", "--no-fail-fast", "-p", "my-crate", "--features", "feat", "--", "flt"])

    def test_a_status_it_does_not_know_is_refused(self):
        with self.assertRaises(ValueError):
            ts.parse_manifest(manifest("| A | `one` | requird | — | — | — | — | |"))

    def test_a_missing_marker_is_refused(self):
        with self.assertRaises(ValueError):
            ts.parse_manifest(HEAD + "| A | `one` | required | — | — | — | — | |\n")

    def test_a_short_row_is_refused(self):
        with self.assertRaises(ValueError):
            ts.parse_manifest(manifest("| A | `one` | required | — |"))

    def test_a_malformed_test_name_is_refused(self):
        with self.assertRaises(ValueError):
            ts.parse_manifest(manifest("| A | `cargo test --all` | required | — | — | — | — | |"))

    def test_nothing_to_run_exits_2(self):
        # The real Browser tier: no build, nothing to run.
        self.assertEqual(ts.main(["--tier", "browser", "--dry-run"]), 2)


if __name__ == "__main__":
    unittest.main()
