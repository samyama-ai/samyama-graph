#!/usr/bin/env python3
"""Tests for scripts/binary_size.py's budget, growth and baseline rules (HW-05, #1623).

    python3 scripts/test_binary_size.py

No build: the sizes are made up, and the rules are what is under test.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import binary_size as bs  # noqa: E402

MIB = bs.MIB
T = "x86_64-unknown-linux-gnu"


class Budgets(unittest.TestCase):
    def test_each_profile_is_held_to_its_own_budget(self):
        got = {p: ok for p, _, _, ok in bs.check_budgets({"server": 40 * MIB, "edge": 16 * MIB})}
        self.assertEqual(got, {"server": True, "edge": False})

    def test_the_budget_is_inclusive(self):
        [(_, _, _, ok)] = bs.check_budgets({"edge": 15 * MIB})
        self.assertTrue(ok)


class Growth(unittest.TestCase):
    base = {T: {"profiles": {"server": {"bytes": 40 * MIB, "commit": "abc"}}}}

    def test_no_baseline_for_the_platform_is_a_note_not_a_failure(self):
        fails, notes = bs.check_growth({"server": 99 * MIB}, {}, T)
        self.assertEqual(fails, [])
        self.assertTrue(any("no baseline" in n for n in notes))

    def test_another_platform_s_baseline_is_not_used(self):
        fails, notes = bs.check_growth({"server": 99 * MIB}, self.base, "aarch64-apple-darwin")
        self.assertEqual(fails, [])

    def test_ten_percent_passes_and_more_fails(self):
        self.assertEqual(bs.check_growth({"server": int(44 * MIB)}, self.base, T)[0], [])
        fails, _ = bs.check_growth({"server": int(44.5 * MIB)}, self.base, T)
        self.assertEqual(len(fails), 1)
        self.assertIn("more than 10%", fails[0])

    def test_shrinking_never_fails(self):
        self.assertEqual(bs.check_growth({"server": 10 * MIB}, self.base, T)[0], [])


class Record(unittest.TestCase):
    base = {T: {"profiles": {"server": {"bytes": 40 * MIB, "commit": "abc"}}}}

    def test_growth_past_the_limit_needs_a_reason(self):
        with self.assertRaises(ValueError):
            bs.record({"server": 50 * MIB}, self.base, T, None)
        out = bs.record({"server": 50 * MIB}, self.base, T, "added the vector index")
        self.assertEqual(out[T]["profiles"]["server"]["bytes"], 50 * MIB)
        self.assertEqual(out[T]["profiles"]["server"]["reason"], "added the vector index")

    def test_recording_one_platform_keeps_the_others(self):
        other = {"aarch64-apple-darwin": {"profiles": {"edge": {"bytes": 1, "commit": "x"}}}}
        out = bs.record({"server": 41 * MIB}, {**self.base, **other}, T, None)
        self.assertEqual(out["aarch64-apple-darwin"], other["aarch64-apple-darwin"])

    def test_recording_one_profile_keeps_the_other(self):
        base = {T: {"profiles": {"server": {"bytes": 40 * MIB, "commit": "a"}, "edge": {"bytes": 30 * MIB, "commit": "a"}}}}
        out = bs.record({"edge": 29 * MIB}, base, T, None)
        self.assertEqual(out[T]["profiles"]["server"]["bytes"], 40 * MIB)


class Profiles(unittest.TestCase):
    def test_edge_builds_without_default_features(self):
        self.assertEqual(bs.PROFILES["edge"], ["--no-default-features", "--features", "edge"])
        self.assertEqual(bs.PROFILES["server"], [])

    def test_the_edge_feature_does_not_bring_back_what_it_leaves_out(self):
        toml = (bs.ROOT / "Cargo.toml").read_text()
        features = toml.split("[features]", 1)[1].split("\n[", 1)[0]
        edge = next(l for l in features.splitlines() if l.startswith("edge ="))
        for left_out in ("arrow", "otel", "gpu", "cuda"):
            self.assertNotIn(left_out, edge, f"`edge` enables {left_out}: {edge}")
        default = next(l for l in features.splitlines() if l.startswith("default ="))
        self.assertIn('"arrow"', default, "the default build lost Arrow and Parquet")


if __name__ == "__main__":
    unittest.main()
