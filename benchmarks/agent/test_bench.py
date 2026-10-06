import argparse
import json
import os
import tempfile
import unittest

import bench


def ev(**kw):
    return json.dumps(kw)


STREAM = [
    ev(type="system", subtype="init", tools=["Read", "Grep", "Glob", "mcp__lidx__lidx"], mcp_servers=[{"name": "lidx", "status": "connected"}],
       model="m"),
    ev(type="assistant", message={"content": [{"type": "text", "text": "hi"},
                                              {"type": "tool_use", "name": "Grep", "input": {}},
                                              {"type": "tool_use", "name": "mcp__lidx__lidx", "input": {}}]}),
    ev(type="user", message={"content": [{"type": "tool_result", "content": "x"}]}),
    ev(type="assistant", message={"content": [{"type": "tool_use", "name": "Grep", "input": {}}]}),
    "not json",
    ev(type="result", subtype="success", is_error=False, result="answer", num_turns=3, duration_ms=1234,
       total_cost_usd=0.05, permission_denials=[],
       usage={"input_tokens": 10, "cache_read_input_tokens": 200, "cache_creation_input_tokens": 30,
              "output_tokens": 7}),
]


class Parse(unittest.TestCase):
    def test_counts_and_tokens(self):
        p = bench.parse_stream(STREAM)
        self.assertEqual(p["tool_calls"], 3)
        self.assertEqual(p["lidx_calls"], 1)
        self.assertEqual(p["tool_counts"], {"Grep": 2, "mcp__lidx__lidx": 1})
        m, err = bench.trial_metrics(p)
        self.assertIsNone(err)
        self.assertEqual(m["input_tokens"], 240)
        self.assertEqual(m["input_components"], {"input": 10, "cache_read": 200, "cache_creation": 30})
        self.assertEqual(m["output_tokens"], 7)

    def test_error_subtype(self):
        bad = STREAM[:-1] + [ev(type="result", subtype="error_max_budget_usd", usage={}, total_cost_usd=0.5)]
        m, err = bench.trial_metrics(bench.parse_stream(bad))
        self.assertIn("error_max_budget_usd", err)

    def test_no_result(self):
        m, err = bench.trial_metrics(bench.parse_stream(STREAM[:2]))
        self.assertIsNone(m)
        self.assertTrue(err)

    def test_init_checks(self):
        init = bench.parse_stream(STREAM)["init"]
        self.assertIsNone(bench.check_init("lidx", init))
        self.assertIn("baseline has MCP", bench.check_init("baseline", dict(init, tools=["Glob", "Grep", "Read"])))
        down = {"mcp_servers": [{"name": "lidx", "status": "failed"}], "tools": init["tools"]}
        self.assertIn("not connected", bench.check_init("lidx", down))
        self.assertIsNone(bench.check_init("baseline", {"mcp_servers": [], "tools": ["Glob", "Grep", "Read"]}))

    def test_tool_set_validation(self):
        base = {"mcp_servers": [], "tools": ["Read", "Grep", "Glob"]}
        self.assertIsNone(bench.check_init("baseline", base))
        self.assertIn("tool set", bench.check_init("baseline", dict(base, tools=["Read", "Grep", "Glob", "Bash"])))
        self.assertIn("tool set", bench.check_init("baseline", dict(base, tools=["Read", "Grep"])))
        lidx = {"mcp_servers": [{"name": "lidx", "status": "connected"}], "tools": base["tools"]}
        self.assertIn("tool set", bench.check_init("lidx", lidx))        # lidx tool missing
        self.assertIsNone(bench.check_init("lidx", dict(lidx, tools=base["tools"] + ["mcp__lidx__lidx"])))

    def test_judge_scores_strict(self):
        ok = {d: 10 for d in bench.DIMS}
        self.assertEqual(bench.parse_scores({"structured_output": ok})["clarity"], 10)
        self.assertEqual(bench.parse_scores({"result": json.dumps(ok)})["clarity"], 10)
        self.assertIsNone(bench.parse_scores({"structured_output": dict(ok, clarity=21)}))
        self.assertIsNone(bench.parse_scores({"structured_output": dict(ok, clarity=9.0)}))
        self.assertIsNone(bench.parse_scores({"structured_output": dict(ok, clarity=True)}))
        self.assertIsNone(bench.parse_scores({"structured_output": {"clarity": 5}}))


def trial(task, profile, n, judge, tokens, calls, wall, status="ok", index=None):
    return {"task": task, "profile": profile, "trial": n, "status": status, "error": None,
            "index_seconds": index, "wall_seconds": wall,
            "metrics": {"input_tokens": tokens, "tool_calls": calls, "cost_usd": 0.1},
            "judge": {"total": judge, "cost_usd": 0.01} if judge is not None else None}


class Report(unittest.TestCase):
    def test_change_and_overall(self):
        ts = [trial("a", "baseline", 1, 50, 1000, 10, 100), trial("a", "lidx", 1, 70, 500, 5, 50, index=3),
              trial("b", "baseline", 1, 60, 3000, 20, 200), trial("b", "lidx", 1, 60, 1500, 10, 100, index=5)]
        rep = bench.aggregate(ts)
        a = rep["rows"][0]
        self.assertEqual(a["change"]["judge"], 20)
        self.assertAlmostEqual(a["change"]["input_tokens"], -50.0)
        self.assertEqual(a["index_seconds"], 3)
        o = rep["overall"]
        self.assertEqual(o["profiles"]["baseline"]["judge"], 55)            # mean
        self.assertEqual(o["profiles"]["baseline"]["input_tokens"], 4000)   # sum of means
        self.assertEqual(o["profiles"]["lidx"]["input_tokens"], 2000)
        self.assertEqual(o["change"]["judge"], 10)
        self.assertAlmostEqual(o["change"]["input_tokens"], -50.0)
        self.assertAlmostEqual(o["change"]["tool_calls"], -50.0)
        self.assertAlmostEqual(rep["cost"]["total_usd"], 0.4 + 0.04)
        self.assertIn("Overall", bench.render(rep))

    def test_disagreement_flag_and_errors(self):
        ts = [trial("a", "baseline", 1, 30, 1000, 10, 100), trial("a", "baseline", 2, 60, 1000, 25, 100),
              trial("a", "lidx", 1, 50, 1000, 10, 100), trial("a", "lidx", 2, 55, 1000, 12, 100),
              trial("a", "lidx", 3, None, 9, 1, 1, status="error")]
        ts[-1]["error"] = "budget"
        rep = bench.aggregate(ts)
        row = rep["rows"][0]
        self.assertEqual(len(row["flags"]["baseline"]), 2)   # judge range 30 > 20 and calls 25 > 2x10
        self.assertNotIn("lidx", row["flags"])
        self.assertEqual(row["profiles"]["lidx"]["judge"]["n"], 2)    # errored trial excluded
        self.assertEqual(len(rep["errors"]), 1)
        self.assertIsNotNone(row["profiles"]["baseline"]["judge"]["sd"])
        self.assertIn("budget", bench.render(rep))


class Paths(unittest.TestCase):
    def test_relative_paths_become_absolute(self):
        import argparse
        import os
        a = argparse.Namespace(out=".work/x", workdir="w", lidx="~/bin/lidx", tasks=["t.json", "/abs/u.json"],
                               scratch=None, other="keep")
        bench.normalize_paths(a)
        self.assertEqual(a.out, os.path.join(os.getcwd(), ".work/x"))
        self.assertEqual(a.workdir, os.path.join(os.getcwd(), "w"))
        self.assertEqual(a.lidx, os.path.expanduser("~/bin/lidx"))
        self.assertEqual(a.tasks, [os.path.join(os.getcwd(), "t.json"), "/abs/u.json"])
        self.assertIsNone(a.scratch)
        self.assertEqual(a.other, "keep")

    def test_deny_rules(self):
        rules = bench.deny_rules(["/Users/x/lidx", "/tmp/run"])
        self.assertIn("Read(//Users/x/lidx/**)", rules)
        self.assertIn("Read(//tmp/run/**)", rules)
        self.assertTrue(all(r.startswith("Read(//") and not r.startswith("Read(///") for r in rules))

    def test_deny_args_in_command(self):
        import argparse
        import os
        a = argparse.Namespace(model="claude-haiku-4-5-20251001", effort=None, budget=0.1, out="/tmp/some/run")
        cmd = bench.claude_cmd(a, "lidx", "/tmp/some/run/mcp.json", "q")
        i = cmd.index("--disallowedTools")
        denied = cmd[i + 1:]
        self.assertIn("Read(//tmp/some/run/**)", denied)
        self.assertIn("Read(//%s/**)" % bench.REPO_ROOT.lstrip("/"), denied)
        self.assertEqual(cmd.count("--disallowedTools"), 1)
        self.assertFalse(os.path.commonpath([bench.default_scratch("/tmp/some/run"), bench.REPO_ROOT]) == bench.REPO_ROOT)


class Variance(unittest.TestCase):
    def test_overall_stdev_over_trial_index(self):
        import statistics
        ts = []
        # trial i: judge a=(40,50) b=(60,70) per profile; lidx = baseline + 10
        for task, vals in (("a", (40, 60)), ("b", (50, 70))):
            for i, v in enumerate(vals, 1):
                ts.append(trial(task, "baseline", i, v, 100 * i, 10, 10))
                ts.append(trial(task, "lidx", i, v + 10, 50 * i, 5, 5))
        o = bench.aggregate(ts)["overall"]
        # trial 1 mean judge 45, trial 2 mean judge 65 -> stdev of [45, 65]
        self.assertAlmostEqual(o["sd"]["baseline"]["judge"], statistics.stdev([45, 65]))
        # tokens: sum over tasks at trial i = 200*i -> [200, 400]
        self.assertAlmostEqual(o["sd"]["baseline"]["input_tokens"], statistics.stdev([200, 400]))
        overall_row = [x for x in bench.render(bench.aggregate(ts)).splitlines() if x.startswith("| **Overall**")][0]
        self.assertIn("±", overall_row)

    def test_single_trial_has_no_stdev(self):
        o = bench.aggregate([trial("a", "baseline", 1, 50, 1, 1, 1), trial("a", "lidx", 1, 50, 1, 1, 1)])["overall"]
        self.assertIsNone(o["sd"]["lidx"]["judge"])


class ErrorAsymmetry(unittest.TestCase):
    def test_flag_and_counts(self):
        bad = trial("a", "lidx", 2, None, 0, 0, 0, status="error")
        bad["error"] = "boom"
        ts = [trial("a", "baseline", 1, 50, 1, 1, 1), trial("a", "baseline", 2, 50, 1, 1, 1),
              trial("a", "lidx", 1, 50, 1, 1, 1), bad,
              trial("b", "baseline", 1, 50, 1, 1, 1), trial("b", "lidx", 1, 50, 1, 1, 1)]
        rep = bench.aggregate(ts)
        a, b = rep["rows"]
        self.assertEqual(a["errors"], {"baseline": 0, "lidx": 1})
        self.assertTrue(a["error_asymmetry"])
        self.assertFalse(b["error_asymmetry"])
        self.assertEqual(rep["overall"]["errors"], {"baseline": 0, "lidx": 1})
        text = bench.render(rep)
        self.assertIn("⚠ errors differ between profiles", text)
        self.assertEqual(sum("errors differ" in line for line in text.splitlines()), 2)   # task a + overall

    def test_all_errored_task_still_listed(self):
        bad = trial("a", "lidx", 1, None, 0, 0, 0, status="error")
        rep = bench.aggregate([bad, trial("a", "baseline", 1, 50, 1, 1, 1)])
        self.assertEqual(rep["rows"][0]["errors"]["lidx"], 1)
        self.assertTrue(rep["rows"][0]["error_asymmetry"])


class ReuseBaseline(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        r = self.tmp.name
        self.tf = os.path.join(r, "t.json")
        bench.write_json(self.tf, {"tasks": []})
        self.src, self.out = os.path.join(r, "src"), os.path.join(r, "out")
        self.tasks = [{"id": "a:1", "source": self.tf}, {"id": "b:2", "source": self.tf}]
        os.makedirs(self.src)
        bench.write_json(os.path.join(self.src, "run.json"), {
            "model": "m", "effort": "high", "budget_usd": 2.0,
            "task_files": {"t.json": bench.file_hash(self.tf)}})
        for t in ("a_1", "b_2"):
            for n in (1, 2):
                self.put(t, "baseline", n, "ok")
        self.put("a_1", "lidx", 1, "ok")
        self.args = argparse.Namespace(model="m", effort="high", budget=2.0, trials=2, out=self.out)

    def put(self, t, profile, n, status):
        d = os.path.join(self.src, t, profile, str(n))
        os.makedirs(d)
        bench.write_json(os.path.join(d, "trial.json"), {"status": status, "task": t})
        for f, body in (("stream.jsonl", "{}"), ("judge.json", "{}")):
            with open(os.path.join(d, f), "w") as fh:
                fh.write(body)

    def test_copies_selected_trials_only(self):
        self.args.trials = 1
        got = bench.reuse_baseline(self.args, self.tasks[:1], self.src)
        self.assertEqual(got, self.src)
        d = os.path.join(self.out, "a_1", "baseline", "1")
        self.assertEqual(sorted(os.listdir(d)), ["judge.json", "stream.jsonl", "trial.json"])
        self.assertFalse(os.path.islink(os.path.join(d, "trial.json")))
        self.assertFalse(os.path.exists(os.path.join(self.out, "a_1", "baseline", "2")))
        self.assertFalse(os.path.exists(os.path.join(self.out, "b_2")))
        self.assertFalse(os.path.exists(os.path.join(self.out, "a_1", "lidx")))

    def test_refuses_on_settings_mismatch(self):
        for k, v in (("model", "other"), ("effort", "low"), ("budget", 3.0)):
            a = argparse.Namespace(**dict(vars(self.args), **{k: v}))
            with self.assertRaises(bench.Fatal):
                bench.reuse_baseline(a, self.tasks, self.src)
        with open(self.tf, "w") as f:
            f.write('{"tasks": [1]}')
        with self.assertRaises(bench.Fatal):
            bench.reuse_baseline(self.args, self.tasks, self.src)
        self.assertFalse(os.path.exists(self.out))

    def test_refuses_on_missing_or_failed_trial(self):
        self.args.trials = 3
        with self.assertRaises(bench.Fatal):
            bench.reuse_baseline(self.args, self.tasks, self.src)
        self.args.trials = 2
        bench.write_json(os.path.join(self.src, "b_2", "baseline", "2", "trial.json"), {"status": "error"})
        with self.assertRaises(bench.Fatal):
            bench.reuse_baseline(self.args, self.tasks, self.src)
        self.assertFalse(os.path.exists(self.out))     # nothing copied on refusal

    def test_header_shows_reuse_and_lidx(self):
        rep = bench.aggregate([trial("a", "baseline", 1, 50, 1, 1, 1)])
        txt = bench.render(rep, {"model": "m", "reused_baseline_from": "/x/run", "lidx_path": "/bin/lidx"})
        self.assertIn("Baseline reused from /x/run", txt)
        self.assertIn("lidx binary: /bin/lidx", txt)


if __name__ == "__main__":
    unittest.main()
