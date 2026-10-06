import json
import unittest

import bench


def ev(**kw):
    return json.dumps(kw)


STREAM = [
    ev(type="system", subtype="init", tools=["Read", "Grep"], mcp_servers=[{"name": "lidx", "status": "connected"}],
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
        self.assertIn("baseline has MCP", bench.check_init("baseline", init))
        down = {"mcp_servers": [{"name": "lidx", "status": "failed"}]}
        self.assertIn("not connected", bench.check_init("lidx", down))
        self.assertIsNone(bench.check_init("baseline", {"mcp_servers": []}))

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


if __name__ == "__main__":
    unittest.main()
