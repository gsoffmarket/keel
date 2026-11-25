"""Research registry and runner (PRD: Strategy research pipeline, registration rules,
FR-R1, governance).

Rules enforced here, not by convention:
- A hypothesis is refused unless every registration field is present: mechanism (who
  pays and why it persists), point-in-time universe rule, cluster key, at most four
  configurations, one primary statistic with alpha and power, guardrails, live loss
  budget, calendar cap, the plausible edge and the sigma it is measured against.
- The minimum detectable edge is computed from the census-measured cluster rate x
  participation x budget days; a plausible edge below it is refused as untestable.
- A registered spec is immutable once a run has started (its hash is checked on every
  run); any change is a new version. Thresholds never change mid-test.
- Every run counts as a trial, whatever its outcome, and records the snapshot id, the
  code version, the seed and an environment hash.
- Verdicts are GO, NO-GO, REDESIGN or INSUFFICIENT; INSUFFICIENT never counts as a pass.
  The simulator and the censuses can only kill: a run whose evidence kind is
  "simulation" or "census" cannot record GO. Rejected and NO-GO hypotheses stay in the
  graveyard; nothing is deleted.
"""
from __future__ import annotations

import hashlib
import json
import platform
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from keel import __version__
from keel.research import mde as M

REQUIRED = ("name", "mechanism", "universe_rule", "cluster_key", "configs", "primary_statistic", "alpha", "power", "guardrails", "live_loss_budget_usdc", "calendar_cap_days", "plausible_edge", "sigma", "clusters_per_week", "participation", "budget_days")
VERDICTS = ("GO", "NO-GO", "REDESIGN", "INSUFFICIENT")
STAGES = ("REGISTERED", "CENSUS", "SIM_SCREEN", "OPS_PAPER", "PROSPECTIVE_PAPER", "PILOT", "SCALED", "RETIRED", "REJECTED")
KILL_ONLY_EVIDENCE = ("simulation", "census", "historical_trades")

SCHEMA = """
CREATE TABLE IF NOT EXISTS hypotheses (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  version INTEGER NOT NULL,
  spec_json TEXT NOT NULL,
  spec_sha256 TEXT NOT NULL,
  mde REAL,
  testable INTEGER NOT NULL,
  stage TEXT NOT NULL,
  reason TEXT,
  created_ns INTEGER NOT NULL,
  UNIQUE(name, version)
);
CREATE TABLE IF NOT EXISTS runs (
  id INTEGER PRIMARY KEY,
  hypothesis_id INTEGER NOT NULL REFERENCES hypotheses(id),
  evaluator TEXT NOT NULL,
  evidence_kind TEXT NOT NULL,
  config_json TEXT NOT NULL,
  snapshot_id TEXT NOT NULL,
  keel_version TEXT NOT NULL,
  git_sha TEXT,
  seed INTEGER NOT NULL,
  env_sha256 TEXT NOT NULL,
  spec_sha256 TEXT NOT NULL,
  started_ns INTEGER NOT NULL,
  finished_ns INTEGER,
  status TEXT NOT NULL,
  metrics_json TEXT,
  result_sha256 TEXT,
  error TEXT
);
CREATE TABLE IF NOT EXISTS verdicts (
  id INTEGER PRIMARY KEY,
  run_id INTEGER NOT NULL REFERENCES runs(id),
  verdict TEXT NOT NULL,
  evidence_sha256 TEXT NOT NULL,
  note TEXT,
  created_ns INTEGER NOT NULL
);
CREATE TRIGGER IF NOT EXISTS spec_immutable BEFORE UPDATE OF spec_json, spec_sha256 ON hypotheses
BEGIN SELECT RAISE(ABORT, 'a registered spec is immutable: register a new version'); END;
CREATE TRIGGER IF NOT EXISTS no_delete_hypotheses BEFORE DELETE ON hypotheses
BEGIN SELECT RAISE(ABORT, 'hypotheses are never deleted (graveyard)'); END;
CREATE TRIGGER IF NOT EXISTS no_delete_runs BEFORE DELETE ON runs
BEGIN SELECT RAISE(ABORT, 'runs are never deleted: every run is a trial'); END;
"""


class RegistrationError(ValueError):
    pass


def _canon(x: Any) -> str:
    return json.dumps(x, sort_keys=True, separators=(",", ":"))


def _sha(x: Any) -> str:
    return hashlib.sha256(_canon(x).encode()).hexdigest()


def git_sha(cwd: Optional[Path] = None) -> Optional[str]:
    try:
        out = subprocess.run(["git", "rev-parse", "HEAD"], cwd=cwd, capture_output=True, text=True, timeout=5)
        sha = out.stdout.strip()
        if out.returncode != 0 or not sha:
            return None
        dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"], cwd=cwd, capture_output=True, text=True, timeout=5).stdout.strip()
        return sha + ("-dirty" if dirty else "")
    except (OSError, subprocess.SubprocessError):
        return None


def env_hash() -> str:
    return _sha({"python": sys.version, "platform": platform.platform(aliased=False, terse=True), "keel": __version__})


def validate(spec: Dict[str, Any]) -> Dict[str, Any]:
    missing = [k for k in REQUIRED if spec.get(k) in (None, "", [], {})]
    if missing:
        raise RegistrationError(f"missing registration fields: {', '.join(missing)}")
    if not isinstance(spec["configs"], list) or not 1 <= len(spec["configs"]) <= 4:
        raise RegistrationError("configs must list 1 to 4 configurations")
    if not (0 < float(spec["alpha"]) < 0.5) or not (0.5 <= float(spec["power"]) < 1):
        raise RegistrationError("alpha must be in (0, 0.5) and power in [0.5, 1)")
    if not isinstance(spec["primary_statistic"], str):
        raise RegistrationError("exactly one primary statistic (a string)")
    for k in ("plausible_edge", "sigma", "clusters_per_week", "participation", "budget_days", "live_loss_budget_usdc", "calendar_cap_days"):
        if float(spec[k]) <= 0:
            raise RegistrationError(f"{k} must be positive")
    if float(spec["participation"]) > 1:
        raise RegistrationError("participation is a share of clusters, at most 1")
    if float(spec["budget_days"]) > float(spec["calendar_cap_days"]):
        raise RegistrationError("budget_days exceeds the calendar cap")
    k = int(spec.get("concurrent_pilots", 1))
    return M.registration_check(
        float(spec["plausible_edge"]), float(spec["sigma"]), float(spec["clusters_per_week"]), float(spec["budget_days"]),
        float(spec["participation"]), k=k, design=float(spec.get("design_effect", 1.0)), max_days=float(spec.get("max_days_to_detect", 180)),
    )


class Registry:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(self.path))
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    # ---------------- hypotheses
    def register(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        chk = validate(spec)
        name = spec["name"]
        cur = self.db.execute("SELECT COALESCE(MAX(version), 0) FROM hypotheses WHERE name = ?", (name,))
        version = int(cur.fetchone()[0]) + 1
        stage = "REGISTERED" if chk["testable"] else "REJECTED"
        self.db.execute(
            "INSERT INTO hypotheses (name, version, spec_json, spec_sha256, mde, testable, stage, reason, created_ns) VALUES (?,?,?,?,?,?,?,?,?)",
            (name, version, _canon(spec), _sha(spec), chk["mde"] if chk["mde"] != float("inf") else None, int(chk["testable"]), stage, chk["reason"], time.time_ns()),
        )
        self.db.commit()
        h = self.get(name, version)
        if not chk["testable"]:
            raise RegistrationError(f"refused as untestable (kept in the graveyard as {name} v{version}): {chk['reason']}")
        return h

    def get(self, name: str, version: Optional[int] = None) -> Dict[str, Any]:
        if version is None:
            row = self.db.execute("SELECT * FROM hypotheses WHERE name = ? ORDER BY version DESC LIMIT 1", (name,)).fetchone()
        else:
            row = self.db.execute("SELECT * FROM hypotheses WHERE name = ? AND version = ?", (name, version)).fetchone()
        if row is None:
            raise KeyError(f"no hypothesis {name}" + (f" v{version}" if version else ""))
        d = dict(row)
        d["spec"] = json.loads(d.pop("spec_json"))
        d["trials"] = self.db.execute("SELECT COUNT(*) FROM runs WHERE hypothesis_id = ?", (d["id"],)).fetchone()[0]
        return d

    def list(self) -> List[Dict[str, Any]]:
        rows = self.db.execute("SELECT id, name, version, stage, mde, testable, reason FROM hypotheses ORDER BY name, version").fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["trials"] = self.db.execute("SELECT COUNT(*) FROM runs WHERE hypothesis_id = ?", (d["id"],)).fetchone()[0]
            v = self.db.execute("SELECT v.verdict FROM verdicts v JOIN runs r ON r.id = v.run_id WHERE r.hypothesis_id = ? ORDER BY v.id DESC LIMIT 1", (d["id"],)).fetchone()
            d["last_verdict"] = v[0] if v else None
            out.append(d)
        return out

    def trial_count(self) -> int:
        return self.db.execute("SELECT COUNT(*) FROM runs").fetchone()[0]

    # ---------------- runs
    def run(self, name: str, evaluator: str, snapshot, config_index: int = 0, seed: int = 0, version: Optional[int] = None, evaluators: Optional[Dict[str, Callable]] = None) -> Dict[str, Any]:
        from keel.research.evaluators import EVALUATORS

        evs = evaluators or EVALUATORS
        h = self.get(name, version)
        if h["stage"] in ("REJECTED", "RETIRED"):
            raise RegistrationError(f"{name} v{h['version']} is {h['stage']}: register a new version")
        if _sha(h["spec"]) != h["spec_sha256"]:
            raise RegistrationError("spec hash mismatch: the registry has been altered")
        if evaluator not in evs:
            raise KeyError(f"unknown evaluator {evaluator!r}; available: {', '.join(sorted(evs))}")
        fn = evs[evaluator]
        configs = h["spec"]["configs"]
        if not 0 <= config_index < len(configs):
            raise IndexError(f"config {config_index} out of range (0..{len(configs) - 1})")
        cfg = configs[config_index]
        cur = self.db.execute(
            "INSERT INTO runs (hypothesis_id, evaluator, evidence_kind, config_json, snapshot_id, keel_version, git_sha, seed, env_sha256, spec_sha256, started_ns, status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (h["id"], evaluator, getattr(fn, "evidence_kind", "simulation"), _canon(cfg), snapshot.id, __version__, git_sha(Path(__file__).parent), int(seed), env_hash(), h["spec_sha256"], time.time_ns(), "running"),
        )
        run_id = cur.lastrowid
        self.db.commit()  # the trial counts from the moment it starts
        try:
            metrics = fn(snapshot, cfg, int(seed))
            res = _sha(metrics)
            self.db.execute("UPDATE runs SET status='finished', finished_ns=?, metrics_json=?, result_sha256=? WHERE id=?", (time.time_ns(), _canon(metrics), res, run_id))
            if h["stage"] == "REGISTERED":
                self.db.execute("UPDATE hypotheses SET stage='CENSUS' WHERE id=? AND stage='REGISTERED'", (h["id"],))
        except Exception as exc:
            self.db.execute("UPDATE runs SET status='failed', finished_ns=?, error=? WHERE id=?", (time.time_ns(), repr(exc)[:2000], run_id))
            self.db.commit()
            raise
        self.db.commit()
        return self.get_run(run_id)

    def get_run(self, run_id: int) -> Dict[str, Any]:
        row = self.db.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row is None:
            raise KeyError(f"no run {run_id}")
        d = dict(row)
        d["config"] = json.loads(d.pop("config_json"))
        d["metrics"] = json.loads(d.pop("metrics_json")) if d.get("metrics_json") else None
        return d

    # ---------------- verdicts
    def verdict(self, run_id: int, verdict: str, note: str = "") -> Dict[str, Any]:
        if verdict not in VERDICTS:
            raise ValueError(f"verdict must be one of {VERDICTS}")
        run = self.get_run(run_id)
        if run["status"] != "finished":
            raise RegistrationError(f"run {run_id} is {run['status']}; a verdict needs a finished run")
        if verdict == "GO" and run["evidence_kind"] in KILL_ONLY_EVIDENCE:
            raise RegistrationError(f"{run['evidence_kind']} evidence can only kill: GO needs prospective paper or pilot evidence")
        ev = _sha({"run": run_id, "result": run["result_sha256"], "snapshot": run["snapshot_id"], "verdict": verdict})
        self.db.execute("INSERT INTO verdicts (run_id, verdict, evidence_sha256, note, created_ns) VALUES (?,?,?,?,?)", (run_id, verdict, ev, note, time.time_ns()))
        if verdict == "NO-GO":
            self.db.execute("UPDATE hypotheses SET stage='REJECTED', reason=? WHERE id=?", (f"NO-GO on run {run_id}: {note}", run["hypothesis_id"]))
        self.db.commit()
        return {"run_id": run_id, "verdict": verdict, "evidence_sha256": ev}
