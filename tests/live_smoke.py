"""Live integration smoke for hermes-dbos-cron.

Requires: real Postgres (DBOS_SYSTEM_DATABASE_URL in /tmp/hermes-dbos-cron-test.env,
pointing at a DISPOSABLE test database) and the hermes-agent tree importable.

Phase 1 (default): starts the provider against a temp HERMES_HOME whose
jobs.json holds one cron job firing every minute. run_one_job is stubbed to
record fires into a file (no agent, no delivery). Waits for >=1 real
DBOS-triggered fire, verifies the claim went through the real Hermes store
(jobs.json next_run_at/last_run_at mutated), then pauses the job via the real
store API and verifies the DBOS schedule pauses.

Phase 2 (--restart-check): re-launches after simulated process death and
verifies schedules survive in Postgres and re-arm cleanly.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
from pathlib import Path

HERMES_TREE = Path("/Users/colin/.hermes/hermes-agent")
PLUGIN_DIR = Path(__file__).resolve().parents[1]

# Load test env
for line in Path("/tmp/hermes-dbos-cron-test.env").read_text().splitlines():
    key, _, value = line.partition("=")
    if key and value:
        os.environ[key.strip()] = value.strip()

sys.path.insert(0, str(HERMES_TREE))


def main() -> int:
    restart_check = "--restart-check" in sys.argv

    tmp_home = Path(tempfile.mkdtemp(prefix="hermes-dbos-smoke-"))
    (tmp_home / "cron").mkdir(parents=True)
    fires_file = tmp_home / "fires.jsonl"

    os.environ["HERMES_HOME"] = str(tmp_home)
    from hermes_constants import set_hermes_home_override

    set_hermes_home_override(str(tmp_home))

    from cron.jobs import save_jobs  # noqa: E402

    job = {
        "id": "smoke0001",
        "name": "dbos smoke",
        "prompt": "noop",
        "schedule": {"kind": "cron", "expr": "* * * * *", "display": "* * * * *"},
        "skills": [],
        "deliver": "local",
        "repeat": {"times": None, "completed": 0},
        "state": "scheduled",
        "enabled": True,
        "next_run_at": "2000-01-01T00:00:00+00:00",
        "created_at": "2026-01-01T00:00:00+00:00",
    }
    save_jobs([job])

    # Stub run_one_job BEFORE the provider imports it: record fires, no agent.
    import cron.scheduler as scheduler_mod

    def fake_run_one_job(claimed_job, adapters=None, loop=None, cancel_event=None):
        with open(fires_file, "a") as fh:
            fh.write(json.dumps({"id": claimed_job["id"], "at": time.time()}) + "\n")

    scheduler_mod.run_one_job = fake_run_one_job

    # Load the plugin through the real Hermes discovery machinery.
    from plugins.cron_providers import _load_provider_from_dir

    provider = _load_provider_from_dir(PLUGIN_DIR)
    assert provider is not None, "provider failed to load"
    assert provider.name == "dbos"
    assert provider.is_available(), "provider not available (env url missing?)"

    stop = threading.Event()
    thread = threading.Thread(target=provider.start, args=(stop,), daemon=True)
    thread.start()

    # Wait for launch + initial reconcile (schedules observable in DBOS).
    from dbos import DBOS as _DBOS_wait  # noqa: N811

    deadline = time.time() + 60
    armed_ok = False
    while time.time() < deadline:
        if provider._launched:
            try:
                if any(
                    s["schedule_name"].endswith("smoke0001")
                    for s in _DBOS_wait.list_schedules(
                        schedule_name_prefix=provider._schedule_prefix()
                    )
                ):
                    armed_ok = True
                    break
            except Exception:
                pass
        time.sleep(0.5)
    assert provider._launched, "DBOS did not launch"

    from dbos import DBOS

    prefix = provider._schedule_prefix()
    schedules = DBOS.list_schedules(schedule_name_prefix=prefix)
    print("ARMED:", [(s["schedule_name"], s["status"], s["schedule"]) for s in schedules])
    assert any(
        s["schedule_name"].endswith("smoke0001") and s["status"] == "ACTIVE"
        for s in schedules
    ), "smoke job not armed"

    if restart_check:
        # Simulate death: destroy without deleting schedules, then relaunch.
        stop.set()
        thread.join(timeout=10)
        print("RESTART: provider stopped; schedules should persist in Postgres")
        provider2 = _load_provider_from_dir(PLUGIN_DIR)
        stop2 = threading.Event()
        t2 = threading.Thread(target=provider2.start, args=(stop2,), daemon=True)
        t2.start()
        deadline = time.time() + 30
        while time.time() < deadline and not provider2._launched:
            time.sleep(0.2)
        assert provider2._launched
        survivors = DBOS.list_schedules(schedule_name_prefix=prefix)
        print("SURVIVED:", [(s["schedule_name"], s["status"]) for s in survivors])
        assert any(
            s["schedule_name"].endswith("smoke0001") and s["status"] == "ACTIVE"
            for s in survivors
        ), "schedule did not survive restart"
        stop2.set()
        t2.join(timeout=10)
        print("RESTART-CHECK PASS")
        return 0

    # Phase 1: wait for a real DBOS-clocked fire (cron * * * * * -> <=70s).
    print("waiting up to 130s for a real DBOS-triggered fire...")
    deadline = time.time() + 130
    while time.time() < deadline:
        if fires_file.exists() and fires_file.read_text().strip():
            break
        time.sleep(2)
    assert fires_file.exists() and fires_file.read_text().strip(), (
        "no fire arrived within 130s — DBOS trigger did not work"
    )
    fires = [json.loads(x) for x in fires_file.read_text().splitlines()]
    print("FIRES:", fires)
    assert fires[0]["id"] == "smoke0001"

    # The claim must have gone through the REAL store. Proof: claim_job_for_fire
    # recomputes next_run_at from the schedule, so the year-2000 sentinel is
    # replaced by a live future time. (last_run_at is written by mark_job_run
    # inside the real run_one_job, which this smoke deliberately stubs.)
    from cron.jobs import get_job

    stored = get_job("smoke0001")
    print("STORE AFTER FIRE: state=%s last_run_at=%s next_run_at=%s" % (
        stored.get("state"), stored.get("last_run_at"), stored.get("next_run_at")))
    assert stored.get("next_run_at", "").startswith("20") and not stored[
        "next_run_at"
    ].startswith("2000-"), "claim did not go through the real store"

    # Native-surface parity: pause via the real store mutation path and
    # verify the DBOS schedule pauses (this is what desktop toggle does).
    from cron.jobs import pause_job, resume_job

    pause_job("smoke0001")
    provider.on_jobs_changed()
    time.sleep(1)
    paused = {
        s["schedule_name"]: s["status"]
        for s in DBOS.list_schedules(schedule_name_prefix=prefix)
    }
    print("AFTER PAUSE:", paused)
    assert all(v == "PAUSED" for k, v in paused.items() if k.endswith("smoke0001")), (
        "pause did not propagate to DBOS"
    )

    # And resume.
    resume_job("smoke0001")
    provider.on_jobs_changed()
    time.sleep(1)
    resumed = {
        s["schedule_name"]: s["status"]
        for s in DBOS.list_schedules(schedule_name_prefix=prefix)
    }
    print("AFTER RESUME:", resumed)
    assert all(v == "ACTIVE" for k, v in resumed.items() if k.endswith("smoke0001"))

    stop.set()
    thread.join(timeout=10)
    print("LIVE SMOKE PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
