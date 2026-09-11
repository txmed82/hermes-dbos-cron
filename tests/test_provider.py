"""Hermetic tests for the hermes-dbos-cron provider.

Runs with NO Postgres and NO live gateway: a fake ``dbos`` module captures
every schedule/workflow call, and fake ``cron.*`` host modules simulate the
Hermes store. What is exercised:

- availability gating (no DB URL / no dbos import -> unavailable)
- reconcile: create, pause, resume, replace-on-change, delete-orphan
- native-surface parity: pause/resume/create/remove via jobs.json converge
- timer arming for interval jobs, idempotent by workflow ID, cancel on removal
- fire path: two-phase claim + run_one_job routing, claim-loss tolerance,
  profile fencing
- arming self-check: raises when DBOS-side state is missing a desired job
"""
from __future__ import annotations

import sys
import threading
import types
import unittest
from pathlib import Path
from unittest import mock

PLUGIN_DIR = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Fake dbos module
# ---------------------------------------------------------------------------

class FakeDBOSState:
    def __init__(self):
        self.schedules = {}       # name -> dict
        self.started_workflows = []  # (workflow_id, fn, args)
        self.cancelled = []
        self.launched = False
        self.destroyed = False
        self.queues = []


def build_fake_dbos(state: FakeDBOSState):
    module = types.ModuleType("dbos")

    class _SetWorkflowID:
        current = None

        def __init__(self, workflow_id):
            self._id = workflow_id

        def __enter__(self):
            _SetWorkflowID.current = self._id
            return self

        def __exit__(self, *exc):
            _SetWorkflowID.current = None
            return False

    class Queue:
        def __init__(self, name, worker_concurrency=1, **kw):
            state.queues.append((name, worker_concurrency))

    class DBOS:
        def __init__(self, config=None):
            self.config = config

        # -- decorators --
        @staticmethod
        def workflow(name=None):
            def deco(fn):
                fn._dbos_workflow_name = name
                return fn

            return deco

        # -- lifecycle --
        @staticmethod
        def launch():
            state.launched = True

        @staticmethod
        def destroy():
            state.destroyed = True

        # -- schedules --
        @staticmethod
        def create_schedule(*, schedule_name, workflow_fn, schedule,
                            cron_timezone=None, context=None,
                            automatic_backfill=False, queue_name=None):
            state.schedules[schedule_name] = {
                "schedule_name": schedule_name,
                "workflow_fn": workflow_fn,
                "schedule": schedule,
                "cron_timezone": cron_timezone,
                "context": context,
                "status": "ACTIVE",
                "queue_name": queue_name,
            }

        @staticmethod
        def apply_schedules(schedules):
            for s in schedules:
                entry = dict(s)
                entry["status"] = "ACTIVE"
                state.schedules[s["schedule_name"]] = entry

        @staticmethod
        def list_schedules(*, schedule_name_prefix=None, status=None, **kw):
            out = []
            for name, s in state.schedules.items():
                if schedule_name_prefix and not name.startswith(schedule_name_prefix):
                    continue
                if status and s.get("status") != status:
                    continue
                out.append(dict(s))
            return out

        @staticmethod
        def pause_schedule(name):
            state.schedules[name]["status"] = "PAUSED"

        @staticmethod
        def resume_schedule(name):
            state.schedules[name]["status"] = "ACTIVE"

        @staticmethod
        def delete_schedule(name):
            state.schedules.pop(name, None)

        # -- workflows --
        @staticmethod
        def start_workflow(fn, *args, **kwargs):
            state.started_workflows.append(
                (_SetWorkflowID.current, fn, args)
            )
            return mock.MagicMock()

        @staticmethod
        def cancel_workflow(workflow_id, **kw):
            state.cancelled.append(workflow_id)

        @staticmethod
        def sleep(seconds):
            pass

    module.DBOS = DBOS
    module.Queue = Queue
    module.SetWorkflowID = _SetWorkflowID
    return module


# ---------------------------------------------------------------------------
# Fake Hermes host modules (cron.*, hermes_cli.config, hermes_constants)
# ---------------------------------------------------------------------------

class FakeStore:
    def __init__(self):
        self.jobs = []
        self.claims = []          # job_ids claim was attempted for
        self.claim_results = {}   # job_id -> dict|None (default: the job)
        self.fired = []           # claimed jobs passed to run_one_job
        self.heartbeats = []
        self.errors = []


def build_fake_host(store: FakeStore, home: Path):
    cron_pkg = types.ModuleType("cron")
    cron_pkg.__path__ = []

    jobs_mod = types.ModuleType("cron.jobs")
    jobs_mod.load_jobs = lambda: [dict(j) for j in store.jobs]
    jobs_mod.get_job = lambda job_id: next(
        (dict(j) for j in store.jobs if j["id"] == job_id), None
    )

    def record_ticker_heartbeat(success=True):
        store.heartbeats.append(success)

    def record_ticker_error(message):
        store.errors.append(message)

    jobs_mod.record_ticker_heartbeat = record_ticker_heartbeat
    jobs_mod.record_ticker_error = record_ticker_error
    jobs_mod.clear_ticker_error = lambda: None

    def claim_job_for_fire(job_id, return_job=True, force=False):
        store.claims.append(job_id)
        if job_id in store.claim_results:
            return store.claim_results[job_id]
        return jobs_mod.get_job(job_id)

    jobs_mod.claim_job_for_fire = claim_job_for_fire

    executions_mod = types.ModuleType("cron.executions")
    executions_mod.create_execution = lambda job_id, source=None: {"id": f"exec-{job_id}"}
    executions_mod.finish_execution = lambda *a, **k: None
    executions_mod.recover_interrupted_executions = lambda: 0

    scheduler_mod = types.ModuleType("cron.scheduler")

    def run_one_job(job, adapters=None, loop=None, cancel_event=None):
        store.fired.append(job)

    scheduler_mod.run_one_job = run_one_job

    # Real provider ABC, minimally faithful: import the genuine one if the
    # hermes tree is importable; otherwise use this reduced clone that matches
    # the ABC surface the plugin relies on.
    provider_mod = types.ModuleType("cron.scheduler_provider")
    import abc

    class CronScheduler(abc.ABC):
        @property
        @abc.abstractmethod
        def name(self):
            ...

        def is_available(self):
            return True

        @abc.abstractmethod
        def start(self, stop_event, *, adapters=None, loop=None, interval=60):
            ...

        def stop(self):
            return None

        def on_jobs_changed(self):
            return None

        def register_job(self, job):
            return None

        def recover_interrupted(self):
            from cron.executions import recover_interrupted_executions

            return recover_interrupted_executions()

        def claim_fire(self, job_id, *, force=False):
            from cron.executions import create_execution, finish_execution
            from cron.jobs import claim_job_for_fire

            execution = create_execution(job_id, source=self.name)
            claimed = claim_job_for_fire(job_id, return_job=True, force=force)
            if not isinstance(claimed, dict):
                finish_execution(execution["id"], success=False, error="claim lost")
                return None
            claimed["execution_id"] = execution["id"]
            return claimed

        def fire_claimed(self, claimed_job, *, adapters=None, loop=None,
                         cancel_event=None):
            from cron.scheduler import run_one_job

            run_one_job(claimed_job, adapters=adapters, loop=loop,
                        cancel_event=cancel_event)
            return True

        def fire_due(self, job_id, *, adapters=None, loop=None, force=False):
            claimed = self.claim_fire(job_id, force=force)
            if claimed is None:
                return False
            return self.fire_claimed(claimed, adapters=adapters, loop=loop)

        def reconcile(self):
            return None

    provider_mod.CronScheduler = CronScheduler

    config_mod = types.ModuleType("hermes_cli.config")
    config_mod.load_config = lambda: {}

    def cfg_get(cfg, *keys, default=""):
        return default

    config_mod.cfg_get = cfg_get
    cli_pkg = types.ModuleType("hermes_cli")
    cli_pkg.__path__ = []

    constants_mod = types.ModuleType("hermes_constants")
    constants_mod.get_hermes_home = lambda: home

    return {
        "cron": cron_pkg,
        "cron.jobs": jobs_mod,
        "cron.executions": executions_mod,
        "cron.scheduler": scheduler_mod,
        "cron.scheduler_provider": provider_mod,
        "hermes_cli": cli_pkg,
        "hermes_cli.config": config_mod,
        "hermes_constants": constants_mod,
    }


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------

def cron_job(job_id, expr="0 9 * * *", tz="America/Chicago", *,
             enabled=True, state="scheduled"):
    return {
        "id": job_id,
        "name": job_id,
        "enabled": enabled,
        "state": state,
        "timezone": tz,
        "schedule": {"kind": "cron", "expr": expr, "display": expr},
        "next_run_at": "2099-01-01T09:00:00+00:00",
    }


def interval_job(job_id, next_run_at="2099-01-01T00:00:00+00:00", *,
                 enabled=True, state="scheduled"):
    return {
        "id": job_id,
        "name": job_id,
        "enabled": enabled,
        "state": state,
        "schedule": {"kind": "interval", "seconds": 3600, "display": "every 1h"},
        "next_run_at": next_run_at,
    }


class ProviderHarness:
    """Builds a fully faked import environment and loads the plugin fresh."""

    def __init__(self, tmp_home: Path):
        self.dbos_state = FakeDBOSState()
        self.store = FakeStore()
        self.home = tmp_home
        self.modules = build_fake_host(self.store, tmp_home)
        self.modules["dbos"] = build_fake_dbos(self.dbos_state)

    def __enter__(self):
        self._patcher = mock.patch.dict(sys.modules, self.modules)
        self._patcher.start()
        # Purge any previously loaded copy of the plugin.
        for name in list(sys.modules):
            if name.startswith("hermes_dbos_cron_under_test"):
                del sys.modules[name]
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "hermes_dbos_cron_under_test",
            str(PLUGIN_DIR / "__init__.py"),
            submodule_search_locations=[str(PLUGIN_DIR)],
        )
        self.plugin = importlib.util.module_from_spec(spec)
        sys.modules["hermes_dbos_cron_under_test"] = self.plugin
        self._env = mock.patch.dict(
            "os.environ",
            {"DBOS_SYSTEM_DATABASE_URL": "postgresql://fake:5432/fake"},
        )
        self._env.start()
        spec.loader.exec_module(self.plugin)
        self.provider = self.plugin.DBOSCronScheduler()
        return self

    def __exit__(self, *exc):
        try:
            with self.plugin._ACTIVE_LOCK:
                self.plugin._ACTIVE = None
        except Exception:
            pass
        self._env.stop()
        self._patcher.stop()
        return False

    def launch(self):
        self.provider._ensure_launched()
        with self.plugin._ACTIVE_LOCK:
            self.plugin._ACTIVE = self.provider


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class AvailabilityTests(unittest.TestCase):
    def test_unavailable_without_database_url(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            with ProviderHarness(Path(tmp)) as h:
                with mock.patch.dict("os.environ", {"DBOS_SYSTEM_DATABASE_URL": ""}):
                    self.assertFalse(h.provider.is_available())

    def test_available_with_url_and_dbos(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            with ProviderHarness(Path(tmp)) as h:
                self.assertTrue(h.provider.is_available())

    def test_name(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            with ProviderHarness(Path(tmp)) as h:
                self.assertEqual(h.provider.name, "dbos")


class ReconcileTests(unittest.TestCase):
    def _harness(self, tmp):
        return ProviderHarness(Path(tmp))

    def test_creates_schedule_for_enabled_cron_job(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, self._harness(tmp) as h:
            h.store.jobs = [cron_job("jobA", "0 7 * * *")]
            h.launch()
            h.provider.reconcile()
            name = h.provider._schedule_name("jobA")
            self.assertIn(name, h.dbos_state.schedules)
            sched = h.dbos_state.schedules[name]
            self.assertEqual(sched["schedule"], "0 7 * * *")
            self.assertEqual(sched["cron_timezone"], "America/Chicago")
            self.assertEqual(sched["status"], "ACTIVE")
            self.assertEqual(
                sched["context"],
                {"profile": h.provider._profile, "job_id": "jobA"},
            )
            # automatic_backfill must be off: misfire policy belongs to the
            # Hermes-native backstop, not DBOS backfill.
            self.assertFalse(sched.get("automatic_backfill", False))

    def test_pause_via_jobs_json_pauses_schedule(self):
        """Toggling a job off in ANY native Hermes UI converges to DBOS."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, self._harness(tmp) as h:
            h.store.jobs = [cron_job("jobA")]
            h.launch()
            h.provider.reconcile()
            name = h.provider._schedule_name("jobA")
            self.assertEqual(h.dbos_state.schedules[name]["status"], "ACTIVE")
            # User pauses in desktop app -> jobs.json state flips -> notify.
            h.store.jobs[0]["state"] = "paused"
            h.provider.on_jobs_changed()
            self.assertEqual(h.dbos_state.schedules[name]["status"], "PAUSED")
            # Resume flips it back.
            h.store.jobs[0]["state"] = "scheduled"
            h.provider.on_jobs_changed()
            self.assertEqual(h.dbos_state.schedules[name]["status"], "ACTIVE")

    def test_disabled_job_schedule_removed(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, self._harness(tmp) as h:
            h.store.jobs = [cron_job("jobA")]
            h.launch()
            h.provider.reconcile()
            h.store.jobs[0]["enabled"] = False
            h.provider.on_jobs_changed()
            self.assertNotIn(
                h.provider._schedule_name("jobA"), h.dbos_state.schedules
            )

    def test_removed_job_schedule_deleted(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, self._harness(tmp) as h:
            h.store.jobs = [cron_job("jobA"), cron_job("jobB")]
            h.launch()
            h.provider.reconcile()
            h.store.jobs = [cron_job("jobA")]
            h.provider.on_jobs_changed()
            self.assertIn(h.provider._schedule_name("jobA"), h.dbos_state.schedules)
            self.assertNotIn(h.provider._schedule_name("jobB"), h.dbos_state.schedules)

    def test_edited_expression_replaces_schedule(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, self._harness(tmp) as h:
            h.store.jobs = [cron_job("jobA", "0 7 * * *")]
            h.launch()
            h.provider.reconcile()
            h.store.jobs[0]["schedule"]["expr"] = "30 8 * * 1-5"
            h.provider.on_jobs_changed()
            sched = h.dbos_state.schedules[h.provider._schedule_name("jobA")]
            self.assertEqual(sched["schedule"], "30 8 * * 1-5")
            self.assertEqual(sched["status"], "ACTIVE")

    def test_interval_job_armed_as_timer_workflow(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, self._harness(tmp) as h:
            h.store.jobs = [interval_job("intA", "2099-06-01T00:00:00+00:00")]
            h.launch()
            h.provider.reconcile()
            self.assertEqual(len(h.dbos_state.started_workflows), 1)
            wf_id, _fn, args = h.dbos_state.started_workflows[0]
            self.assertEqual(
                wf_id,
                h.provider._timer_workflow_id("intA", "2099-06-01T00:00:00+00:00"),
            )
            self.assertEqual(args[0], "2099-06-01T00:00:00+00:00")

    def test_timer_arming_is_idempotent(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, self._harness(tmp) as h:
            h.store.jobs = [interval_job("intA")]
            h.launch()
            h.provider.reconcile()
            h.provider.reconcile()
            h.provider.reconcile()
            self.assertEqual(len(h.dbos_state.started_workflows), 1)

    def test_timer_cancelled_when_job_removed(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, self._harness(tmp) as h:
            h.store.jobs = [interval_job("intA")]
            h.launch()
            h.provider.reconcile()
            h.store.jobs = []
            h.provider.on_jobs_changed()
            self.assertEqual(len(h.dbos_state.cancelled), 1)

    def test_register_job_arms_new_cron_job(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, self._harness(tmp) as h:
            h.launch()
            job = cron_job("fresh")
            h.store.jobs = [job]
            h.provider.register_job(job)
            self.assertIn(
                h.provider._schedule_name("fresh"), h.dbos_state.schedules
            )


class FirePathTests(unittest.TestCase):
    def test_fire_routes_through_claim_and_run_one_job(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, ProviderHarness(Path(tmp)) as h:
            h.store.jobs = [cron_job("jobA")]
            h.launch()
            result = h.plugin._fire_workflow_impl(
                None, {"profile": h.provider._profile, "job_id": "jobA"}
            )
            self.assertTrue(result["fired"])
            self.assertEqual(h.store.claims, ["jobA"])
            self.assertEqual(len(h.store.fired), 1)
            self.assertEqual(h.store.fired[0]["id"], "jobA")

    def test_claim_loss_is_not_an_error(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, ProviderHarness(Path(tmp)) as h:
            h.store.jobs = [cron_job("jobA")]
            h.store.claim_results["jobA"] = None  # another trigger won CAS
            h.launch()
            result = h.plugin._fire_workflow_impl(
                None, {"profile": h.provider._profile, "job_id": "jobA"}
            )
            self.assertFalse(result["fired"])
            self.assertEqual(h.store.fired, [])

    def test_profile_fencing_refuses_foreign_fires(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, ProviderHarness(Path(tmp)) as h:
            h.store.jobs = [cron_job("jobA")]
            h.launch()
            result = h.plugin._fire_workflow_impl(
                None, {"profile": "some-other-profile", "job_id": "jobA"}
            )
            self.assertFalse(result["fired"])
            self.assertEqual(h.store.claims, [])

    def test_no_active_provider_is_safe(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, ProviderHarness(Path(tmp)) as h:
            result = h.plugin._fire_workflow_impl(None, {"job_id": "jobA"})
            self.assertFalse(result["fired"])


class SelfCheckTests(unittest.TestCase):
    def test_armed_check_passes_when_converged(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, ProviderHarness(Path(tmp)) as h:
            h.store.jobs = [cron_job("jobA")]
            h.launch()
            h.provider.reconcile()
            h.provider._verify_armed()  # must not raise

    def test_armed_check_raises_and_records_when_missing(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, ProviderHarness(Path(tmp)) as h:
            h.store.jobs = [cron_job("jobA")]
            h.launch()
            # Deliberately do NOT reconcile: desired job has no schedule.
            with self.assertRaises(RuntimeError):
                h.provider._verify_armed()
            self.assertTrue(any("not armed" in e for e in h.store.errors))


class LifecycleTests(unittest.TestCase):
    def test_start_launches_reconciles_and_stops(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, ProviderHarness(Path(tmp)) as h:
            h.store.jobs = [cron_job("jobA")]
            stop = threading.Event()
            t = threading.Thread(
                target=h.provider.start, args=(stop,), daemon=True
            )
            t.start()
            for _ in range(100):
                if h.dbos_state.launched and h.dbos_state.schedules:
                    break
                threading.Event().wait(0.05)
            self.assertTrue(h.dbos_state.launched)
            self.assertIn(
                h.provider._schedule_name("jobA"), h.dbos_state.schedules
            )
            self.assertIn(True, h.store.heartbeats)
            stop.set()
            t.join(timeout=5)
            self.assertFalse(t.is_alive())
            self.assertTrue(h.dbos_state.destroyed)

    def test_queue_name_is_profile_unique(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, ProviderHarness(Path(tmp)) as h:
            h.launch()
            names = [q[0] for q in h.dbos_state.queues]
            self.assertEqual(names, [f"hermes-cron-fires-{h.provider._profile}"])


if __name__ == "__main__":
    unittest.main()
