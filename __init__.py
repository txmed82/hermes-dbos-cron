"""hermes-dbos-cron — DBOS-backed cron scheduler provider for Hermes Agent.

Replaces the trigger ("Axis B") of the Hermes cron subsystem with DBOS
durable execution (https://github.com/dbos-inc/dbos-transact-py). Everything
else stays native Hermes:

- ``jobs.json`` remains the single source of truth. The desktop app, the
  ``cronjob`` tool, ``hermes cron`` CLI, and the web dashboard keep reading
  and writing it exactly as with the built-in ticker. Pause/resume/create/
  edit/delete in any native surface converge into DBOS via
  ``on_jobs_changed()`` / ``register_job()`` reconciliation.
- Execution and delivery stay in ``cron.scheduler.run_one_job`` via the
  provider ABC's two-phase ``claim_fire`` / ``fire_claimed`` (store-level
  compare-and-set: at-most-once per fire, native execution audit rows and
  ``last_status`` badges).

What DBOS adds over the built-in 60s ticker:

- Schedule state and fire history live in Postgres, not in a process loop.
- Cron-kind jobs become real DBOS cron schedules (server-side clocking).
- Interval / one-shot jobs become durable-sleep timer workflows that survive
  process restarts (DBOS recovers PENDING workflows and resumes the sleep).
- Every fire is a journaled workflow with a deterministic, idempotent ID —
  a late duplicate replays instead of double-firing, and the Hermes store
  claim de-duplicates anything that slips through.

Missed fires while the gateway is down are covered by the Hermes-native
misfire backstop (``cron.scheduler_provider.fire_overdue_jobs``, gateway
housekeeping) — the same mechanism Chronos relies on — plus DBOS recovery of
in-flight timer workflows.

Selection: ``cron.provider: dbos`` in config.yaml. If DBOS or its database
is unavailable, ``resolve_cron_scheduler`` falls back to the built-in ticker
with a warning — cron never loses its trigger.

Configuration (secrets in ``.env``, settings in ``config.yaml``):

- ``DBOS_SYSTEM_DATABASE_URL`` (env, required) — Postgres URL for the DBOS
  system database.
- ``cron.dbos.queue_concurrency`` (config, optional, default 4).
- ``cron.dbos.app_name`` (config, optional) — override the DBOS application
  name; default ``hermes-cron-<profile>``.

Multiplex-profile gateways are not supported (the host falls back to the
built-in for those); run one gateway per profile, each with its own
``HERMES_HOME``. Schedule names are profile-prefixed so many profiles can
safely share one Postgres.
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from cron.scheduler_provider import CronScheduler

logger = logging.getLogger("cron.dbos")

PLUGIN_VERSION = "0.1.0"

# Module-level singleton so the DBOS workflow function (registered once per
# process) can route fires back to the live provider instance.
_ACTIVE: Optional["DBOSCronScheduler"] = None
_ACTIVE_LOCK = threading.Lock()

# DBOS Queue declarations and workflow registrations are process-global and
# cannot be repeated. Track them at module level so a stop/start cycle within
# one process (gateway soft-restart, tests) relaunches cleanly.
_PROCESS_QUEUES: set = set()
_PROCESS_WORKFLOWS: Dict[str, Any] = {}


def _cfg(*keys: str, default: Any = "") -> Any:
    """Read a config value (no network)."""
    try:
        from hermes_cli.config import cfg_get, load_config

        return cfg_get(load_config(), *keys, default=default)
    except Exception:
        return default


def _profile_name() -> str:
    """Profile identity from HERMES_HOME (``default`` for the root home)."""
    try:
        from hermes_constants import get_hermes_home

        home = Path(get_hermes_home())
        if home.parent.name == "profiles":
            return home.name
    except Exception:
        pass
    return "default"


def _database_url() -> str:
    url = os.environ.get("DBOS_SYSTEM_DATABASE_URL", "").strip()
    if url:
        return url
    # The gateway normally exports .env into the process environment before
    # plugins load. As a fallback read the profile .env directly (never log
    # the value).
    try:
        from hermes_constants import get_hermes_home

        env_file = Path(get_hermes_home()) / ".env"
        if env_file.is_file():
            for line in env_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith("DBOS_SYSTEM_DATABASE_URL"):
                    _, _, value = line.partition("=")
                    return value.strip().strip('"').strip("'")
    except Exception:
        pass
    return ""


def _fire_workflow_impl(scheduled_at: Any, context: Dict[str, Any]) -> Dict[str, Any]:
    """Body of the DBOS workflow that fires one Hermes cron job.

    Runs inside the gateway process on a DBOS worker. Routes through the
    provider's two-phase claim/fire so Hermes-native admission, audit, and
    delivery semantics hold. Claim loss (another trigger won the CAS) is a
    normal outcome, not an error.
    """
    with _ACTIVE_LOCK:
        provider = _ACTIVE
    job_id = str((context or {}).get("job_id") or "")
    if provider is None or not job_id:
        return {"fired": False, "reason": "no active provider or job_id"}
    expected_profile = str((context or {}).get("profile") or "")
    if expected_profile and expected_profile != provider._profile:
        # A schedule from another profile's gateway must never fire here.
        return {"fired": False, "reason": "profile mismatch"}
    try:
        claimed = provider.claim_fire(job_id)
    except Exception as exc:  # job vanished, store locked, etc.
        logger.warning("DBOS fire: claim failed for %s: %s", job_id, exc)
        return {"fired": False, "reason": f"claim failed: {type(exc).__name__}"}
    if claimed is None:
        return {"fired": False, "reason": "claim lost or job not runnable"}
    try:
        provider.fire_claimed(
            claimed, adapters=provider._adapters, loop=provider._loop
        )
    finally:
        # Re-arm interval/once successors and converge schedule state.
        try:
            provider.reconcile()
        except Exception as exc:
            logger.debug("DBOS fire: post-fire reconcile failed: %s", exc)
    return {"fired": True, "job_id": job_id}


def _timer_workflow_impl(fire_at_iso: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """Durable timer for interval/one-shot jobs: sleep until due, then fire.

    ``DBOS.sleep`` is durable — a restart recovers the PENDING workflow and
    resumes the remaining sleep, which is what makes non-cron schedules
    survive process death.
    """
    from dbos import DBOS

    try:
        fire_at = datetime.fromisoformat(fire_at_iso)
        if fire_at.tzinfo is None:
            fire_at = fire_at.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return {"fired": False, "reason": "bad fire_at"}
    remaining = (fire_at - datetime.now(timezone.utc)).total_seconds()
    if remaining > 0:
        DBOS.sleep(remaining)
    return _fire_workflow_impl(fire_at_iso, context)


def _hermes_timezone_name() -> str:
    """The timezone Hermes itself uses for cron expressions.

    Hermes core computes cron next-run times with ``hermes_time.now()`` in the
    profile's configured timezone; DBOS cron schedules must clock in that SAME
    zone or every expression shifts by the UTC offset. Falls back to the
    server's local zone, then UTC.
    """
    try:
        from hermes_time import get_timezone

        tz = get_timezone()
        if tz is not None:
            return str(tz)
    except Exception:
        pass
    try:
        from datetime import datetime as _dt

        local = _dt.now().astimezone().tzinfo
        # tzname like 'CDT' is not a valid IANA zone; only trust ZoneInfo keys.
        key = getattr(local, "key", None)
        if key:
            return str(key)
    except Exception:
        pass
    # POSIX hosts (incl. macOS): /etc/localtime resolves into a zoneinfo tree
    # whose path suffix IS the IANA name (".../zoneinfo/America/Chicago").
    try:
        real = os.path.realpath("/etc/localtime")
        marker = "zoneinfo"
        if marker in real:
            candidate = real.split(marker, 1)[1].lstrip("/")
            # Strip any versioned subdir prefix artifacts; validate via ZoneInfo.
            from zoneinfo import ZoneInfo

            ZoneInfo(candidate)
            return candidate
    except Exception:
        pass
    return "UTC"


class DBOSCronScheduler(CronScheduler):
    """DBOS-backed external cron trigger provider."""

    def __init__(self) -> None:
        self._profile = _profile_name()
        self._adapters: Any = None
        self._loop: Any = None
        self._launched = False
        self._launch_lock = threading.Lock()
        self._fire_workflow = None
        self._timer_workflow = None
        self._known_timers: Dict[str, str] = {}  # job_id -> armed fire_at

    # -- identity / availability -----------------------------------------

    @property
    def name(self) -> str:
        return "dbos"

    def is_available(self) -> bool:
        """Config/import presence only — no network, per the ABC contract."""
        if not _database_url():
            return False
        try:
            import dbos  # noqa: F401
        except Exception:
            return False
        return True

    # -- naming -----------------------------------------------------------

    @property
    def _app_name(self) -> str:
        override = str(_cfg("cron", "dbos", "app_name", default="") or "").strip()
        return override or f"hermes-cron-{self._profile}"

    @property
    def _queue_name(self) -> str:
        # Unique per profile+plugin. Never reuse a name that also exists as a
        # database-backed queue from another application — a name collision
        # between an in-memory and a DB-backed queue makes DBOS ignore one of
        # them (observed in the wild as a fleet-wide cron outage).
        return f"hermes-cron-fires-{self._profile}"

    def _schedule_name(self, job_id: str) -> str:
        return f"hermes-cron:{self._profile}:{job_id}"

    def _schedule_prefix(self) -> str:
        return f"hermes-cron:{self._profile}:"

    def _timer_workflow_id(self, job_id: str, fire_at: str) -> str:
        return f"hermes-timer:{self._profile}:{job_id}:{fire_at}"

    # -- DBOS lifecycle ---------------------------------------------------

    def _ensure_launched(self) -> None:
        with self._launch_lock:
            if self._launched:
                return
            from dbos import DBOS, Queue

            concurrency = int(_cfg("cron", "dbos", "queue_concurrency", default=4) or 4)
            if self._queue_name not in _PROCESS_QUEUES:
                Queue(self._queue_name, worker_concurrency=max(1, concurrency))
                _PROCESS_QUEUES.add(self._queue_name)
            DBOS(
                config={
                    "name": self._app_name,
                    "system_database_url": _database_url(),
                    "application_version": PLUGIN_VERSION,
                }
            )
            # Register workflows BEFORE launch (once per process; DBOS keeps
            # workflow registrations globally).
            if "fire" not in _PROCESS_WORKFLOWS:
                _PROCESS_WORKFLOWS["fire"] = DBOS.workflow(name="hermes_cron_fire")(
                    _fire_workflow_impl
                )
                _PROCESS_WORKFLOWS["timer"] = DBOS.workflow(name="hermes_cron_timer")(
                    _timer_workflow_impl
                )
            self._fire_workflow = _PROCESS_WORKFLOWS["fire"]
            self._timer_workflow = _PROCESS_WORKFLOWS["timer"]
            DBOS.launch()
            self._launched = True
            logger.info(
                "DBOS cron provider launched (app=%s queue=%s profile=%s)",
                self._app_name,
                self._queue_name,
                self._profile,
            )

    def start(self, stop_event, *, adapters=None, loop=None, interval=60):
        """Launch DBOS, converge schedules, then wait for shutdown.

        Runs in the caller's daemon thread (like the built-in). All actual
        clocking is DBOS-side; this thread only waits so teardown is clean.
        """
        global _ACTIVE
        self._adapters = adapters
        self._loop = loop
        with _ACTIVE_LOCK:
            _ACTIVE = self
        recovered = self.recover_interrupted()
        if recovered:
            logger.warning(
                "Marked %d interrupted cron execution(s) unknown after restart",
                recovered,
            )
        self._ensure_launched()
        try:
            self.reconcile()
        except Exception as exc:
            logger.error("DBOS cron initial reconcile failed: %s", exc, exc_info=True)
        self._record_heartbeat(success=True)
        # Startup self-check: the scheduler must be observably armed, not
        # assumed armed. Verify our schedules actually exist DB-side.
        try:
            self._verify_armed()
        except Exception as exc:
            logger.error("DBOS cron arming self-check failed: %s", exc)
        # Periodic light heartbeat so `hermes cron status` shows liveness and
        # drift gets re-converged even if a notify hook was missed. This is
        # NOT the trigger (DBOS is); 5-minute cadence, cheap reconcile.
        while not stop_event.is_set():
            stop_event.wait(300)
            if stop_event.is_set():
                break
            try:
                self.reconcile()
                self._record_heartbeat(success=True)
            except Exception as exc:
                logger.warning("DBOS cron periodic reconcile failed: %s", exc)
                self._record_error(f"{type(exc).__name__}: {exc}")
        self.stop()

    def stop(self) -> None:
        global _ACTIVE
        with _ACTIVE_LOCK:
            if _ACTIVE is self:
                _ACTIVE = None
        if self._launched:
            try:
                from dbos import DBOS

                DBOS.destroy()
            except Exception:
                pass
            self._launched = False

    # -- host notifications ------------------------------------------------

    def on_jobs_changed(self) -> None:
        """Store mutated via any native surface — converge DBOS to match."""
        if not self._launched:
            return
        try:
            self.reconcile()
        except Exception as exc:
            logger.warning("DBOS cron on_jobs_changed reconcile failed: %s", exc)

    def register_job(self, job: Dict[str, Any]) -> None:
        """First registration for a new job — allowed to raise so the create
        surface can report a saved-but-unregistered job honestly."""
        if not self._launched:
            self._ensure_launched()
        schedule = job.get("schedule") or {}
        if str(schedule.get("kind") or "") == "cron" and schedule.get("expr"):
            from dbos import DBOS

            prefix = self._schedule_prefix()
            existing = {
                s["schedule_name"]: s
                for s in DBOS.list_schedules(schedule_name_prefix=prefix)
            }
            self._converge_cron_schedule(job, existing)
        elif job.get("next_run_at"):
            self._arm_timer(job)

    # -- reconcile ---------------------------------------------------------

    def reconcile(self) -> None:
        """Converge DBOS schedule/timer state toward jobs.json.

        jobs.json is the desired state. DBOS-side artifacts are derived and
        disposable — an orphan is cancelled, a missing one is created, a
        paused job's schedule is paused. Never the other way around.
        """
        if not self._launched:
            return
        from cron.jobs import load_jobs

        from dbos import DBOS

        jobs = {str(j.get("id")): j for j in load_jobs() if j.get("id")}
        desired_cron: Dict[str, Dict[str, Any]] = {}
        desired_timer: Dict[str, Dict[str, Any]] = {}
        for job_id, job in jobs.items():
            if not job.get("enabled") or job.get("state") not in ("scheduled", "running"):
                continue
            schedule = job.get("schedule") or {}
            kind = str(schedule.get("kind") or "")
            if kind == "cron" and schedule.get("expr"):
                desired_cron[job_id] = job
            elif job.get("next_run_at"):
                desired_timer[job_id] = job

        prefix = self._schedule_prefix()
        existing = {
            s["schedule_name"]: s
            for s in DBOS.list_schedules(schedule_name_prefix=prefix)
        }

        # --- cron-kind jobs → DBOS cron schedules ---
        for job_id, job in desired_cron.items():
            self._converge_cron_schedule(job, existing)

        # --- schedules that no longer correspond to an active cron job ---
        for schedule_name, sched in existing.items():
            job_id = schedule_name[len(prefix):]
            if job_id not in desired_cron:
                job = jobs.get(job_id)
                if job is not None and job.get("state") == "paused":
                    if sched.get("status") == "ACTIVE":
                        DBOS.pause_schedule(schedule_name)
                else:
                    try:
                        DBOS.delete_schedule(schedule_name)
                    except Exception as exc:
                        logger.warning(
                            "DBOS cron: failed to delete orphan schedule %s: %s",
                            schedule_name,
                            exc,
                        )

        # --- interval / one-shot jobs → durable timer workflows ---
        for job_id, job in desired_timer.items():
            self._arm_timer(job)
        for job_id in list(self._known_timers.keys()):
            if job_id not in desired_timer:
                self._cancel_timer(job_id)

    def _converge_cron_schedule(
        self, job: Dict[str, Any], existing: Dict[str, Any]
    ) -> None:
        from dbos import DBOS

        assert self._fire_workflow is not None  # set in _ensure_launched
        job_id = str(job["id"])
        schedule = job.get("schedule") or {}
        expr = str(schedule.get("expr"))
        tz = str(
            job.get("timezone")
            or schedule.get("timezone")
            or _hermes_timezone_name()
        )
        name = self._schedule_name(job_id)
        context = {"profile": self._profile, "job_id": job_id}
        prior = existing.get(name)
        if prior is None:
            DBOS.create_schedule(
                schedule_name=name,
                workflow_fn=self._fire_workflow,
                schedule=expr,
                cron_timezone=tz,
                context=context,
                automatic_backfill=False,
                queue_name=self._queue_name,
            )
            return
        if (
            prior.get("schedule") != expr
            or prior.get("cron_timezone") != tz
            or prior.get("context") != context
        ):
            # Definition changed in jobs.json → replace, never drift-error.
            # jobs.json is authoritative by design.
            DBOS.apply_schedules(
                [
                    dict(
                        schedule_name=name,
                        workflow_fn=self._fire_workflow,
                        schedule=expr,
                        cron_timezone=tz,
                        context=context,
                        automatic_backfill=False,
                        queue_name=self._queue_name,
                    )
                ]
            )
        if prior.get("status") != "ACTIVE":
            DBOS.resume_schedule(name)

    # -- timers -------------------------------------------------------------

    def _arm_timer(self, job: Dict[str, Any]) -> None:
        from dbos import DBOS, SetWorkflowID

        assert self._timer_workflow is not None  # set in _ensure_launched
        job_id = str(job["id"])
        fire_at = str(job.get("next_run_at") or "")
        if not fire_at:
            return
        if self._known_timers.get(job_id) == fire_at:
            return
        # Idempotent by workflow ID: re-arming the same (job, fire_at) is a
        # no-op replay, so reconcile can run as often as it likes.
        workflow_id = self._timer_workflow_id(job_id, fire_at)
        context = {"profile": self._profile, "job_id": job_id}
        try:
            with SetWorkflowID(workflow_id):
                DBOS.start_workflow(self._timer_workflow, fire_at, context)
            self._known_timers[job_id] = fire_at
        except Exception as exc:
            logger.warning("DBOS cron: failed to arm timer for %s: %s", job_id, exc)
            raise

    def _cancel_timer(self, job_id: str) -> None:
        from dbos import DBOS

        fire_at = self._known_timers.pop(job_id, None)
        if not fire_at:
            return
        try:
            DBOS.cancel_workflow(self._timer_workflow_id(job_id, fire_at))
        except Exception as exc:
            logger.debug("DBOS cron: cancel timer %s: %s", job_id, exc)

    # -- arming self-check ---------------------------------------------------

    def _verify_armed(self) -> None:
        """Read back what DBOS actually holds and compare against desired.

        The failure mode this exists for: a scheduler that logs 'registered'
        and then silently never fires. Verification reads the database, not
        our own intent.
        """
        from cron.jobs import load_jobs

        from dbos import DBOS

        desired = [
            str(j["id"])
            for j in load_jobs()
            if j.get("enabled")
            and j.get("state") == "scheduled"
            and (j.get("schedule") or {}).get("kind") == "cron"
        ]
        armed = {
            s["schedule_name"]
            for s in DBOS.list_schedules(
                schedule_name_prefix=self._schedule_prefix(), status="ACTIVE"
            )
        }
        missing = [
            job_id for job_id in desired if self._schedule_name(job_id) not in armed
        ]
        if missing:
            message = f"DBOS cron arming self-check: {len(missing)} job(s) not armed: {missing}"
            self._record_error(message)
            raise RuntimeError(message)
        logger.info(
            "DBOS cron arming self-check passed: %d cron schedule(s) active",
            len(desired),
        )

    # -- heartbeat glue (native `hermes cron status` liveness) ---------------

    def _record_heartbeat(self, *, success: bool) -> None:
        try:
            from cron.jobs import record_ticker_heartbeat

            record_ticker_heartbeat(success=success)
        except Exception:
            pass

    def _record_error(self, message: str) -> None:
        try:
            from cron.jobs import record_ticker_error

            record_ticker_error(message)
        except Exception:
            pass


def register(ctx) -> None:
    """Plugin entrypoint — mirrors the Chronos/memory-provider shape."""
    ctx.register_cron_scheduler(DBOSCronScheduler())
