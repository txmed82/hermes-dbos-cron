# hermes-dbos-cron

A [DBOS](https://github.com/dbos-inc/dbos-transact-py)-backed cron scheduler
provider for [Hermes Agent](https://github.com/NousResearch/hermes-agent).

Replaces the trigger of the Hermes cron subsystem (the part that decides
*when* a due job fires) with DBOS durable execution on Postgres. Everything
else stays native Hermes — which means **all existing cron UIs keep working
unchanged**: the desktop app's recurring-task toggles, `hermes cron`
list/pause/resume/edit, the `cronjob` agent tool, and the web dashboard all
read and write `jobs.json` exactly as before, and this provider converges
DBOS to match.

## Why

The built-in Hermes cron trigger is an in-process 60-second ticker. It works,
but its schedule state lives in the gateway process: if the process is down,
wedged, or fd-exhausted, nothing fires and nothing remembers what should have
fired. With this provider:

- **Schedule state lives in Postgres.** Cron-kind jobs become real DBOS cron
  schedules; interval and one-shot jobs become durable-sleep timer workflows
  that survive process restarts.
- **Every fire is a journaled workflow** with a deterministic, idempotent ID.
  Late duplicates replay instead of double-firing, and Hermes' own
  store-level compare-and-set claim de-duplicates anything that slips
  through.
- **Arming is verified, not assumed.** On startup the provider reads back
  what DBOS actually holds and compares it against `jobs.json`, recording a
  ticker error visible in `hermes cron status` when anything is missing —
  a scheduler that silently never arms is the failure mode this plugin was
  born from.

## Design: one source of truth

`jobs.json` is authoritative. DBOS-side schedules and timers are derived,
disposable artifacts:

```
jobs.json (Hermes native store — desktop, CLI, dashboard, agent tool)
    │  on_jobs_changed() / register_job() / periodic reconcile
    ▼
DBOS schedules & timer workflows (Postgres, durable clock)
    │  fire → queue → workflow
    ▼
provider.claim_fire()  → Hermes store CAS claim (at-most-once)
provider.fire_claimed() → cron.scheduler.run_one_job (native execution,
                          delivery, audit, last_status)
```

There is no second job table, no parity hashes, no admission gate — the
entire class of "the mirror disagrees with the store so the job silently
never runs" failures is structurally absent.

## Install

```bash
hermes plugins install <this-repo>          # or clone into ~/.hermes/plugins/dbos
```

Add the Postgres URL to your profile's `.env` (secrets never go in
config.yaml):

```
DBOS_SYSTEM_DATABASE_URL=postgresql://user:pass@host:5432/hermes_cron
```

Select the provider (settings go in config.yaml):

```bash
hermes config set cron.provider dbos
```

Restart the gateway. On startup you should see:

```
DBOS cron provider launched (app=hermes-cron-<profile> ...)
DBOS cron arming self-check passed: N cron schedule(s) active
```

If DBOS or its database is unavailable, Hermes falls back to the built-in
ticker with a warning — cron never loses its trigger.

## Configuration

| key | where | meaning |
|---|---|---|
| `DBOS_SYSTEM_DATABASE_URL` | `.env` (required) | Postgres URL for the DBOS system database |
| `cron.provider` | config.yaml | `dbos` to activate (empty = built-in ticker) |
| `cron.dbos.queue_concurrency` | config.yaml | parallel job fires (default 4) |
| `cron.dbos.app_name` | config.yaml | override the DBOS application name |

Multiple profiles can share one Postgres database: schedule names, queue
names, and application names are all profile-prefixed, and fires are
profile-fenced (a schedule created by one profile's gateway never executes in
another's).

## Missed fires

While the gateway is down, DBOS cannot run workflows in it (the workers live
in the gateway process). Coverage on restart:

- In-flight **timer workflows** (interval/one-shot) are recovered by DBOS and
  resume their durable sleep.
- Overdue **cron-kind jobs** are picked up by the Hermes-native misfire
  backstop (`cron.misfire_grace_minutes`, default 10 — the same gateway
  housekeeping sweep the hosted Chronos provider relies on), which routes
  through this provider's two-phase claim so the fire is still at-most-once.
- DBOS `automatic_backfill` is deliberately **off**: misfire policy belongs
  in one place, and Hermes already owns it.

## Limitations

- Multiplex-profile gateways (`multiplex_profiles: true`) are not supported —
  the host falls back to the built-in ticker for those by design. Run one
  gateway per profile.
- The gateway must be up for jobs to execute (as with the built-in). What
  DBOS adds is durable schedule state, journaled fires, and restart recovery,
  not execution while down.
- Requires Postgres. If you don't run one, the built-in ticker or the hosted
  Chronos provider are the right choices.

## Tests

```bash
# Hermetic (no Postgres, no gateway): fake dbos + fake host modules
python -m pytest tests/test_provider.py

# Live integration (needs a disposable Postgres DB and the hermes-agent tree):
python tests/live_smoke.py                 # real DBOS-clocked fire + pause/resume parity
python tests/live_smoke.py --restart-check # schedule survival across process death
```

What the live smoke proves end to end: a `* * * * *` job armed from
`jobs.json` fires on the real DBOS clock within the minute; the claim goes
through the genuine Hermes store (`next_run_at` advances); pausing via the
native `pause_job` API pauses the DBOS schedule; resuming re-activates it;
and schedules survive process death and re-launch.

## License

MIT
