# CI Concurrency — one thing touches an environment at a time

> **Status (2026-10-02): in place and verified on a real overlap.** Two pushes
> landed on `qa` while an integration test was running; the qa deploy waited in
> GitHub until the test finished, and the two pushes collapsed into one deploy of
> the newer commit. See [Verification](#verification).

## What

GitHub Actions **concurrency groups** that serialize everything touching one
environment. A second deploy or test that arrives while one is running **waits in
GitHub** until the first finishes, instead of running alongside it or waiting
inside Databricks.

| Group | Jobs in it | Effect |
|---|---|---|
| ~~`dev-environment`~~ | ~~`deploy-dev`~~ | removed 2026-10-02 with the job itself: CI no longer deploys dev (see [NON_PROD_IDENTITY.md](NON_PROD_IDENTITY.md)) |
| **`qa-environment`** | `deploy-qa` **and** `integration-tests` | a qa deploy and a qa test never overlap |
| `prod-environment` | `deploy-prod` | two manual prod runs deploy one after the other |

Every group uses `cancel-in-progress: false`: a deploy or test that has started is
never cut off. Stopping one halfway is worse than waiting for it.

## Why

On **2026-10-01** two commits were pushed to `qa` **10 minutes apart**, and two
separate things went wrong.

**1. A healthy test failed.** The second integration run queued behind the first —
the Databricks job allows one run at a time (`max_concurrent_runs: 1`). Its first
task, `reset_environment`, has a 30-minute timeout, and **Databricks counted the
time spent queued against it**:

| Run | Created | Queued for | First task | Result |
|---|---|---|---|---|
| `1108499243703739` (test A) | 20:40:02 | 7 s | ran 20:40–20:41 | ✅ SUCCESS at 21:22:36 |
| `801626137950969` (test B) | 20:50:02 | **1,811 s (30.2 min)** | never started | ❌ "timed out before the task was started" at 21:20:13 |

Nothing was wrong with the code: a re-run the next morning
(`939289020577241`) passed. The timeout that tripped had been added the day before
(see the README's *Timeouts and retries*) — a correct fix with a side effect
nobody had tested: overlapping runs.

**2. A test ran against two commits' code.** Test B's run was created at
**20:50:02**, and a test run is only created after its deploy completes — so
commit B was deployed to qa by 20:50, while test A was running from 20:40 to
21:22. Databricks loads each notebook when its task starts, so test A's early
tasks ran commit A and its later tasks ran commit B. **Its green result certified
neither commit cleanly.** This would have happened with or without the timeouts;
the timeout only made it visible.

## When it applies

| Situation | What happens |
|---|---|
| A single push | Nothing visible — no waiting |
| Two pushes to the same environment, the second while the first's deploy or test is running | The second **waits** in GitHub, then deploys and is tested on its own |
| Three or more quick pushes | One runs, **one** waits. An older *waiting* run is replaced by the newer one and shows as "canceled" in GitHub — the newer commit contains the older one's changes, so the branch tip still gets deployed and tested |
| Pushes to **different** environments (dev and qa) | No interaction — different groups |

## Where

| File | What changed |
|---|---|
| [`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml) | `concurrency` on `deploy-qa`, `deploy-prod` (and on `deploy-dev` until that job was removed) |
| [`.github/workflows/integration-tests.yml`](../.github/workflows/integration-tests.yml) | `concurrency` on `integration-tests`, same group as `deploy-qa` |

GitHub concurrency groups are **repository-wide**: the same group name in two
different workflow files is one group. That is what lets a deploy in one file wait
for a test in another.

## Which approach, and which were rejected

| Option | Verdict |
|---|---|
| **GitHub `concurrency` per environment** | **Chosen.** Waiting moves out of Databricks, where it counted against timeouts, into GitHub, where it costs nothing; and qa's deploy and test become mutually exclusive |
| Raise the Databricks task timeouts | Rejected. Hides symptom 1, leaves symptom 2 — a test still runs on mixed code |
| Separate groups for deploys and for tests | Rejected. Fixes the false failure but not the mixed code: deploy B could still land mid-test A |
| **Per-run or per-PR isolated environments** (e.g. `pr_123_bronze` schemas created and dropped per run) | The at-scale answer, used by larger teams: nothing ever waits. **Not chosen** here — Free Edition caps the whole account at 5 concurrent tasks, so parallel runs would starve each other, and there is one developer pushing straight to branches |

## How it works

```yaml
concurrency:
  group: qa-environment        # identical in deploy.yml and integration-tests.yml
  cancel-in-progress: false    # never stop something already running
```

GitHub lets **one** job per group run. A second job in the same group goes to
*pending* and starts when the first finishes. Pending time is not counted against
the job's `timeout-minutes`, and no Databricks run exists yet, so no Databricks
timeout is running either.

## Before and after

**Before** — two qa pushes 10 minutes apart (what actually happened):

```
20:37 push A ─► deploy A ─► test A (20:40) ───────────────────────────► ✅ 21:22
20:47 push B ─► deploy B (by 20:50, while test A is running!)
                   └─► test B queues in Databricks ─► ❌ 30-min timeout, 21:20
```

**After** — the same two pushes:

```
push A ─► deploy A ─► test A ──────────────► ✅
push B ─► deploy B ⏸ pending in GitHub .....► deploy B ─► test B ─► ✅
```

| | Before | After |
|---|---|---|
| Test B | false failure | runs, after test A |
| Test A's code | mix of commits A and B | commit A only |
| Where waiting happens | inside Databricks, counted against task timeouts | in GitHub, uncounted |
| Time for B to finish | ~30 min (to a false failure) | ~40 min longer, to a real result |
| What a green test certifies | not one commit | exactly one commit |

## The cost, stated plainly

Overlapping pushes finish **later**: push B's deploy now starts only after test A,
roughly 40 minutes on. That is slower than before, but the faster path produced a
false failure and a muddled pass. The real speed lever is the integration suite's
runtime, which is mostly serverless task startup rather than work — not the
serialization.

## Known gap

The integration test is triggered when the **deploy workflow completes**, so there
is a window of a few seconds between deploy A finishing and test A starting. If
deploy B is already pending, it can take the group in that window, deploy, and
then test A runs against commit B (and test B tests it again afterwards). It is
rare and causes no false failure. Closing it fully means running the test inside
the same workflow as the deploy — a larger restructure, not done.

## Verification

Done:

- both workflows parse; exactly three groups exist; `qa-environment` is held by
  exactly the two qa jobs; nothing has `cancel-in-progress: true`
- normal single pushes run through the groups

**Real overlap, 2026-10-02.** Two commits were pushed to `qa` while integration
test `245142866425796` was running against an earlier one:

| Time | Event |
|---|---|
| 09:36:16 | integration test `245142866425796` starts |
| **09:44:02** | push to `qa` (`ef6231e`, toolchain pinning) |
| **10:04:32** | push to `qa` (`f0242d6`, action upgrades) |
| **10:16:20** | the running test finishes — SUCCESS |
| **10:16:51** | **qa redeploys, 31 s later** — the first qa deploy since 09:36:00 |
| 10:17:07 | integration test `551573030909467` starts; Databricks queue time **0.3 s** |

What it shows:

- **The deploy waited ~32 minutes in GitHub** and never ran under the live test —
  before, it would have landed about two minutes after the 09:44 push, mid-test.
- **Nothing waited inside Databricks** (0.3 s queued), so no task timeout was
  counting; the day before, the same kind of wait was 30 minutes and ended in a
  false failure.
- **The two pushes collapsed into one deploy** of the newer commit, the documented
  behaviour for three or more pending items: the first push's deploy was replaced
  while waiting, and `f0242d6` — which contains it — was deployed and tested.

Evidence comes from Databricks, not GitHub's UI: qa's bundle `deployment.json`
modification time, and the integration runs' start, end and queue times.

## Process change that came with it

Commit B was promoted to `main` without waiting for its qa test — which is how the
failure went unnoticed until the next morning. Promotion to `main` now waits for a
green qa integration run, every time, including docs-only commits.

## See also

- README → *CI/CD* and *Timeouts and retries*
- [GitHub: control the concurrency of workflows and jobs](https://docs.github.com/en/actions/using-jobs/using-concurrency)
