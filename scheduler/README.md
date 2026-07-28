# actions-scheduler

A single Cloudflare Worker that reliably triggers GitHub Actions across many
repos, replacing GitHub's flaky built-in `schedule:` cron.

## Why

GitHub's `schedule` event is **best-effort**: runs are queued and delivered when
there's spare capacity, are routinely delayed 50–80 minutes, and are **silently
dropped** under load — especially at congested minute marks like `:00` and `:30`.
The `workflow_dispatch` REST endpoint, by contrast, is a direct command that runs
immediately. This Worker's Cron Trigger fires on time and calls `workflow_dispatch`
for every repo in `TARGETS`, so a dropped GitHub cron tick can't cost you a run.

One Worker, one token, one list — add a repo by adding one line.

## One-time setup

1. **Install Wrangler** (Cloudflare's CLI) and log in:
   ```bash
   npm install -g wrangler
   wrangler login
   ```

2. **Create a GitHub token** with permission to dispatch workflows on all target
   repos. A fine-grained PAT is best:
   - GitHub → Settings → Developer settings → Fine-grained tokens → Generate new.
   - Resource owner: `hdcapital`; select the ~dozen repos (or all).
   - Repository permissions → **Actions: Read and write**.
   - Copy the token.

3. **Store the token as a Worker secret** (never commit it):
   ```bash
   cd scheduler
   wrangler secret put GH_PAT            # paste the PAT
   wrangler secret put TRIGGER_SECRET    # optional: any random string, guards the test URL
   ```

4. **Fill in `TARGETS`** in `src/worker.js` — one entry per scheduled workflow:
   ```js
   { owner: "hdcapital", repo: "your-repo", workflow: "daily.yml", ref: "main",
     tz: "Australia/Sydney", at: "10:17", days: [1,2,3,4,5] },
   ```
   - `workflow` — the workflow *file name*. `ref` — the branch to run.
   - `tz` — the IANA timezone the time is in (`Australia/Sydney`, `Europe/London`,
     `UTC`, …). Matching in the target's own zone means **daylight-saving is
     handled automatically**.
   - `at` — local time `"HH:MM"` (24-hour).
   - `days` — array of local weekdays it may run (0=Sun…6=Sat), or `null` for daily.

   You never edit the trigger — one every-minute Cron Trigger drives everything.

5. **Deploy:**
   ```bash
   wrangler deploy
   ```

6. **Test on demand** (if you set `TRIGGER_SECRET`):
   ```bash
   curl "https://actions-scheduler.<your-subdomain>.workers.dev/?key=YOUR_TRIGGER_SECRET"
   ```
   You should see a JSON summary and new runs appear in each repo's Actions tab.

## Cut over each repo

Once you've confirmed the Worker dispatches successfully, remove the unreliable
`schedule:` block from each target repo's workflow so it only runs via dispatch
(and doesn't double-run). Keep `workflow_dispatch:` so both this Worker and the
manual "Run workflow" button still work:

```yaml
on:
  workflow_dispatch:
  # schedule:            # ← delete; the Cloudflare Worker drives timing now
  #   - cron: "..."
```

## Changing a schedule

Edit the repo's entry in `TARGETS` (`tz` / `at` / `days`) in `src/worker.js` and
redeploy (or paste the updated code in the dashboard). You do **not** touch the
Cron Trigger — the single every-minute trigger stays as-is.

## Cost

Comfortably within Cloudflare's free tier. The Worker wakes once a minute
(~1,440 tiny invocations/day, far under the free 100k/day) and does nothing
unless a repo is due.

## Reliability note

Cloudflare Cron Triggers are far more reliable than GitHub's `schedule`, but a
single minute tick is still not a 100% guarantee. For daily digests a rare
missed minute is acceptable. If you ever need at-least-once certainty, add a
Cloudflare KV namespace and record a per-target "last fired" date, then widen
the match to a few minutes and skip if already fired that day.
