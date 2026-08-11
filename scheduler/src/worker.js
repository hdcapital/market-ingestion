/**
 * Cross-repo GitHub Actions scheduler (timezone-aware, catch-up capable).
 *
 * Why this exists: GitHub's `schedule:` event is best-effort — it queues runs,
 * delivers them 50–80 min late, and silently DROPS them under load at congested
 * minute marks. This Worker calls the GitHub `workflow_dispatch` REST API, which
 * runs immediately.
 *
 * How it works: one Cron Trigger fires this Worker every minute ("* * * * *").
 * On each tick it computes the current LOCAL time for every repo in TARGETS and
 * dispatches the ones whose local time matches. Because matching is done in the
 * target's own timezone, daylight-saving is handled automatically — no seasonal
 * cron juggling, and you never touch the trigger again when adding a repo.
 *
 * To add a repo: add one line to TARGETS. That's it.
 *
 * Reliability notes (why this is more than an exact-minute match):
 *   - Cloudflare's Cron Triggers are best-effort too. A dropped or delayed tick
 *     against an exact-minute equality test means the dispatch is skipped for
 *     the whole day, silently — the same failure this Worker exists to fix.
 *     So: ticks are matched against a GRACE window, and each fire is recorded
 *     in KV so the catch-up cannot double-dispatch.
 *   - Binding KV is optional. Without it there is nowhere to record what has
 *     already fired, so the grace window is forced to zero and behaviour falls
 *     back to exact-minute matching. See the setup note at the bottom.
 *   - Dispatches retry on transient GitHub failures, and every failure is
 *     logged and (optionally) pushed to a webhook. A scheduler that fails
 *     quietly is indistinguishable from a day with no news.
 */

// --- Edit this list. One entry per scheduled workflow. -----------------------
//   owner/repo/workflow : workflow is the FILE NAME in .github/workflows/.
//   ref                 : branch to run (usually "main").
//   tz                  : IANA timezone the time is expressed in
//                         (e.g. "Australia/Sydney", "Europe/London", "UTC").
//   at                  : local time "HH:MM" (24-hour, zero-padded).
//   days                : array of local weekdays it may run on
//                         (0=Sun,1=Mon,…,6=Sat), or null for every day.
const TARGETS = [
  // --- market-ingestion (the S3 lake): the four daily scrapes. GitHub's
  // schedule: event silently never fired for this repo, so the Worker owns
  // the timing. Pinned in UTC deliberately: the consumer fires below
  // (01:30 / 01:35 UTC) are sequenced against these and that spacing must
  // hold in both hemispheres' DST. Re-running an ingest is always safe
  // (the lake dedupes), so manual catch-up dispatches need no special care.
  { owner: "hdcapital", repo: "market-ingestion", workflow: "ingest-asx.yml", ref: "main",
    tz: "UTC", at: "00:35", days: [1, 2, 3, 4, 5] },
  { owner: "hdcapital", repo: "market-ingestion", workflow: "ingest-uk.yml", ref: "main",
    tz: "UTC", at: "21:45", days: [1, 2, 3, 4, 5] },
  // US runs AFTER EDGAR publishes the daily form index (~02:00 UTC for the
  // prior US trading day). At the old 22:15 slot the same-day index never
  // existed yet, so every run ingested a day late and one quiet overlap
  // produced a misleading ok_empty marker on a busy weekday. Tue-Sat at
  // 02:30 UTC ingests Mon-Fri filings within ~30 min of the index landing.
  { owner: "hdcapital", repo: "market-ingestion", workflow: "ingest-us.yml", ref: "main",
    tz: "UTC", at: "02:30", days: [2, 3, 4, 5, 6] },
  { owner: "hdcapital", repo: "market-ingestion", workflow: "ingest-otc.yml", ref: "main",
    tz: "UTC", at: "22:30", days: [1, 2, 3, 4, 5] },

  // --- the two lake CONSUMERS: ONE fire each per day, in the single window
  // where the whole lake is fresh. At 01:30 UTC the day's ASX ingest (00:35,
  // takes 31-50 min) has just finished AND last night's UK/OTC ingests
  // (21:45/22:30) are ~3 hours old. US ingests at 02:30 UTC (after these
  // fires — EDGAR's daily index isn't published until ~02:00), so consumers
  // read US from the previous 02:30 run; moving these fires to ~03:10 would
  // gain a day of US freshness at the cost of staler ASX-open timing.
  // Mon-Sat: the Saturday fire exists only so Friday's UK/US/OTC data isn't
  // stuck waiting until Monday (there is no Saturday ASX; it reads as dupes).
  // Pinned in UTC, not Sydney: the ingests are UTC-pinned, and a Sydney-local
  // time would drift an hour against them at each DST change and race the
  // ASX ingest for half the year.
  { owner: "hdcapital", repo: "global-analyst", workflow: "daily.yml", ref: "main",
    tz: "UTC", at: "01:30", days: [1, 2, 3, 4, 5, 6] },

  // 5 min behind the screener, same window and rationale.
  { owner: "hdcapital", repo: "global-watchlist-openai", workflow: "daily_watchlist.yml", ref: "main",
    tz: "UTC", at: "01:35", days: [1, 2, 3, 4, 5, 6] },

  // 07:45 London, daily — DST handled automatically (was BST/GMT dual-cron).
  { owner: "hdcapital", repo: "investegate-scraper", workflow: "morning.yml", ref: "main",
    tz: "Europe/London", at: "07:45", days: null },

  // 19:45 London, daily.
  { owner: "hdcapital", repo: "investegate-scraper", workflow: "evening.yml", ref: "main",
    tz: "Europe/London", at: "19:45", days: null },

  // uk-watchlist — same times as Investegate (07:45 + 19:45 London, daily).
  // NOTE: workflow filenames assumed to match Investegate (morning.yml /
  // evening.yml) — still unverified. A wrong filename fails with HTTP 404 and
  // is now reported as such; check `wrangler tail` (or the GET endpoint) after
  // the first fire rather than assuming these are right.
  { owner: "hdcapital", repo: "uk-watchlist", workflow: "morning.yml", ref: "main",
    tz: "Europe/London", at: "07:45", days: null },
  { owner: "hdcapital", repo: "uk-watchlist", workflow: "evening.yml", ref: "main",
    tz: "Europe/London", at: "19:45", days: null },
];

// Optional per-workflow inputs, keyed by "owner/repo/workflow". Most workflows
// need none (their workflow_dispatch defaults are used). Example:
//   "hdcapital/foo/bar.yml": { some_input: "value" }
const INPUTS = {};

// --- Tunables ---------------------------------------------------------------

// How many minutes after its scheduled time a target may still be dispatched as
// a catch-up. Requires the SCHEDULER KV binding; without it this is forced to 0
// (see dueTargets) because there would be no way to avoid re-firing every
// minute of the window. 10 min is comfortably inside the 30-min spacing between
// the ingest slots, so a catch-up can never jump its neighbour's turn.
const GRACE_MINUTES = 10;

// Transient-failure retries. 4xx is deterministic — a bad token or a missing
// workflow file will not fix itself in three seconds — so only 429/5xx and
// outright network errors are retried.
const MAX_ATTEMPTS = 3;
const RETRY_BACKOFF_MS = [1000, 3000];

// A daily slot must not be re-fired, but the namespace should not grow forever.
const CLAIM_TTL_SECONDS = 90_000; // 25 hours

const WEEKDAY = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const labelFor = (t) => `${t.owner}/${t.repo}/${t.workflow}@${t.ref}`;

const text = (body, status) =>
  new Response(body, { status, headers: { "Content-Type": "text/plain; charset=utf-8" } });

// --- Time -------------------------------------------------------------------

/** Local wall-clock facts for an instant, in one Intl pass. */
function localParts(tz, date) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: tz,
    hourCycle: "h23", // explicit: hour12:false can still yield "24" on some ICU builds
    weekday: "short",
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit",
  }).formatToParts(date);
  const p = Object.fromEntries(parts.map((x) => [x.type, x.value]));
  const hour = p.hour === "24" ? "00" : p.hour; // Intl midnight quirk, belt and braces
  return {
    minutes: Number(hour) * 60 + Number(p.minute),
    weekday: WEEKDAY[p.weekday],
    date: `${p.year}-${p.month}-${p.day}`,
  };
}

/** "HH:MM" → minutes since local midnight, or null if malformed. */
function parseAt(at) {
  const match = /^(\d{2}):(\d{2})$/.exec(at ?? "");
  if (!match) return null;
  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  if (hours > 23 || minutes > 59) return null;
  return hours * 60 + minutes;
}

/**
 * Targets whose slot is open right now, each tagged with the occurrence it
 * belongs to. A malformed target is skipped here and reported by validate().
 */
function dueTargets(now, graceMinutes) {
  const due = [];
  for (const target of TARGETS) {
    const atMinutes = parseAt(target.at);
    if (atMinutes === null) continue;

    const { minutes } = localParts(target.tz, now);
    // Modular distance, so a window that straddles local midnight still works.
    const late = (minutes - atMinutes + 1440) % 1440;
    if (late > graceMinutes) continue;

    // Attribute the fire to the occurrence it belongs to rather than to "now",
    // so a catch-up just after local midnight books against yesterday's slot —
    // and is weekday-filtered against yesterday too.
    const occurrence = new Date(now.getTime() - late * 60_000);
    const { weekday, date } = localParts(target.tz, occurrence);
    if (target.days && !target.days.includes(weekday)) continue;

    due.push({ target, late, key: `${labelFor(target)}|${date}T${target.at}` });
  }
  return due;
}

// --- Config validation ------------------------------------------------------

/** One human-readable problem per malformed target; empty array when clean. */
function validate() {
  const problems = [];
  for (const target of TARGETS) {
    const label = labelFor(target);
    if (!target.owner || !target.repo || !target.workflow || !target.ref) {
      problems.push(`${label}: missing owner, repo, workflow or ref`);
      continue;
    }
    if (parseAt(target.at) === null) {
      problems.push(`${label}: invalid "at" ${JSON.stringify(target.at)} — expected "HH:MM"`);
    }
    if (target.days != null && !Array.isArray(target.days)) {
      problems.push(`${label}: "days" must be an array of weekday numbers, or null`);
    }
    try {
      new Intl.DateTimeFormat("en-US", { timeZone: target.tz });
    } catch {
      problems.push(`${label}: unknown timezone ${JSON.stringify(target.tz)}`);
    }
  }
  return problems;
}

// --- Dispatch ---------------------------------------------------------------

async function dispatch(target, token) {
  const { owner, repo, workflow, ref } = target;
  const url = `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflow}/dispatches`;
  const body = { ref };
  const inputs = INPUTS[`${owner}/${repo}/${workflow}`];
  if (inputs) body.inputs = inputs;
  const label = labelFor(target);

  let detail = "no attempt made";
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    let res;
    try {
      res = await fetch(url, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: "application/vnd.github+json",
          "X-GitHub-Api-Version": "2022-11-28",
          "User-Agent": "cf-actions-scheduler",
          "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
      });
    } catch (err) {
      detail = `network error: ${err}`;
      if (attempt < MAX_ATTEMPTS) {
        await sleep(RETRY_BACKOFF_MS[attempt - 1] ?? 3000);
        continue;
      }
      return { label, ok: false, detail, attempts: attempt };
    }

    if (res.status === 204) return { label, ok: true, attempts: attempt };

    const responseBody = (await res.text().catch(() => "")).slice(0, 300);
    detail = `HTTP ${res.status}: ${responseBody}`;
    // The two failures worth naming, because both look identical in the logs
    // otherwise and both have bitten this setup before.
    if (res.status === 404) {
      detail += ' — workflow file not found on that ref, OR the PAT has no access to this repo';
    } else if (res.status === 403) {
      detail += " — PAT lacks the actions:write scope, or is rate limited";
    }

    const retryable = res.status === 429 || res.status >= 500;
    if (!retryable || attempt === MAX_ATTEMPTS) {
      return { label, ok: false, status: res.status, detail, attempts: attempt };
    }
    await sleep(RETRY_BACKOFF_MS[attempt - 1] ?? 3000);
  }
  return { label, ok: false, detail };
}

/** Optional failure notification. Silence is the enemy; this is opt-in. */
async function notify(env, lines) {
  if (!env.ALERT_WEBHOOK || !lines.length) return;
  try {
    await fetch(env.ALERT_WEBHOOK, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text: `Actions scheduler problems:\n${lines.join("\n")}` }),
    });
  } catch (err) {
    console.error(`❌ alert webhook failed: ${err}`);
  }
}

/**
 * Claim a slot before dispatching so overlapping ticks cannot double-fire.
 * With no KV bound there is nothing to claim (grace is 0, so exact-minute
 * matching already gives one chance per slot).
 */
async function claim(kv, key) {
  if (!kv) return true;
  if (await kv.get(key)) return false;
  await kv.put(key, new Date().toISOString(), { expirationTtl: CLAIM_TTL_SECONDS });
  return true;
}

async function runDue(due, env) {
  if (!env.GH_PAT) {
    const message = "GH_PAT secret is not set — nothing can be dispatched";
    console.error(`❌ ${message}`);
    await notify(env, [message]);
    return [];
  }
  const kv = env.SCHEDULER ?? null;

  const claimed = [];
  for (const item of due) {
    if (await claim(kv, item.key)) claimed.push(item);
    else console.log(`↩︎ already fired this slot, skipping ${item.key}`);
  }
  if (!claimed.length) return [];

  const settled = await Promise.allSettled(
    claimed.map((item) => dispatch(item.target, env.GH_PAT)),
  );

  const problems = [];
  const results = [];
  for (const [index, outcome] of settled.entries()) {
    const item = claimed[index];
    const result = outcome.status === "fulfilled"
      ? outcome.value
      : { label: labelFor(item.target), ok: false, detail: `threw: ${outcome.reason}` };
    results.push({ ...result, late: item.late });

    if (result.ok) {
      const suffix = item.late ? ` (catch-up, ${item.late} min late)` : "";
      console.log(`✅ dispatched ${result.label}${suffix}`);
    } else {
      // Release the claim so the remaining grace window can retry this slot.
      if (kv) await kv.delete(item.key);
      const line = `${result.label} → ${result.detail}`;
      console.error(`❌ ${line}`);
      problems.push(line);
    }
  }
  await notify(env, problems);
  return results;
}

// --- Entry points -----------------------------------------------------------

export default {
  // Fires every minute (cron "* * * * *"); dispatches whatever is due.
  async scheduled(event, env, ctx) {
    // scheduledTime is the minute this tick was MEANT for. Using it instead of
    // Date.now() means a late delivery still books against the right slot.
    const now = new Date(event.scheduledTime ?? Date.now());

    // Logged every tick rather than thrown: one typo must not take the whole
    // scheduler down, but it should be impossible to miss in `wrangler tail`.
    for (const problem of validate()) console.error(`❌ config: ${problem}`);

    const due = dueTargets(now, env.SCHEDULER ? GRACE_MINUTES : 0);
    if (due.length) ctx.waitUntil(runDue(due, env));
  },

  // Manual endpoint, locked down:
  //   - Disabled entirely unless the TRIGGER_SECRET secret is configured.
  //   - Key may be sent as the X-Trigger-Key header (preferred — query strings
  //     leak into shell history and logs) or as ?key= for convenience.
  //   - GET is read-only: config check and the schedule as the Worker sees it.
  //   - POST dispatches, and requires ?repo=NAME — "fire everything" is never
  //     the default. Where a repo has more than one workflow, &workflow=NAME is
  //     required too, so you cannot fire morning.yml at eight in the evening.
  //
  //   curl -sS -H "X-Trigger-Key: $SECRET" "$BASE/"                    # inspect
  //   curl -sS -X POST -H "X-Trigger-Key: $SECRET" \
  //        "$BASE/?repo=market-ingestion&workflow=ingest-asx.yml"      # fire one
  async fetch(request, env) {
    if (!env.TRIGGER_SECRET) {
      return text("disabled: set the TRIGGER_SECRET secret to enable manual fires", 403);
    }
    const url = new URL(request.url);
    const provided = request.headers.get("X-Trigger-Key") ?? url.searchParams.get("key");
    if (provided !== env.TRIGGER_SECRET) return text("forbidden", 403);

    const configProblems = validate();

    if (request.method === "GET") {
      const now = new Date();
      return Response.json({
        ok: configProblems.length === 0 && Boolean(env.GH_PAT),
        configProblems,
        ghPat: env.GH_PAT ? "set" : "MISSING — dispatches will fail",
        catchUp: env.SCHEDULER
          ? `enabled (${GRACE_MINUTES} min grace, KV-deduped)`
          : "disabled — bind a KV namespace as SCHEDULER to survive a dropped tick",
        alerts: env.ALERT_WEBHOOK ? "webhook configured" : "none — failures only reach the Worker log",
        targets: TARGETS.map((t) => ({
          label: labelFor(t),
          tz: t.tz,
          at: t.at,
          days: t.days ?? "daily",
          localNow: (() => {
            const { minutes } = localParts(t.tz, now);
            return `${String(Math.floor(minutes / 60)).padStart(2, "0")}:${String(minutes % 60).padStart(2, "0")}`;
          })(),
        })),
      });
    }

    if (request.method !== "POST") {
      return text("use GET to inspect, POST to dispatch", 405);
    }

    const only = url.searchParams.get("repo");
    if (!only) {
      return text("missing ?repo=NAME (firing all targets at once is not allowed)", 400);
    }
    const wanted = url.searchParams.get("workflow");
    let targets = TARGETS.filter((t) => t.repo === only);
    if (wanted) targets = targets.filter((t) => t.workflow === wanted);

    if (!targets.length) {
      const suffix = wanted ? ` with workflow "${wanted}"` : "";
      return text(`no target matches repo "${only}"${suffix}`, 404);
    }
    if (targets.length > 1) {
      const names = targets.map((t) => t.workflow).join(", ");
      return text(`repo "${only}" has ${targets.length} workflows (${names}); add &workflow=NAME`, 409);
    }
    if (!env.GH_PAT) return text("GH_PAT secret is not set", 500);

    // Manual fires deliberately bypass the KV claim: they are out-of-band, and
    // must not consume the day's slot or be blocked by it.
    const settled = await Promise.allSettled(targets.map((t) => dispatch(t, env.GH_PAT)));
    const results = settled.map((outcome, index) =>
      outcome.status === "fulfilled"
        ? outcome.value
        : { label: labelFor(targets[index]), ok: false, detail: `threw: ${outcome.reason}` },
    );
    for (const result of results) {
      if (result.ok) console.log(`✅ dispatched ${result.label} (manual)`);
      else console.error(`❌ ${result.label} → ${result.detail} (manual)`);
    }
    const ok = results.every((r) => r.ok);
    return Response.json({ ok, configProblems, dispatched: results }, { status: ok ? 200 : 502 });
  },
};

/* -----------------------------------------------------------------------------
 * Setup for the catch-up store (optional but recommended — without it a dropped
 * Cloudflare tick still loses that day's run):
 *
 *   wrangler kv namespace create SCHEDULER
 *
 * then add the returned id to wrangler.toml:
 *
 *   [[kv_namespaces]]
 *   binding = "SCHEDULER"
 *   id = "<the id wrangler printed>"
 *
 * Optional failure alerts (any endpoint accepting {"text": "..."} JSON):
 *
 *   wrangler secret put ALERT_WEBHOOK
 *
 * Verify after deploy:
 *
 *   curl -sS -H "X-Trigger-Key: $SECRET" https://<worker>/ | jq
 * -------------------------------------------------------------------------- */
