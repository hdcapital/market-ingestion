/**
 * Cross-repo GitHub Actions scheduler (timezone-aware).
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
  // the timing. Pinned in UTC deliberately: the global-analyst fires below
  // (01:05 / 23:05 UTC) are sequenced ~30 min after these and that spacing
  // must hold in both hemispheres' DST. Re-running an ingest is always safe
  // (the lake dedupes), so manual catch-up dispatches need no special care.
  { owner: "hdcapital", repo: "market-ingestion", workflow: "ingest-asx.yml", ref: "main",
    tz: "UTC", at: "00:35", days: [1, 2, 3, 4, 5] },
  { owner: "hdcapital", repo: "market-ingestion", workflow: "ingest-uk.yml", ref: "main",
    tz: "UTC", at: "21:45", days: [1, 2, 3, 4, 5] },
  { owner: "hdcapital", repo: "market-ingestion", workflow: "ingest-us.yml", ref: "main",
    tz: "UTC", at: "22:15", days: [1, 2, 3, 4, 5] },
  { owner: "hdcapital", repo: "market-ingestion", workflow: "ingest-otc.yml", ref: "main",
    tz: "UTC", at: "22:30", days: [1, 2, 3, 4, 5] },

  // asx-analyst is retired: global-analyst now screens ASX (and UK/US/OTC) by
  // reading the market-ingestion lake, so it fires twice a day instead:
  //
  // ~30 min after the ASX ingest (00:35 UTC weekdays) — same-day ASX ideas.
  { owner: "hdcapital", repo: "global-analyst", workflow: "daily.yml", ref: "main",
    tz: "UTC", at: "01:05", days: [1, 2, 3, 4, 5] },

  // After the UK (21:45) / US (22:15) / OTC (22:30 + browser runtime) evening
  // ingests — the overnight UK/US/OTC ideas, Sun–Fri as the old 22:30 fire was.
  { owner: "hdcapital", repo: "global-analyst", workflow: "daily.yml", ref: "main",
    tz: "UTC", at: "23:05", days: [0, 1, 2, 3, 4, 5] },

  // 11:10 Sydney, daily — AFTER the ASX lake ingest (00:35 UTC = ~10:35 Sydney),
  // so the watchlist reads same-day ASX documents from the lake. Its old 10:07
  // fire predated the lake and would see only yesterday's ASX.
  { owner: "hdcapital", repo: "global-watchlist-openai", workflow: "daily_watchlist.yml", ref: "main",
    tz: "Australia/Sydney", at: "11:10", days: null },

  // 07:45 London, daily — DST handled automatically (was BST/GMT dual-cron).
  { owner: "hdcapital", repo: "investegate-scraper", workflow: "morning.yml", ref: "main",
    tz: "Europe/London", at: "07:45", days: null },

  // 19:45 London, daily.
  { owner: "hdcapital", repo: "investegate-scraper", workflow: "evening.yml", ref: "main",
    tz: "Europe/London", at: "19:45", days: null },

  // uk-watchlist — same times as Investegate (07:45 + 19:45 London, daily).
  // NOTE: workflow filenames assumed to match Investegate (morning.yml /
  // evening.yml) — this session can't read that repo to confirm. Verify in the
  // repo's Actions tab and correct if different.
  { owner: "hdcapital", repo: "uk-watchlist", workflow: "morning.yml", ref: "main",
    tz: "Europe/London", at: "07:45", days: null },
  { owner: "hdcapital", repo: "uk-watchlist", workflow: "evening.yml", ref: "main",
    tz: "Europe/London", at: "19:45", days: null },
];

// Optional per-workflow inputs, keyed by "owner/repo/workflow". Most workflows
// need none (their workflow_dispatch defaults are used). Example:
//   "hdcapital/foo/bar.yml": { some_input: "value" }
const INPUTS = {};

const WEEKDAY = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };

function localNow(tz, date) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: tz, hour12: false, weekday: "short", hour: "2-digit", minute: "2-digit",
  }).formatToParts(date);
  const p = Object.fromEntries(parts.map((x) => [x.type, x.value]));
  const hour = p.hour === "24" ? "00" : p.hour; // Intl midnight quirk
  return { hhmm: `${hour}:${p.minute}`, weekday: WEEKDAY[p.weekday] };
}

function dueNow(target, date) {
  const { hhmm, weekday } = localNow(target.tz, date);
  if (hhmm !== target.at) return false;
  if (target.days && !target.days.includes(weekday)) return false;
  return true;
}

async function dispatch(target, token) {
  const { owner, repo, workflow, ref } = target;
  const url = `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflow}/dispatches`;
  const body = { ref };
  const inputs = INPUTS[`${owner}/${repo}/${workflow}`];
  if (inputs) body.inputs = inputs;

  const res = await fetch(url, {
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

  const label = `${owner}/${repo}:${workflow}@${ref}`;
  if (res.status === 204) return { label, ok: true };
  const detail = await res.text().catch(() => "");
  return { label, ok: false, status: res.status, detail: detail.slice(0, 300) };
}

async function dispatchTargets(targets, token) {
  if (!token) throw new Error("GH_PAT secret is not set");
  const results = await Promise.all(targets.map((t) => dispatch(t, token)));
  for (const r of results) {
    if (r.ok) console.log(`✅ dispatched ${r.label}`);
    else console.error(`❌ ${r.label} → HTTP ${r.status}: ${r.detail}`);
  }
  return results;
}

export default {
  // Fires every minute (cron "* * * * *"); dispatches whatever is due this minute.
  async scheduled(event, env, ctx) {
    const now = new Date();
    const due = TARGETS.filter((t) => dueNow(t, now));
    if (due.length) ctx.waitUntil(dispatchTargets(due, env.GH_PAT));
  },

  // Manual test endpoint. Guard with a shared secret query param.
  //   .../?key=YOUR_SECRET            -> fires ALL targets now
  //   .../?key=YOUR_SECRET&repo=NAME  -> fires only that repo now
  async fetch(request, env) {
    const url = new URL(request.url);
    if (env.TRIGGER_SECRET && url.searchParams.get("key") !== env.TRIGGER_SECRET) {
      return new Response("forbidden", { status: 403 });
    }
    const only = url.searchParams.get("repo");
    const targets = only ? TARGETS.filter((t) => t.repo === only) : TARGETS;
    const results = await dispatchTargets(targets, env.GH_PAT);
    return Response.json({ dispatched: results });
  },
};
