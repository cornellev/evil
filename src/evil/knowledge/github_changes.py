"""What changed in the team's repos, and who wrote what (user 11-19: "understanding changes more deeply in a repo").

- changes(): one commit, one pull request (description, files, reviews, comments) or a time window / two refs
  compared, optionally narrowed to a path (a file's or folder's history). Each file comes with its +/- counts and its
  patch; lockfiles, generated and binary files go as counts only. Patches are cut to a budget (~6-8k tokens a call,
  measured 11-19: questions peak around 13k of a 32k context), biggest changes first; what's left over is listed with
  counts so the model can ask for that path alone.
- blame(): who last changed each block of a file's lines (GitHub's GraphQL blame; REST has none).
- people(): per-author commits and lines added / removed across repos in a window (GitHub's contributor stats).
Everything is read-only, goes through the credential filter, and is untrusted text (a PR description or a commit
message is written by anyone with push access): the server's instructions say never to follow instructions in it.
"""

from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath

from evil.knowledge import github as gh
from evil.knowledge import github_index
from evil.knowledge.filters import scrub
from evil.knowledge.textify import Block

DIFF_BUDGET_CHARS = 28_000       # ~7k tokens of patches per call
FILE_PATCH_CHARS = 6_000
MAX_FILES = 60
MAX_COMMITS = 40
MAX_REVIEWS = 15
BLAME_MAX_LINES = 400
STATS_TRIES = 5                  # GitHub computes contributor stats on first ask (202): wait and ask again
STATS_WAIT_SEC = 1.5
STATS_WORKERS = 8
_GENERATED = re.compile(r"(\.min\.\w+|\.map|\.lock|-lock\.json|\.pb\.go|_pb2\.py|\.snap|\.svg|\.ipynb)$|"
                        r"(^|/)(dist|build|vendor|node_modules|third_party|generated|__generated__)/", re.I)


def _clean(text: str) -> str:
    clean, _ = scrub([Block("code", text)])
    return "\n".join(b.text for b in clean)


def _skip_patch(name: str) -> bool:
    p = PurePosixPath(name)
    return (p.name.lower() in github_index.LOCKFILES or bool(_GENERATED.search(name)))


def _files(files: list[dict], path: str | None) -> dict:
    """Files with their patches, the biggest changes first, inside the budget."""
    if path:
        path = path.strip("/")
        files = [f for f in files if f["filename"] == path or f["filename"].startswith(path + "/")]
    files = sorted(files, key=lambda f: -(f.get("additions", 0) + f.get("deletions", 0)))
    out, budget, omitted = [], DIFF_BUDGET_CHARS, 0
    for f in files[:MAX_FILES]:
        item = {"file": f["filename"], "status": f.get("status"), "added": f.get("additions", 0),
                "removed": f.get("deletions", 0)}
        if f.get("previous_filename"):
            item["renamed_from"] = f["previous_filename"]
        patch = f.get("patch")
        if patch is None or _skip_patch(f["filename"]):
            item["patch"] = "(no text diff: binary, generated or a lockfile)"
        elif budget <= 0:
            item["patch_omitted"] = True
            omitted += 1
        else:
            text = _clean(patch)
            cut = text[:min(FILE_PATCH_CHARS, budget)]
            item["patch"] = cut + ("\n[... cut: ask with path= this file for all of it]" if len(cut) < len(text) else "")
            budget -= len(cut)
        out.append(item)
    summary = {"files_changed": len(files), "lines_added": sum(f.get("additions", 0) for f in files),
               "lines_removed": sum(f.get("deletions", 0) for f in files), "files": out}
    if len(files) > MAX_FILES:
        summary["more_files"] = len(files) - MAX_FILES
    if omitted:
        summary["note"] = (f"{omitted} file(s) shown without their patch (over the size budget): call again with "
                           f"path= one of them to see it.")
    return summary


def _commit_line(c: dict) -> dict:
    return {"sha": c["sha"][:7], "date": c["commit"]["committer"]["date"],
            "author": (c["commit"].get("author") or {}).get("name"),
            "message": _clean(c["commit"]["message"])[:600], "url": c.get("html_url")}


def changes(client: gh.GitHubClient, full: str, commit: str | None = None, pr: int | None = None,
            days: int | None = None, base: str | None = None, head: str | None = None,
            path: str | None = None) -> dict:
    info = client.get(f"/repos/{full}")
    branch = info["default_branch"]
    if pr:
        p = client.get(f"/repos/{full}/pulls/{int(pr)}")
        files = list(client.pages(f"/repos/{full}/pulls/{int(pr)}/files"))
        reviews = client.get(f"/repos/{full}/pulls/{int(pr)}/reviews", {"per_page": MAX_REVIEWS})
        comments = client.get(f"/repos/{full}/issues/{int(pr)}/comments", {"per_page": MAX_REVIEWS})
        review_comments = client.get(f"/repos/{full}/pulls/{int(pr)}/comments", {"per_page": MAX_REVIEWS})
        commits = client.get(f"/repos/{full}/pulls/{int(pr)}/commits", {"per_page": MAX_COMMITS})
        return {"repo": full, "pull_request": {
            "number": p["number"], "title": p["title"], "url": p["html_url"], "author": (p.get("user") or {}).get("login"),
            "state": "merged" if p.get("merged_at") else p["state"], "draft": p.get("draft"),
            "created": p["created_at"], "merged": p.get("merged_at"), "base": p["base"]["ref"], "head": p["head"]["ref"],
            "description": _clean(p.get("body") or "")[:3000],
            "reviews": [{"by": (r.get("user") or {}).get("login"), "state": r["state"], "text": _clean(r.get("body") or "")[:800]}
                        for r in reviews if r.get("body") or r["state"] in ("APPROVED", "CHANGES_REQUESTED")],
            "comments": [{"by": (c.get("user") or {}).get("login"), "text": _clean(c.get("body") or "")[:800]}
                         for c in comments + review_comments][:MAX_REVIEWS * 2]},
            "commits": [_commit_line(c) for c in commits], **_files(files, path)}
    if commit:
        c = client.get(f"/repos/{full}/commits/{commit}")
        return {"repo": full, "commit": _commit_line(c), **_files(c.get("files") or [], path)}
    if not (base and head):
        days = max(1, min(int(days or 7), 90))
        since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
        listed = client.get(f"/repos/{full}/commits", {"since": since, "sha": branch, "per_page": MAX_COMMITS,
                                                       **({"path": path} if path else {})})
        before = client.get(f"/repos/{full}/commits", {"until": since, "sha": branch, "per_page": 1})
        if not listed:
            return {"repo": full, "since": since, "commits": [], "files_changed": 0, "files": [],
                    "note": f"no commits on {branch}" + (f" touching {path}" if path else "") + f" in the last {days} days"}
        head = branch
        base = before[0]["sha"] if before else None
        if base is None:                         # the repo is younger than the window: from its first commit on
            base = listed[-1]["sha"]
        cmp = client.get(f"/repos/{full}/compare/{base}...{head}")
        return {"repo": full, "since": since, "commits": [_commit_line(c) for c in listed],
                **_files(cmp.get("files") or [], path)}
    cmp = client.get(f"/repos/{full}/compare/{base}...{head}")
    return {"repo": full, "base": base, "head": head,
            "commits": [_commit_line(c) for c in (cmp.get("commits") or [])][-MAX_COMMITS:], **_files(cmp.get("files") or [], path)}


BLAME_QUERY = """query($owner: String!, $name: String!, $ref: String!, $path: String!) {
  repository(owner: $owner, name: $name) { object(expression: $ref) { ... on Commit {
    blame(path: $path) { ranges { startingLine endingLine
      commit { abbreviatedOid committedDate messageHeadline url author { name } } } } } } } }"""


def blame(client: gh.GitHubClient, full: str, path: str, lines: str | None, text: str, ref: str) -> list[dict]:
    """[{lines, sha, date, author, message, code}] for the blocks overlapping `lines` ("40-80"; default: the file's
    first BLAME_MAX_LINES lines)."""
    lo, hi = 1, BLAME_MAX_LINES
    m = re.match(r"^\s*(\d+)\s*(?:[-:]\s*(\d+))?\s*$", str(lines or ""))
    if m:
        lo = int(m.group(1))
        hi = int(m.group(2) or m.group(1))
        hi = min(hi, lo + BLAME_MAX_LINES - 1)
    owner, name = full.split("/")
    data = client.graphql(BLAME_QUERY, {"owner": owner, "name": name, "ref": ref, "path": path})
    obj = ((data.get("repository") or {}).get("object") or {})
    ranges = ((obj.get("blame") or {}).get("ranges")) or []
    code = text.splitlines()
    out = []
    for r in ranges:
        a, b = r["startingLine"], r["endingLine"]
        if b < lo or a > hi:
            continue
        c = r["commit"]
        snippet = "\n".join(code[max(a, lo) - 1:min(b, hi)])
        out.append({"lines": f"{max(a, lo)}-{min(b, hi)}", "sha": c["abbreviatedOid"], "date": c["committedDate"],
                    "author": (c.get("author") or {}).get("name"), "message": _clean(c.get("messageHeadline") or ""),
                    "url": c.get("url"), "code": _clean(snippet)[:1500]})
    return out


def people(client: gh.GitHubClient, names: list[str], days: int) -> dict:
    """Per-author commits and lines added / removed in the window, summed over `names` (GitHub's weekly contributor
    stats: the window is rounded to whole weeks; merge commits aren't counted). days=0: all time."""
    all_time = int(days or 0) <= 0
    days = 0 if all_time else max(7, min(int(days), 365))
    start = None if all_time else datetime.now(timezone.utc) - timedelta(days=days)
    totals: dict[str, dict] = {}
    missing = []
    def fetch(full: str):
        for _ in range(STATS_TRIES):
            stats = client.get(f"/repos/{full}/stats/contributors")
            if isinstance(stats, list):
                return full, stats
            time.sleep(STATS_WAIT_SEC)                    # 202: GitHub is computing them
        return full, None

    with ThreadPoolExecutor(max_workers=STATS_WORKERS) as pool:    # 11-19: one by one took over a minute
        fetched = list(pool.map(fetch, names))
    for full, stats in fetched:
        if stats is None:
            missing.append(full)
            continue
        for s in stats:
            who = (s.get("author") or {}).get("login") or "unknown"
            t = totals.setdefault(who, {"person": who, "commits": 0, "lines_added": 0, "lines_removed": 0, "repos": set()})
            for w in s.get("weeks") or []:
                if start is not None and datetime.fromtimestamp(w["w"], timezone.utc) + timedelta(days=7) < start:
                    continue
                if w.get("c"):
                    t["repos"].add(full.split("/")[1])
                t["commits"] += w.get("c", 0)
                t["lines_added"] += w.get("a", 0)
                t["lines_removed"] += w.get("d", 0)
    rows = [dict(t, repos=sorted(t["repos"]), lines_changed=t["lines_added"] + t["lines_removed"])
            for t in totals.values() if t["commits"]]
    rows.sort(key=lambda r: -r["lines_changed"])
    out = {"since": "all time" if start is None else start.strftime("%Y-%m-%d"), "days": days or "all time",
           "repos_counted": len(names) - len(missing),
           "people": rows, "note": "GitHub logins; weekly totals (the window is rounded to whole weeks); merge commits "
                                   "are not counted; lockfile and generated-file churn is included in the line counts."}
    if missing:
        out["stats_not_ready"] = missing
    return out
