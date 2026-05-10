"""Weekly incremental crawler that writes mature new IDs into a dedicated delta SQLite DB.

This crawler:
- reads `last_known_id` from a JSON state file
- finds the highest *mature* item id (> last_known_id and older than maturity window)
- crawls only the id range [upper_bound_id .. last_known_id+1] into a fresh delta DB
- advances state only if no non-terminal failures remain unresolved
"""

from __future__ import annotations

import argparse
import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from crawler import Pr0Crawler, to_int, utc_now


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Incremental pr0gramm crawler: crawl only new mature IDs into a fresh delta SQLite DB."
        )
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        required=True,
        help='JSON state file containing at least {"last_known_id": <int>}.',
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("delta"),
        help="Directory where per-run delta SQLite DB files are created.",
    )
    parser.add_argument(
        "--db-prefix",
        default="pr0_delta",
        help="Filename prefix for delta DB files.",
    )
    parser.add_argument(
        "--maturity-days",
        type=int,
        default=14,
        help="Only crawl items with created timestamp older than this many days (default: 14).",
    )
    parser.add_argument(
        "--flags",
        type=int,
        default=31,
        help="Item flags mask used for feed scanning (default: 31).",
    )
    parser.add_argument(
        "--base-url",
        default="https://pr0gramm.com",
        help="Base URL for pr0gramm.",
    )
    parser.add_argument(
        "--session-file",
        default="SESSION",
        help="Path to a saved session file created by main.py.",
    )
    parser.add_argument(
        "--cookie",
        action="append",
        default=[],
        help="Optional cookie as name=value; pass multiple times if needed.",
    )
    parser.add_argument(
        "--proxy",
        default=None,
        help=(
            "Optional proxy URL or ip:port (example: https://54.38.92.94:10000). "
            "Bare ip:port defaults to HTTPS first."
        ),
    )
    parser.add_argument(
        "--expected-public-ip",
        default=None,
        help="Expected egress IP for startup check (example: 89.56.51.32).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Max retries for temporary request errors.",
    )
    parser.add_argument(
        "--req-threads",
        type=int,
        default=1,
        help="Parallel threads for item-detail requests (/items/info).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Delay between main-thread HTTP requests in seconds (default: 0.0).",
    )
    parser.add_argument(
        "--page-sleep",
        type=float,
        default=0.0,
        help="Optional delay between pages in seconds (default: 0.0).",
    )
    parser.add_argument(
        "--scan-max-pages",
        type=int,
        default=None,
        help="Optional safety limit for feed scan pages.",
    )

    args = parser.parse_args()
    if args.maturity_days < 1:
        parser.error("--maturity-days must be >= 1")
    if args.req_threads < 1:
        parser.error("--req-threads must be >= 1")
    if args.scan_max_pages is not None and args.scan_max_pages < 1:
        parser.error("--scan-max-pages must be >= 1")
    return args


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(
            f"State file not found: {path}. Create it with {{\"last_known_id\": <int>}}."
        )
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"State file must contain a JSON object: {path}")

    last_known_id = to_int(payload.get("last_known_id"))
    if last_known_id is None or last_known_id < 1:
        raise ValueError(
            f"State file must contain a positive integer 'last_known_id': {path}"
        )
    payload["last_known_id"] = last_known_id
    return payload


def save_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=path.name + ".",
        suffix=".tmp",
        delete=False,
    ) as tmp:
        tmp.write(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True))
        tmp.write("\n")
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def iso_week_label(now: datetime) -> str:
    iso = now.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def build_delta_db_path(out_dir: Path, prefix: str, upper_id: int, lower_id: int) -> Path:
    week = iso_week_label(datetime.now(UTC))
    return out_dir / f"{prefix}_{week}_ids_{upper_id}_{lower_id}.sqlite3"


def find_mature_upper_bound_id(
    *,
    args: argparse.Namespace,
    last_known_id: int,
    maturity_cutoff_ts: int,
) -> tuple[int | None, int | None, int]:
    scanner = Pr0Crawler(
        db_path=Path(":memory:"),
        base_url=args.base_url,
        flags=args.flags,
        timeout=args.timeout,
        sleep_seconds=args.sleep,
        page_sleep_seconds=0.0,
        max_retries=args.max_retries,
        target_oldest_id=1,
        req_threads=1,
        session_file=Path(args.session_file) if args.session_file else None,
        cookie_pairs=args.cookie,
        proxy_url=args.proxy,
    )
    scanner.check_public_ip(expected_public_ip=args.expected_public_ip)

    cursor: int | None = None
    pages = 0
    newest_seen_above_last_known: int | None = None
    upper_bound_id: int | None = None

    try:
        while True:
            if args.scan_max_pages is not None and pages >= args.scan_max_pages:
                break

            page = scanner.fetch_items(older=cursor)
            items = page.get("items")
            if not isinstance(items, list) or len(items) == 0:
                break

            page_item_ids: list[int] = []
            hit_last_known = False
            for item in items:
                if not isinstance(item, dict):
                    continue
                item_id = to_int(item.get("id"))
                if item_id is None:
                    continue
                page_item_ids.append(item_id)

                if item_id <= last_known_id:
                    hit_last_known = True
                    break

                if newest_seen_above_last_known is None or item_id > newest_seen_above_last_known:
                    newest_seen_above_last_known = item_id

                created_at = to_int(item.get("created"))
                if created_at is not None and created_at <= maturity_cutoff_ts:
                    if upper_bound_id is None or item_id > upper_bound_id:
                        upper_bound_id = item_id

            pages += 1
            if hit_last_known:
                break
            if not page_item_ids:
                break
            cursor = min(page_item_ids)
    finally:
        scanner.close()

    return upper_bound_id, newest_seen_above_last_known, pages


def main() -> int:
    args = parse_args()
    state = load_state(args.state_file)
    last_known_id = int(state["last_known_id"])
    lower_id = last_known_id + 1
    maturity_cutoff_ts = utc_now() - (args.maturity_days * 86400)

    print(
        "scan_start "
        f"last_known_id={last_known_id} lower_id={lower_id} "
        f"maturity_days={args.maturity_days} maturity_cutoff_ts={maturity_cutoff_ts}"
    )

    upper_bound_id, newest_seen_above_last_known, scanned_pages = find_mature_upper_bound_id(
        args=args,
        last_known_id=last_known_id,
        maturity_cutoff_ts=maturity_cutoff_ts,
    )

    print(
        "scan_done "
        f"scanned_pages={scanned_pages} newest_seen_above_last_known={newest_seen_above_last_known} "
        f"mature_upper_bound_id={upper_bound_id}"
    )

    if upper_bound_id is None or upper_bound_id < lower_id:
        print(
            "No mature new items found. "
            "Nothing crawled, no delta DB created, state unchanged."
        )
        return 0

    args.out_dir.mkdir(parents=True, exist_ok=True)
    delta_db_path = build_delta_db_path(args.out_dir, args.db_prefix, upper_bound_id, lower_id)
    if delta_db_path.exists():
        raise FileExistsError(f"Delta DB already exists: {delta_db_path}")

    crawler = Pr0Crawler(
        db_path=delta_db_path,
        base_url=args.base_url,
        flags=args.flags,
        timeout=args.timeout,
        sleep_seconds=args.sleep,
        page_sleep_seconds=args.page_sleep,
        max_retries=args.max_retries,
        target_oldest_id=lower_id,
        req_threads=args.req_threads,
        session_file=Path(args.session_file) if args.session_file else None,
        cookie_pairs=args.cookie,
        proxy_url=args.proxy,
    )
    crawler.check_public_ip(expected_public_ip=args.expected_public_ip)

    try:
        print(f"crawl_start db={delta_db_path} range={upper_bound_id}..{lower_id}")
        crawler.backfill_id_range(start_id=upper_bound_id, end_id=lower_id)

        non_terminal_failures = int(
            crawler.db.execute(
                """
                SELECT COUNT(*)
                FROM item_detail_failures
                WHERE
                    item_id BETWEEN ? AND ?
                    AND terminal = 0
                    AND resolved_at IS NULL
                """,
                (lower_id, upper_bound_id),
            ).fetchone()[0]
        )
        terminal_failures = int(
            crawler.db.execute(
                """
                SELECT COUNT(*)
                FROM item_detail_failures
                WHERE
                    item_id BETWEEN ? AND ?
                    AND terminal = 1
                """,
                (lower_id, upper_bound_id),
            ).fetchone()[0]
        )
        item_rows = int(crawler.db.execute("SELECT COUNT(*) FROM items").fetchone()[0])
        comment_rows = int(crawler.db.execute("SELECT COUNT(*) FROM comments").fetchone()[0])

        print(
            "crawl_done "
            f"item_rows={item_rows} comment_rows={comment_rows} "
            f"terminal_failures={terminal_failures} non_terminal_failures={non_terminal_failures}"
        )

        if non_terminal_failures > 0:
            print(
                "State not advanced because unresolved non-terminal failures exist. "
                f"state_file={args.state_file}"
            )
            return 2

        state["last_known_id"] = upper_bound_id
        state["last_run_at"] = datetime.now(UTC).isoformat()
        state["last_output_db"] = str(delta_db_path)
        state["last_range_from"] = upper_bound_id
        state["last_range_to"] = lower_id
        save_state(args.state_file, state)
        print(
            "state_advanced "
            f"state_file={args.state_file} old_last_known_id={last_known_id} "
            f"new_last_known_id={upper_bound_id}"
        )
        return 0
    finally:
        crawler.close()


if __name__ == "__main__":
    raise SystemExit(main())
