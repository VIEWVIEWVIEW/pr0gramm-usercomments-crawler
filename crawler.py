"""Resumable pr0gramm crawler that stores items and comments in SQLite."""

from __future__ import annotations

import argparse
import atexit
import json
import queue
import socket
import signal
import sqlite3
import ssl
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import requests

from session_store import load_session_from_file


def utc_now() -> int:
    return int(time.time())


def format_duration(seconds: float) -> str:
    total = max(0, int(seconds))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours > 0:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes > 0:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


@dataclass(slots=True)
class DetailFetchResult:
    item_id: int
    detail: dict[str, Any] | None
    error: Exception | None


class Pr0Crawler:
    def __init__(
        self,
        db_path: Path,
        *,
        base_url: str = "https://pr0gramm.com",
        flags: int = 31,
        timeout: int = 30,
        sleep_seconds: float = 0.0,
        page_sleep_seconds: float = 0.0,
        max_retries: int = 3,
        target_oldest_id: int = 1,
        req_threads: int = 1,
        session_file: Path | None = None,
        cookie_pairs: list[str] | None = None,
        proxy_url: str | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_base = f"{self.base_url}/api"
        self.flags = flags
        self.timeout = timeout
        self.sleep_seconds = max(0.0, sleep_seconds)
        self.page_sleep_seconds = max(0.0, page_sleep_seconds)
        self.max_retries = max_retries
        self.target_oldest_id = max(1, target_oldest_id)
        self.req_threads = max(1, req_threads)
        self.stop_requested = False
        self._closed = False
        self.request_count = 0
        self._last_request_at: float | None = None
        self._run_started: float | None = None
        self._run_request_start_count = 0
        self._progress_page_index = 0
        self._progress_item_index = 0
        self._progress_page_total = 0
        self._progress_item_count_total = 0
        self._progress_comment_count_total = 0
        self._progress_cursor: int | None = None
        self._progress_crawl_start_id: int | None = None
        self._progress_current_item_id: int | None = None
        self._detail_seen_cache: set[int] = set()
        self._detail_requested_cache: set[int] = set()
        self._detail_request_lock = threading.Lock()
        self._detail_work_queue: queue.Queue[int | object] | None = None
        self._detail_result_queue: queue.Queue[DetailFetchResult] | None = None
        self._detail_workers: list[threading.Thread] = []
        self._detail_worker_stop_token = object()
        self._detail_pending_jobs = 0
        self._detail_attempts: dict[int, int] = {}
        self._detail_retry_requeued = 0
        self._detail_dedupe_skipped = 0
        self._bootstrapped = False
        self._rate_limit_until: float | None = None
        self._main_thread_id = threading.get_ident()
        self._request_state_lock = threading.Lock()
        self._thread_local = threading.local()
        self._worker_sessions: list[requests.Session] = []
        self._worker_sessions_lock = threading.Lock()
        self.proxy_url = normalize_proxy_url(proxy_url) if proxy_url else None

        self.session = requests.Session()
        self.session.headers.update(
            {
                "accept": "application/json, text/javascript, */*; q=0.01",
                "user-agent": "stylometry-crawler/0.1",
                "x-requested-with": "XMLHttpRequest",
                "referer": f"{self.base_url}/new",
            }
        )
        self._configure_proxy()
        if session_file is not None and session_file.exists():
            loaded = load_session_from_file(self.session, session_file)
            print(f"Loaded {loaded} cookies from session file: {session_file}")
        self._apply_cookie_pairs(cookie_pairs or [])

        self.db_path = db_path
        self.db = sqlite3.connect(self.db_path, timeout=30)
        self._configure_sqlite()
        self._ensure_schema()

    def close(self) -> None:
        if self._closed:
            return
        self._shutdown_detail_fetch_workers()
        try:
            self.db.commit()
        except sqlite3.Error:
            pass
        try:
            self.db.close()
        except sqlite3.Error:
            pass
        self.session.close()
        with self._worker_sessions_lock:
            worker_sessions = list(self._worker_sessions)
            self._worker_sessions.clear()
        for worker_session in worker_sessions:
            worker_session.close()
        self._closed = True

    def request_stop(self) -> None:
        self.stop_requested = True

    def _should_interrupt_request_for_stop(self) -> bool:
        if not self.stop_requested:
            return False
        # Graceful stop: allow detail worker threads to finish the active page.
        thread_name = threading.current_thread().name
        return not thread_name.startswith("req-")

    def _configure_sqlite(self) -> None:
        self.db.execute("PRAGMA busy_timeout = 5000")
        try:
            self.db.execute("PRAGMA journal_mode = WAL")
        except sqlite3.OperationalError:
            # UNC/network-like mounts can reject journal changes.
            # Continue with SQLite defaults instead of failing startup.
            try:
                self.db.execute("PRAGMA journal_mode = DELETE")
            except sqlite3.OperationalError:
                pass
        self.db.execute("PRAGMA synchronous = NORMAL")
        self.db.execute("PRAGMA foreign_keys = ON")

    def _apply_cookie_pairs(self, pairs: list[str]) -> None:
        for pair in pairs:
            if "=" not in pair:
                raise ValueError(f"Cookie must be name=value, got: {pair!r}")
            name, value = pair.split("=", 1)
            name = name.strip()
            if not name:
                raise ValueError(f"Cookie name is empty: {pair!r}")
            self.session.cookies.set(name, value, domain="pr0gramm.com")

    def _configure_proxy(self) -> None:
        if self.proxy_url is None:
            return
        proxy_url = self.proxy_url
        parsed = urlparse(proxy_url)
        if parsed.scheme.lower() == "https" and not self._supports_https_proxy_tls(
            host=parsed.hostname,
            port=parsed.port,
        ):
            fallback = parsed._replace(scheme="http").geturl()
            print(
                "startup proxy_check "
                f"https_proxy_unreachable={proxy_url} fallback={fallback}"
            )
            proxy_url = fallback
            self.proxy_url = fallback
        self.session.proxies.update({"http": proxy_url, "https": proxy_url})

    def _supports_https_proxy_tls(self, *, host: str | None, port: int | None) -> bool:
        if host is None or port is None:
            return False
        timeout = min(5.0, max(1.0, float(self.timeout)))
        try:
            with socket.create_connection((host, port), timeout=timeout) as sock:
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                with context.wrap_socket(sock, server_hostname=host):
                    return True
        except OSError:
            return False

    def _bootstrap_session(self) -> None:
        """Prime cookies that the API may expect from a regular site visit."""
        if self._bootstrapped:
            return
        self._bootstrapped = True
        try:
            self.session.get(f"{self.base_url}/new", timeout=self.timeout)
        except requests.RequestException:
            # Not fatal; the API request path below will report concrete errors.
            pass

    def _get_request_session(self) -> requests.Session:
        if threading.get_ident() == self._main_thread_id:
            return self.session

        worker_session = getattr(self._thread_local, "session", None)
        if worker_session is not None:
            return worker_session

        worker_session = requests.Session()
        worker_session.headers.update(dict(self.session.headers))
        worker_session.cookies.update(self.session.cookies)
        worker_session.proxies.update(dict(self.session.proxies))
        self._thread_local.session = worker_session
        with self._worker_sessions_lock:
            self._worker_sessions.append(worker_session)
        return worker_session

    def check_public_ip(
        self,
        *,
        expected_public_ip: str | None,
        check_url: str = "https://wtfismyip.com/json",
    ) -> None:
        response = self.session.get(check_url, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected IP check response payload: {type(payload)!r}")

        public_ip = first_str(
            payload.get("YourFuckingIPAddress"),
            payload.get("IPAddress"),
            payload.get("ip"),
            payload.get("IP"),
            payload.get("address"),
        )
        if public_ip is None:
            raise ValueError(f"Could not parse public IP from response: {payload!r}")

        route = self.proxy_url if self.proxy_url else "direct"
        print(f"startup ip_check public_ip={public_ip} route={route}")
        if expected_public_ip and public_ip != expected_public_ip:
            raise RuntimeError(
                f"Public IP mismatch: expected {expected_public_ip}, got {public_ip}."
            )

    def _ensure_schema(self) -> None:
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS crawl_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS items (
                item_id INTEGER PRIMARY KEY,
                promoted INTEGER,
                flags INTEGER,
                created_at INTEGER,
                user_name TEXT,
                source_feed TEXT NOT NULL,
                crawl_time INTEGER NOT NULL,
                detail_crawl_time INTEGER,
                raw_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS comments (
                comment_id INTEGER PRIMARY KEY,
                item_id INTEGER NOT NULL,
                parent_comment_id INTEGER,
                comment_time INTEGER,
                score INTEGER,
                upvotes INTEGER,
                downvotes INTEGER,
                crawl_time INTEGER NOT NULL,
                user_name TEXT,
                user_id INTEGER,
                user_profile_url TEXT,
                body TEXT,
                raw_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS item_detail_failures (
                item_id INTEGER PRIMARY KEY,
                first_failed_at INTEGER NOT NULL,
                last_failed_at INTEGER NOT NULL,
                attempt_count INTEGER NOT NULL,
                last_error TEXT NOT NULL,
                terminal INTEGER NOT NULL DEFAULT 0,
                resolved_at INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_comments_item_id ON comments(item_id);
            CREATE INDEX IF NOT EXISTS idx_comments_user_name ON comments(user_name);
            CREATE INDEX IF NOT EXISTS idx_comments_parent ON comments(parent_comment_id);
            CREATE INDEX IF NOT EXISTS idx_item_detail_failures_unresolved
                ON item_detail_failures(resolved_at, terminal);
            """
        )
        self._ensure_items_detail_column()
        self._ensure_items_item_id_unique()
        self.db.commit()

    def _ensure_items_detail_column(self) -> None:
        columns = {
            str(row[1]) for row in self.db.execute("PRAGMA table_info(items)").fetchall()
        }
        if "detail_crawl_time" in columns:
            return

        self.db.execute("ALTER TABLE items ADD COLUMN detail_crawl_time INTEGER")
        # Older crawler versions always requested /items/info for each item,
        # so mark existing rows as already detailed to avoid re-fetching them.
        self.db.execute(
            "UPDATE items SET detail_crawl_time = crawl_time WHERE detail_crawl_time IS NULL"
        )

    def _ensure_items_item_id_unique(self) -> None:
        columns = self.db.execute("PRAGMA table_info(items)").fetchall()
        for row in columns:
            if str(row[1]) == "item_id" and int(row[5]) > 0:
                return
        try:
            self.db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_items_item_id_unique ON items(item_id)"
            )
        except sqlite3.IntegrityError as exc:
            raise RuntimeError(
                "items.item_id must be unique; existing duplicate item_id values were found."
            ) from exc

    def get_state(self, key: str) -> str | None:
        row = self.db.execute(
            "SELECT value FROM crawl_state WHERE key = ?",
            (key,),
        ).fetchone()
        return None if row is None else str(row[0])

    def set_state(self, key: str, value: str) -> None:
        now = utc_now()
        self.db.execute(
            """
            INSERT INTO crawl_state(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (key, value, now),
        )

    def clear_state(self, key: str) -> None:
        self.db.execute("DELETE FROM crawl_state WHERE key = ?", (key,))

    def _get_json(self, endpoint: str, *, params: dict[str, Any]) -> dict[str, Any]:
        if self._should_interrupt_request_for_stop():
            raise KeyboardInterrupt("Stop requested")
        url = f"{self.api_base}{endpoint}"
        last_error: Exception | None = None
        session = self._get_request_session()
        is_main_thread = threading.get_ident() == self._main_thread_id

        for attempt in range(1, self.max_retries + 1):
            try:
                if self._should_interrupt_request_for_stop():
                    raise KeyboardInterrupt("Stop requested")
                with self._request_state_lock:
                    rate_limit_until = self._rate_limit_until
                if rate_limit_until is not None:
                    wait_left = rate_limit_until - time.monotonic()
                    if wait_left > 0:
                        time.sleep(wait_left)
                    with self._request_state_lock:
                        if (
                            self._rate_limit_until is not None
                            and self._rate_limit_until <= time.monotonic()
                        ):
                            self._rate_limit_until = None
                if is_main_thread and self._last_request_at is not None and self.sleep_seconds > 0:
                    elapsed = time.monotonic() - self._last_request_at
                    if elapsed < self.sleep_seconds:
                        time.sleep(self.sleep_seconds - elapsed)
                with self._request_state_lock:
                    self.request_count += 1
                    request_number = self.request_count - self._run_request_start_count
                response = session.get(url, params=params, timeout=self.timeout)
                if is_main_thread:
                    self._last_request_at = time.monotonic()
                request_item_id = to_int(params.get("itemId")) if endpoint == "/items/info" else None
                self._log_request_progress(
                    endpoint=endpoint,
                    status_code=response.status_code,
                    request_number=request_number,
                    request_item_id=request_item_id,
                )
                if response.status_code in (401, 403):
                    if not self._bootstrapped:
                        self._bootstrap_session()
                        continue
                    raise PermissionError(
                        f"HTTP {response.status_code} for {url}. "
                        "Try providing authenticated cookies via --session-file "
                        "(or --cookie me=<session_cookie>)."
                    )
                if response.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(
                        f"Temporary HTTP {response.status_code} for {url}",
                        response=response,
                    )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError(f"Unexpected JSON payload type: {type(payload)!r}")
                return payload
            except requests.HTTPError as exc:
                last_error = exc
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code is not None and status_code not in (429, 500, 502, 503, 504):
                    raise
                if attempt >= self.max_retries:
                    break
                backoff_seconds = self._compute_backoff_seconds(attempt=attempt, error=exc)
                if backoff_seconds > 0:
                    self._log_retry_wait(
                        endpoint=endpoint,
                        attempt=attempt,
                        error=exc,
                        backoff_seconds=backoff_seconds,
                    )
                    with self._request_state_lock:
                        retry_at = time.monotonic() + backoff_seconds
                        if self._rate_limit_until is None:
                            self._rate_limit_until = retry_at
                        else:
                            self._rate_limit_until = max(self._rate_limit_until, retry_at)
                    time.sleep(backoff_seconds)
            except PermissionError:
                raise
            except KeyboardInterrupt:
                raise
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= self.max_retries:
                    break
                backoff_seconds = self._compute_backoff_seconds(attempt=attempt, error=exc)
                if backoff_seconds > 0:
                    self._log_retry_wait(
                        endpoint=endpoint,
                        attempt=attempt,
                        error=exc,
                        backoff_seconds=backoff_seconds,
                    )
                    with self._request_state_lock:
                        retry_at = time.monotonic() + backoff_seconds
                        if self._rate_limit_until is None:
                            self._rate_limit_until = retry_at
                        else:
                            self._rate_limit_until = max(self._rate_limit_until, retry_at)
                    time.sleep(backoff_seconds)

        assert last_error is not None
        raise RuntimeError(f"Failed request for {endpoint}: {last_error}") from last_error

    def _compute_backoff_seconds(self, *, attempt: int, error: Exception) -> float:
        # Exponential backoff with a conservative cap; 429s may require longer waits.
        base_backoff = min(float(2**attempt), 60.0)
        if isinstance(error, requests.HTTPError) and error.response is not None:
            if error.response.status_code == 429:
                retry_after = self._parse_retry_after_seconds(error.response)
                if retry_after is not None:
                    return max(retry_after, base_backoff)
                return max(5.0, base_backoff)
        return base_backoff

    def _parse_retry_after_seconds(self, response: requests.Response) -> float | None:
        raw = response.headers.get("Retry-After")
        if raw is None:
            return None
        raw = raw.strip()
        if raw == "":
            return None

        try:
            delay = float(raw)
            return max(0.0, delay)
        except ValueError:
            pass

        try:
            retry_at = parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)

        return max(0.0, retry_at.timestamp() - time.time())

    def _log_retry_wait(
        self,
        *,
        endpoint: str,
        attempt: int,
        error: Exception,
        backoff_seconds: float,
    ) -> None:
        status = "error"
        retry_after_header: str | None = None
        retry_after_seconds: float | None = None
        if isinstance(error, requests.HTTPError) and error.response is not None:
            status = str(error.response.status_code)
            header_value = error.response.headers.get("Retry-After")
            if header_value is not None and header_value.strip() != "":
                retry_after_header = header_value.strip()
                retry_after_seconds = self._parse_retry_after_seconds(error.response)
        elif isinstance(error, requests.RequestException):
            status = "request-exception"

        details = ""
        if retry_after_header is not None:
            details = f" retry_after_header={retry_after_header}"
            if retry_after_seconds is not None:
                details += f" retry_after_seconds={retry_after_seconds:.2f}s"

        print(
            f"retry status={status} endpoint={endpoint} "
            f"attempt={attempt}/{self.max_retries} wait={backoff_seconds:.2f}s{details}"
        )

    def _log_request_progress(
        self,
        *,
        endpoint: str,
        status_code: int,
        request_number: int,
        request_item_id: int | None,
    ) -> None:
        if self._run_started is None:
            return
        run_elapsed = max(time.monotonic() - self._run_started, 1e-9)
        req_per_sec = request_number / run_elapsed

        estimate = ""
        if self._progress_crawl_start_id is not None and self._progress_cursor is not None:
            processed_span = max(0, self._progress_crawl_start_id - self._progress_cursor)
            total_span = max(1, self._progress_crawl_start_id - self.target_oldest_id)
            remaining_span = max(0, self._progress_cursor - self.target_oldest_id)
            progress_pct = min(100.0, (processed_span / total_span) * 100.0)
            ids_per_sec = processed_span / run_elapsed if run_elapsed > 0 else 0.0
            eta_seconds = remaining_span / ids_per_sec if ids_per_sec > 0 else -1.0
            eta = format_duration(eta_seconds) if eta_seconds >= 0 else "?"
            eta_hours = f"{(eta_seconds / 3600.0):.2f}h" if eta_seconds >= 0 else "?"
            estimate = (
                f" overall_progress={progress_pct:.3f}% "
                f"start_id={self._progress_crawl_start_id} current_cursor={self._progress_cursor} "
                f"target_id={self.target_oldest_id} remaining_ids={remaining_span} "
                f"eta={eta} eta_h={eta_hours}"
            )

        item_detail = (
            f" request_item_id={request_item_id}" if request_item_id is not None else ""
        )
        detail_metrics = (
            f" detail_queue={self._detail_pending_jobs}"
            f" detail_retry_requeued={self._detail_retry_requeued}"
            f" detail_dedupe_skipped={self._detail_dedupe_skipped}"
        )
        print(
            f"progress req={request_number} status={status_code} endpoint={endpoint}{item_detail} "
            f"page={self._progress_page_index} item={self._progress_item_index}/{self._progress_page_total} "
            f"current_item_id={self._progress_current_item_id} "
            f"total_items={self._progress_item_count_total} total_comments={self._progress_comment_count_total} "
            f"req_per_sec={req_per_sec:.2f}{detail_metrics}{estimate}"
        )

    def fetch_items(self, *, older: int | None) -> dict[str, Any]:
        params: dict[str, Any] = {"flags": self.flags}
        if older is not None:
            params["older"] = older
        return self._get_json("/items/get", params=params)

    def _extract_page_item_ids(self, page: dict[str, Any]) -> list[int]:
        items = page.get("items")
        if not isinstance(items, list):
            return []
        item_ids: list[int] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            item_id = to_int(item.get("id"))
            if item_id is not None:
                item_ids.append(item_id)
        return item_ids

    def _fetch_following_page(
        self,
        page_future: Future[dict[str, Any]],
    ) -> dict[str, Any] | None:
        page = page_future.result()
        item_ids = self._extract_page_item_ids(page)
        if not item_ids:
            return None
        return self.fetch_items(older=min(item_ids))

    def fetch_item_info(self, item_id: int) -> dict[str, Any]:
        return self._get_json("/items/info", params={"itemId": item_id})

    def has_item_detail(self, item_id: int) -> bool:
        if item_id in self._detail_seen_cache:
            return True
        row = self.db.execute(
            "SELECT detail_crawl_time FROM items WHERE item_id = ?",
            (item_id,),
        ).fetchone()
        seen = row is not None and row[0] is not None
        if seen:
            self._detail_seen_cache.add(item_id)
        return seen

    def _start_detail_fetch_workers(self) -> None:
        if self._detail_work_queue is not None:
            return
        self._detail_work_queue = queue.Queue()
        self._detail_result_queue = queue.Queue()
        self._detail_workers = []
        self._detail_pending_jobs = 0
        self._detail_attempts.clear()
        self._detail_retry_requeued = 0
        self._detail_dedupe_skipped = 0
        with self._detail_request_lock:
            self._detail_requested_cache.clear()
        for idx in range(self.req_threads):
            worker = threading.Thread(
                target=self._detail_fetch_worker_loop,
                name=f"req-{idx + 1}",
                daemon=True,
            )
            worker.start()
            self._detail_workers.append(worker)

    def _clear_pending_detail_jobs(self) -> None:
        work_queue = self._detail_work_queue
        if work_queue is None:
            return
        while True:
            try:
                queued_item = work_queue.get_nowait()
            except queue.Empty:
                break
            work_queue.task_done()
            if queued_item is self._detail_worker_stop_token:
                continue
            if self._detail_pending_jobs > 0:
                self._detail_pending_jobs -= 1

    def _shutdown_detail_fetch_workers(self) -> None:
        work_queue = self._detail_work_queue
        if work_queue is None:
            return
        if self.stop_requested:
            self._clear_pending_detail_jobs()
        for _ in self._detail_workers:
            work_queue.put(self._detail_worker_stop_token)
        for worker in self._detail_workers:
            worker.join()
        self._detail_workers.clear()
        self._detail_work_queue = None
        self._detail_result_queue = None
        self._detail_pending_jobs = 0
        self._detail_attempts.clear()
        self._detail_retry_requeued = 0
        self._detail_dedupe_skipped = 0
        with self._detail_request_lock:
            self._detail_requested_cache.clear()

    def _detail_fetch_worker_loop(self) -> None:
        work_queue = self._detail_work_queue
        result_queue = self._detail_result_queue
        if work_queue is None or result_queue is None:
            return

        while True:
            queue_item = work_queue.get()
            try:
                if queue_item is self._detail_worker_stop_token:
                    return
                item_id = int(queue_item)
                try:
                    detail = self.fetch_item_info(item_id)
                    result_queue.put(
                        DetailFetchResult(item_id=item_id, detail=detail, error=None)
                    )
                except KeyboardInterrupt:
                    result_queue.put(
                        DetailFetchResult(
                            item_id=item_id,
                            detail=None,
                            error=RuntimeError("Stop requested"),
                        )
                    )
                except Exception as exc:  # noqa: BLE001
                    result_queue.put(
                        DetailFetchResult(item_id=item_id, detail=None, error=exc)
                    )
            finally:
                work_queue.task_done()

    def enqueue_item_detail(self, item_id: int) -> bool:
        if self.has_item_detail(item_id):
            return False
        work_queue = self._detail_work_queue
        if work_queue is None:
            raise RuntimeError("Detail worker queue is not initialized")
        with self._detail_request_lock:
            if item_id in self._detail_requested_cache:
                self._detail_dedupe_skipped += 1
                return False
            self._detail_requested_cache.add(item_id)
        self._detail_pending_jobs += 1
        work_queue.put(item_id)
        return True

    def _requeue_item_detail(self, item_id: int) -> None:
        work_queue = self._detail_work_queue
        if work_queue is None:
            raise RuntimeError("Detail worker queue is not initialized")
        self._detail_pending_jobs += 1
        work_queue.put(item_id)

    def _dequeue_detail_results(self, *, block: bool) -> list[DetailFetchResult]:
        result_queue = self._detail_result_queue
        if result_queue is None:
            return []

        results: list[DetailFetchResult] = []
        if block and self._detail_pending_jobs > 0:
            while True:
                try:
                    first = result_queue.get(timeout=0.25)
                    results.append(first)
                    break
                except queue.Empty:
                    if self.stop_requested and self._detail_pending_jobs <= 0:
                        return results
        while True:
            try:
                results.append(result_queue.get_nowait())
            except queue.Empty:
                break
        return results

    def _consume_detail_result(
        self,
        result: DetailFetchResult,
        *,
        crawl_time: int,
    ) -> tuple[str, int]:
        item_id = result.item_id
        self._progress_current_item_id = item_id
        attempts = self._detail_attempts.get(item_id, 0) + 1
        self._detail_attempts[item_id] = attempts

        if result.error is not None:
            if (
                not self.stop_requested
                and attempts == 1
                and not is_terminal_detail_error(result.error)
            ):
                self._detail_retry_requeued += 1
                self._requeue_item_detail(item_id)
                return "retry", 0
            self.record_item_detail_failure(item_id, result.error)
            self.db.commit()
            print(f"Failed item {item_id}: {result.error}")
            return "failed", 0

        if result.detail is None:
            error = RuntimeError("No detail payload returned")
            self.record_item_detail_failure(item_id, error)
            self.db.commit()
            print(f"Failed item {item_id}: {error}")
            return "failed", 0

        try:
            comment_count = self.process_item_detail(item_id, detail=result.detail, crawl_time=crawl_time)
            self.db.commit()
            return "success", comment_count
        except Exception as exc:  # noqa: BLE001
            self.record_item_detail_failure(item_id, exc)
            self.db.commit()
            print(f"Failed item {item_id}: {exc}")
            return "failed", 0

    def mark_item_detail_fetched(self, item_id: int, *, crawl_time: int) -> None:
        self.db.execute(
            """
            UPDATE items
            SET detail_crawl_time = COALESCE(detail_crawl_time, ?)
            WHERE item_id = ?
            """,
            (crawl_time, item_id),
        )
        self.db.execute(
            """
            UPDATE item_detail_failures
            SET resolved_at = ?, terminal = 0
            WHERE item_id = ?
            """,
            (crawl_time, item_id),
        )
        self._detail_seen_cache.add(item_id)
        with self._detail_request_lock:
            self._detail_requested_cache.discard(item_id)

    def record_item_detail_failure(self, item_id: int, error: Exception) -> None:
        now = utc_now()
        terminal = 1 if is_terminal_detail_error(error) else 0
        resolved_at = now if terminal else None
        self.db.execute(
            """
            INSERT INTO item_detail_failures(
                item_id, first_failed_at, last_failed_at, attempt_count,
                last_error, terminal, resolved_at
            )
            VALUES(?, ?, ?, 1, ?, ?, ?)
            ON CONFLICT(item_id) DO UPDATE SET
                last_failed_at = excluded.last_failed_at,
                attempt_count = item_detail_failures.attempt_count + 1,
                last_error = excluded.last_error,
                terminal = excluded.terminal,
                resolved_at = excluded.resolved_at
            """,
            (item_id, now, now, summarize_error(error), terminal, resolved_at),
        )

    def has_terminal_item_detail_failure(self, item_id: int) -> bool:
        row = self.db.execute(
            """
            SELECT 1
            FROM item_detail_failures
            WHERE item_id = ? AND terminal = 1 AND resolved_at IS NOT NULL
            """,
            (item_id,),
        ).fetchone()
        return row is not None

    def upsert_item(
        self, item: dict[str, Any], *, crawl_time: int, source_feed: str = "new"
    ) -> None:
        item_id = to_int(item.get("id"))
        if item_id is None:
            raise ValueError(f"Item has no usable id: {item!r}")
        self.db.execute(
            """
            INSERT INTO items(
                item_id, promoted, flags, created_at, user_name, source_feed, crawl_time, detail_crawl_time, raw_json
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(item_id) DO UPDATE SET
                promoted = excluded.promoted,
                flags = excluded.flags,
                created_at = excluded.created_at,
                user_name = excluded.user_name,
                source_feed = CASE
                    WHEN items.source_feed = 'direct' AND excluded.source_feed <> 'direct'
                    THEN excluded.source_feed
                    ELSE items.source_feed
                END,
                crawl_time = excluded.crawl_time,
                raw_json = excluded.raw_json
            """,
            (
                item_id,
                to_int(item.get("promoted")),
                to_int(item.get("flags")),
                to_int(item.get("created")),
                to_str(item.get("user")),
                source_feed,
                crawl_time,
                None,
                json.dumps(item, ensure_ascii=True, separators=(",", ":")),
            ),
        )

    def ensure_direct_item_row(
        self, item_id: int, *, detail: dict[str, Any], crawl_time: int
    ) -> None:
        if self.db.execute("SELECT 1 FROM items WHERE item_id = ?", (item_id,)).fetchone():
            return

        detail_item = extract_item_payload(detail, item_id=item_id)
        if detail_item is not None:
            self.upsert_item(detail_item, crawl_time=crawl_time, source_feed="direct")
            return

        placeholder = {
            "id": item_id,
            "flags": self.flags,
            "source": "direct-backfill",
            "detail": detail,
        }
        self.upsert_item(placeholder, crawl_time=crawl_time, source_feed="direct")

    def upsert_comment(self, comment: dict[str, Any], *, item_id: int, crawl_time: int) -> None:
        comment_id = to_int(comment.get("id"))
        if comment_id is None:
            return

        username = first_str(
            comment.get("name"),
            comment.get("userName"),
            comment.get("user"),
        )
        user_id = to_int(comment.get("userId"))
        if user_id is None and isinstance(comment.get("user"), dict):
            user_id = to_int(comment["user"].get("id"))

        upvotes = to_int(comment.get("up"))
        downvotes = to_int(comment.get("down"))
        score = to_int(comment.get("score"))
        if score is None and upvotes is not None and downvotes is not None:
            score = upvotes - downvotes

        profile_url = None
        if username:
            profile_url = f"{self.base_url}/user/{quote(username)}"

        self.db.execute(
            """
            INSERT INTO comments(
                comment_id,
                item_id,
                parent_comment_id,
                comment_time,
                score,
                upvotes,
                downvotes,
                crawl_time,
                user_name,
                user_id,
                user_profile_url,
                body,
                raw_json
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(comment_id) DO UPDATE SET
                item_id = excluded.item_id,
                parent_comment_id = excluded.parent_comment_id,
                comment_time = excluded.comment_time,
                score = excluded.score,
                upvotes = excluded.upvotes,
                downvotes = excluded.downvotes,
                crawl_time = excluded.crawl_time,
                user_name = excluded.user_name,
                user_id = excluded.user_id,
                user_profile_url = excluded.user_profile_url,
                body = excluded.body,
                raw_json = excluded.raw_json
            """,
            (
                comment_id,
                item_id,
                to_int(comment.get("parent")),
                first_int(
                    comment.get("created"),
                    comment.get("createdTs"),
                    comment.get("time"),
                ),
                score,
                upvotes,
                downvotes,
                crawl_time,
                username,
                user_id,
                profile_url,
                first_str(
                    comment.get("content"),
                    comment.get("comment"),
                    comment.get("text"),
                ),
                json.dumps(comment, ensure_ascii=True, separators=(",", ":")),
            ),
        )

    def extract_comments(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """Flatten comments from item info payload while staying resilient to shape drift."""
        initial = payload.get("comments")
        if not isinstance(initial, list):
            initial = []

        stack: list[Any] = list(initial)
        result: list[dict[str, Any]] = []
        seen: set[int] = set()

        while stack:
            node = stack.pop()
            if isinstance(node, list):
                stack.extend(node)
                continue
            if not isinstance(node, dict):
                continue

            for child_key in ("children", "replies"):
                child_value = node.get(child_key)
                if isinstance(child_value, list):
                    stack.extend(child_value)

            comment_id = to_int(node.get("id"))
            if comment_id is None:
                continue
            if not looks_like_comment(node):
                continue
            if comment_id in seen:
                continue

            seen.add(comment_id)
            result.append(node)

        return result

    def process_item(self, item: dict[str, Any], *, crawl_time: int) -> tuple[int, int]:
        item_id = to_int(item.get("id"))
        if item_id is None:
            return 0, 0

        self.upsert_item(item, crawl_time=crawl_time)
        if self.has_item_detail(item_id):
            return 1, 0
        try:
            detail = self.fetch_item_info(item_id)
        except Exception as exc:  # noqa: BLE001
            self.record_item_detail_failure(item_id, exc)
            raise
        comment_count = self.process_item_detail(item_id, detail=detail, crawl_time=crawl_time)
        return 1, comment_count

    def process_item_detail(
        self, item_id: int, *, detail: dict[str, Any], crawl_time: int
    ) -> int:
        self.ensure_direct_item_row(item_id, detail=detail, crawl_time=crawl_time)
        comments = self.extract_comments(detail)
        for comment in comments:
            self.upsert_comment(comment, item_id=item_id, crawl_time=crawl_time)
        self.mark_item_detail_fetched(item_id, crawl_time=crawl_time)
        return len(comments)

    def select_missing_detail_item_ids(
        self,
        *,
        from_id: int | None = None,
        to_id: int | None = None,
        limit: int | None = None,
    ) -> list[int]:
        clauses = [
            "items.detail_crawl_time IS NULL",
            """
            (
                item_detail_failures.item_id IS NULL
                OR item_detail_failures.terminal = 0
                OR item_detail_failures.resolved_at IS NULL
            )
            """,
        ]
        params: list[int] = []
        if from_id is not None:
            clauses.append("items.item_id >= ?")
            params.append(from_id)
        if to_id is not None:
            clauses.append("items.item_id <= ?")
            params.append(to_id)

        sql = f"""
            SELECT items.item_id
            FROM items
            LEFT JOIN item_detail_failures
                ON item_detail_failures.item_id = items.item_id
            WHERE {' AND '.join(clauses)}
            ORDER BY items.item_id DESC
        """
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return [int(row[0]) for row in self.db.execute(sql, params).fetchall()]

    def retry_missing_details(
        self,
        *,
        from_id: int | None = None,
        to_id: int | None = None,
        limit: int | None = None,
    ) -> None:
        item_ids = self.select_missing_detail_item_ids(
            from_id=from_id,
            to_id=to_id,
            limit=limit,
        )
        self.process_detail_ids(item_ids, label="retry-missing-details")

    def backfill_id_range(
        self,
        *,
        start_id: int,
        end_id: int,
        limit: int | None = None,
    ) -> None:
        step = 1 if end_id >= start_id else -1
        item_ids = list(range(start_id, end_id + step, step))
        item_ids = [
            item_id
            for item_id in item_ids
            if not self.has_item_detail(item_id)
            and not self.has_terminal_item_detail_failure(item_id)
        ]
        if limit is not None:
            item_ids = item_ids[:limit]
        self.process_detail_ids(item_ids, label="backfill-id-range")

    def process_detail_ids(self, item_ids: list[int], *, label: str) -> None:
        started = time.monotonic()
        self._run_started = started
        self._run_request_start_count = self.request_count
        self._progress_page_index = 1
        self._progress_page_total = 0
        self._progress_item_count_total = 0
        self._progress_comment_count_total = 0

        processed = 0
        failed = 0
        comments = 0
        crawl_time = utc_now()
        self._start_detail_fetch_workers()
        try:
            planned_count = 0
            for item_id in item_ids:
                if self.enqueue_item_detail(item_id):
                    planned_count += 1
            if planned_count == 0:
                print(f"{label}: no item details to fetch.")
                return

            self._progress_page_total = planned_count
            print(f"{label}: fetching {planned_count} item details")
            while self._detail_pending_jobs > 0:
                results = self._dequeue_detail_results(block=True)
                if not results:
                    continue
                for result in results:
                    if self._detail_pending_jobs > 0:
                        self._detail_pending_jobs -= 1
                    status, count = self._consume_detail_result(result, crawl_time=crawl_time)
                    if status == "retry":
                        continue
                    processed += 1
                    self._progress_item_index = processed
                    if status == "failed":
                        failed += 1
                    else:
                        comments += count
                    self._progress_item_count_total = processed - failed
                    self._progress_comment_count_total = comments
        finally:
            self._shutdown_detail_fetch_workers()

        elapsed = format_duration(time.monotonic() - started)
        print(
            f"{label}: processed={processed} failed={failed} "
            f"comments={comments} elapsed={elapsed}"
        )

    def run(self, *, max_pages: int | None, start_older: int | None) -> None:
        if start_older is not None:
            self.set_state("older_cursor", str(start_older))
            self.set_state("start_id", str(start_older))
            self.db.commit()

        cursor_str = self.get_state("older_cursor")
        cursor = int(cursor_str) if cursor_str is not None else None
        start_id_str = self.get_state("start_id")
        crawl_start_id = int(start_id_str) if start_id_str is not None else cursor

        page_count = 0
        item_count = 0
        comment_count = 0
        run_started = time.monotonic()
        request_start_count = self.request_count
        self._run_started = run_started
        self._run_request_start_count = request_start_count

        page_prefetch_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="page")
        prefetched_page_future: Future[dict[str, Any]] | None = None
        prefetched_cursor: int | None = None
        prefetched_following_page_future: Future[dict[str, Any] | None] | None = None
        self._start_detail_fetch_workers()
        try:
            while True:
                if self.stop_requested:
                    print("Stop requested; finishing cleanly.")
                    break
                self._progress_page_index = page_count + 1
                self._progress_item_index = 0
                self._progress_page_total = 0

                if prefetched_page_future is not None:
                    cursor = prefetched_cursor
                    page = prefetched_page_future.result()
                    next_page_ids = self._extract_page_item_ids(page)
                    if prefetched_following_page_future is not None and next_page_ids:
                        prefetched_page_future = prefetched_following_page_future
                        prefetched_cursor = min(next_page_ids)
                    else:
                        prefetched_page_future = None
                        prefetched_cursor = None
                    prefetched_following_page_future = None
                else:
                    page = self.fetch_items(older=cursor)
                items = page.get("items")
                if not isinstance(items, list) or len(items) == 0:
                    print("No more items returned. Crawl finished.")
                    break
                page_total = len(items)
                page_index = self._progress_page_index
                self._progress_page_total = page_total
                self._progress_cursor = cursor
                self._progress_crawl_start_id = crawl_start_id

                # Pipeline the next page fetch while this page is being processed.
                can_prefetch_next = (
                    not self.stop_requested
                    and (max_pages is None or (page_count + 1) < max_pages)
                )
                next_page_ids = self._extract_page_item_ids(page)
                if can_prefetch_next and prefetched_page_future is None and next_page_ids:
                    prefetched_cursor = min(next_page_ids)
                    prefetched_page_future = page_prefetch_executor.submit(
                        self.fetch_items,
                        older=prefetched_cursor,
                    )

                can_prefetch_following = (
                    not self.stop_requested
                    and prefetched_page_future is not None
                    and prefetched_following_page_future is None
                    and (max_pages is None or (page_count + 2) < max_pages)
                )
                if can_prefetch_following:
                    prefetched_following_page_future = page_prefetch_executor.submit(
                        self._fetch_following_page,
                        prefetched_page_future,
                    )

                now = utc_now()
                page_items = 0
                page_comments = 0
                item_ids: list[int] = []

                for idx, item in enumerate(items, start=1):
                    if not isinstance(item, dict):
                        continue
                    self._progress_item_index = idx
                    current_id = to_int(item.get("id"))
                    self._progress_current_item_id = current_id
                    if current_id is not None:
                        item_ids.append(current_id)
                    try:
                        self.upsert_item(item, crawl_time=now)
                        page_items += 1
                        if current_id is not None:
                            self.enqueue_item_detail(current_id)
                        self._progress_item_count_total = item_count + page_items
                        self._progress_comment_count_total = comment_count + page_comments
                    except Exception as exc:  # noqa: BLE001
                        print(f"Failed item {item.get('id')}: {exc}")

                if item_ids:
                    if crawl_start_id is None:
                        crawl_start_id = max(item_ids)
                        self.set_state("start_id", str(crawl_start_id))
                    cursor = min(item_ids)
                    self.set_state("older_cursor", str(cursor))
                    self._progress_cursor = cursor
                    self._progress_crawl_start_id = crawl_start_id

                self.db.commit()

                while self._detail_pending_jobs > 0:
                    results = self._dequeue_detail_results(block=True)
                    if not results:
                        continue
                    for result in results:
                        if self._detail_pending_jobs > 0:
                            self._detail_pending_jobs -= 1
                        status, c_count = self._consume_detail_result(result, crawl_time=now)
                        if status != "success":
                            continue
                        page_comments += c_count
                        self._progress_comment_count_total = comment_count + page_comments

                page_count += 1
                item_count += page_items
                comment_count += page_comments
                self._progress_item_count_total = item_count
                self._progress_comment_count_total = comment_count
                print(
                    f"page={page_count} items={page_items} comments={page_comments} "
                    f"total_items={item_count} total_comments={comment_count} cursor={cursor}"
                )

                if max_pages is not None and page_count >= max_pages:
                    print(f"Reached --max-pages={max_pages}; stopping.")
                    break

                if self.stop_requested:
                    print("Stop requested; finishing cleanly.")
                    break

                if self.page_sleep_seconds > 0:
                    time.sleep(self.page_sleep_seconds)
        finally:
            page_prefetch_executor.shutdown(wait=False, cancel_futures=True)
            self._shutdown_detail_fetch_workers()


def to_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return None
    return None


def to_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def first_int(*values: Any) -> int | None:
    for value in values:
        parsed = to_int(value)
        if parsed is not None:
            return parsed
    return None


def first_str(*values: Any) -> str | None:
    for value in values:
        text = to_str(value)
        if text is not None and text != "":
            return text
    return None


def looks_like_comment(node: dict[str, Any]) -> bool:
    has_author = first_str(node.get("name"), node.get("userName"), node.get("user")) is not None
    has_body = first_str(node.get("content"), node.get("comment"), node.get("text")) is not None
    has_parent = "parent" in node
    return has_author and (has_body or has_parent)


def extract_item_payload(payload: dict[str, Any], *, item_id: int) -> dict[str, Any] | None:
    candidates: list[Any] = [payload]
    for key in ("item", "info", "upload"):
        candidates.append(payload.get(key))

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        candidate_id = to_int(candidate.get("id"))
        if candidate_id is None and candidate is not payload:
            item = dict(candidate)
            item["id"] = item_id
            return item
        if candidate_id == item_id:
            return candidate
    return None


def http_status_from_error(error: Exception) -> int | None:
    if isinstance(error, requests.HTTPError) and error.response is not None:
        return error.response.status_code
    cause = error.__cause__
    if isinstance(cause, requests.HTTPError) and cause.response is not None:
        return cause.response.status_code
    return None


def is_terminal_detail_error(error: Exception) -> bool:
    status_code = http_status_from_error(error)
    return status_code is not None and 400 <= status_code < 500 and status_code != 429


def summarize_error(error: Exception, *, limit: int = 2000) -> str:
    text = str(error)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def normalize_proxy_url(proxy: str) -> str:
    value = proxy.strip()
    if value == "":
        raise ValueError("Proxy URL must not be empty.")
    if "://" not in value:
        value = f"https://{value}"

    parsed = urlparse(value)
    if not parsed.scheme or not parsed.hostname or parsed.port is None:
        raise ValueError(
            "Proxy must be URL-like (example: https://54.38.92.94:10000 or 54.38.92.94:10000)."
        )
    if parsed.scheme.lower() not in {"http", "https"}:
        raise ValueError("Proxy scheme must be http:// or https://.")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl pr0gramm items/comments into SQLite with resume support."
    )
    parser.add_argument("--db", default="pr0gramm.sqlite3", help="SQLite database path.")
    parser.add_argument("--flags", type=int, default=31, help="Item flags mask (default: 31).")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional page limit for one run.",
    )
    parser.add_argument(
        "--start-older",
        type=int,
        default=None,
        help="Initialize or override resume cursor (older item id).",
    )
    parser.add_argument(
        "--reset-cursor",
        action="store_true",
        help="Delete stored cursor before crawling.",
    )
    parser.add_argument(
        "--retry-missing-details",
        action="store_true",
        help="Retry items already in the DB whose /items/info fetch has not completed.",
    )
    parser.add_argument(
        "--backfill-id-range",
        nargs=2,
        type=int,
        metavar=("FROM_ID", "TO_ID"),
        default=None,
        help="Fetch /items/info for every item id in the inclusive range.",
    )
    parser.add_argument(
        "--from-id",
        type=int,
        default=None,
        help="Lower item id bound for --retry-missing-details.",
    )
    parser.add_argument(
        "--to-id",
        type=int,
        default=None,
        help="Upper item id bound for --retry-missing-details.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of item details to fetch in a recovery mode.",
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
        help="Parallel threads for item-detail requests (/items/info). Default: 1.",
    )
    parser.add_argument(
        "--target-oldest-id",
        type=int,
        default=1,
        help="Estimate remaining crawl distance down to this item id (default: 1).",
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
    args = parser.parse_args()
    if args.req_threads < 1:
        parser.error("--req-threads must be >= 1")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be >= 1")
    if args.from_id is not None and args.to_id is not None and args.from_id > args.to_id:
        parser.error("--from-id must be <= --to-id")
    return args


def main() -> None:
    args = parse_args()
    crawler = Pr0Crawler(
        db_path=Path(args.db),
        base_url=args.base_url,
        flags=args.flags,
        timeout=args.timeout,
        sleep_seconds=args.sleep,
        page_sleep_seconds=args.page_sleep,
        max_retries=args.max_retries,
        req_threads=args.req_threads,
        target_oldest_id=args.target_oldest_id,
        session_file=Path(args.session_file) if args.session_file else None,
        cookie_pairs=args.cookie,
        proxy_url=args.proxy,
    )
    crawler.check_public_ip(expected_public_ip=args.expected_public_ip)

    def _handle_signal(signum: int, _frame: Any) -> None:
        signal_name = signal.Signals(signum).name
        print(f"Received {signal_name}; stopping crawler AFTER current page.")
        crawler.request_stop()

    atexit.register(crawler.close)
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        if args.reset_cursor:
            crawler.clear_state("older_cursor")
            crawler.db.commit()
        if args.retry_missing_details:
            crawler.retry_missing_details(
                from_id=args.from_id,
                to_id=args.to_id,
                limit=args.limit,
            )
        if args.backfill_id_range is not None:
            start_id, end_id = args.backfill_id_range
            crawler.backfill_id_range(
                start_id=start_id,
                end_id=end_id,
                limit=args.limit,
            )
        if not args.retry_missing_details and args.backfill_id_range is None:
            crawler.run(max_pages=args.max_pages, start_older=args.start_older)
    finally:
        crawler.close()
        atexit.unregister(crawler.close)


if __name__ == "__main__":
    main()
