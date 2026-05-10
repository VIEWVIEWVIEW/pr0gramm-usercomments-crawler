# pr0gramm Endpoint Notes for a Comment Crawler

This document summarizes the live, JSON-based API routes that are most relevant for crawling comments on pr0gramm and storing metadata for later stylometry analysis.

The site exposes a live API under:

- `https://pr0gramm.com/api`

The API is explicitly described by the maintainers as unstable and not versioned, so code that uses it should be defensive about response-shape changes.

Sources:

- [Live OpenAPI spec](https://pr0gramm.com/api/spec/latest)
- [Official API docs repository](https://github.com/pr0gramm-com/api-docs)
- [Live `/new` feed](https://pr0gramm.com/new)

## What To Crawl

For broad coverage across the site, use the item feed with the combined flag mask:

- `flags=31`

That includes:

- `1` = `SWF`
- `2` = `NSFW`
- `4` = `NSFL`
- `8` = `NSFP`
- `16` = `POL`

If you want to follow the site’s “new” stream, the best discovery path is:

1. Crawl `/new`
2. Pull item IDs from the feed
3. Fetch the comment tree for each item ID

That is preferable to scraping the infinite scroll UI directly, because the API gives you stable item IDs to checkpoint against.

## Core API Endpoints

### `GET /api/items/get`

List uploads for a feed or filtered search.

Important query parameters:

- `flags` required
- `id` optional
- `tags` optional
- `user` optional
- `promoted` optional
- `collection` optional
- `following` optional
- `newer` optional
- `older` optional
- `self` optional
- `show_junk` optional

Recommended use for your crawler:

- `GET /api/items/get?flags=31`

Notes:

- This is the feed/discovery endpoint.
- Use it to enumerate uploads before fetching comment chains.

Response schema:

- `ItemsGetResponse`

### `GET /api/items/info`

Fetch the detailed payload for a single upload.

Required query parameter:

- `itemId`

Recommended use:

- `GET /api/items/info?itemId=<upload_id>`

Notes:

- This is the best endpoint for getting the post detail payload that includes the comment tree under a given upload.
- If the goal is “all comments on the site,” this is the endpoint to call after you have an item ID.

Response schema:

- `ItemsInfoResponse`

### `GET /api/profile/info`

Fetch a user profile.

Required query parameters:

- `name`
- `flags`

Recommended use:

- `GET /api/profile/info?name=<username>&flags=31`

Notes:

- Use this to enrich comment authors with profile data if the comment payload does not include enough metadata.
- The exact profile URL format on the site is user-facing and may be embedded in the response or constructible from the username, but the crawler should store the canonical username and resolve the link consistently in code.

Response schema:

- `ProfileInfoResponse`

### `GET /api/comments/preview`

Lightweight preview data for a comment.

Required query parameter:

- `identifier`

Notes:

- This is not the full comment source.
- Useful for hover-style previews, not for full archival crawling.

Response schema:

- `CommentsPreviewResponse`

## Suggested Crawl Strategy

The simplest robust crawler is:

1. Start from `GET /api/items/get?flags=31`
2. Capture item IDs
3. For each item ID, call `GET /api/items/info?itemId=...`
4. Walk the full comment tree in the response
5. Persist the raw comment record plus your own crawl timestamp
6. Repeat with `newer` / `older` paging so you do not miss new items

If the `/new` front page is your discovery source, keep a checkpoint of the newest item ID you have fully processed. That gives you a cheap resume point after interruptions.

## Comment Fields To Store

For each comment, store at least:

- `comment_id`
- `item_id`
- `parent_comment_id`
- `time` or `created_at`
- `score`
- `crawl_time`
- `user_name`
- `user_profile_url`
- `body`

Recommended extras:

- `depth` in the thread
- `reply_count` if available
- `raw_response_json` for debugging and re-parsing later
- `item_flags` so you know which section the comment came from

## Normalized Data Model

A practical storage model looks like this:

### `items`

- `item_id`
- `flags`
- `created_at`
- `author_name`
- `title` if available

### `comments`

- `comment_id`
- `item_id`
- `parent_comment_id`
- `user_name`
- `user_profile_url`
- `comment_time`
- `comment_score`
- `crawl_time`
- `body`
- `depth`

### `comment_snapshots`

Store the raw API response along with:

- `item_id`
- `fetched_at`
- `source_endpoint`
- `source_params`

That makes the crawler easier to debug when the API response shape changes.

## Python Crawler Outline

Below is a high-level outline, not production code:

```python
import time
import requests

BASE = "https://pr0gramm.com/api"
FLAGS = 31

session = requests.Session()

def get_items(page_id=None):
    params = {"flags": FLAGS}
    if page_id is not None:
        params["older"] = page_id
    return session.get(f"{BASE}/items/get", params=params, timeout=30).json()

def get_item_info(item_id):
    return session.get(f"{BASE}/items/info", params={"itemId": item_id}, timeout=30).json()

def get_profile(name):
    return session.get(
        f"{BASE}/profile/info",
        params={"name": name, "flags": FLAGS},
        timeout=30,
    ).json()

def crawl():
    checkpoint = None
    while True:
        feed = get_items(checkpoint)
        items = feed.get("items", [])
        if not items:
            break

        for item in items:
            item_id = item["id"]
            detail = get_item_info(item_id)
            # extract comments from detail here
            # store each comment with crawl timestamp and parent linkage

        checkpoint = items[-1]["id"]
        time.sleep(0.5)
```

Implementation notes:

- Use retries with exponential backoff.
- Save raw JSON before parsing so you can recover if the schema changes.
- Deduplicate by `comment_id`.
- If the API returns paged comment subsets, continue until no more children are returned.

## Practical Caveats

- The API is unstable and may change without a version bump.
- The site’s public UI is JS-driven, so the API is preferable for crawling.
- If you want complete coverage, do not rely only on visible UI infinite scroll. Use item IDs and direct item-detail requests.
- Treat `flags=31` as the broad site-wide crawl mode, but still record the individual item flags returned for each upload.

## Recommended Next Step

If you want, the next useful document would be a concrete `crawler.md` or `crawler.py` scaffold that:

- pages through `/api/items/get`
- fetches `/api/items/info` for each upload
- normalizes comment trees
- writes CSV or SQLite

## Implemented Crawler

This workspace now includes a resumable SQLite crawler:

- `crawler.py`

It stores:

- items in `items`
- comments in `comments`
- resume cursor in `crawl_state` with key `older_cursor`

Usage examples:

```bash
.venv/bin/python crawler.py --db pr0gramm.sqlite3 --flags 31
```

Resume behavior:

- By default, it resumes from stored `older_cursor`.
- To reset and start from the current newest feed page:

```bash
.venv/bin/python crawler.py --db pr0gramm.sqlite3 --reset-cursor
```

Run a bounded chunk:

```bash
.venv/bin/python crawler.py --db pr0gramm.sqlite3 --max-pages 25
```

Override cursor manually:

```bash
.venv/bin/python crawler.py --db pr0gramm.sqlite3 --start-older 12345678
```

If the API responds with `403`, pass authenticated cookies:

```bash
.venv/bin/python crawler.py --db pr0gramm.sqlite3 --cookie me=<session_cookie>
```

Preferred approach: persist a session from `main.py` (or notebook) and reuse it:

```bash
.venv/bin/python main.py --session-file SESSION
.venv/bin/python crawler.py --db pr0gramm.sqlite3 --session-file SESSION
```

Notebook/programmatic usage:

```python
from main import Pr0grammLoginClient

client = Pr0grammLoginClient()
# ... run captcha + login flow ...
client.save_session("SESSION")
```

Then:

```bash
.venv/bin/python crawler.py --db pr0gramm.sqlite3 --session-file SESSION
```

Environment note:

- SQLite locking can fail when using Windows Python against a `\\wsl.localhost\...` path.
- In that case, either run the crawler with the Linux venv interpreter in Linux context, or place the `.sqlite3` file on a local Windows path.
