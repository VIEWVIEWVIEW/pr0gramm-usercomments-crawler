# pr0gramm-usercomments

This crawler creates a weekly SQLite export of pr0gramm.com item metadata and comments. Comments are only crawled on items older than 2 weeks, so the item gets actual comments by the users and to keep pressure on the pr0gramm.com api low (I only crawl on the night from Sunday to Monay and respect ``Retry-After`` header).

The dataset is available here: https://huggingface.co/datasets/VIEWVIEWVIEW/pr0gramm-usercomments/tree/main

## Tables

The database contains:

- `comments`
- `items`

### comments schema

- `comment_id` INTEGER PRIMARY KEY
- `item_id` INTEGER NOT NULL
- `parent_comment_id` INTEGER
- `comment_time` INTEGER
- `score` INTEGER
- `upvotes` INTEGER
- `downvotes` INTEGER
- `user_name` TEXT
- `user_id` INTEGER
- `user_profile_url` TEXT
- `body` TEXT
- `raw_json` TEXT NOT NULL

### items schema

- `item_id` INTEGER PRIMARY KEY
- `promoted` INTEGER
- `flags` INTEGER
- `created_at` INTEGER
- `user_name` TEXT
- `source_feed` TEXT NOT NULL
- `raw_json` TEXT NOT NULL

## Example rows

Example row from `comments`:

```sql
comment_id: 83842203
item_id: 480055
parent_comment_id: 0
comment_time: 1776609541
score: 1
upvotes: 1
downvotes: 0
user_name: "1111111101"
user_id: NULL
user_profile_url: "https://pr0gramm.com/user/1111111101"
body: "Damals war OC noch gut"
raw_json (truncated): {"id":83842203,"parent":0,"content":"Damals war OC noch gut","created":1776609541,"up":1,"down":0,"confidence":0.206543,"name":"1111111101","mark":10}
```

Example row from `items`:

```sql
item_id: 6986879
promoted: 0
flags: 1
created_at: 1776453456
user_name: "Ometen"
source_feed: "new"
raw_json (truncated): {"id":6986879,"promoted":0,"userId":132720,"up":1,"down":0,"created":1776453456,"image":"2026/04/17/8804ec2d7b3e5297-h264-ultra_hd.mp4","thumb":"2026/04/17/8804ec2d7b3e5297.jpg",...}
```

## Notes

- Timestamps are stored as Unix seconds.
- `raw_json` contains API payload snapshots and may include fields not normalized into separate columns. These are scrubbed for the release on huggingface.co btw.

## Weekly Incremental Delta Crawl

Use `crawler_incremental.py` to crawl only *new* item IDs into a fresh delta SQLite DB.

Behavior:

- Reads `last_known_id` from a JSON state file.
- Crawls only IDs `> last_known_id`.
- Applies a maturity delay (default `14` days): only items older than that are included.
- Writes one new delta DB per run with week + range in filename:
  - `pr0_delta_YYYY-Www_ids_<upper>_<lower>.sqlite3`
- After crawling, the delta DB schema is normalized to match the vacuumed schema exactly:
  - tables: `items`, `comments`
  - no crawler-internal tables (e.g. `crawl_state`, `item_detail_failures`)
  - no crawler-only columns (e.g. `crawl_time`, `detail_crawl_time`)
- Advances `last_known_id` only if no unresolved non-terminal failures remain.

### 1) Create state file once

```json
{
  "last_known_id": 6986879
}
```

### 2) Run weekly crawl

```bash
.venv/Scripts/python.exe crawler_incremental.py \
  --state-file state_incremental.json \
  --out-dir delta \
  --session-file SESSION \
  --maturity-days 14 \
  --req-threads 4
```

Useful exit codes:

- `0`: success (or no mature new items found)
- `2`: crawl finished, but unresolved non-terminal failures exist, so state was not advanced

### GitHub Action (weekly)

Workflow file:

- `.github/workflows/weekly-incremental-crawl.yml`

It runs every Monday at `02:15 UTC` and also supports manual trigger via `workflow_dispatch`.

Required repository secret:

- `PR0_COOKIE_ME` = value of your `me` cookie for pr0gramm
- `HF_TOKEN` = Hugging Face token with write access to `VIEWVIEWVIEW/pr0gramm-usercomments`

The workflow:

- runs `crawler_incremental.py` with `--maturity-days 14`
- uploads the newly created delta SQLite file to:
  - `https://huggingface.co/datasets/VIEWVIEWVIEW/pr0gramm-usercomments` (path: repo root, filename only)
- updates `state_incremental.json`
- amends the latest GitHub commit with `state_incremental.json` only (`git commit --amend --no-edit`)
- force-pushes with lease (`git push --force-with-lease`)

Important:

- `delta/` files are **not committed** to GitHub by the action.
