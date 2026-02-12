# Reddit Platform

The Reddit platform (`PLATFORM=reddit`) is the default and includes specialized features for processing Reddit data dumps from [Arctic Shift](https://github.com/ArthurHeitmann/arctic_shift).

## Data Types

| Type | File Pattern | Description |
|------|-------------|-------------|
| `submissions` | `RS_YYYY-MM.zst` | Reddit posts/submissions |
| `comments` | `RC_YYYY-MM.zst` | Reddit comments |

## File Detection

File patterns (from `config/platforms/reddit/platform.yaml`):

| Type | Pattern | Example |
|------|---------|---------|
| Compressed | `RS_(\d{4}-\d{2})\.zst` | `RS_2024-01.zst` |
| Decompressed | `RS_(\d{4}-\d{2})` | `RS_2024-01` |
| Parsed CSV | `RS_(\d{4}-\d{2})\.csv` | `RS_2024-01.csv` |

Supports both flat directory and torrent directory structure (`submissions/RS_*.zst`, `comments/RC_*.zst`).

## Mandatory Fields

These fields are always included regardless of field_list.yaml:

| Field | Type | Description |
|-------|------|-------------|
| `dataset` | char(7) | Derived from filename (e.g., `RS_2024-01` → `2024-01`) |
| `id` | varchar(7) | Reddit's base-36 unique identifier |
| `retrieved_utc` | integer | Unix timestamp of when data was retrieved |

### Format Compatibility

The `retrieved_utc` field handles multiple Reddit dump formats:
- **Old format**: Uses `retrieved_on` field if `retrieved_utc` is missing
- **New format** (2023-11+): Uses `_meta.retrieved_2nd_on` when available (second retrieval is more reliable)

## Computed Fields

### id10 (Base-36 Conversion)

Reddit IDs are base-36 (digits 0-9 and letters a-z). The `id10` field provides the base-10 equivalent as a `bigint` for applications requiring numeric IDs or sorting.

Include `id10` in your `field_list.yaml` to enable this conversion.

### is_deleted / removal_type (Waterfall Algorithm)

The parser automatically detects deleted and removed content using a waterfall algorithm. The algorithm checks multiple data sources in priority order — the first match wins.

#### Priority Order

| Priority | Source Field | Era | Detection |
|----------|-------------|-----|-----------|
| 1 | `_meta.removal_type` | 2023-11+ | Arctic Shift ground truth — pass through as canonical value |
| 2 | `_meta.was_deleted_later` | 2023-11+ | Marks as deleted, continues checking for specific removal type |
| 3 | `removed_by_category` | 2018+ | Gold standard from Reddit API |
| 4 | `spam` (boolean) | 2020+ | Reddit API spam flag |
| 5 | `removed` (boolean) | 2020+ | Reddit API removed flag |
| 6 | `banned_by` | 2008-2018 | Legacy field — `true` = Reddit spam filter, `"AutoModerator"` = automod, other string = moderator |
| 7 | Text content | All | `[deleted]` or `[removed]` markers in body/selftext |
| 8 | `author` | All | `author == '[deleted]'` |

#### Canonical removal_type Values

| Value | Description |
|-------|-------------|
| `deleted` | User deleted their own content |
| `moderator` | Removed by subreddit moderator |
| `reddit` | Removed by Reddit admin or spam filter (includes anti_evil_ops, shadowbans) |
| `automod_filtered` | Removed by AutoModerator |
| `content_takedown` | Legal/DMCA takedown |
| `copyright_takedown` | Copyright-specific takedown |
| `community_ops` | Reddit Community Operations |
| `''` (empty) | Not removed |

#### removed_by_category Mapping

The `removed_by_category` field (Priority 3) maps to canonical values:
- `deleted`, `author` → `deleted`
- `moderator` → `moderator`
- `reddit`, `anti_evil_ops`, `admin` → `reddit`
- `automod_filtered` → `automod_filtered`
- `content_takedown` → `content_takedown`
- `copyright_takedown` → `copyright_takedown`
- `community_ops` → `community_ops`
- Unknown categories → `moderator` (conservative default)

## Field List

### Submissions (43 fields configured)

**Core:**
`created_utc`, `id10`, `score`, `upvote_ratio`, `num_comments`, `num_crossposts`, `total_awards_received`

**Subreddit:**
`subreddit` (lowercased), `subreddit_subscribers`

**Status:**
`stickied`, `gilded`, `distinguished`, `locked`, `quarantine`, `over_18`, `is_deleted`, `removal_type`

**Author:**
`author` (lowercased), `author_flair_text`, `author_created_utc`

**Content:**
`link_flair_text`, `domain`, `url`, `title`, `selftext`

### Comments (27 fields configured)

**Core:**
`created_utc`, `link_id`, `parent_id`, `score`, `controversiality`, `total_awards_received`

**Subreddit:**
`subreddit` (lowercased)

**Status:**
`stickied`, `gilded`, `distinguished`, `is_deleted`, `removal_type`

**Author:**
`author` (lowercased), `author_flair_text`, `author_created_utc`, `is_submitter`

**Content:**
`body`

Edit `config/platforms/reddit/field_list.yaml` to customize which fields are extracted.

## Field Types

Defined in `config/platforms/reddit/field_types.yaml`:

| Type | Fields |
|------|--------|
| `integer` | created_utc, author_created_utc, score, gilded, controversiality, num_comments, num_crossposts, subreddit_subscribers, total_awards_received |
| `bigint` | id10 |
| `float` | upvote_ratio, lang_prob, lang2_prob |
| `boolean` | stickied, is_submitter, is_deleted, locked, quarantine, over_18 |
| `text` | author, subreddit, body, title, selftext, distinguished, author_flair_text, link_flair_text, domain, url, removal_type |
| `varchar(10)` | link_id, parent_id |
| `varchar(2)` | lang, lang2 |

## Database Indexes

Default indexes (from `platform.yaml`):

**Submissions:** `dataset`, `author`, `subreddit`, `domain`, `created_utc`

**Comments:** `dataset`, `author`, `subreddit`, `link_id`, `created_utc`

Override via `user.yaml` in the postgres profile directory.
