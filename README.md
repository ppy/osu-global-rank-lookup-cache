# osu-global-rank-lookup-cache [![dev chat](https://discordapp.com/api/guilds/188630481301012481/widget.png?style=shield)](https://discord.gg/ppy)


A memory-based caching layer for beatmap-rank lookups which cannot be easily optimised as a database/SQL level.

Intended to handle queries which iterate over large sections of (already indexed) scores, where the overhead of counting rows becomes an issue.

## Environment variables

- `DB_HOST`: Database host to use when performing lookups (default: `localhost`)
- `DB_USER`: Database user to use when performing lookups (default: `root`)
- `USE_NEW_TABLES`:
  - When `false`, scores are looked up from `osu_scores_*_high` tables, using the ID scheme used by those tables.
  - When `true`, scores are looked up from the unified `scores` table, using the ID scheme used by this table.
  - Mandatory, with no default.

## Query API

### `GET /ranklookup?beatmapId={$beatmapId}&score={$score}&rulesetId={$mode}`

#### Parameters

- `$beatmapId` - The beatmap ID to lookup
- `$score` - The achieved total score value
- `$mode` - The ruleset ID (0..3)

#### Response

Three comma-delimited values:

- An zero-index integer denoting the rank in the leaderboard for the provided score
- The count of all scores on the beatmap's leaderboard
- A boolean indicating whether both preceding counts are accurate or best-effort estimates

## TODO

- Currently this is only useful for global leaderboards. Mod filters cannot be applied, for instance.
- More correctly handling top 50 lookups where score collisions are feasible (need to fallback to ID).

