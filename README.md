# osu-global-rank-lookup-cache [![dev chat](https://discordapp.com/api/guilds/188630481301012481/widget.png?style=shield)](https://discord.gg/ppy)


A memory-based caching layer for beatmap-rank lookups which cannot be easily optimised as a database/SQL level.

Intended to handle queries which iterate over large sections of (already indexed) scores, where the overhead of counting rows becomes an issue.

# Query API

`/ranklookup?beatmapId={$beatmapId}&score={$score}&rulesetId={$mode}`

`$beatmapId` - The beatmap ID to lookup
`$score` - The achieved total score value
`$mode` - The ruleset ID (0..3)

# Response

An zero-index integer denoting the rank in the leaderboard for the provided score.

# TODO

- Currently this is only useful for global leaderboards. Mod filters cannot be applied, for instance.
- After an initial query, the queried beatmap will be tracked permanently. There is no cache expiry, so the potential of saturating available memory is real. LRU expiry should be implemented.
- More correctly handling top 50 lookups where score collisions are feasible (need to fallback to ID).

