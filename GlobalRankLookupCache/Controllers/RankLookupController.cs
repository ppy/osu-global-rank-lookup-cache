using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace GlobalRankLookupCache.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class RankLookupController : ControllerBase
    {
        private readonly ILogger<RankLookupController> logger;

        public RankLookupController(ILogger<RankLookupController> logger)
        {
            this.logger = logger;
        }

        private static readonly BeatmapRankCache[] beatmap_rank_cache = {
            new BeatmapRankCache("osu_scores_high"),
            new BeatmapRankCache("osu_scores_taiko_high"),
            new BeatmapRankCache("osu_scores_fruits_high"),
            new BeatmapRankCache("osu_scores_mania_high")
        };

        [HttpGet]
        public long Get(int rulesetId, int beatmapId, long score)
        {
            return beatmap_rank_cache[rulesetId].Lookup(beatmapId, score);
        }
    }

    internal class BeatmapRankCache
    {
        private readonly string highScoresTable;

        private readonly ConcurrentDictionary<int, Lazy<List<long>>> beatmapScoresLookup = new ConcurrentDictionary<int, Lazy<List<long>>>();

        public BeatmapRankCache(string highScoresTable)
        {
            this.highScoresTable = highScoresTable;
        }

        public long Lookup(int beatmapId, in long score)
        {
            var scores = beatmapScoresLookup.GetOrAdd(beatmapId,
                new Lazy<List<long>>(() => getScoresForBeatmap(beatmapId),
                    LazyThreadSafetyMode.ExecutionAndPublication));

            int result = scores.Value.BinarySearch(score);

            return result < 0 ? ~result : result;
        }

        private List<long> getScoresForBeatmap(int beatmapId)
        {
            var scores = new List<long>();

            using (var db = Program.GetDatabaseConnection())
            using (var cmd = db.CreateCommand())
            {
                var users = new HashSet<int>();

                cmd.CommandText = $"SELECT user_id, score FROM {highScoresTable} WHERE beatmap_id = {beatmapId} AND hidden = 0";

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        int userId = reader.GetInt32(0);
                        int score = reader.GetInt32(1);

                        // we want one score per user at most
                        if (users.Contains(userId))
                            continue;

                        users.Add(userId);
                        scores.Add(score);
                    }
                }
            }

            return scores;
        }
    }
}