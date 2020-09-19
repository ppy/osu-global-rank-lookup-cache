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

        private static readonly BeatmapRankCache[] beatmap_rank_cache =
        {
            new BeatmapRankCache("osu_scores_high"),
            new BeatmapRankCache("osu_scores_taiko_high"),
            new BeatmapRankCache("osu_scores_fruits_high"),
            new BeatmapRankCache("osu_scores_mania_high")
        };

        [HttpGet]
        public int Get(int rulesetId, int beatmapId, int score)
        {
            return beatmap_rank_cache[rulesetId].Lookup(beatmapId, score);
        }
    }

    internal class BeatmapRankCache
    {
        private readonly string highScoresTable;

        private readonly ConcurrentDictionary<int, Lazy<List<int>>> beatmapScoresLookup = new ConcurrentDictionary<int, Lazy<List<int>>>();

        public BeatmapRankCache(string highScoresTable)
        {
            this.highScoresTable = highScoresTable;
        }

        public int Lookup(int beatmapId, in int score, in int? userId = null)
        {
            var scores = beatmapScoresLookup.GetOrAdd(beatmapId,
                new Lazy<List<int>>(() => getScoresForBeatmap(beatmapId),
                    LazyThreadSafetyMode.ExecutionAndPublication));

            int result = scores.Value.BinarySearch(score + 1);

            return scores.Value.Count - (result < 0 ? ~result : result);
        }

        private List<int> getScoresForBeatmap(int beatmapId)
        {
            var scores = new List<int>();

            using (var db = Program.GetDatabaseConnection())
            using (var cmd = db.CreateCommand())
            {
                var users = new HashSet<int>();

                cmd.CommandText = $"SELECT user_id, score FROM {highScoresTable} WHERE beatmap_id = {beatmapId} AND hidden = 0";

                Console.WriteLine($"Populating for {beatmapId}...");

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

                scores.Reverse();

                Console.WriteLine($"Populated for {beatmapId} ({scores.Count} scores).");
            }

            return scores;
        }
    }
}