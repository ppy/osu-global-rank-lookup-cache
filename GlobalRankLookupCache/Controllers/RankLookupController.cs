using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using MySqlConnector;

namespace GlobalRankLookupCache.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class RankLookupController : ControllerBase
    {
        private readonly ILogger<RankLookupController> _logger;

        public RankLookupController(ILogger<RankLookupController> logger)
        {
            _logger = logger;
        }

        private static BeatmapRankCache[] beatmapRankCache = new[]
        {
            new BeatmapRankCache("osu_scores_high"),
            new BeatmapRankCache("osu_scores_taiko_high"),
            new BeatmapRankCache("osu_scores_fruits_high"),
            new BeatmapRankCache("osu_scores_mania_high")
        };

        [HttpGet]
        public long Get(int rulesetId, int beatmapId, long score)
        {
            return beatmapRankCache[rulesetId].Lookup(beatmapId, score);
        }
    }

    internal class BeatmapRankCache
    {
        private readonly string highScoresTable;

        private readonly ConcurrentDictionary<int, List<long>> beatmapScoresLookup = new ConcurrentDictionary<int, List<long>>();

        public BeatmapRankCache(string highScoresTable)
        {
            this.highScoresTable = highScoresTable;
        }

        public long Lookup(in int beatmapId, in long score)
        {
            var scores = beatmapScoresLookup.GetOrAdd(beatmapId, getScoresForBeatmap);

            int result = scores.BinarySearch(score);

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