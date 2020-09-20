using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using osu.Framework.Extensions;

namespace GlobalRankLookupCache.Controllers
{
    internal class BeatmapRankCache
    {
        private readonly string highScoresTable;

        private readonly ConcurrentDictionary<int, Lazy<List<int>>> beatmapScoresLookup = new ConcurrentDictionary<int, Lazy<List<int>>>();

        public BeatmapRankCache(string highScoresTable)
        {
            this.highScoresTable = highScoresTable;
        }

        public void Update(int beatmapId, in int oldScore, in int newScore)
        {
            if (!beatmapScoresLookup.TryGetValue(beatmapId, out var scores))
                return; // if we're not tracking this beatmap we can just ignore.

            lock (scores.Value)
            {
                scores.Value.Remove(oldScore);
                scores.Value.AddInPlace(newScore);
            }
        }

        public int Lookup(int beatmapId, in int score)
        {
            var scores = beatmapScoresLookup.GetOrAdd(beatmapId,
                new Lazy<List<int>>(() => getScoresForBeatmap(beatmapId),
                    LazyThreadSafetyMode.ExecutionAndPublication));

            try
            {
                int result = scores.Value.BinarySearch(score + 1);

                lock (scores.Value)
                {
                    return scores.Value.Count - (result < 0 ? ~result : result);
                }
            }
            catch
            {
                // lazy will cache any exception, but we don't want that.
                beatmapScoresLookup.TryRemove(beatmapId, out var _);
                throw;
            }
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