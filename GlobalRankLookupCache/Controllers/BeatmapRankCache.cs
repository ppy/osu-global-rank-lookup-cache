using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using osu.Framework.Threading;

namespace GlobalRankLookupCache.Controllers
{
    internal class BeatmapRankCache
    {
        private readonly int beatmapId;
        private readonly string highScoresTable;

        public List<int> Scores;

        public BeatmapRankCache(int beatmapId, string highScoresTable)
        {
            this.beatmapId = beatmapId;
            this.highScoresTable = highScoresTable;
        }

        private bool populationInProgress;

        private DateTimeOffset lastPopulation;

        private readonly ManualResetEventSlim populated = new ManualResetEventSlim();

        public async Task<(int position, int total)> Lookup(int score)
        {
            if (!waitForPopulation())
            {
                // do quick lookup
                using (var db = await Program.GetDatabaseConnection())
                using (var cmd = db.CreateCommand())
                {
                    cmd.CommandTimeout = 10;
                    cmd.CommandText = $"select count(*) from {highScoresTable} where beatmap_id = {beatmapId} and score > {score} and hidden = 0";
                    int pos = (int)(long)(await cmd.ExecuteScalarAsync())!;

                    cmd.CommandTimeout = 10;
                    cmd.CommandText = $"select count(*) from {highScoresTable} where beatmap_id = {beatmapId} and hidden = 0";
                    int total = (int)(long)(await cmd.ExecuteScalarAsync())!;

                    Console.WriteLine($"quick lookup for {beatmapId} = {pos}/{total}");

                    return (pos, total);
                }
            }

            // may change due to re-population; use a local copy
            var scores = Scores;

            // check whether last update was too long ago
            if ((DateTime.Now - lastPopulation).TotalSeconds > scores.Count)
                queuePopulation();

            int result = scores.BinarySearch(score + 1);
            return (scores.Count - (result < 0 ? ~result : result), scores.Count);
        }

        private bool waitForPopulation()
        {
            if (populated.IsSet)
                return true;

            queuePopulation();

            return populated.Wait(1000);
        }

        private void queuePopulation()
        {
            if (populationInProgress)
                return;

            // ensure only one population occurs
            lock (this)
            {
                if (!populationInProgress)
                {
                    populationInProgress = true;
                    task_factory.StartNew(repopulateScores);
                }
            }
        }

        private static readonly ThreadedTaskScheduler task_scheduler = new ThreadedTaskScheduler(10, "retrieval");

        private static readonly TaskFactory task_factory = new TaskFactory(task_scheduler);

        private async Task repopulateScores()
        {
            var scores = new List<int>();

            try
            {
                using (var db = await Program.GetDatabaseConnection())
                using (var cmd = db.CreateCommand())
                {
                    var users = new HashSet<int>();

                    cmd.CommandTimeout = 120;
                    cmd.CommandText = $"SELECT user_id, score FROM {highScoresTable} WHERE beatmap_id = {beatmapId} AND hidden = 0";

                    Console.WriteLine(Scores != null
                        ? $"Repopulating for {beatmapId} after {(int)(DateTimeOffset.Now - lastPopulation).TotalMinutes} minutes..."
                        : $"Populating for {beatmapId}...");

                    using (var reader = await cmd.ExecuteReaderAsync())
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

                Scores = scores;
                populated.Set();
                lastPopulation = DateTimeOffset.Now;
            }
            catch
            {
                // will retry next lookup
            }

            populationInProgress = false;
        }
    }
}
