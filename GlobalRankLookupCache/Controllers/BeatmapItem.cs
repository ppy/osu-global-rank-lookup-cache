using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace GlobalRankLookupCache.Controllers
{
    internal class BeatmapItem
    {
        private readonly int beatmapId;
        private readonly string highScoresTable;

        public List<int> Scores;

        public BeatmapItem(int beatmapId, string highScoresTable)
        {
            this.beatmapId = beatmapId;
            this.highScoresTable = highScoresTable;
        }

        private bool populationInProgress;

        private DateTimeOffset lastPopulation;

        private readonly TaskCompletionSource<bool> populated = new TaskCompletionSource<bool>();

        public async Task<(int position, int total)> Lookup(int score)
        {
            bool success = await waitForPopulation();

            if (!success)
            {
                // do quick lookup
                using (var db = await Program.GetDatabaseConnection())
                using (var cmd = db.CreateCommand())
                {
                    Console.WriteLine($"performing quick lookup for {beatmapId}...");

                    cmd.CommandTimeout = 10;
                    cmd.CommandText = $"select count(*) from {highScoresTable} where beatmap_id = {beatmapId} and score > {score} and hidden = 0";
                    int pos = (int)(long)(await cmd.ExecuteScalarAsync())!;

                    cmd.CommandTimeout = 10;
                    cmd.CommandText = $"select count(*) from {highScoresTable} where beatmap_id = {beatmapId} and hidden = 0";
                    int total = (int)(long)(await cmd.ExecuteScalarAsync())!;

                    Console.WriteLine($"performed quick lookup for {beatmapId} = {pos}/{total}");

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

        private async Task<bool> waitForPopulation()
        {
            if (populated.Task.IsCompleted)
                return true;

            queuePopulation();

            await Task.WhenAny(Task.Delay(1000), populated.Task);

            return populated.Task.IsCompleted;
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
                    Task.Run(repopulateScores);
                }
            }
        }

        private static readonly SemaphoreSlim population_tasks_semaphore = new SemaphoreSlim(10);

        private async Task repopulateScores()
        {
            await population_tasks_semaphore.WaitAsync();

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
                            if (users.Add(userId))
                                scores.Add(score);
                        }
                    }

                    scores.Reverse();

                    Console.WriteLine($"Populated for {beatmapId} ({scores.Count} scores).");
                }

                Scores = scores;
                populated.SetResult(true);
                lastPopulation = DateTimeOffset.Now;
            }
            catch
            {
                // will retry next lookup
            }

            population_tasks_semaphore.Release();
            populationInProgress = false;
        }
    }
}
