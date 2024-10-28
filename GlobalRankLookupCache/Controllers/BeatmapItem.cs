using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace GlobalRankLookupCache.Controllers
{
    internal class BeatmapItem
    {
        public List<int> Scores;

        private readonly int beatmapId;
        private readonly string highScoresTable;

        private DateTimeOffset lastPopulation;
        private int requestsSinceLastPopulation;

        private readonly TaskCompletionSource<bool> populated = new TaskCompletionSource<bool>();

        public BeatmapItem(int beatmapId, string highScoresTable)
        {
            this.beatmapId = beatmapId;
            this.highScoresTable = highScoresTable;
        }

        public async Task<(int position, int total, bool accurate)> Lookup(int score)
        {
            Interlocked.Increment(ref requestsSinceLastPopulation);

            if (!populated.Task.IsCompleted && population_tasks_semaphore.CurrentCount > 0)
                await Task.WhenAny(Task.Delay(1000), Task.Run(repopulateScores));

            // may change due to re-population; use a local copy
            var scores = Scores;

            if (!populated.Task.IsCompleted)
            {
                // do quick lookup
                using (var db = await Program.GetDatabaseConnection())
                using (var cmd = db.CreateCommand())
                {
                    cmd.CommandTimeout = 10;

                    Interlocked.Increment(ref RankLookupController.Misses);

                    cmd.CommandText = $"select count(*) from {highScoresTable} where beatmap_id = {beatmapId} and hidden = 0";
                    int total = (int)(long)(await cmd.ExecuteScalarAsync())!;

                    if (total < 2000)
                    {
                        cmd.CommandText = $"select count(DISTINCT user_id) from {highScoresTable} where beatmap_id = {beatmapId} and hidden = 0";
                        total = (int)(long)(await cmd.ExecuteScalarAsync())!;

                        cmd.CommandText = $"select count(DISTINCT user_id) from {highScoresTable} where beatmap_id = {beatmapId} and score > {score} and hidden = 0";
                        int pos = (int)(long)(await cmd.ExecuteScalarAsync())!;
                        return (pos, total, true);
                    }
                    else
                    {
                        cmd.CommandText = $"select count(*) from {highScoresTable} where beatmap_id = {beatmapId} and score > {score} and hidden = 0";
                        int pos = (int)(long)(await cmd.ExecuteScalarAsync())!;

                        return (pos, total, false);
                    }
                }
            }

            // check whether last update was too long ago
            if ((DateTime.Now - lastPopulation).TotalSeconds > scores.Count)
            {
                // For seldom requested beatmaps, skip re-population.
                if (requestsSinceLastPopulation >= 5)
                {
                    _ = Task.Run(repopulateScores);
                }
                else
                {
                    Console.Write(".");
                    lastPopulation = DateTimeOffset.Now;
                    requestsSinceLastPopulation--;
                }
            }

            Interlocked.Increment(ref RankLookupController.Hits);
            int result = scores.BinarySearch(score + 1);
            return (scores.Count - (result < 0 ? ~result : result), scores.Count, true);
        }

        private readonly SemaphoreSlim populationSemaphore = new SemaphoreSlim(1);

        private static readonly SemaphoreSlim population_tasks_semaphore = new SemaphoreSlim(10);

        private async Task repopulateScores()
        {
            // Drop excess requests. If they are common they will arrive again.
            if (population_tasks_semaphore.CurrentCount == 0)
                return;

            if (!await populationSemaphore.WaitAsync(100))
                return;

            await population_tasks_semaphore.WaitAsync();

            var scores = new List<int>();

            bool isRepopulate = Scores != null;

            try
            {
                using (var db = await Program.GetDatabaseConnection())
                using (var cmd = db.CreateCommand())
                {
                    var users = new HashSet<int>();

                    cmd.CommandTimeout = 600;
                    cmd.CommandText = $"SELECT user_id, score FROM {highScoresTable} WHERE beatmap_id = {beatmapId} AND hidden = 0";

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

                    Console.Write(isRepopulate ? "R" : "P");
                }

                Scores = scores;
                populated.SetResult(true);
                lastPopulation = DateTimeOffset.Now;
                requestsSinceLastPopulation = 0;
                Interlocked.Increment(ref RankLookupController.Populations);
            }
            catch (Exception e)
            {
                Console.WriteLine();
                Console.WriteLine(e.ToString());
                // will retry next lookup
            }

            population_tasks_semaphore.Release();
            populationSemaphore.Release();
        }
    }
}
