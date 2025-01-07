using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;

namespace GlobalRankLookupCache.Controllers
{
    internal class BeatmapItem
    {
        public List<int> Scores;

        private readonly int beatmapId;
        private readonly string highScoresTable;

        private DateTimeOffset lastPopulation;
        private int requestsSinceLastPopulation;

        private bool isQualified;

        private TaskCompletionSource<bool> populated = new TaskCompletionSource<bool>();

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
            List<int> scores = Scores;

            if (!populated.Task.IsCompleted)
            {
                // do quick lookup
                using (var db = await Program.GetDatabaseConnection())
                using (var cmd = db.CreateCommand())
                {
                    Interlocked.Increment(ref RankLookupController.Misses);

                    cmd.CommandTimeout = 10;
                    cmd.CommandText = $"select count(*) from {highScoresTable} where beatmap_id = {beatmapId} and hidden = 0";

                    int roughTotal = (int)(long)(await cmd.ExecuteScalarAsync())!;

                    if (roughTotal < 2000)
                    {
                        cmd.CommandText = $"select count(DISTINCT user_id) from {highScoresTable} where beatmap_id = {beatmapId} and hidden = 0";
                        int accurateTotal = (int)(long)(await cmd.ExecuteScalarAsync())!;

                        int accuratePosition = await getAccuratePosition(db, score);
                        return (accuratePosition, accurateTotal, true);
                    }
                    else
                    {
                        cmd.CommandText = $"select count(*) from {highScoresTable} where beatmap_id = {beatmapId} and score > {score} and hidden = 0";
                        int roughPosition = (int)(long)(await cmd.ExecuteScalarAsync())!;

                        return (roughPosition, roughTotal, false);
                    }
                }
            }

            // For qualified beatmaps, ensure that transitions to ranked state are handled almost immediately.
            // Generally qualified beatmaps should not have many scores, so the overhead here should not be too important.
            if (isQualified && (DateTime.Now - lastPopulation).TotalSeconds > 60)
                _ = Task.Run(repopulateScores);
            // Re-populate if enough time has passed and enough requests were made.
            else if ((DateTime.Now - lastPopulation).TotalSeconds > Scores.Count && (Scores.Count < 1000 || requestsSinceLastPopulation >= 5))
                _ = Task.Run(repopulateScores);

            Interlocked.Increment(ref RankLookupController.Hits);
            int result = scores.BinarySearch(score + 1);
            int position = (scores.Count - (result < 0 ? ~result : result));

            // A new top score was achieved.
            // To ensure medals and profiles are updated accurately, require a re-fetch at this point.
            if (position < 500)
            {
                using (var db = await Program.GetDatabaseConnection())
                    position = await getAccuratePosition(db, score);
            }

            return (position, scores.Count, true);
        }

        private async Task<int> getAccuratePosition(MySqlConnection db, int score)
        {
            using (var cmd = db.CreateCommand())
            {
                cmd.CommandTimeout = 10;
                cmd.CommandText = $"select count(DISTINCT user_id) from {highScoresTable} where beatmap_id = {beatmapId} and score > {score} and hidden = 0";
                return (int)(long)(await cmd.ExecuteScalarAsync())!;
            }
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
                    cmd.CommandTimeout = 600;

                    cmd.CommandText = $"select count(*) from {highScoresTable} where beatmap_id = {beatmapId} and hidden = 0";
                    int liveCount = (int)(long)(await cmd.ExecuteScalarAsync())!;

                    cmd.CommandText = $"select approved from osu_beatmaps where beatmap_id = {beatmapId}";
                    isQualified = (sbyte)(await cmd.ExecuteScalarAsync())! == 3;

                    // Check whether things actually changed enough to matter. If not, skip this update.
                    // Of note, if scores *reduced* we should update immediately. This may be a foul play score removed from the header of the leaderboard.
                    //
                    // Note that this might not work great if a user improves a score.
                    if (isRepopulate && liveCount >= scores.Count && liveCount - scores.Count < 10)
                    {
                        Console.Write("-");
                    }
                    else
                    {
                        var users = new HashSet<int>();

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
                        Scores = scores;

                        Console.Write(isRepopulate ? "R" : "P");
                    }
                }

                lastPopulation = DateTimeOffset.Now;
                requestsSinceLastPopulation = 0;
                Interlocked.Increment(ref RankLookupController.Populations);

                if (!populated.Task.IsCompleted)
                    populated.SetResult(true);
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
