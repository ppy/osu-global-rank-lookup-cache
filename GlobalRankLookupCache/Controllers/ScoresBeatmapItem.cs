using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Nest;

namespace GlobalRankLookupCache.Controllers
{
    internal class ScoresBeatmapItem
    {
        private List<int> scores;

        private readonly int beatmapId;
        private readonly int rulesetId;

        private DateTimeOffset lastPopulation;
        private int requestsSinceLastPopulation;

        private bool isQualified;

        private readonly TaskCompletionSource<bool> populated = new TaskCompletionSource<bool>();

        private string whereCondition => $"`beatmap_id` = {beatmapId} AND `ruleset_id` = {rulesetId} AND `preserve` = 1 AND `ranked` = 1";

        public ScoresBeatmapItem(int beatmapId, int rulesetId)
        {
            this.beatmapId = beatmapId;
            this.rulesetId = rulesetId;
        }

        public async Task<BeatmapRankLookupResult> Lookup(int score)
        {
            Interlocked.Increment(ref requestsSinceLastPopulation);

            if (!populated.Task.IsCompleted && population_tasks_semaphore.CurrentCount > 0)
                await Task.WhenAny(Task.Delay(1000), Task.Run(repopulateScores));

            // may change due to re-population; use a local copy
            List<int> localScores = scores;

            if (!populated.Task.IsCompleted)
            {
                // use elastic as a fallback while cache is still populating
                return await getResultFromElastic(score);
            }

            // For qualified beatmaps, ensure that transitions to ranked state are handled almost immediately.
            // Generally qualified beatmaps should not have many scores, so the overhead here should not be too important.
            if (isQualified && (DateTime.Now - lastPopulation).TotalSeconds > 60)
                _ = Task.Run(repopulateScores);
            // Re-populate if enough time has passed and enough requests were made.
            else if ((DateTime.Now - lastPopulation).TotalSeconds > localScores.Count && (localScores.Count < 1000 || requestsSinceLastPopulation >= 5))
                _ = Task.Run(repopulateScores);

            Interlocked.Increment(ref RankLookupController.Hits);
            int result = localScores.BinarySearch(score + 1);
            int position = (localScores.Count - (result < 0 ? ~result : result));

            // A new top score was achieved.
            // To ensure medals and profiles are updated accurately, require a re-fetch at this point.
            if (position < 500)
                return await getResultFromElastic(score);

            return new BeatmapRankLookupResult(position, localScores.Count, true);
        }

        private async Task<BeatmapRankLookupResult> getResultFromElastic(int score)
        {
            var query = new BoolQuery
            {
                Filter =
                [
                    new TermQuery { Field = "is_legacy", Value = false },
                    new TermQuery { Field = "ruleset_id", Value = rulesetId },
                    new TermsQuery { Field = "beatmap_id", Terms = [beatmapId] },
                ],
            };
            var req = new SearchRequest
            {
                Query = query,
                Aggregations = new CardinalityAggregation("by_user", field: "user_id")
            };

            var totalScoreCountResponse = await Program.ElasticClient.SearchAsync<dynamic>(req, CancellationToken.None);
            int totalScoreCount = (int)(totalScoreCountResponse.Aggregations.Cardinality("by_user").Value ?? 0);

            query.Must =
            [
                new BoolQuery
                {
                    MinimumShouldMatch = 1,
                    Should =
                    [
                        new NumericRangeQuery { Field = "total_score", GreaterThan = score }
                    ]
                }
            ];
            var userScoreCountResponse = await Program.ElasticClient.SearchAsync<dynamic>(req, CancellationToken.None);
            int userScoreCount = (int)(userScoreCountResponse.Aggregations.Cardinality("by_user").Value ?? 0);

            return new BeatmapRankLookupResult(userScoreCount, totalScoreCount, Accurate: true);
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

            var newScores = new List<int>();

            bool isRepopulate = this.scores != null;

            try
            {
                using (var db = await Program.GetDatabaseConnection())
                using (var cmd = db.CreateCommand())
                {
                    cmd.CommandTimeout = 600;

                    cmd.CommandText = $"SELECT COUNT(*) FROM `scores` WHERE {whereCondition}";
                    int liveCount = (int)(long)(await cmd.ExecuteScalarAsync())!;

                    cmd.CommandText = $"SELECT `approved` FROM `osu_beatmaps` WHERE `beatmap_id` = {beatmapId}";
                    isQualified = (sbyte)(await cmd.ExecuteScalarAsync())! == 3;

                    // Check whether things actually changed enough to matter. If not, skip this update.
                    // Of note, if scores *reduced* we should update immediately. This may be a foul play score removed from the header of the leaderboard.
                    //
                    // Note that this might not work great if a user improves a score.
                    if (isRepopulate && liveCount >= newScores.Count && liveCount - newScores.Count < 10)
                    {
                        Console.Write("-");
                    }
                    else
                    {
                        var users = new HashSet<int>();

                        cmd.CommandText = $"SELECT `user_id`, `total_score` FROM `scores` WHERE {whereCondition}";

                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            while (reader.Read())
                            {
                                int userId = reader.GetInt32(0);
                                int score = reader.GetInt32(1);

                                // we want one score per user at most
                                if (users.Add(userId))
                                    newScores.Add(score);
                            }
                        }

                        newScores.Reverse();
                        scores = newScores;

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
