using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;

namespace GlobalRankLookupCache.Controllers
{
    internal class ScoresBeatmapRankCache
    {
        private readonly MemoryCache beatmapScoresLookup = new MemoryCache(new MemoryCacheOptions
        {
            SizeLimit = 768000,
        });

        public long Count => beatmapScoresLookup.Count;

        public Task<BeatmapRankLookupResult> Lookup(int beatmapId, int rulesetId, int score)
        {
            return beatmapScoresLookup.GetOrCreate(beatmapId, e =>
            {
                var item = new ScoresBeatmapItem(beatmapId, rulesetId);

                e.SetSlidingExpiration(TimeSpan.FromDays(1));
                e.Size = 1;
                e.Value = item;

                return item;
            }).Lookup(score);
        }
    }
}
