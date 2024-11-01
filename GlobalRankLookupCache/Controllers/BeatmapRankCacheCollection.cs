using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;

namespace GlobalRankLookupCache.Controllers
{
    internal class BeatmapRankCacheCollection
    {
        private readonly string highScoresTable;

        private readonly MemoryCache beatmapScoresLookup = new MemoryCache(new MemoryCacheOptions
        {
            SizeLimit = 192000,
        });

        public long Count => beatmapScoresLookup.Count;

        public BeatmapRankCacheCollection(string highScoresTable)
        {
            this.highScoresTable = highScoresTable;
        }

        // public void Update(int beatmapId, in int oldScore, in int newScore)
        // {
        //     if (!beatmapScoresLookup.TryGetValue(beatmapId, out var beatmapCache))
        //         return; // if we're not tracking this beatmap we can just ignore.
        //
        //     lock (beatmapCache)
        //     {
        //         beatmapCache.Scores.Remove(oldScore);
        //         beatmapCache.Scores.AddInPlace(newScore);
        //     }
        // }
        //
        // public bool Clear(in int beatmapId) => beatmapScoresLookup.TryRemove(beatmapId, out var _);

        public Task<(int position, int total, bool accurate)> Lookup(int beatmapId, in int score)
        {
            return beatmapScoresLookup.GetOrCreate(beatmapId, e =>
            {
                var item = new BeatmapItem(beatmapId, highScoresTable);

                e.SetSlidingExpiration(TimeSpan.FromDays(1));
                e.Size = 1;
                e.Value = item;

                return item;
            }).Lookup(score);
        }
    }
}
