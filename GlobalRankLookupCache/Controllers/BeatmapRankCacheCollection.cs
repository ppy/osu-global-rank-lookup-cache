using System.Collections.Concurrent;
using System.Threading.Tasks;
using osu.Framework.Extensions;

namespace GlobalRankLookupCache.Controllers
{
    internal class BeatmapRankCacheCollection
    {
        private readonly string highScoresTable;

        private readonly ConcurrentDictionary<int, BeatmapRankCache> beatmapScoresLookup = new ConcurrentDictionary<int, BeatmapRankCache>();

        public BeatmapRankCacheCollection(string highScoresTable)
        {
            this.highScoresTable = highScoresTable;
        }

        public void Update(int beatmapId, in int oldScore, in int newScore)
        {
            if (!beatmapScoresLookup.TryGetValue(beatmapId, out var beatmapCache))
                return; // if we're not tracking this beatmap we can just ignore.

            lock (beatmapCache)
            {
                beatmapCache.Scores.Remove(oldScore);
                beatmapCache.Scores.AddInPlace(newScore);
            }
        }

        public bool Clear(in int beatmapId) => beatmapScoresLookup.TryRemove(beatmapId, out var _);

        public Task<int> Lookup(int beatmapId, in int score)
        {
            return beatmapScoresLookup.GetOrAdd(beatmapId, new BeatmapRankCache(beatmapId, highScoresTable)).Lookup(score);
        }
    }
}