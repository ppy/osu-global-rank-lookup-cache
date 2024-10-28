using System.Collections.Concurrent;
using System.Threading.Tasks;
using osu.Framework.Extensions;

namespace GlobalRankLookupCache.Controllers
{
    internal class BeatmapRankCacheCollection
    {
        private readonly string highScoresTable;

        private readonly ConcurrentDictionary<int, BeatmapItem> beatmapScoresLookup = new ConcurrentDictionary<int, BeatmapItem>();

        public long Count => beatmapScoresLookup.Count;

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

        public Task<(int position, int total)> Lookup(int beatmapId, in int score)
        {
            return beatmapScoresLookup.GetOrAdd(beatmapId, new BeatmapItem(beatmapId, highScoresTable)).Lookup(score);
        }
    }
}
