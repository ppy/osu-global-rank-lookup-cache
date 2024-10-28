using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using osu.Framework.Extensions;

namespace GlobalRankLookupCache.Controllers
{
    internal class BeatmapRankCacheCollection
    {
        public static int Hits;
        public static int Misses;
        public static int Populations;

        private readonly string highScoresTable;

        private readonly ConcurrentDictionary<int, BeatmapItem> beatmapScoresLookup = new ConcurrentDictionary<int, BeatmapItem>();

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

        private long lastReport = DateTimeOffset.Now.ToUnixTimeSeconds();

        public Task<(int position, int total)> Lookup(int beatmapId, in int score)
        {
            long unixSeconds = DateTimeOffset.Now.ToUnixTimeSeconds();

            if (unixSeconds - lastReport >= 10)
            {
                if (unixSeconds - Interlocked.Exchange(ref lastReport, unixSeconds) > 10)
                    output();
            }

            return beatmapScoresLookup.GetOrAdd(beatmapId, new BeatmapItem(beatmapId, highScoresTable)).Lookup(score);
        }

        private void output()
        {
            double hits = Interlocked.Exchange(ref Hits, 0);
            double misses = Interlocked.Exchange(ref Misses, 0);
            double populations = Interlocked.Exchange(ref Populations, 0);

            double hitRate = hits / (hits + misses);

            long memory = GC.GetTotalMemory(false);

            Console.WriteLine($"mem: {memory / 1048576:N0} MB beatmaps: {beatmapScoresLookup.Count} hr:{hitRate:P0} h:{hits:N0} m:{misses:N0} p:{populations:N0}");
        }
    }
}
