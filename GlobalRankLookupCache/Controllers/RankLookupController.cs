using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace GlobalRankLookupCache.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class RankLookupController : ControllerBase
    {
        public static int Hits;
        public static int Misses;
        public static int Populations;

        private static long lastReport = DateTimeOffset.Now.ToUnixTimeSeconds();

        private static readonly BeatmapRankCacheCollection[] beatmap_rank_cache =
        {
            new BeatmapRankCacheCollection("osu_scores_high"),
            new BeatmapRankCacheCollection("osu_scores_taiko_high"),
            new BeatmapRankCacheCollection("osu_scores_fruits_high"),
            new BeatmapRankCacheCollection("osu_scores_mania_high")
        };

        [HttpGet]
        public async Task<IActionResult> Get(int rulesetId, int beatmapId, int score)
        {
            long unixSeconds = DateTimeOffset.Now.ToUnixTimeSeconds();

            if (unixSeconds - lastReport >= 10)
            {
                if (unixSeconds - Interlocked.Exchange(ref lastReport, unixSeconds) > 10)
                    output();
            }

            (int position, int total) = await beatmap_rank_cache[rulesetId].Lookup(beatmapId, score);
            return Content($"{position},{total}");
        }

        private void output()
        {
            double hits = Interlocked.Exchange(ref Hits, 0);
            double misses = Interlocked.Exchange(ref Misses, 0);
            double populations = Interlocked.Exchange(ref Populations, 0);

            double hitRate = hits / (hits + misses);

            long memory = GC.GetTotalMemory(false);

            Console.WriteLine();
            Console.WriteLine($"mem:{memory / 1048576:N0} MB beatmaps:{beatmap_rank_cache.Sum(c => c.Count)} hr:{hitRate:P0} h:{hits:N0} m:{misses:N0} p:{populations:N0}");
        }
    }
}
