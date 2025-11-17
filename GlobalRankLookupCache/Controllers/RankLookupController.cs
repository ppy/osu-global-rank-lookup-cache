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

        private static readonly LegacyBeatmapRankCache[] legacy_rank_caches =
        {
            new LegacyBeatmapRankCache("osu_scores_high"),
            new LegacyBeatmapRankCache("osu_scores_taiko_high"),
            new LegacyBeatmapRankCache("osu_scores_fruits_high"),
            new LegacyBeatmapRankCache("osu_scores_mania_high")
        };

        [HttpGet]
        public async Task<IActionResult> Get(int rulesetId, int beatmapId, int score, bool useNewTables)
        {
            long unixSeconds = DateTimeOffset.Now.ToUnixTimeSeconds();

            if (unixSeconds - lastReport >= 10)
            {
                if (unixSeconds - Interlocked.Exchange(ref lastReport, unixSeconds) >= 10)
                    output();
            }

            var result = await legacy_rank_caches[rulesetId].Lookup(beatmapId, score);
            return Content($"{result.Position},{result.Total},{result.Accurate}");
        }

        private void output()
        {
            double hits = Interlocked.Exchange(ref Hits, 0);
            double misses = Interlocked.Exchange(ref Misses, 0);
            double populations = Interlocked.Exchange(ref Populations, 0);

            double hitRate = hits / (hits + misses);

            long memory = GC.GetTotalMemory(false);

            Console.WriteLine();
            Console.WriteLine($"mem:{memory / 1048576:N0} MB beatmaps:{legacy_rank_caches.Sum(c => c.Count)} hr:{hitRate:P0} h:{hits:N0} m:{misses:N0} p:{populations:N0}");
        }
    }
}
