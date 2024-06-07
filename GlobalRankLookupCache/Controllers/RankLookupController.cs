using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace GlobalRankLookupCache.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class RankLookupController : ControllerBase
    {
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
            (int position, int total) = await beatmap_rank_cache[rulesetId].Lookup(beatmapId, score);
            return Content($"{position},{total}");
        }
    }
}
