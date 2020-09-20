using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace GlobalRankLookupCache.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class RankLookupController : ControllerBase
    {
        private readonly ILogger<RankLookupController> logger;

        public RankLookupController(ILogger<RankLookupController> logger)
        {
            this.logger = logger;
        }

        private static readonly BeatmapRankCache[] beatmap_rank_cache =
        {
            new BeatmapRankCache("osu_scores_high"),
            new BeatmapRankCache("osu_scores_taiko_high"),
            new BeatmapRankCache("osu_scores_fruits_high"),
            new BeatmapRankCache("osu_scores_mania_high")
        };

        [HttpGet]
        public int Get(int rulesetId, int beatmapId, int score)
        {
            return beatmap_rank_cache[rulesetId].Lookup(beatmapId, score);
        }
    }
}