using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace GlobalRankLookupCache.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class RankLookupController : ControllerBase
    {
        private readonly ILogger<RankLookupController> _logger;

        public RankLookupController(ILogger<RankLookupController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public int Get()
        {
            return 1234;
        }
    }
}