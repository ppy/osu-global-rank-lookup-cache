using System;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;
using MySqlConnector;

namespace GlobalRankLookupCache
{
    public static class Program
    {
        public static MySqlConnection GetDatabaseConnection()
        {
            string host = (Environment.GetEnvironmentVariable("DB_HOST") ?? "localhost");
            string user = (Environment.GetEnvironmentVariable("DB_USER") ?? "root");

            var connection = new MySqlConnection($"Server={host};Database=osu;User ID={user};ConnectionReset=false;Pooling=true;Max Pool Size=100;Connection Timeout=30;Connection Lifetime=300;");
            connection.Open();
            return connection;
        }

        public static void Main(string[] args)
        {
            createHostBuilder(args).Build().Run();
        }

        private static IHostBuilder createHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder => { webBuilder.UseStartup<Startup>(); });
    }
}
