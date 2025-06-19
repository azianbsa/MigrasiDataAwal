using Dapper;
using DotNetEnv;
using Microsoft.Data.Sqlite;
using Migrasi.Commands;
using Migrasi.Helpers;
using Spectre.Console.Cli;
using SQLitePCL;
using System.Reflection;
using Environment = Migrasi.Enums.Environment;

namespace Migrasi
{
    internal partial class Program
    {
        public static async Task<int> Main(string[] args)
        {
            Env.Load(".env");
            AppSettings.Environment = (Environment)Enum.Parse(typeof(Environment), Env.GetString("ENVIRONMENT", "Development"));

            Console.WriteLine($"{Assembly.GetExecutingAssembly().GetName().Name} {Assembly.GetExecutingAssembly().GetName().Version} {AppSettings.Environment}");
            Console.WriteLine();

            await LoadEnv();
            await PrepareColumnMappingConfigurations();

            try
            {
                var app = new CommandApp();
                app.Configure(config =>
                {
#if DEBUG
                    config.PropagateExceptions();
#endif
                    config.AddCommand<NewCommand>("new")
                        .WithDescription("Setup pdam baru");
                    config.AddCommand<BacameterCommand>("bacameter")
                        .WithDescription("Migrasi data bacameter");
                    config.AddCommand<BasicCommand>("basic")
                        .WithDescription("Migrasi data basic");
                    config.AddCommand<GenerateExampleQueryCommand>("example")
                        .WithDescription("Generate contoh query");
                });

                return app.Run(args);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return -99;
            }
        }

        private static async Task LoadEnv()
        {
            await Utils.TrackProgress("Load env", async () =>
            {
                var dbSuffix = AppSettings.Environment switch
                {
                    Environment.Development => "_DEV",
                    Environment.Staging => "_STG",
                    Environment.Production => "_PRD",
                    _ => "_DEV"
                };

                AppSettings.ConfigHost = Env.GetString($"DB_HOST_CONF{dbSuffix}");
                AppSettings.ConfigPort = (uint)Env.GetInt($"DB_PORT_CONF{dbSuffix}");
                AppSettings.ConfigUserId = Env.GetString($"DB_USER_CONF{dbSuffix}");
                AppSettings.ConfigPassword = Env.GetString($"DB_PASSWORD_CONF{dbSuffix}");
                AppSettings.ConfigDatabase = Env.GetString($"DB_NAME_CONF{dbSuffix}");

                AppSettings.MainHost = Env.GetString($"DB_HOST{dbSuffix}");
                AppSettings.MainPort = (uint)Env.GetInt($"DB_PORT{dbSuffix}");
                AppSettings.MainUserId = Env.GetString($"DB_USER{dbSuffix}");
                AppSettings.MainPassword = Env.GetString($"DB_PASSWORD{dbSuffix}");
                AppSettings.MainDatabase = Env.GetString($"DB_NAME{dbSuffix}");

                AppSettings.StagingHost = Env.GetString($"DB_HOST_STG");
                AppSettings.StagingPort = (uint)Env.GetInt($"DB_PORT_STG");
                AppSettings.StagingUserId = Env.GetString($"DB_USER_STG");
                AppSettings.StagingPassword = Env.GetString($"DB_PASSWORD_STG");
                AppSettings.StagingDatabase = Env.GetString($"DB_NAME_STG");

                AppSettings.BsbsHost = Env.GetString($"DB_HOST_BILLING");
                AppSettings.BsbsPort = (uint)Env.GetInt($"DB_PORT_BILLING");
                AppSettings.BsbsUserId = Env.GetString($"DB_USER_BILLING");
                AppSettings.BsbsPassword = Env.GetString($"DB_PASSWORD_BILLING");
                AppSettings.BsbsDatabase = Env.GetString($"DB_NAME_BILLING");

                AppSettings.BacameterHost = Env.GetString($"DB_HOST_BACAMETER");
                AppSettings.BacameterPort = (uint)Env.GetInt($"DB_PORT_BACAMETER");
                AppSettings.BacameterUserId = Env.GetString($"DB_USER_BACAMETER");
                AppSettings.BacameterPassword = Env.GetString($"DB_PASSWORD_BACAMETER");
                AppSettings.BacameterDatabase = Env.GetString($"DB_NAME_BACAMETER");

                AppSettings.LoketHost = Env.GetString($"DB_HOST_LOKET");
                AppSettings.LoketPort = (uint)Env.GetInt($"DB_PORT_LOKET");
                AppSettings.LoketUserId = Env.GetString($"DB_USER_LOKET");
                AppSettings.LoketPassword = Env.GetString($"DB_PASSWORD_LOKET");
                AppSettings.LoketDatabase = Env.GetString($"DB_NAME_LOKET");

                AppSettings.TampungHost = Env.GetString($"DB_HOST_TAMPUNG");
                AppSettings.TampungPort = (uint)Env.GetInt($"DB_PORT_TAMPUNG");
                AppSettings.TampungUserId = Env.GetString($"DB_USER_TAMPUNG");
                AppSettings.TampungPassword = Env.GetString($"DB_PASSWORD_TAMPUNG");
                AppSettings.TampungDatabase = Env.GetString($"DB_NAME_TAMPUNG");

                await Task.FromResult(0);
            });
        }

        private static async Task PrepareColumnMappingConfigurations()
        {
            await Utils.TrackProgress("Setup db column mapping", async() =>
            {
                Batteries.Init();
                await using var conn = new SqliteConnection("Data Source=column_mapping_configurations.db");

                try
                {
                    await conn.OpenAsync();
                    string query = await File.ReadAllTextAsync("column_mappings_seeder.sql");
                    await conn.ExecuteAsync(query);

                    AppSettings.ColumnMappings = (await conn.QueryAsync<ColumnMapping>("SELECT * FROM column_mappings")).ToList();
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    await conn.CloseAsync();
                }
            });
        }
    }
}
