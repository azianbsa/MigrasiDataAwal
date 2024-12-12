using Dapper;
using DotNetEnv;
using Microsoft.Data.Sqlite;
using Migrasi.Commands;
using Migrasi.Helpers;
using Spectre.Console;
using Spectre.Console.Cli;
using System.Diagnostics;
using Environment = Migrasi.Enums.Environment;

namespace Migrasi
{
    internal class Program
    {
        public static async Task<int> Main(string[] args)
        {
            #region environment variables

            Env.Load(".env");

            AppSettings.Environment = (Environment)Enum.Parse(typeof(Environment), Env.GetString("ENVIRONMENT", "Development"));

            var dbSuffix = (Environment)Enum.Parse(typeof(Environment), Env.GetString("ENVIRONMENT", "Development")) switch
            {
                Environment.Development => "_DEV",
                Environment.Staging => "_STG",
                Environment.Production => "_PRD",
                _ => "_DEV"
            };

            AppSettings.DBHost = Env.GetString($"DB_HOST{dbSuffix}");
            AppSettings.DBPort = (uint)Env.GetInt($"DB_PORT{dbSuffix}");
            AppSettings.DBUser = Env.GetString($"DB_USER{dbSuffix}");
            AppSettings.DBPassword = Env.GetString($"DB_PASSWORD{dbSuffix}");
            AppSettings.DBName = Env.GetString($"DB_NAME{dbSuffix}");

            AppSettings.DBHostStaging = Env.GetString($"DB_HOST_STG");
            AppSettings.DBPortStaging = (uint)Env.GetInt($"DB_PORT_STG");
            AppSettings.DBUserStaging = Env.GetString($"DB_USER_STG");
            AppSettings.DBPasswordStaging = Env.GetString($"DB_PASSWORD_STG");
            AppSettings.DBNameStaging = Env.GetString($"DB_NAME_STG");

            AppSettings.DBHostBilling = Env.GetString($"DB_HOST_BILLING");
            AppSettings.DBPortBilling = (uint)Env.GetInt($"DB_PORT_BILLING");
            AppSettings.DBUserBilling = Env.GetString($"DB_USER_BILLING");
            AppSettings.DBPasswordBilling = Env.GetString($"DB_PASSWORD_BILLING");
            AppSettings.DBNameBilling = Env.GetString($"DB_NAME_BILLING");

            AppSettings.DBHostBacameter = Env.GetString($"DB_HOST_BACAMETER");
            AppSettings.DBPortBacameter = (uint)Env.GetInt($"DB_PORT_BACAMETER");
            AppSettings.DBUserBacameter = Env.GetString($"DB_USER_BACAMETER");
            AppSettings.DBPasswordBacameter = Env.GetString($"DB_PASSWORD_BACAMETER");
            AppSettings.DBNameBacameter = Env.GetString($"DB_NAME_BACAMETER");

            AppSettings.DBHostLoket = Env.GetString($"DB_HOST_LOKET");
            AppSettings.DBPortLoket = (uint)Env.GetInt($"DB_PORT_LOKET");
            AppSettings.DBUserLoket = Env.GetString($"DB_USER_LOKET");
            AppSettings.DBPasswordLoket = Env.GetString($"DB_PASSWORD_LOKET");
            AppSettings.DBNameLoket = Env.GetString($"DB_NAME_LOKET");

            #endregion

            var app = new CommandApp();

            app.Configure(config =>
            {
                config.PropagateExceptions();
                config.AddCommand<NewCommand>("new");
                config.AddCommand<NewCopyCommand>("newcopy");
                config.AddCommand<PaketCommand>("paket");
                config.AddCommand<PiutangCommand>("piutang");
            });

            var sw = Stopwatch.StartNew();
            try
            {
                using SqliteConnection conn = await Utils.SqliteConnectionFactory();
                var cek = await conn.QueryFirstOrDefaultAsync<int?>("SELECT 1 FROM proses_manager WHERE flagproses=1");
                if (cek != null && Utils.ConfirmationPrompt(message: "Proses ulang?", defaultChoice: false))
                {
                    await conn.ExecuteAsync("DELETE FROM proses_manager");
                }

                return app.Run(args);
            }
            catch (Exception ex)
            {
                AnsiConsole.WriteException(ex, ExceptionFormats.ShortenEverything);
                return -99;
            }
            finally
            {
                sw.Stop();
                AnsiConsole.MarkupLine("");
                AnsiConsole.MarkupLine($"[bold green]Program exit. (elapsed {sw.Elapsed})[/]");
            }
        }
    }
}
