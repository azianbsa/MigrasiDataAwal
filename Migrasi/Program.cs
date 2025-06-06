﻿using Dapper;
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

            AppSettings.HostStaging = Env.GetString($"DB_HOST_STG");
            AppSettings.PortStaging = (uint)Env.GetInt($"DB_PORT_STG");
            AppSettings.UserStaging = Env.GetString($"DB_USER_STG");
            AppSettings.PasswordStaging = Env.GetString($"DB_PASSWORD_STG");
            AppSettings.DatabaseStaging = Env.GetString($"DB_NAME_STG");

            AppSettings.HostBsbs = Env.GetString($"DB_HOST_BILLING");
            AppSettings.PortBsbs = (uint)Env.GetInt($"DB_PORT_BILLING");
            AppSettings.UserBsbs = Env.GetString($"DB_USER_BILLING");
            AppSettings.PasswordBsbs = Env.GetString($"DB_PASSWORD_BILLING");
            AppSettings.DatabaseBsbs = Env.GetString($"DB_NAME_BILLING");

            AppSettings.HostBacameter = Env.GetString($"DB_HOST_BACAMETER");
            AppSettings.PortBacameter = (uint)Env.GetInt($"DB_PORT_BACAMETER");
            AppSettings.UserBacameter = Env.GetString($"DB_USER_BACAMETER");
            AppSettings.PasswordBacameter = Env.GetString($"DB_PASSWORD_BACAMETER");
            AppSettings.DatabaseBacameter = Env.GetString($"DB_NAME_BACAMETER");

            AppSettings.LoketHost = Env.GetString($"DB_HOST_LOKET");
            AppSettings.LoketPort = (uint)Env.GetInt($"DB_PORT_LOKET");
            AppSettings.LoketUserId = Env.GetString($"DB_USER_LOKET");
            AppSettings.LoketPassword = Env.GetString($"DB_PASSWORD_LOKET");
            AppSettings.LoketDatabase = Env.GetString($"DB_NAME_LOKET");

            #endregion

            var app = new CommandApp();

            app.Configure(config =>
            {
                config.PropagateExceptions();
                config.AddCommand<NewCommand>("new")
                    .WithDescription("Setup new pdam untuk paket basic");
                config.AddCommand<NewBacameterCommand>("new-bacameter")
                    .WithDescription("Setup new pdam untuk paket bacameter only");
                config.AddCommand<PaketCommand>("paket")
                    .WithDescription("Migrasi data paket bacameter only, basic");
            });

            try
            {
                using SqliteConnection conn = await Utils.SqliteConnectionFactory();
                var cek = await conn.QueryFirstOrDefaultAsync<int?>("SELECT 1 FROM proses_manager WHERE flagproses=1");
                if (cek != null && Utils.ConfirmationPrompt(message: "Proses ulang?", defaultChoice: true))
                {
                    await conn.ExecuteAsync("DELETE FROM proses_manager");
                }

                return app.Run(args);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return -99;
            }
            finally
            {
                AnsiConsole.MarkupLine("");
                AnsiConsole.MarkupLine($"Program exit.");
            }
        }
    }
}
