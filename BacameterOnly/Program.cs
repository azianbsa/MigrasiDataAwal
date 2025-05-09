using Dapper;
using DotNetEnv;
using MySqlConnector;
using Spectre.Console;
using Spectre.Console.Cli;
using System.ComponentModel;
using System.Reflection;

namespace BacameterOnly
{
    internal class Program
    {
        static int Main(string[] args)
        {
            LoadSettingFromEnv();

            var app = new CommandApp();

            app.Configure(config =>
            {
#if DEBUG
                config.PropagateExceptions();
#endif
                config.AddCommand<RegisterCommand>("register");
                config.AddCommand<MigrateCommand>("migrate");
                config.AddCommand<VersionCommand>("version");
            });

            return app.Run(args);
        }

        static void LoadSettingFromEnv()
        {
            Env.Load(".env");
            AppSettings.DBConfigHost = Env.GetString("DB_CONFIG_HOST");
            AppSettings.DBConfigPort = uint.Parse(Env.GetString("DB_CONFIG_PORT"));
            AppSettings.DBConfigUser = Env.GetString("DB_CONFIG_USER");
            AppSettings.DBConfigPassword = Env.GetString("DB_CONFIG_PASSWORD");
            AppSettings.DBConfigDatabase = Env.GetString("DB_CONFIG_DATABASE");

            AppSettings.DBMainHost = Env.GetString("DB_MAIN_HOST");
            AppSettings.DBMainPort = uint.Parse(Env.GetString("DB_MAIN_PORT"));
            AppSettings.DBMainUser = Env.GetString("DB_MAIN_USER");
            AppSettings.DBMainPassword = Env.GetString("DB_MAIN_PASSWORD");
            AppSettings.DBMainDatabase = Env.GetString("DB_MAIN_DATABASE");
        }
    }

    class RegisterCommand : AsyncCommand<RegisterCommand.Settings>
    {
        public class Settings : CommandSettings
        {
            public int IdPdam { get; set; }
            [CommandOption("--name")]
            public string? Name { get; set; }
            public TimeZoneInfo Timezone { get; set; }
            public int CopyPdamId { get; set; }
        }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            settings.Name ??= AnsiConsole.Prompt(
                new TextPrompt<string>("Nama PDAM?"));

            SettingTimezone(settings);
            IEnumerable<dynamic> pdam = await GetAllPdamAsync();
            settings.IdPdam = pdam.OrderByDescending(s => s.idpdam).FirstOrDefault()?.idpdam + 1;
            SettingCopyPdam(settings, pdam, out dynamic copyPdam);

            var summaryTable = new Table();
            summaryTable.AddColumn(new TableColumn(""));
            summaryTable.AddColumn(new TableColumn(""));
            summaryTable.AddRow("ID", $"{settings.IdPdam}");
            summaryTable.AddRow("Nama PDAM", $"{settings.Name}");
            summaryTable.AddRow("Zone waktu", $"({(settings.Timezone.BaseUtcOffset >= TimeSpan.Zero ? "+" : "")}{settings.Timezone.BaseUtcOffset:hh}) {settings.Timezone.Id}");
            summaryTable.AddRow("Copy configuration data from PDAM", $"({settings.CopyPdamId}) {copyPdam.namapdam}");
            AnsiConsole.Write(summaryTable);

            var confirmation = AnsiConsole.Prompt(
                new TextPrompt<bool>("Do you want to continue?")
                    .AddChoice(true)
                    .AddChoice(false)
                    .DefaultValue(false)
                    .WithConverter(choice => choice ? "y" : "n")
                );

            if (!confirmation)
            {
                return 0;
            }

            return await Task.FromResult(0);
        }

        void SettingTimezone(Settings settings)
        {
            var indonesiaTz = TimeZoneInfo.GetSystemTimeZones()
                .Where(tz => tz.BaseUtcOffset.Hours >= 7 && tz.BaseUtcOffset.Hours <= 9)
                .ToList();

            settings.Timezone = AnsiConsole.Prompt(
                new SelectionPrompt<TimeZoneInfo>()
                    .Title("Pilih zona waktu")
                    .AddChoices(indonesiaTz)
                    .UseConverter(tz => $"({(tz.BaseUtcOffset >= TimeSpan.Zero ? "+" : "")}{tz.BaseUtcOffset:hh}) {tz.Id}")
                );
        }

        async Task<IEnumerable<dynamic>> GetAllPdamAsync()
        {
            using (var conn = new MySqlConnection(AppSettings.DBConfigConnectionString))
            {
                try
                {
                    await conn.OpenAsync();
                    return await conn.QueryAsync(@"select idpdam,namapdam from master_pdam order by idpdam");
                }
                catch (Exception)
                {
                    Console.WriteLine("Ops, gagal fetch list pdam");
                    throw;
                }
            }
        }

        void SettingCopyPdam(Settings settings, IEnumerable<dynamic> listPdam, out dynamic copyPdam)
        {
            copyPdam = AnsiConsole.Prompt(
                new SelectionPrompt<dynamic>()
                    .Title("Pilih PDAM")
                    .AddChoices(listPdam)
                    .UseConverter(x => $"({x.idpdam}) {x.namapdam}")
                );
            settings.CopyPdamId = copyPdam.idpdam;
        }
    }

    class MigrateCommand : Command<MigrateCommand.Settings>
    {
        public class Settings : CommandSettings
        {
        }
        public override int Execute(CommandContext context, Settings settings)
        {
            // Implement the logic for the migrate command here
            return 0;
        }
    }

    class VersionCommand : Command<VersionCommand.Settings>
    {
        public class Settings : CommandSettings { }
        public override int Execute(CommandContext context, Settings settings)
        {
            var assembly = Assembly.GetExecutingAssembly();
            Console.WriteLine($"{assembly.GetName().Name} version {assembly.GetName().Version}");
            return 0;
        }
    }

    class AppSettings
    {
        public static string DBConfigHost { get; set; }
        public static uint DBConfigPort { get; set; }
        public static string DBConfigUser { get; set; }
        public static string DBConfigPassword { get; set; }
        public static string DBConfigDatabase { get; set; }
        public static string DBConfigConnectionString => new MySqlConnectionStringBuilder
        {
            Server = DBConfigHost,
            Port = DBConfigPort,
            UserID = DBConfigUser,
            Password = DBConfigPassword,
            Database = DBConfigDatabase,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
        }.ConnectionString;

        public static string DBMainHost { get; set; }
        public static uint DBMainPort { get; set; }
        public static string DBMainUser { get; set; }
        public static string DBMainPassword { get; set; }
        public static string DBMainDatabase { get; set; }
        public static string DBMainConnectionString => new MySqlConnectionStringBuilder
        {
            Server = DBMainHost,
            Port = DBMainPort,
            UserID = DBMainUser,
            Password = DBMainPassword,
            Database = DBMainDatabase,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
        }.ConnectionString;
    }
}
