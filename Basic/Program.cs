using Dapper;
using DotNetEnv;
using MySqlConnector;
using Spectre.Console;
using Spectre.Console.Cli;
using System.Diagnostics;

namespace Basic
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            LoadEnv();

            var app = new CommandApp();

            app.Configure(config =>
            {
                config.AddCommand<RegisterCommandAsync>("register");
                config.AddCommand<BacameterCommandAsync>("bacameter");
            });

            await app.RunAsync(args);
        }

        static void LoadEnv()
        {
            Env.Load(".env");

            ApplicationSettings.ConfigHost = Env.GetString("config_host");
            ApplicationSettings.ConfigUserId = Env.GetString("config_userid");
            ApplicationSettings.ConfigPassword = Env.GetString("config_password");
            ApplicationSettings.ConfigPort = uint.Parse(Env.GetString("config_port", "3306"));
            ApplicationSettings.ConfigDatabase = Env.GetString("config_database");

            ApplicationSettings.MainHost = Env.GetString("main_host");
            ApplicationSettings.MainUserId = Env.GetString("main_userid");
            ApplicationSettings.MainPassword = Env.GetString("main_password");
            ApplicationSettings.MainPort = uint.Parse(Env.GetString("main_port", "3306"));
            ApplicationSettings.MainDatabase = Env.GetString("main_database");

            ApplicationSettings.LoketHost = Env.GetString("loket_host");
            ApplicationSettings.LoketUserId = Env.GetString("loket_userid");
            ApplicationSettings.LoketPassword = Env.GetString("loket_password");
            ApplicationSettings.LoketPort = uint.Parse(Env.GetString("loket_port", "3306"));
            ApplicationSettings.LoketDatabase = Env.GetString("loket_database");
        }
    }

    static class ApplicationSettings
    {
        public static string ConfigHost { get; set; }
        public static string ConfigUserId { get; set; }
        public static string ConfigPassword { get; set; }
        public static uint ConfigPort { get; set; }
        public static string ConfigDatabase { get; set; }
        public static string ConfigConnectionString => new MySqlConnectionStringBuilder
        {
            Server = ConfigHost,
            UserID = ConfigUserId,
            Password = ConfigPassword,
            Port = ConfigPort,
            Database = ConfigDatabase,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
        }.ConnectionString;

        public static string MainHost { get; set; }
        public static string MainUserId { get; set; }
        public static string MainPassword { get; set; }
        public static uint MainPort { get; set; }
        public static string MainDatabase { get; set; }
        public static string MainConnectionString => new MySqlConnectionStringBuilder
        {
            Server = MainHost,
            UserID = MainUserId,
            Password = MainPassword,
            Port = MainPort,
            Database = MainDatabase,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
        }.ConnectionString;

        public static string LoketHost { get; set; }
        public static string LoketUserId { get; set; }
        public static string LoketPassword { get; set; }
        public static uint LoketPort { get; set; }
        public static string LoketDatabase { get; set; }
        public static string LoketConnectionString => new MySqlConnectionStringBuilder
        {
            Server = LoketHost,
            UserID = LoketUserId,
            Password = LoketPassword,
            Port = LoketPort,
            Database = LoketDatabase,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
        }.ConnectionString;
    }

    class RegisterCommandAsync : AsyncCommand<RegisterCommandAsync.Settings>
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
            var existing = AnsiConsole.Prompt(
                new TextPrompt<bool>("Pdam sudah ada di config?")
                    .AddChoice(true)
                    .AddChoice(false)
                    .DefaultValue(false)
                    .WithConverter(choice => choice ? "y" : "n")
                );


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

            await AnsiConsole.Status()
                .StartAsync("Proccessing...", async context =>
                {
                    context.Status("Setup partition...");
                    await SetupPartitionAsync(settings);
                    context.Status("Setup config db...");
                    await SetupConfigDbAsync(settings);
                    context.Status("Setup main db...");
                    await SetupMainDbAsync(settings);
                });

            return 0;
        }
        static async Task SetupPartitionAsync(Settings settings)
        {
            await using var conn = new MySqlConnection(ApplicationSettings.MainConnectionString);
            try
            {
                await conn.OpenAsync();

                var partisiTable = await conn.QueryAsync<string>(
                    sql: "SELECT table_name FROM information_schema.PARTITIONS WHERE table_schema=@schema AND partition_method='list' GROUP BY table_name",
                    param: new
                    {
                        schema = ApplicationSettings.MainDatabase
                    });
                if (partisiTable.Any())
                {
                    foreach (var table in partisiTable)
                    {
                        var cek = await conn.QueryFirstOrDefaultAsync<int?>(
                            sql: "SELECT 1 FROM information_schema.PARTITIONS WHERE table_schema=@schema AND partition_method='list' AND table_name=@table AND partition_name=@partisi",
                            param: new
                            {
                                schema = ApplicationSettings.MainDatabase,
                                table = table,
                                partisi = $"pdam{settings.IdPdam}"
                            });
                        if (!cek.HasValue)
                        {
                            await conn.ExecuteAsync(
                                sql: $"ALTER TABLE {table} ADD PARTITION (PARTITION pdam{settings.IdPdam} VALUES IN (@value) ENGINE = INNODB)",
                                param: new
                                {
                                    value = settings.IdPdam
                                });
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Gagal setup partition: {e.Message}");
                throw;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
        static async Task SetupConfigDbAsync(Settings settings)
        {
            await using var conn = new MySqlConnection(ApplicationSettings.ConfigConnectionString);
            try
            {
                await conn.OpenAsync();

                var query = await File.ReadAllTextAsync(@"queries\setup_config_db.sql");
                await conn.ExecuteAsync(
                    sql: query,
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        namapdam = settings.Name,
                        idpdamcopy = settings.CopyPdamId,
                        timezone = settings.Timezone.Id,
                    });
            }
            catch (Exception e)
            {
                Console.WriteLine($"Gagal setup config db: {e.Message}");
                throw;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
        static async Task SetupMainDbAsync(Settings settings)
        {
            await using var conn = new MySqlConnection(ApplicationSettings.MainConnectionString);
            try
            {
                await conn.OpenAsync();

                var query = await File.ReadAllTextAsync(@"queries\setup_main_db.sql");
                await conn.ExecuteAsync(
                    sql: query,
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        namapdam = settings.Name,
                        idpdamcopy = settings.CopyPdamId
                    });
            }
            catch (Exception e)
            {
                Console.WriteLine($"Gagal setup main db: {e.Message}");
                throw;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
        static void SettingTimezone(Settings settings)
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
        static async Task<IEnumerable<dynamic>> GetAllPdamAsync()
        {
            using var conn = new MySqlConnection(ApplicationSettings.ConfigConnectionString);
            try
            {
                await conn.OpenAsync();
                return await conn.QueryAsync(@"select idpdam,namapdam from master_pdam order by idpdam");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Gagal fetch list pdam: {e.Message}");
                throw;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
        static void SettingCopyPdam(Settings settings, IEnumerable<dynamic> listPdam, out dynamic copyPdam)
        {
            copyPdam = AnsiConsole.Prompt(
                new SelectionPrompt<dynamic>()
                    .Title("Copy konfigurasi data dari PDAM")
                    .AddChoices(listPdam)
                    .UseConverter(x => $"({x.idpdam}) {x.namapdam}")
                );
            settings.CopyPdamId = copyPdam.idpdam;
        }
    }

    class BacameterCommandAsync : AsyncCommand<BacameterCommandAsync.Settings>
    {
        public class Settings : CommandSettings
        {
        }
        public override Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            throw new NotImplementedException();
        }
    }
}
