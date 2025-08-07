using Dapper;
using Migrasi.Helpers;
using Spectre.Console;
using Spectre.Console.Cli;

namespace Migrasi.Commands
{
    public class ConfigCommand : AsyncCommand<ConfigCommand.Settings>
    {
        public class Settings : CommandSettings
        {
            [CommandArgument(0, "<idpdam>")]
            public int IdPdam { get; set; }

            [CommandArgument(1, "<namapdam>")]
            public string NamaPdam { get; set; }

            [CommandArgument(2, "<timezone>")]
            public string Timezone { get; set; } = "wib";

            [CommandArgument(3, "<dbhost>")]
            public string DbHost { get; set; }

            [CommandArgument(4, "<dbport>")]
            public string DbPort { get; set; }

            [CommandArgument(5, "<dbname>")]
            public string DbName { get; set; }

            [CommandArgument(6, "<dbuser>")]
            public string DbUser { get; set; }

            [CommandArgument(7, "<dbpassword>")]
            public string DbPassword { get; set; }

            [CommandArgument(8, "<storage>")]
            public string Storage { get; set; } = "GCS";
        }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            var tableSummary = new Table();
            tableSummary.AddColumn(new TableColumn("Parameter"));
            tableSummary.AddColumn(new TableColumn("Value"));
            tableSummary.AddRow("ID PDAM", settings.IdPdam.ToString());
            tableSummary.AddRow("Nama PDAM", settings.NamaPdam);
            tableSummary.AddRow("Timezone", settings.Timezone.ToUpper());
            tableSummary.AddRow("Host", settings.DbHost);
            tableSummary.AddRow("Port", settings.DbPort);
            tableSummary.AddRow("Database", settings.DbName);
            tableSummary.AddRow("User", settings.DbUser);
            tableSummary.AddRow("Password", settings.DbPassword);
            tableSummary.AddRow("Storage", settings.Storage.ToUpper());
            AnsiConsole.Write(tableSummary);

            if (!Utils.ConfirmationPrompt("Yakin untuk melanjutkan?"))
            {
                return 0;
            }

            try
            {
                await AnsiConsole.Status()
                    .StartAsync("Sedang diproses...", async _ =>
                    {
                        await Utils.TrackProgress("setup db config", async () =>
                        {
                            await SetupDbConfig(settings);
                        });
                    });
            }
            catch (Exception)
            {
                throw;
            }

            return 0;
        }

        public static async Task SetupDbConfig(Settings settings)
        {
            await Utils.ConfigConnectionWrapper(async (conn, trans) =>
            {
                var tzMap = new Dictionary<string, string>
                {
                    { "wib", "SE Asia Standard Time" },
                    { "wita", "Singapore Standard Time" },
                    { "wit", "Tokyo Standard Time" }
                };

                var query = await File.ReadAllTextAsync(@"queries\bacameter\setup_config.sql");
                await conn.ExecuteAsync(
                    sql: query,
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        namapdam = settings.NamaPdam,
                        timezone = tzMap[settings.Timezone],
                        dbhost = settings.DbHost,
                        dbport = settings.DbPort,
                        dbname = settings.DbName,
                        dbuser = settings.DbUser,
                        dbpassword = settings.DbPassword,
                        storage = settings.Storage.ToUpper(),
                    },
                    transaction: trans);
            });
        }
    }
}
