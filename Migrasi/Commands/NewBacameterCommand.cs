using Dapper;
using Migrasi.Helpers;
using Spectre.Console;
using Spectre.Console.Cli;
using System.ComponentModel;

namespace Migrasi.Commands
{
    public class NewBacameterCommand : AsyncCommand<NewBacameterCommand.Settings>
    {
        public class Settings : CommandSettings
        {
            [CommandOption("-i|--idpdam")]
            public int? IdPdam { get; set; }

            [CommandOption("-n|--nama-pdam")]
            public string? NamaPdam { get; set; }

            [CommandOption("-s|--sumber")]
            [Description("Copy data dari pdam existing")]
            public int? IdPdamCopy { get; set; }
        }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            settings.IdPdam ??= AnsiConsole.Ask<int>("ID PDAM:");
            settings.NamaPdam ??= AnsiConsole.Ask<string>("Nama :");
            settings.IdPdamCopy ??= AnsiConsole.Ask<int>("PDAM Copy:");

            AnsiConsole.Write(
                new Table()
                .AddColumn(new TableColumn("Setting"))
                .AddColumn(new TableColumn("Value"))
                .AddRow("PDAM", $"{settings.IdPdam} {settings.NamaPdam}")
                .AddRow("PDAM Copy", $"Pdam {settings.IdPdamCopy}")
                .AddRow("Config DB", $"{AppSettings.ConfigConnectionString}")
                .AddRow("Main DB", $"{AppSettings.ConnectionString}")
                .AddRow("Environment", AppSettings.Environment.ToString()));

            if (!Utils.ConfirmationPrompt("Yakin untuk melanjutkan?"))
            {
                return 0;
            }

            try
            {
                await AnsiConsole.Status()
                    .StartAsync("Processing...", async _ =>
                    {
                        await Utils.TrackProgress("setup partition", async () =>
                        {
                            await SetupPartition(settings);
                        });

                        await Utils.TrackProgress("setup db config", async () =>
                        {
                            await SetupDbConfig(settings);
                        });

                        await Utils.TrackProgress("setup pdam", async () =>
                        {
                            await SetupPdam(settings);
                        });
                    });

                AnsiConsole.MarkupLine("");
                AnsiConsole.MarkupLine($"[bold green]Setup {settings.NamaPdam} finish.[/]");
            }
            catch (Exception)
            {
                throw;
            }

            return 0;
        }

        public async Task SetupPartition(Settings settings)
        {
            await Utils.Client(async (conn, trans) =>
            {
                var partisiTable = await conn.QueryAsync<string>(
                    sql: "SELECT table_name FROM information_schema.PARTITIONS WHERE table_schema=@schema AND partition_method='list' GROUP BY table_name",
                    param: new { schema = AppSettings.Database },
                    transaction: trans);
                if (partisiTable.Any())
                {
                    foreach (var table in partisiTable)
                    {
                        var cek = await conn.QueryFirstOrDefaultAsync<int?>(
                            sql: "SELECT 1 FROM information_schema.PARTITIONS WHERE table_schema=@schema AND partition_method='list' AND table_name=@table AND partition_name=@partisi",
                            param: new
                            {
                                schema = AppSettings.Database,
                                table = table,
                                partisi = $"pdam{settings.IdPdam}"
                            },
                            transaction: trans);
                        if (!cek.HasValue)
                        {
                            await conn.ExecuteAsync(
                                sql: $"ALTER TABLE {table} ADD PARTITION (PARTITION pdam{settings.IdPdam} VALUES IN (@value) ENGINE = INNODB)",
                                param: new { value = settings.IdPdam },
                                transaction: trans);
                        }
                    }
                }
            });
        }

        public async Task SetupPdam(Settings settings)
        {
            await Utils.Client(async (conn, trans) =>
            {
                var query = await File.ReadAllTextAsync(@"Queries\bacameter\setup_pdam.sql");
                await conn.ExecuteAsync(
                    sql: query,
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        namapdam = settings.NamaPdam,
                        idpdamcopy = settings.IdPdamCopy
                    },
                    transaction: trans);
            });
        }

        public async Task SetupDbConfig(Settings settings)
        {
            await Utils.ClientConfig(async (conn, trans) =>
            {
                var query = await File.ReadAllTextAsync(@"Queries\bacameter\setup_config.sql");
                await conn.ExecuteAsync(
                    sql: query,
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        namapdam = settings.NamaPdam,
                        idpdamcopy = settings.IdPdamCopy
                    },
                    transaction: trans);
            });
        }
    }
}
